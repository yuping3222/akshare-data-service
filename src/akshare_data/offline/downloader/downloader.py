"""批量下载器 - 主调度器"""

from __future__ import annotations

import logging
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional


from akshare_data.offline.core.paths import paths
from akshare_data.offline.downloader.rate_limiter import DomainRateLimiter
from akshare_data.offline.downloader.task_builder import DownloadTask, TaskBuilder
from akshare_data.offline.downloader.executor import TaskExecutor
from akshare_data.offline.downloader.progress import ProgressTracker
from akshare_data.core.config_cache import ConfigCache

logger = logging.getLogger("akshare_data")


class BatchDownloader:
    """批量下载器"""

    DEFAULT_MAX_WORKERS = 4
    DEFAULT_BATCH_SIZE = 50

    def __init__(
        self,
        cache_manager=None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        rate_limiter_config: Optional[Dict[str, tuple]] = None,
    ):
        self._max_workers = max_workers
        self._batch_size = batch_size
        self._cache_manager = cache_manager

        paths.ensure_dirs()
        self._registry = self._load_registry()
        self._rate_limits = self._load_rate_limits()

        intervals = {k: v.get("interval", 0.5) for k, v in self._rate_limits.items()}
        self._rate_limiter = DomainRateLimiter(intervals)

        self._task_builder = TaskBuilder()
        self._priority_config = self._load_priority()

    def _load_registry(self) -> Dict[str, Any]:
        interfaces = ConfigCache.load_interfaces()
        registry = ConfigCache.load_registry()
        interfaces_with_meta = dict(interfaces)

        for iface_name, iface_def in interfaces.items():
            for source in iface_def.get("sources", []):
                func_name = source.get("func")
                if func_name and func_name in registry.get("interfaces", {}):
                    reg_def = registry["interfaces"][func_name]
                    if "signature" not in iface_def and "signature" in reg_def:
                        interfaces_with_meta[iface_name]["signature"] = reg_def[
                            "signature"
                        ]
                    if "domains" not in iface_def and "domains" in reg_def:
                        interfaces_with_meta[iface_name]["domains"] = reg_def["domains"]
                    break

        return {"interfaces": interfaces_with_meta}

    def _load_rate_limits(self) -> Dict[str, Any]:
        rate_file = paths.legacy_rate_limits_file
        if not rate_file.exists():
            return {"default": {"interval": 0.5}}
        with open(rate_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {"default": {"interval": 0.5}}

    def _load_priority(self) -> Dict[str, Any]:
        priority_file = paths.priority_file
        if not priority_file.exists():
            return {}
        with open(priority_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    # 接口特殊限制（最大回溯天数）
    INTERFACE_MAX_DAYS = {
        "limit_down_pool": 30,
        "limit_up_pool": 30,
    }

    def download_incremental(
        self,
        stock_list: Optional[List[str]] = None,
        start: Optional[str] = None,
        days_back: int = 1,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """增量下载（跳过已缓存的接口）"""
        end = datetime.now().strftime("%Y-%m-%d")
        if start is None:
            start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        all_interfaces = self._registry.get("interfaces", {})
        incremental_interfaces = [
            name
            for name, defn in all_interfaces.items()
            if any(
                p in (defn.get("signature") or [])
                for p in ("start_date", "end_date", "date")
            )
        ][:10]

        if not incremental_interfaces:
            incremental_interfaces = list(all_interfaces.keys())[:10]

        # 检查缓存，跳过已完全覆盖该日期范围的接口
        interfaces_to_download = []
        skipped = []
        for iface_name in incremental_interfaces:
            table = self._task_builder._resolve_cache_table(iface_name)
            if self._cache_manager and self._cache_manager.has_range(
                table, start=start, end=end
            ):
                skipped.append(iface_name)
                logger.info(
                    "Skipping %s: already cached for %s to %s", iface_name, start, end
                )
            else:
                interfaces_to_download.append(iface_name)

        if skipped:
            logger.info("Skipped %d cached interfaces: %s", len(skipped), skipped)

        if not interfaces_to_download:
            logger.info("All interfaces already cached for %s to %s", start, end)
            return {
                "success": True,
                "total": 0,
                "success_count": 0,
                "failed_count": 0,
                "skipped": len(skipped),
                "message": "All data already cached",
            }

        tasks = self._task_builder.build_tasks(
            interfaces_to_download, start, end, self._registry
        )

        # 过滤掉超过接口限制的日期范围的任务
        filtered_tasks = []
        for task in tasks:
            max_days = self.INTERFACE_MAX_DAYS.get(task.interface)
            if max_days:
                start_dt = datetime.strptime(start, "%Y-%m-%d")
                end_dt = datetime.strptime(end, "%Y-%m-%d")
                if (end_dt - start_dt).days > max_days:
                    logger.warning(
                        "Skipping %s: date range %s-%s exceeds max %d days",
                        task.interface,
                        start,
                        end,
                        max_days,
                    )
                    continue
            filtered_tasks.append(task)

        result = self._execute_tasks(filtered_tasks, progress_callback)
        result["skipped"] = len(skipped)
        return result

    def download_full(
        self,
        interfaces: Optional[List[str]] = None,
        start: str = "2020-01-01",
        end: Optional[str] = None,
        force: bool = False,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """全量下载"""
        if end is None:
            end = datetime.now().strftime("%Y-%m-%d")
        if interfaces is None:
            interfaces = list(self._registry.get("interfaces", {}).keys())[:20]

        tasks = self._task_builder.build_tasks(interfaces, start, end, self._registry)
        return self._execute_tasks(tasks, progress_callback)

    def _execute_tasks(
        self,
        tasks: List[DownloadTask],
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """执行任务列表"""
        tracker = ProgressTracker(len(tasks), progress_callback, self._batch_size)
        executor = TaskExecutor(self._rate_limiter, self._cache_manager)

        success_count = 0
        failed_count = 0
        failed_stocks = []

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(executor.execute, task): task for task in tasks}
            for future in as_completed(futures):
                result = future.result()
                if result.get("success"):
                    success_count += 1
                else:
                    failed_count += 1
                    failed_stocks.append(
                        (result.get("task", "unknown"), result.get("error", ""))
                    )
                tracker.update(success=result.get("success", False))

        summary = tracker.finish()
        summary["success_count"] = success_count
        summary["failed_count"] = failed_count
        summary["failed_stocks"] = failed_stocks[:20]
        return summary

    @staticmethod
    def _get_stock_list_static() -> List[str]:
        """获取A股代码列表"""
        import akshare as ak

        try:
            df = ak.stock_zh_a_spot_em()
            raw_codes = [str(code).strip() for code in df["代码"].tolist()]
            # stock_zh_a_daily 仅稳定支持沪深主板/创业板代码，过滤掉北交所等代码，
            # 避免在 akshare_sina 数据源稳定抛出 KeyError('date')。
            filtered = [
                code
                for code in raw_codes
                if code.isdigit() and code.startswith(("0", "3", "6"))
            ]
            return filtered[:100]
        except Exception:
            return ["000001", "000002", "600000"]

    @staticmethod
    def _get_symbol_list_static(category: str) -> List[str]:
        """按类别获取代码列表"""
        import akshare as ak

        try:
            if category == "index":
                df = ak.stock_zh_index_spot_em()
            elif category == "fund":
                df = ak.fund_etf_spot_em()
            elif category == "futures":
                df = ak.futures_main_sina()
            else:
                return ["000001"]
            return df.iloc[:, 0].tolist()[:50]
        except Exception:
            return ["000001"]
