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
        registry_file = paths.legacy_registry_file
        if not registry_file.exists():
            return {}
        with open(registry_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

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

    def download_incremental(
        self,
        stock_list: Optional[List[str]] = None,
        start: Optional[str] = None,
        days_back: int = 1,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """增量下载"""
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

        tasks = self._task_builder.build_tasks(
            incremental_interfaces, start, end, self._registry
        )

        return self._execute_tasks(tasks, progress_callback)

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
            return df["代码"].tolist()[:100]
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
