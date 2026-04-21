"""下载任务构建器"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DownloadTask:
    """单个下载任务"""
    interface: str
    func: str
    table: str
    kwargs: Dict[str, Any]
    rate_limit_key: str = "default"
    primary_key: Optional[List[str]] = None
    output_mapping: Dict[str, str] = field(default_factory=dict)


class TaskBuilder:
    """下载任务构建器"""

    DATE_PARAMS = {"start_date", "end_date", "start", "end", "begin", "finish"}

    def build_tasks(
        self,
        interfaces: List[str],
        start_date: str,
        end_date: str,
        registry: Dict[str, Any],
    ) -> List[DownloadTask]:
        """根据接口定义构建任务列表"""
        tasks = []
        for interface in interfaces:
            iface_def = registry.get("interfaces", {}).get(interface)
            if not iface_def:
                continue

            category = iface_def.get("category", "other")
            func_name = iface_def.get("func", interface)
            table = f"{interface}"
            rate_limit_key = iface_def.get("rate_limit_key", "default")
            signature = iface_def.get("signature", []) or []

            if category in ("equity", "stock"):
                tasks.extend(
                    self._build_equity_tasks(interface, func_name, table, rate_limit_key, start_date, end_date, signature)
                )
            elif category in ("index", "fund", "futures"):
                tasks.extend(
                    self._build_symbol_tasks(interface, func_name, table, rate_limit_key, start_date, end_date, category, signature)
                )
            else:
                kwargs = self._build_kwargs_for_interface(func_name, start_date, end_date, signature)
                tasks.append(
                    DownloadTask(
                        interface=interface,
                        func=func_name,
                        table=table,
                        kwargs=kwargs,
                        rate_limit_key=rate_limit_key,
                    )
                )

        return tasks

    @staticmethod
    def _filter_kwargs(kwargs: Dict[str, Any], signature: List[str]) -> Dict[str, Any]:
        """只保留 signature 中声明的参数"""
        if not signature:
            return {}
        filtered = {k: v for k, v in kwargs.items() if k in signature}
        for key in ("start_date", "end_date"):
            if key in filtered and isinstance(filtered[key], str) and "-" in filtered[key]:
                filtered[key] = filtered[key].replace("-", "")
        return filtered

    def _build_kwargs_for_interface(
        self, func_name: str, start_date: str, end_date: str, signature: List[str] | None = None
    ) -> Dict[str, Any]:
        """根据函数签名和注册表 signature 构建合适的 kwargs"""
        import akshare as ak
        import inspect

        func = getattr(ak, func_name, None)
        if func is None:
            return {}

        try:
            sig = inspect.signature(func)
            param_names = set(sig.parameters.keys())
        except (ValueError, TypeError):
            return {}

        sig_set = set(signature) if signature else set()

        kwargs = {}
        for dp in self.DATE_PARAMS:
            if dp in param_names and (not sig_set or dp in sig_set):
                if dp in ("start_date", "start", "begin"):
                    kwargs[dp] = start_date.replace("-", "")
                elif dp in ("end_date", "end", "finish"):
                    kwargs[dp] = end_date.replace("-", "")

        if "period" in param_names and (not sig_set or "period" in sig_set):
            kwargs["period"] = "daily"

        if "adjust" in param_names and (not sig_set or "adjust" in sig_set):
            kwargs["adjust"] = "qfq"

        return kwargs

    def _build_equity_tasks(
        self, interface, func_name, table, rate_limit_key, start_date, end_date, signature
    ) -> List[DownloadTask]:
        """构建股票类任务（需要股票列表）"""
        from akshare_data.offline.downloader.downloader import BatchDownloader
        stock_list = BatchDownloader._get_stock_list_static()
        if not stock_list:
            return []
        tasks = []
        base_kwargs = self._build_kwargs_for_interface(func_name, start_date, end_date, signature)
        for symbol in stock_list[:100]:
            kwargs = {"symbol": symbol}
            kwargs.update(base_kwargs)
            tasks.append(
                DownloadTask(
                    interface=interface,
                    func=func_name,
                    table=table,
                    kwargs=kwargs,
                    rate_limit_key=rate_limit_key,
                )
            )
        return tasks

    def _build_symbol_tasks(
        self, interface, func_name, table, rate_limit_key, start_date, end_date, category, signature
    ) -> List[DownloadTask]:
        """构建指数/基金/期货类任务"""
        from akshare_data.offline.downloader.downloader import BatchDownloader
        symbol_list = BatchDownloader._get_symbol_list_static(category)
        if not symbol_list:
            return []
        tasks = []
        base_kwargs = self._build_kwargs_for_interface(func_name, start_date, end_date, signature)
        for symbol in symbol_list[:50]:
            kwargs = {"symbol": symbol}
            kwargs.update(base_kwargs)
            tasks.append(
                DownloadTask(
                    interface=interface,
                    func=func_name,
                    table=table,
                    kwargs=kwargs,
                    rate_limit_key=rate_limit_key,
                )
            )
        return tasks
