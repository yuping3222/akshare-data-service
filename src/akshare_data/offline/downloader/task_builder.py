"""下载任务构建器"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from akshare_data.core.schema import get_table_schema

logger = logging.getLogger("akshare_data")


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
    use_multi_source: bool = False


class TaskBuilder:
    """下载任务构建器"""

    DATE_PARAMS = {"start_date", "end_date", "start", "end", "begin", "finish"}
    # 离线下载接口名 -> 缓存层 legacy 表名
    # 目的：写入表名与 core/schema.py 保持一致，避免接口名直接落盘导致 Served 层读不到。
    INTERFACE_TABLE_ALIASES = {
        "equity_daily": "stock_daily",
        "equity_minute": "stock_minute",
        "equity_realtime": "spot_snapshot",
        "stock_zh_a_spot_em": "spot_snapshot",
        "north_money_flow": "north_flow",
        "trading_days": "trade_calendar",
        "tool_trade_date_hist_sina": "trade_calendar",
        "securities_list": "securities",
        "security_info": "company_info",
        "industry_stocks": "industry_components",
        "concept_stocks": "concept_components",
        "insider_trading": "insider_trade",
        "shareholder_changes": "holding_change",
        "capital_change": "share_change",
        "management_info": "company_management",
        "macro_shibor": "shibor_rate",
        "macro_social_financing": "social_financing",
        "fof_list": "fof_fund",
        "lof_list": "lof_fund",
        "sector_fund_flow": "sector_flow_snapshot",
        "repurchase_data": "repurchase",
    }

    @classmethod
    def _resolve_cache_table(cls, interface_name: str) -> str:
        aliased = cls.INTERFACE_TABLE_ALIASES.get(interface_name)
        if aliased and get_table_schema(aliased) is not None:
            return aliased
        return interface_name

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
            func_name = iface_def.get("func")
            use_multi_source = False
            if not func_name:
                sources = iface_def.get("sources", [])
                enabled_source = None
                for src in sources:
                    if isinstance(src, dict) and src.get("enabled", True):
                        enabled_source = src
                        break
                if enabled_source:
                    func_name = enabled_source.get("func")
                    use_multi_source = True
                else:
                    logger.debug("Skipping %s: no enabled sources", interface)
                    continue
            if not func_name:
                func_name = interface
            table = self._resolve_cache_table(interface)
            rate_limit_key = iface_def.get("rate_limit_key", "default")
            signature = iface_def.get("signature", []) or []

            if category in ("equity", "stock"):
                tasks.extend(
                    self._build_equity_tasks(
                        interface,
                        func_name,
                        table,
                        rate_limit_key,
                        start_date,
                        end_date,
                        signature,
                        use_multi_source=use_multi_source,
                    )
                )
            elif category in ("index", "fund", "futures"):
                tasks.extend(
                    self._build_symbol_tasks(
                        interface,
                        func_name,
                        table,
                        rate_limit_key,
                        start_date,
                        end_date,
                        category,
                        signature,
                    )
                )
            else:
                kwargs = self._build_kwargs_for_interface(
                    func_name, start_date, end_date, signature
                )
                tasks.append(
                    DownloadTask(
                        interface=interface,
                        func=interface if use_multi_source else func_name,
                        table=table,
                        kwargs=kwargs,
                        rate_limit_key=rate_limit_key,
                        use_multi_source=use_multi_source,
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
            if (
                key in filtered
                and isinstance(filtered[key], str)
                and "-" in filtered[key]
            ):
                filtered[key] = filtered[key].replace("-", "")
        return filtered

    def _build_kwargs_for_interface(
        self,
        func_name: str,
        start_date: str,
        end_date: str,
        signature: List[str] | None = None,
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

        has_date = "date" in param_names and (not sig_set or "date" in sig_set)
        has_start = (
            "start_date" in param_names
            or "start" in param_names
            or "begin" in param_names
        ) and (not sig_set or bool(sig_set & {"start_date", "start", "begin"}))
        has_end = (
            "end_date" in param_names or "end" in param_names or "finish" in param_names
        ) and (not sig_set or bool(sig_set & {"end_date", "end", "finish"}))

        if has_date and not has_start and not has_end:
            kwargs["date"] = end_date.replace("-", "")
        else:
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

        if "year" in param_names and (not sig_set or "year" in sig_set):
            kwargs["year"] = start_date[:4]

        return kwargs

    def _build_equity_tasks(
        self,
        interface,
        func_name,
        table,
        rate_limit_key,
        start_date,
        end_date,
        signature,
        use_multi_source: bool = False,
    ) -> List[DownloadTask]:
        """构建股票类任务（需要股票列表）"""
        import akshare as ak
        import inspect

        ak_func = getattr(ak, func_name, None)
        accepts_symbol = False
        if ak_func is not None:
            try:
                sig = inspect.signature(ak_func)
                accepts_symbol = "symbol" in sig.parameters
            except (ValueError, TypeError):
                pass

        if not accepts_symbol:
            base_kwargs = self._build_kwargs_for_interface(
                func_name, start_date, end_date, signature
            )
            return [
                DownloadTask(
                    interface=interface,
                    func=interface if use_multi_source else func_name,
                    table=table,
                    kwargs=base_kwargs,
                    rate_limit_key=rate_limit_key,
                    use_multi_source=use_multi_source,
                )
            ]

        from akshare_data.offline.downloader.downloader import BatchDownloader

        stock_list = BatchDownloader._get_stock_list_static()
        if not stock_list:
            return []
        tasks = []
        base_kwargs = self._build_kwargs_for_interface(
            func_name, start_date, end_date, signature
        )
        for symbol in stock_list[:100]:
            kwargs = {"symbol": symbol}
            kwargs.update(base_kwargs)
            tasks.append(
                DownloadTask(
                    interface=interface,
                    func=interface if use_multi_source else func_name,
                    table=table,
                    kwargs=kwargs,
                    rate_limit_key=rate_limit_key,
                    use_multi_source=use_multi_source,
                )
            )
        return tasks

    def _build_symbol_tasks(
        self,
        interface,
        func_name,
        table,
        rate_limit_key,
        start_date,
        end_date,
        category,
        signature,
    ) -> List[DownloadTask]:
        """构建指数/基金/期货类任务"""
        from akshare_data.offline.downloader.downloader import BatchDownloader

        symbol_list = BatchDownloader._get_symbol_list_static(category)
        if not symbol_list:
            return []
        tasks = []
        base_kwargs = self._build_kwargs_for_interface(
            func_name, start_date, end_date, signature
        )
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