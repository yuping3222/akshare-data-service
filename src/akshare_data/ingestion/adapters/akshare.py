"""AkShare adapter for the ingestion layer.

Configuration-driven thin dispatcher that routes ``get_xxx()`` calls to
the AkShare fetcher via ``akshare_registry.yaml``.

This adapter inherits from ``ingestion.base.DataSource`` and is
orchestrated by ``ingestion.router.MultiSourceRouter``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from akshare_data.ingestion.base import DataSource
from akshare_data.core.errors import SourceUnavailableError, ErrorCode
from akshare_data.core.symbols import (
    jq_code_to_ak as _jq_code_to_ak,
    ak_code_to_jq as _ak_code_to_jq,
)
from akshare_data.sources.akshare.fetcher import fetch

logger = logging.getLogger(__name__)

# Method name -> interface name mapping (mirrors the legacy adapter).
_METHOD_TO_INTERFACE: Dict[str, str] = {
    "get_daily_data": "equity_daily",
    "get_minute_data": "equity_minute",
    "get_realtime_data": "equity_realtime",
    "get_index_daily": "index_daily",
    "get_etf_daily": "etf_daily",
    "get_futures_hist_data": "futures_daily",
    "get_futures_realtime_data": "futures_realtime",
    "get_futures_main_contracts": "futures_main_contracts",
    "get_options_chain": "options_chain",
    "get_options_realtime_data": "options_realtime",
    "get_options_expirations": "options_expirations",
    "get_options_hist_data": "options_hist",
    "get_lpr_rate": "macro_lpr",
    "get_pmi_index": "macro_pmi",
    "get_cpi_data": "macro_cpi",
    "get_ppi_data": "macro_ppi",
    "get_m2_supply": "macro_m2",
    "get_finance_indicator": "finance_indicator",
    "get_balance_sheet": "balance_sheet",
    "get_income_statement": "income_statement",
    "get_cash_flow": "cash_flow",
    "get_basic_info": "basic_info",
    "get_money_flow": "money_flow",
    "get_north_money_flow": "north_money_flow",
    "get_dragon_tiger_list": "dragon_tiger_list",
    "get_block_deal": "block_deal",
    "get_margin_data": "margin_data",
    "get_call_auction": "call_auction",
    "get_securities_list": "securities_list",
    "get_security_info": "security_info",
    "get_trading_days": "tool_trade_date_hist_sina",
    "get_st_stocks": "st_stocks",
    "get_suspended_stocks": "suspended_stocks",
    "get_index_stocks": "index_components",
    "get_index_components": "index_components",
    "get_index_list": "index_list",
    "get_etf_list": "etf_list",
    "get_lof_list": "lof_list",
    "get_fund_manager_info": "fund_manager_info",
    "get_fund_net_value": "fund_net_value",
    "get_industry_stocks": "industry_stocks",
    "get_industry_mapping": "industry_mapping",
    "get_news_data": "disclosure_news",
}


class AkShareAdapter(DataSource):
    """AkShare data source adapter — config-driven thin dispatcher.

    All ``get_xxx()`` calls are dynamically routed to ``fetcher.fetch()``
    via ``__getattr__``.  Interface definitions, field mappings, and
    multi-source fallback are driven by ``akshare_registry.yaml``.
    """

    name = "akshare"
    source_type = "real"

    def __init__(
        self,
        use_cache: bool = True,
        cache_ttl_hours: int = 24,
        offline_mode: bool = False,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        data_sources: Optional[List[str]] = None,
    ):
        self._use_cache = use_cache
        self._cache_ttl_hours = cache_ttl_hours
        self._offline_mode = offline_mode
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._data_sources = data_sources or [
            "sina",
            "east_money",
            "tushare",
            "baostock",
        ]

        self._akshare_available = False
        self._akshare = None
        try:
            import akshare

            self._akshare = akshare
            self._akshare_available = True
            logger.debug("akshare version: %s", akshare.__version__)
        except ImportError:
            logger.warning("akshare not installed; data source will be unavailable")

        self._scipy_available = False
        self._np_available = False
        try:
            from scipy.stats import norm
            from scipy.optimize import brentq
            import numpy as np

            self._norm = norm
            self._brentq = brentq
            self._np = np
            self._scipy_available = True
            self._np_available = True
        except ImportError:
            logger.warning(
                "scipy/numpy not installed; Greeks/IV calculations unavailable"
            )

    def is_configured(self) -> bool:
        return self._akshare_available

    # -- Dynamic routing ---------------------------------------------------

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        interface_name = _METHOD_TO_INTERFACE.get(name)
        if interface_name is None and name.startswith("get_"):
            candidate = name[4:]
            interface_name = _METHOD_TO_INTERFACE.get(candidate)

        if interface_name is None:
            raise AttributeError(
                f"{self.__class__.__name__} has no method '{name}' "
                f"and no matching entry in akshare_registry.yaml"
            )

        def dispatcher(*args, **kwargs):
            if args:
                param_names = ["symbol", "start_date", "end_date"]
                for i, arg in enumerate(args):
                    if i < len(param_names) and param_names[i] not in kwargs:
                        kwargs[param_names[i]] = arg

            if interface_name == "dragon_tiger_list" and "date" in kwargs:
                date_val = kwargs.pop("date")
                if isinstance(date_val, (datetime, date)):
                    date_val = date_val.strftime("%Y-%m-%d")
                kwargs.setdefault("start_date", date_val)
                kwargs.setdefault("end_date", date_val)

            if self._offline_mode:
                raise SourceUnavailableError(
                    "No cached data in offline mode",
                    error_code=ErrorCode.SOURCE_UNAVAILABLE,
                    source=self.name,
                    symbol=kwargs.get("symbol", ""),
                )
            if not self._akshare_available:
                raise SourceUnavailableError(
                    "akshare is not available",
                    source=self.name,
                    symbol=kwargs.get("symbol", ""),
                )
            return fetch(interface_name, akshare=self._akshare, **kwargs)

        return dispatcher

    # -- DataSource abstract method implementations ------------------------

    def get_daily_data(
        self,
        symbol: str,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        return self.__getattr__("get_daily_data")(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            **kwargs,
        )

    def get_index_stocks(self, index_code: str, **kwargs) -> List[str]:
        return self.__getattr__("get_index_stocks")(index_code=index_code, **kwargs)

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        return self.__getattr__("get_index_components")(
            index_code=index_code, include_weights=include_weights, **kwargs
        )

    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> List[str]:
        return self.__getattr__("get_trading_days")(
            start_date=start_date, end_date=end_date, **kwargs
        )

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self.__getattr__("get_securities_list")(
            security_type=security_type, date=date, **kwargs
        )

    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        if not self._akshare_available:
            return {"code": symbol, "type": "unknown"}
        return self.__getattr__("get_security_info")(symbol=symbol, **kwargs)

    def get_minute_data(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        kwargs["period"] = freq.replace("min", "")
        return self.__getattr__("get_minute_data")(
            symbol=symbol, start_date=start_date, end_date=end_date, **kwargs
        )

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self.__getattr__("get_money_flow")(
            symbol=symbol, start_date=start_date, end_date=end_date, **kwargs
        )

    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self.__getattr__("get_north_money_flow")(
            start_date=start_date, end_date=end_date, **kwargs
        )

    def get_industry_stocks(
        self, industry_code: str, level: int = 1, **kwargs
    ) -> List[str]:
        return self.__getattr__("get_industry_stocks")(
            industry_code=industry_code, level=level, **kwargs
        )

    def get_industry_mapping(self, symbol: str, level: int = 1, **kwargs) -> str:
        if not self._akshare_available:
            return ""
        return self.__getattr__("get_industry_mapping")(
            symbol=symbol, level=level, **kwargs
        )

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self.__getattr__("get_finance_indicator")(
            symbol=symbol,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    def get_call_auction(
        self, symbol: str, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        return self.__getattr__("get_call_auction")(symbol=symbol, date=date, **kwargs)

    # -- Helpers -----------------------------------------------------------

    def _normalize_symbol(self, symbol: str) -> str:
        return _jq_code_to_ak(symbol)

    def _normalize_date(self, val) -> str:
        if isinstance(val, str):
            return val
        if isinstance(val, (datetime, date)):
            return val.strftime("%Y-%m-%d")
        return str(val)

    def _to_jq_format(self, code: str) -> str:
        return _ak_code_to_jq(code)

    def health_check(self) -> Dict[str, Any]:
        return {
            "status": "ok" if self._akshare_available else "degraded",
            "akshare_available": self._akshare_available,
            "cache_enabled": self._use_cache,
        }

    def get_source_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.source_type,
            "description": "AkShare data source adapter (config-driven)",
            "akshare_available": self._akshare_available,
            "cache_enabled": self._use_cache,
            "offline_mode": self._offline_mode,
            "data_sources": self._data_sources,
        }
