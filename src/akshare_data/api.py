"""Thin compatibility wrapper for the online read-only API.

Business logic is split into:
- `service_facade.py`: read/query/backfill facade delegating to Served DataService
- `legacy_adapter.py`: legacy source compatibility proxies/warnings
- `namespace_assembly.py`: market namespace assembly (cn/hk/us/macro)

Online API does not synchronously fetch from source adapters and does not write
cache data.
"""

__all__ = [
    "DataService",
    "get_service",
    "SourceProxy",
    "CNStockQuoteAPI",
    "CNStockFinanceAPI",
    "CNStockCapitalAPI",
    "CNIndexQuoteAPI",
    "CNIndexMetaAPI",
    "CNETFQuoteAPI",
    "CNStockEventAPI",
    "HKStockQuoteAPI",
    "HKMarketAPI",
    "USStockQuoteAPI",
    "USMarketAPI",
    "MacroChinaAPI",
    "MacroAPI",
    "CNMarketAPI",
]
import logging
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union
from types import SimpleNamespace

import pandas as pd

from akshare_data.service.data_service import (
    DataService as ServedDataService,
    QueryResult,
)
from akshare_data.service.missing_data_policy import MissingAction
from akshare_data.core.symbols import normalize_symbol

logger = logging.getLogger("akshare_data")
_service_lock = threading.Lock()


# --- Namespace Classes for Categorized API Access (Read-Only Served) ---


class CNStockQuoteAPI:
    def __init__(self, service):
        self.service = service

    def daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query_daily(
            table="stock_daily",
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )
        return result.data

    def call_auction(
        self,
        symbol: str,
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {"date": date} if date else None
        result = self.service._served.query(
            table="call_auction",
            where=where,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def minute(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {}
        if start_date:
            where["datetime"] = (
                start_date,
                end_date or datetime.today().strftime("%Y-%m-%d"),
            )
        result = self.service._served.query(
            table=f"stock_minute_{freq}",
            where=where or None,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def realtime(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="spot_snapshot",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data


class CNStockFinanceAPI:
    def __init__(self, service):
        self.service = service

    def balance_sheet(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="balance_sheet",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def income_statement(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="income_statement",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def cash_flow(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="cash_flow",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def indicators(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {}
        if start_date and end_date:
            where["report_date"] = (start_date, end_date)
        elif start_date:
            where["report_date"] = (start_date, datetime.today().strftime("%Y-%m-%d"))
        result = self.service._served.query(
            table="finance_indicator",
            where=where or None,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data


class CNStockCapitalAPI:
    def __init__(self, service):
        self.service = service

    def money_flow(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self.service._served.query(
            table="money_flow",
            where=where or None,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def northbound_holdings(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="northbound_holdings",
            where={"date": (start_date, end_date)},
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def block_deal(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self.service._served.query(
            table="block_deal",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def dragon_tiger(
        self, date: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self.service._served.query(
            table="dragon_tiger_list",
            where={"date": date},
        )
        return result.data

    def margin(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="margin_detail",
            where={"date": (start_date, end_date)},
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def north(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self.service._served.query(
            table="north_flow",
            where=where or None,
        )
        return result.data


class CNIndexQuoteAPI:
    def __init__(self, service):
        self.service = service

    def daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="index_daily",
            where={"date": (start_date, end_date), "symbol": sym},
        )
        return result.data


class CNIndexMetaAPI:
    def __init__(self, service):
        self.service = service

    def components(
        self, index_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        code = normalize_symbol(index_code)
        result = self.service._served.query(
            table="index_components",
            partition_by="index_code",
            partition_value=code,
        )
        return result.data


class CNETFQuoteAPI:
    def __init__(self, service):
        self.service = service

    def daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="etf_daily",
            where={"date": (start_date, end_date), "symbol": sym},
        )
        return result.data


class CNStockEventAPI:
    def __init__(self, service):
        self.service = service

    def dividend(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self.service._served.query(
            table="dividend",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def restricted_release(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self.service._served.query(
            table="restricted_release",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data


class HKStockQuoteAPI:
    def __init__(self, service):
        self.service = service

    def daily(
        self, symbol: Optional[str], source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self.service._served.query(table="hk_stock_daily")
        df = result.data
        if df.empty or not symbol:
            return df
        for col in ("stockCode", "symbol", "code"):
            if col in df.columns:
                return df[df[col].astype(str).str.contains(symbol, na=False)]
        logger.warning(
            f"Cannot filter HK stocks by symbol: no known symbol column found in {list(df.columns)}"
        )
        return df


class HKMarketAPI:
    def __init__(self, service):
        self.service = service
        self.stock = SimpleNamespace(quote=HKStockQuoteAPI(service))


class USStockQuoteAPI:
    def __init__(self, service):
        self.service = service

    def daily(
        self, symbol: Optional[str], source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self.service._served.query(table="us_stock_daily")
        df = result.data
        if df.empty or not symbol:
            return df
        for col in ("stockCode", "symbol", "code"):
            if col in df.columns:
                return df[df[col].astype(str).str.contains(symbol, na=False)]
        logger.warning(
            f"Cannot filter US stocks by symbol: no known symbol column found in {list(df.columns)}"
        )
        return df


class USMarketAPI:
    def __init__(self, service):
        self.service = service
        self.stock = SimpleNamespace(quote=USStockQuoteAPI(service))


class MacroChinaAPI:
    def __init__(self, service):
        self.service = service

    def interest_rate(
        self,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        result = self.service._served.query(
            table="shibor_rate",
            where={"date": (start_date, end_date)},
        )
        return result.data

    def gdp(
        self,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        result = self.service._served.query(
            table="macro_gdp",
            where={"date": (start_date, end_date)},
        )
        return result.data

    def social_financing(
        self,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        result = self.service._served.query(
            table="social_financing",
            where={"date": (start_date, end_date)},
        )
        return result.data


class CNMarketAPI:
    def __init__(self, service):
        self.service = service
        self.stock = SimpleNamespace(
            quote=CNStockQuoteAPI(service),
            finance=CNStockFinanceAPI(service),
            capital=CNStockCapitalAPI(service),
            event=CNStockEventAPI(service),
        )
        self.index = SimpleNamespace(
            quote=CNIndexQuoteAPI(service), meta=CNIndexMetaAPI(service)
        )
        self.fund = SimpleNamespace(quote=CNETFQuoteAPI(service))

    def trade_calendar(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        result = self.service._served.query(table="trade_calendar")
        df = result.data
        if df is None or df.empty or "date" not in df.columns:
            return []
        date_list = df["date"].tolist()
        if start_date:
            date_list = [d for d in date_list if d >= str(start_date)]
        if end_date:
            date_list = [d for d in date_list if d <= str(end_date)]
        return date_list


class SourceProxy:
    """Dynamic proxy for source method dispatch.

    Captures method calls and delegates to DataService._execute_source_method.
    Used for backward compatibility with source-based fetching patterns.
    """

    def __init__(self, service, requested_source=None):
        self.service = service
        self.requested_source = requested_source

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            return self.service._execute_source_method(
                name, self.requested_source, *args, **kwargs
            )

        return wrapper


class MacroAPI:
    def __init__(self, service):
        self.service = service
        self.china = MacroChinaAPI(service)


class DataService:
    """Unified data service (Read-Only Served strategy).

    This facade delegates to the internal ServedDataService for all queries.
    It NEVER synchronously fetches from source adapters.

    Backward compatibility:
    - All existing method signatures are preserved
    - Namespace API (cn, hk, us, macro) still works
    - When Served has no data, returns empty DataFrame
    """

    def __init__(
        self,
        cache_manager=None,
        router=None,
        access_logger=None,
        source=None,
    ):
        from akshare_data.store.manager import get_cache_manager as _get_cm

        cm = cache_manager or _get_cm()
        self._served = ServedDataService(cache_manager=cm)

        if router is not None:
            logger.warning(
                "router parameter is deprecated in read-only mode; source adapters are not used"
            )
        if source is not None:
            logger.warning(
                "source parameter is deprecated in read-only mode; source adapters are not used"
            )

        self.router = router
        self.access_logger = access_logger

        self._custom_source = source

        # Backward compatibility: set up adapter references
        if source is not None:
            self.adapters = {source.name: source}
            self.lixinger = source
            self.akshare = source
        else:
            from akshare_data.ingestion.adapters.mock import MockAdapter

            _mock = MockAdapter()
            self.adapters = {"mock": _mock}
            self.lixinger = _mock
            self.akshare = _mock

        self.cn = CNMarketAPI(self)
        self.hk = HKMarketAPI(self)
        self.us = USMarketAPI(self)
        self.macro = MacroAPI(self)

    @property
    def cache(self):
        """Backward-compatible access to underlying CacheManager."""
        return self._served._reader._cache

    def _get_source(self, requested_source=None):
        logger.warning(
            "_get_source called but source adapters are disabled in read-only mode. "
            "Use offline downloader to populate data first."
        )
        return SourceProxy(self, requested_source)

    def _execute_source_method(self, method_name, requested_source, *args, **kwargs):
        """Execute a method on the specified source adapter.

        Read-only mode does not execute source methods synchronously.
        Returns None and emits a warning for backward compatibility.
        """
        logger.warning(
            "_execute_source_method is disabled in read-only mode for '%s' "
            "(requested_source=%s). Use offline downloader/backfill instead.",
            method_name,
            requested_source,
        )
        return None

    def _resolve_sources(self, requested_source, method_name):
        """Resolve which source(s) to use for a given method.

        Returns a list of source names to try in order.
        """
        if requested_source is not None:
            if isinstance(requested_source, list):
                return requested_source
            return [requested_source]

        # Default: try adapters in order
        available = list(self.adapters.keys())
        if available:
            return available
        return ["mock"]

    def _build_security_info_df(self, symbol):
        """Build a security info DataFrame for a given symbol.

        Backward compatibility method.
        """
        info = self.get_security_info(symbol)
        if info:
            return pd.DataFrame([info])
        return pd.DataFrame()

    def cached_fetch(
        self,
        table: str,
        storage_layer: str | None = None,
        partition_by: str | None = None,
        partition_value: str | None = None,
        date_col: str = "date",
        fetch_fn: Callable[[], pd.DataFrame | None] | None = None,
        **params,
    ) -> pd.DataFrame:
        """Read-only fetch from Served/cache only.

        ``fetch_fn`` is ignored in read-only mode and retained only for
        backward-compatible call signatures.
        """
        where = {}
        for k, v in params.items():
            if k in ("start_date", "end_date") and v:
                if "start_date" in params and "end_date" in params:
                    where[date_col] = (params.get("start_date"), params.get("end_date"))
                    break

        result = self._served.query(
            table=table,
            where=where or None,
            partition_by=partition_by,
            partition_value=partition_value,
        )
        if result.data is not None and not result.data.empty:
            return result.data

        if fetch_fn is not None:
            logger.warning(
                "cached_fetch fetch_fn is ignored in read-only mode for table='%s'",
                table,
            )

        return result.data if result.data is not None else pd.DataFrame()

    # --- Public API Facade (Backward Compatibility) ---

    def get_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.quote.daily(symbol, start_date, end_date, adjust, source)

    def get_minute(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.quote.minute(symbol, freq, start_date, end_date, source)

    def get_index(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.index.quote.daily(index_code, start_date, end_date, source)

    def get_etf(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.fund.quote.daily(symbol, start_date, end_date, source)

    def get_index_stocks(
        self, index_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> List[str]:
        code = normalize_symbol(index_code)
        result = self._served.query(
            table="index_components",
            partition_by="index_code",
            partition_value=code,
        )
        if (
            result.data is not None
            and not result.data.empty
            and "code" in result.data.columns
        ):
            return result.data["code"].dropna().tolist()
        return []

    def get_index_components(
        self,
        index_code: str,
        include_weights: bool = True,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        code = normalize_symbol(index_code)
        result = self._served.query(
            table="index_components",
            partition_by="index_code",
            partition_value=code,
        )
        return result.data

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        result = self._served.query(
            table="securities",
            partition_by="security_type",
            partition_value=security_type,
        )
        return result.data

    def get_industry_stocks(
        self,
        industry_code: str,
        level: int = 1,
        source: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        result = self._served.query(
            table="industry_components",
            partition_by="industry_code",
            partition_value=industry_code,
        )
        if (
            result.data is not None
            and not result.data.empty
            and "code" in result.data.columns
        ):
            return result.data["code"].dropna().tolist()
        return []

    def get_trading_days(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        return self.cn.trade_calendar(start_date, end_date, source)

    def get_suspended_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="suspended_stocks")
        return result.data

    def get_st_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="st_stocks")
        return result.data

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.capital.money_flow(symbol, start_date, end_date, source)

    def get_north_money_flow(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.capital.north(start_date, end_date, source)

    def get_finance_indicator(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.finance.indicators(symbol, start_date, end_date, source)

    def get_realtime_data(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.cn.stock.quote.realtime(symbol, source)

    def get_security_info(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="company_info",
            partition_by="symbol",
            partition_value=sym,
        )
        if result.data is not None and not result.data.empty:
            return result.data.iloc[0].to_dict()
        return {}

    def get_industry_mapping(
        self,
        symbol: str,
        level: int = 1,
        source: Optional[Union[str, List[str]]] = None,
    ) -> str:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="industry_mapping",
            partition_by="symbol",
            partition_value=sym,
        )
        if (
            result.data is not None
            and not result.data.empty
            and "industry_code" in result.data.columns
        ):
            return result.data["industry_code"].iloc[0]
        return ""

    def get_basic_info(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="stock_basic_info",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_balance_sheet(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.cn.stock.finance.balance_sheet(symbol, source)

    def get_income_statement(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.cn.stock.finance.income_statement(symbol, source)

    def get_cash_flow(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.cn.stock.finance.cash_flow(symbol, source)

    def get_financial_metrics(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="financial_metrics",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_stock_valuation(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="stock_valuation",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_index_valuation(
        self, index_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        code = normalize_symbol(index_code)
        result = self._served.query(
            table="index_valuation",
            partition_by="index_code",
            partition_value=code,
        )
        return result.data

    def get_top_shareholders(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="top_shareholders",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_institution_holdings(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="institution_holdings",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_latest_holder_number(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="holder_number",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_northbound_holdings(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.capital.northbound_holdings(
            symbol, start_date, end_date, source
        )

    def get_dragon_tiger_list(
        self, date: str, source: Optional[Union[str, List[str]]] = None, **kwargs
    ) -> pd.DataFrame:
        return self.cn.stock.capital.dragon_tiger(date, source)

    def get_block_deal(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.capital.block_deal(symbol, start_date, end_date, source)

    def get_margin_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.capital.margin(symbol, start_date, end_date, source)

    def get_dividend_data(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None, **kwargs
    ) -> pd.DataFrame:
        return self.cn.stock.event.dividend(symbol, source)

    def get_hk_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.hk.stock.quote.daily(None, source=source)

    def get_us_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.us.stock.quote.daily(None, source=source)

    def get_new_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="new_stocks")
        return result.data

    def get_ipo_info(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="ipo_info")
        return result.data

    def get_restricted_release_detail(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="restricted_release_detail",
            where=where or None,
        )
        return result.data

    def get_shareholder_changes(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="shareholder_changes",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_insider_trading(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="insider_trading",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_equity_freeze(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="equity_freeze",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_capital_change(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="capital_change",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_earnings_forecast(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="earnings_forecast",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_fund_open_daily(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="fund_open_daily")
        return result.data

    def get_fund_open_nav(
        self,
        fund_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="fund_open_nav",
            where=where or None,
            partition_by="fund_code",
            partition_value=fund_code,
        )
        return result.data

    def get_fund_open_info(
        self, fund_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        result = self._served.query(
            table="fund_open_info",
            partition_by="fund_code",
            partition_value=fund_code,
        )
        if result.data is not None and not result.data.empty:
            return result.data.iloc[0].to_dict()
        return {}

    def get_equity_pledge(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="equity_pledge",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_equity_pledge_rank(
        self,
        date: Optional[str] = None,
        top_n: int = 50,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {"date": date} if date else None
        result = self._served.query(
            table="equity_pledge_rank",
            where=where,
            limit=top_n,
        )
        return result.data

    def get_restricted_release(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.event.restricted_release(
            symbol, start_date, end_date, source
        )

    def get_restricted_release_calendar(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="restricted_release_calendar",
            where=where or None,
        )
        return result.data

    def get_goodwill_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="goodwill",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_goodwill_impairment(
        self, date: Optional[str] = None, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        where = {"date": date} if date else None
        result = self._served.query(
            table="goodwill_impairment",
            where=where,
        )
        return result.data

    def get_goodwill_by_industry(
        self, date: Optional[str] = None, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        where = {"date": date} if date else None
        result = self._served.query(
            table="goodwill_by_industry",
            where=where,
        )
        return result.data

    def get_repurchase_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="repurchase",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_esg_rating(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="esg_rating",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_esg_rank(
        self,
        date: Optional[str] = None,
        top_n: int = 50,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {"date": date} if date else None
        result = self._served.query(
            table="esg_rank",
            where=where,
            limit=top_n,
        )
        return result.data

    def get_performance_forecast(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="performance_forecast",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_performance_express(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="performance_express",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_analyst_rank(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="analyst_rank",
            where=where or None,
        )
        return result.data

    def get_research_report(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        partition_value = normalize_symbol(symbol) if symbol else None
        result = self._served.query(
            table="research_report",
            where=where or None,
            partition_by="symbol" if symbol else None,
            partition_value=partition_value,
        )
        return result.data

    def get_chip_distribution(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="chip_distribution",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_stock_bonus(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="stock_bonus",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_rights_issue(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="rights_issue",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_dividend_by_date(
        self, date: Optional[str] = None, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        where = {"date": date} if date else None
        result = self._served.query(
            table="dividend_by_date",
            where=where,
        )
        return result.data

    def get_management_info(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="company_management",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_name_history(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="name_history",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_shibor_rate(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="shibor_rate",
            where=where or None,
        )
        return result.data

    def get_social_financing(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.macro.china.social_financing(start_date, end_date, source)

    def get_macro_gdp(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.macro.china.gdp(start_date, end_date, source)

    def get_macro_exchange_rate(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="macro_exchange_rate",
            where=where or None,
        )
        return result.data

    def get_fof_list(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="fof_fund")
        return result.data

    def get_fof_nav(
        self,
        fund_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="fof_nav",
            where=where or None,
            partition_by="fund_code",
            partition_value=fund_code,
        )
        return result.data

    def get_lof_spot(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="lof_fund")
        return result.data

    def get_lof_nav(
        self, fund_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(
            table="lof_nav",
            partition_by="fund_code",
            partition_value=fund_code,
        )
        return result.data

    def get_convert_bond_premium(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="convert_bond_premium")
        return result.data

    def get_convert_bond_spot(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="convert_bond_spot")
        return result.data

    def get_industry_performance(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "日k",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="industry_performance",
            where=where or None,
            partition_by="symbol",
            partition_value=symbol,
        )
        return result.data

    def get_concept_performance(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "daily",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="concept_performance",
            where=where or None,
            partition_by="symbol",
            partition_value=symbol,
        )
        return result.data

    def get_stock_industry(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="stock_industry",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_hot_rank(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="hot_rank")
        return result.data

    def get_conversion_bond_list(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="conversion_bond_list")
        return result.data

    def get_conversion_bond_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="conversion_bond_daily",
            where=where or None,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_option_list(
        self,
        source: Optional[Union[str, List[str]]] = None,
        market: str = "sse",
    ) -> pd.DataFrame:
        result = self._served.query(
            table="option_list",
            partition_by="market",
            partition_value=market,
        )
        return result.data

    def get_option_daily(
        self,
        symbol: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="option_daily",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_lof_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="lof_fund",
            where=where or None,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_futures_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="futures_daily",
            where=where or None,
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_futures_spot(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="futures_spot")
        return result.data

    def get_spot_em(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="spot_em")
        return result.data

    def get_stock_hist(
        self,
        symbol: str,
        period: str = "daily",
        start_date: str = "",
        end_date: str = "",
        adjust: str = "",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        if period == "daily":
            return self.get_daily(sym, start_date, end_date, adjust or "qfq", source)
        return self.cn.stock.quote.minute(
            sym, freq=period, start_date=start_date, end_date=end_date, source=source
        )

    def get_sw_industry_list(
        self,
        level: str = "1",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        result = self._served.query(
            table="sw_industry_list",
            partition_by="level",
            partition_value=level,
        )
        return result.data

    def get_sw_industry_daily(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        where = {}
        if start_date and end_date:
            where["date"] = (start_date, end_date)
        result = self._served.query(
            table="sw_industry_daily",
            where=where or None,
            partition_by="index_code",
            partition_value=index_code,
        )
        return result.data

    def get_concept_list(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="concept_list")
        return result.data

    def get_concept_stocks(
        self, concept_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(
            table="concept_components",
            partition_by="concept_code",
            partition_value=concept_code,
        )
        return result.data

    def get_stock_concepts(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        result = self._served.query(
            table="stock_concepts",
            partition_by="symbol",
            partition_value=sym,
        )
        return result.data

    def get_call_auction(
        self,
        symbol: str,
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.cn.stock.quote.call_auction(symbol, date, source)

    # --- Served-specific methods (new) ---

    def query(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        partition_by: Optional[str] = None,
        partition_value: Optional[str] = None,
        version: Optional[str] = None,
    ) -> QueryResult:
        """Direct query to Served layer with full metadata."""
        return self._served.query(
            table=table,
            where=where,
            columns=columns,
            order_by=order_by,
            limit=limit,
            partition_by=partition_by,
            partition_value=partition_value,
            version=version,
        )

    def request_backfill(
        self,
        table: str,
        params: Optional[Dict[str, Any]] = None,
        priority: str = "normal",
    ) -> str:
        """Request async backfill for missing data."""
        return self._served.request_backfill(table, params, priority)

    def get_backfill_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a backfill request."""
        return self._served.get_backfill_status(request_id)

    def list_pending_backfills(self) -> List[Dict[str, Any]]:
        """List all pending backfill requests."""
        return self._served.list_pending_backfills()

    def set_missing_action(self, table: str, action: MissingAction) -> None:
        """Configure missing data action for a specific table."""
        self._served.set_missing_action(table, action)

    def get_table_info(self, table: str) -> Dict[str, Any]:
        """Get metadata about a served table."""
        return self._served.get_table_info(table)

    def list_tables(self) -> List[str]:
        """List all available served tables."""
        return self._served.list_tables()

    def table_exists(self, table: str) -> bool:
        """Check if a table has any data in Served."""
        return self._served.table_exists(table)

    def has_data_for_range(
        self,
        table: str,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> bool:
        """Check if Served has data for the given symbol and date range."""
        return self._served.has_data_for_range(table, symbol, start_date, end_date)


_default_service: Optional[DataService] = None


def get_service() -> DataService:
    """Get global DataService singleton."""
    global _default_service
    if _default_service is None:
        with _service_lock:
            if _default_service is None:
                _default_service = DataService()
    return _default_service
