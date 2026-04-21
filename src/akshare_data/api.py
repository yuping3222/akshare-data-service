"""Online data service: Cache-First unified API.

DataService is the central orchestrator that:
1. Checks cache first (via CacheManager)
2. Falls back to data source (via AkShareAdapter / MultiSourceRouter)
3. Writes results back to cache
4. Returns data to caller

Sources know NOTHING about caching. The API layer owns the cache-first strategy.
"""

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union
from types import SimpleNamespace

import pandas as pd

from akshare_data.store.manager import CacheManager, get_cache_manager
from akshare_data.store.fetcher import CachedFetcher, FetchConfig
from akshare_data.sources.router import MultiSourceRouter, SourceHealthMonitor
from akshare_data.sources.akshare_source import AkShareAdapter
from akshare_data.sources.lixinger_source import LixingerAdapter
from akshare_data.core.base import DataSource

try:
    from akshare_data.sources.tushare_source import TushareAdapter
except ImportError:
    TushareAdapter = None
from akshare_data.core.symbols import normalize_symbol
from akshare_data.core.schema import SCHEMA_REGISTRY, get_table_schema
from akshare_data.offline.access_logger import AccessLogger

logger = logging.getLogger("akshare_data")


class SourceProxy:
    def __init__(
        self, service, requested_source: Optional[Union[str, List[str]]] = None
    ):
        self.service = service
        self.requested_source = requested_source

    def __getattr__(self, method_name: str):
        def wrapper(*args, **kwargs):
            return self.service._execute_source_method(
                method_name, self.requested_source, *args, **kwargs
            )

        return wrapper


# --- Namespace Classes for Categorized API Access ---


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
        """Fetch daily stock data."""
        return self.service.cached_fetch(
            table="stock_daily",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=normalize_symbol(symbol),
            fetch_fn=lambda: self.service._get_source(source).get_daily_data(
                normalize_symbol(symbol), start_date, end_date, adjust
            ),
            symbol=normalize_symbol(symbol),
            start_date=start_date,
            end_date=end_date,
        )

    def call_auction(
        self,
        symbol: str,
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch call auction data (real-time, bypasses cache)."""
        sym = normalize_symbol(symbol)
        return self.service._get_source(source).get_call_auction(sym, date)

    def minute(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch minute stock data."""
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table=f"stock_minute_{freq}",
            storage_layer="minute",
            partition_by="symbol",
            partition_value=sym,
            date_col="datetime",
            fetch_fn=lambda: self.service._get_source(source).get_minute_data(
                sym, freq, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def realtime(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch real-time quote."""
        return self.service._get_source(source).get_realtime_data(symbol)


class CNStockFinanceAPI:
    def __init__(self, service):
        self.service = service

    def balance_sheet(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch balance sheet data."""
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="balance_sheet",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            date_col="report_date",
            fetch_fn=lambda: self.service._get_source(source).get_balance_sheet(sym),
            symbol=sym,
        )

    def income_statement(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch income statement data."""
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="income_statement",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            date_col="report_date",
            fetch_fn=lambda: self.service._get_source(source).get_income_statement(sym),
            symbol=sym,
        )

    def cash_flow(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch cash flow statement data."""
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="cash_flow",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            date_col="report_date",
            fetch_fn=lambda: self.service._get_source(source).get_cash_flow(sym),
            symbol=sym,
        )

    def indicators(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="finance_indicator",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            date_col="report_date",
            fetch_fn=lambda: self.service._get_source(source).get_finance_indicator(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )


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
        return self.service.cached_fetch(
            table="money_flow",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_money_flow(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def northbound_holdings(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="northbound_holdings",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_northbound_holdings(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def block_deal(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol) if symbol else None
        return self.service.cached_fetch(
            table="block_deal",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_block_deal(
                sym, start_date, end_date
            ) if sym else self.service._get_source(source).get_block_deal(),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def dragon_tiger(
        self, date: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        return self.service.cached_fetch(
            table="dragon_tiger_list",
            storage_layer="daily",
            fetch_fn=lambda: self.service._get_source(source).get_dragon_tiger_list(date),
            date=date,
        )

    def margin(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="margin_detail",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_margin_data(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def north(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch north money flow."""
        return self.service.cached_fetch(
            table="north_flow",
            storage_layer="daily",
            fetch_fn=lambda: self.service._get_source(source).get_north_money_flow(
                start_date=start_date, end_date=end_date
            ),
            start_date=start_date,
            end_date=end_date,
        )


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
        return self.service.cached_fetch(
            table="index_daily",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_index_daily(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )


class CNIndexMetaAPI:
    def __init__(self, service):
        self.service = service

    def components(
        self, index_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        code = normalize_symbol(index_code)
        return self.service.cached_fetch(
            table="index_components",
            storage_layer="meta",
            partition_by="index_code",
            partition_value=code,
            fetch_fn=lambda: self.service._get_source(source).get_index_components(code),
            index_code=code,
        )


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
        return self.service.cached_fetch(
            table="etf_daily",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_etf_daily(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )


class CNStockEventAPI:
    def __init__(self, service):
        self.service = service

    def dividend(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol)
        return self.service.cached_fetch(
            table="dividend",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self.service._get_source(source).get_dividend_data(sym),
            symbol=sym,
        )

    def restricted_release(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        sym = normalize_symbol(symbol) if symbol else None
        return self.service.cached_fetch(
            table="restricted_release",
            storage_layer="daily",
            fetch_fn=lambda: self.service._get_source(source).get_restricted_release(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )


class HKStockQuoteAPI:
    def __init__(self, service):
        self.service = service

    def daily(
        self, symbol: Optional[str], source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        df = self.service._get_source(source).get_hk_stocks()
        if df.empty or not symbol:
            return df
        # Client-side symbol filter (common column names used by Lixinger)
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
        df = self.service._get_source(source).get_us_stocks()
        if df.empty or not symbol:
            return df
        # Client-side symbol filter (common column names used by Lixinger)
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
        return self.service.cached_fetch(
            table="shibor_rate",
            storage_layer="daily",
            fetch_fn=lambda: self.service._get_source(source).get_shibor_rate(
                start_date, end_date
            ),
            start_date=start_date,
            end_date=end_date,
        )

    def gdp(
        self,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.service.cached_fetch(
            table="macro_gdp",
            storage_layer="daily",
            fetch_fn=lambda: self.service._get_source(source).get_macro_gdp(start_date, end_date),
            start_date=start_date,
            end_date=end_date,
        )

    def social_financing(
        self,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        return self.service.cached_fetch(
            table="social_financing",
            storage_layer="daily",
            fetch_fn=lambda: self.service._get_source(source).get_social_financing(
                start_date, end_date
            ),
            start_date=start_date,
            end_date=end_date,
        )


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
        """Fetch trading calendar."""
        def fetch_fn():
            days = self.service._get_source(source).get_trading_days(
                start_date, end_date
            )
            if days is None:
                return pd.DataFrame(columns=["date"])
            if isinstance(days, pd.DataFrame):
                if days.empty:
                    return pd.DataFrame(columns=["date"])
                col = days.columns[0]
                date_list = pd.to_datetime(days[col]).dt.strftime("%Y-%m-%d").tolist()
                return pd.DataFrame({"date": date_list})
            return pd.DataFrame({"date": list(days)})

        df = self.service.cached_fetch(
            table="trade_calendar",
            storage_layer="meta",
            fetch_fn=fetch_fn,
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or df.empty or "date" not in df.columns:
            return []
        result = df["date"].tolist()
        if start_date:
            result = [d for d in result if d >= str(start_date)]
        if end_date:
            result = [d for d in result if d <= str(end_date)]
        return result


class MacroAPI:
    def __init__(self, service):
        self.service = service
        self.china = MacroChinaAPI(service)


class DataService:
    """Unified data service (Cache-First strategy)."""

    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        router: Optional[MultiSourceRouter] = None,
        access_logger: Optional[AccessLogger] = None,
        source: Optional[DataSource] = None,
    ):
        self.cache = cache_manager or get_cache_manager()
        self.router = router
        self.fetcher = CachedFetcher(self.cache)
        self.access_logger = access_logger

        # When a custom DataSource (e.g. MockSource) is injected, use it
        # as the primary/only source instead of creating real adapters.
        if source is not None:
            self._custom_source = source
            self.adapters = {source.name: source}
            # Also keep the built-in adapter references for backward compat;
            # point them at the custom source when available.
            self.lixinger = source
            self.akshare = source
        else:
            self._custom_source = None
            self.lixinger = LixingerAdapter()
            self.akshare = AkShareAdapter()
            self.adapters = {"lixinger": self.lixinger, "akshare": self.akshare}

        # Wire optional adapters (Tushare)
        if TushareAdapter is not None and source is None:
            self.tushare = TushareAdapter()
            self.adapters["tushare"] = self.tushare

        # Persistent health monitor so circuit-breaker state survives across calls
        self._source_health = SourceHealthMonitor()

        # Namespaced API surface
        self.cn = CNMarketAPI(self)
        self.hk = HKMarketAPI(self)
        self.us = USMarketAPI(self)
        self.macro = MacroAPI(self)

    def _get_source(self, requested_source: Optional[Union[str, List[str]]] = None):
        return SourceProxy(self, requested_source)

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
        """统一缓存获取入口 — 替代所有内联 cache.read/write 模式

        用法示例:
            # 全量缓存 (meta/snapshot)
            return self.cached_fetch(
                table="stock_bonus", storage_layer="daily",
                partition_by="symbol", partition_value=symbol,
                fetch_fn=lambda: source.get_stock_bonus(symbol),
                symbol=symbol,
            )

            # 增量缓存 (时序数据)
            return self.cached_fetch(
                table="stock_daily", storage_layer="daily",
                partition_by="symbol", partition_value=symbol,
                fetch_fn=lambda: source.get_daily(symbol, start, end),
                symbol=symbol, start_date=start, end_date=end,
            )
        """
        if fetch_fn is None:
            fetch_fn = params.pop("fetch_fn", None)
        if fetch_fn is None:
            return pd.DataFrame()

        config = FetchConfig(
            table=table,
            storage_layer=storage_layer,
            partition_by=partition_by,
            partition_value=partition_value,
            date_col=date_col,
        )
        return self.fetcher.execute(config, fetch_fn, **params)

    _NON_DF_METHODS = {
        "get_index_stocks",
        "get_industry_stocks",
        "get_trading_days",
        "get_concept_stocks",
        "get_stock_concepts",
    }

    def _build_router(self, callables, *router_args, **router_kw):
        """Build a MultiSourceRouter sharing the persistent health monitor."""
        providers = [(n, f) for n, f in callables.items()]
        router = MultiSourceRouter(providers=providers, *router_args, **router_kw)
        router._health = self._source_health
        return router

    def _resolve_sources(
        self,
        requested_source: Optional[Union[str, List[str]]],
        method_name: str,
    ) -> List[str]:
        """Resolve ordered list of candidate sources for a method call."""
        if requested_source is not None:
            sources = (
                [requested_source]
                if isinstance(requested_source, str)
                else requested_source
            )
        elif self._custom_source is not None:
            sources = [self._custom_source.name]
        else:
            sources = ["lixinger", "akshare"]
        return [
            s for s in sources
            if self.adapters.get(s) and hasattr(self.adapters.get(s), method_name)
        ]

    def _execute_source_method(
        self,
        method_name: str,
        requested_source: Optional[Union[str, List[str]]],
        *args,
        **kwargs,
    ):
        # Resolve candidate sources and build an ad-hoc router for all methods
        # (both DataFrame and non-DataFrame). The router's _validate_result
        # accepts non-DataFrame data when no min_rows/required_columns are set.
        candidates = self._resolve_sources(requested_source, method_name)
        if not candidates:
            logger.error(
                "None of specified sources %s supports %s",
                requested_source,
                method_name,
            )
            return (
                [] if "stocks" in method_name or "trading_days" in method_name else None
            )

        callables = {
            src: (lambda s: lambda *a, **kw: getattr(self.adapters.get(s), method_name)(*a, **kw))(src)
            for src in candidates
        }
        router_inst = self._build_router(callables)
        res = router_inst.execute(*args, **kwargs)

        if res.success and res.data is not None:
            return res.data

        # Fallback return value for non-DF methods when all sources fail
        return (
            [] if "stocks" in method_name or "trading_days" in method_name else None
        )

    # --- Public API Facade (Backward Compatibility) ---

    def get_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch daily data: cache first -> incremental fill -> merge."""
        return self.cn.stock.quote.daily(symbol, start_date, end_date, adjust, source)

    def get_minute(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch minute data: cache first -> incremental fill."""
        return self.cn.stock.quote.minute(symbol, freq, start_date, end_date, source)

    def get_index(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch index daily data."""
        return self.cn.index.quote.daily(index_code, start_date, end_date, source)

    def get_etf(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch ETF daily data."""
        return self.cn.fund.quote.daily(symbol, start_date, end_date, source)

    def get_index_stocks(
        self, index_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> List[str]:
        """Fetch index constituents."""
        code = normalize_symbol(index_code)
        df = self.cached_fetch(
            table="index_components",
            storage_layer="meta",
            partition_by="index_code",
            partition_value=code,
            fetch_fn=lambda: self._build_index_stocks_df(code, source),
            index_code=code,
        )
        if df is not None and not df.empty and "code" in df.columns:
            return df["code"].dropna().tolist()
        return []

    def _build_index_stocks_df(self, index_code, source):
        stocks = self._get_source(source).get_index_stocks(index_code)
        if stocks:
            return pd.DataFrame({"index_code": [index_code] * len(stocks), "code": stocks})
        return pd.DataFrame()

    def get_index_components(
        self,
        index_code: str,
        include_weights: bool = True,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch index constituents with weights."""
        code = normalize_symbol(index_code)
        return self.cached_fetch(
            table="index_components",
            storage_layer="meta",
            partition_by="index_code",
            partition_value=code,
            fetch_fn=lambda: self._get_source(source).get_index_components(
                code, include_weights=include_weights
            ),
            index_code=code,
        )

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch securities list (stock/ETF/index)."""
        return self.cached_fetch(
            table="securities",
            storage_layer="meta",
            partition_by="security_type",
            partition_value=security_type,
            fetch_fn=lambda: self._get_source(source).get_securities_list(
                security_type, date
            ),
            security_type=security_type,
            date=date,
        )

    def get_industry_stocks(
        self,
        industry_code: str,
        level: int = 1,
        source: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        """Fetch industry constituents."""
        df = self.cached_fetch(
            table="industry_components",
            storage_layer="meta",
            partition_by="industry_code",
            partition_value=industry_code,
            fetch_fn=lambda: self._build_industry_stocks_df(
                industry_code, level, source
            ),
            industry_code=industry_code,
            level=level,
        )
        if df is not None and not df.empty and "code" in df.columns:
            return df["code"].dropna().tolist()
        return []

    def _build_industry_stocks_df(self, industry_code, level, source):
        stocks = self._get_source(source).get_industry_stocks(industry_code, level)
        if stocks:
            return pd.DataFrame({
                "industry_code": [industry_code] * len(stocks),
                "level": [level] * len(stocks),
                "code": stocks,
            })
        return pd.DataFrame()

    def get_trading_days(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        """Fetch trading calendar."""
        return self.cn.trade_calendar(start_date, end_date, source)

    def get_suspended_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch suspended stocks."""
        return self.cached_fetch(
            table="suspended_stocks",
            storage_layer="meta",
            fetch_fn=lambda: self._get_source(source).get_suspended_stocks(),
        )

    def get_st_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch ST stocks."""
        return self.cached_fetch(
            table="st_stocks",
            storage_layer="meta",
            fetch_fn=lambda: self._get_source(source).get_st_stocks(),
        )

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch money flow."""
        return self.cn.stock.capital.money_flow(symbol, start_date, end_date, source)

    def get_north_money_flow(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch northbound fund flow."""
        return self.cn.stock.capital.north(start_date, end_date, source)

    def get_finance_indicator(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch finance indicators."""
        return self.cn.stock.finance.indicators(symbol, start_date, end_date, source)

    def get_realtime_data(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch real-time quotes (no caching)."""
        symbol = normalize_symbol(symbol)
        source = self._get_source(source)
        return source.get_realtime_data(symbol)

    def get_security_info(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Fetch security info."""
        sym = normalize_symbol(symbol)
        df = self.cached_fetch(
            table="company_info",
            storage_layer="meta",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._build_security_info_df(sym, source),
            symbol=sym,
        )
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return {}

    def _build_security_info_df(self, symbol, source):
        info = self._get_source(source).get_security_info(symbol)
        if info:
            return pd.DataFrame([info])
        return pd.DataFrame()

    def get_industry_mapping(
        self,
        symbol: str,
        level: int = 1,
        source: Optional[Union[str, List[str]]] = None,
    ) -> str:
        """Fetch industry mapping."""
        sym = normalize_symbol(symbol)
        df = self.cached_fetch(
            table="industry_mapping",
            storage_layer="meta",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._build_industry_mapping_df(sym, level, source),
            symbol=sym,
        )
        if df is not None and not df.empty and "industry_code" in df.columns:
            return df["industry_code"].iloc[0]
        return ""

    def _build_industry_mapping_df(self, symbol, level, source):
        result = self._get_source(source).get_industry_mapping(symbol, level)
        if result:
            return pd.DataFrame({"symbol": symbol, "industry_code": result})
        return pd.DataFrame()

    # ── Financial Data ─────────────────────────────────────────────

    def get_basic_info(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch stock basic info."""
        source = self._get_source(source)
        return source.get_basic_info(symbol)

    def get_balance_sheet(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch balance sheet."""
        source = self._get_source(source)
        return source.get_balance_sheet(symbol)

    def get_income_statement(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch income statement."""
        source = self._get_source(source)
        return source.get_income_statement(symbol)

    def get_cash_flow(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch cash flow statement."""
        source = self._get_source(source)
        return source.get_cash_flow(symbol)

    def get_financial_metrics(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch financial metrics."""
        source = self._get_source(source)
        return source.get_financial_metrics(symbol)

    # ── Valuation ──────────────────────────────────────────────────

    def get_stock_valuation(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch stock valuation."""
        source = self._get_source(source)
        return source.get_stock_valuation(symbol)

    def get_index_valuation(
        self, index_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch index valuation."""
        source = self._get_source(source)
        return source.get_index_valuation(index_code)

    # ── Shareholder ────────────────────────────────────────────────

    def get_top_shareholders(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch top shareholders."""
        source = self._get_source(source)
        return source.get_top_shareholders(symbol)

    def get_institution_holdings(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch institution holdings."""
        source = self._get_source(source)
        return source.get_institution_holdings(symbol)

    def get_latest_holder_number(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch latest holder number."""
        source = self._get_source(source)
        return source.get_latest_holder_number(symbol)

    # ── Northbound ─────────────────────────────────────────────────

    # ── Northbound ─────────────────────────────────────────────────

    def get_northbound_holdings(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch northbound holdings."""
        return self.cn.stock.capital.northbound_holdings(
            symbol, start_date, end_date, source
        )

    # ── Dragon Tiger ───────────────────────────────────────────────

    def get_dragon_tiger_list(
        self, date: str, source: Optional[Union[str, List[str]]] = None, **kwargs
    ) -> pd.DataFrame:
        """Fetch dragon tiger list."""
        return self.cn.stock.capital.dragon_tiger(date, source)

    # ── Block Deal ─────────────────────────────────────────────────

    def get_block_deal(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch block deal.

        Note: akshare 源不支持 date/symbol/start_date/end_date 参数，
        只返回最新交易日数据。lixinger 源支持 symbol/start_date/end_date。
        """
        if symbol and start_date and end_date:
            return self.cn.stock.capital.block_deal(symbol, start_date, end_date, source)
        return self.cn.stock.capital.block_deal(source)

    # ── Margin ─────────────────────────────────────────────────────

    def get_margin_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch margin data."""
        return self.cn.stock.capital.margin(symbol, start_date, end_date, source)

    # ── Restricted Release ─────────────────────────────────────────

    def get_dividend_data(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None, **kwargs
    ) -> pd.DataFrame:
        """Fetch dividend data."""
        return self.cn.stock.event.dividend(symbol, source)

    def get_hk_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch HK stocks."""
        return self.hk.stock.quote.daily(None, source=source)

    def get_us_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch US stocks."""
        return self.us.stock.quote.daily(None, source=source)

    # ── IPO/New Stocks ─────────────────────────────────────────────

    def get_new_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch new stocks."""
        source = self._get_source(source)
        return source.get_new_stocks()

    def get_ipo_info(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch IPO info."""
        source = self._get_source(source)
        return source.get_ipo_info()

    # ========================================================================
    # Restricted Share Release (解禁数据)
    # ========================================================================

    def get_restricted_release_detail(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch restricted release detail (market-wide calendar)."""
        source = self._get_source(source)
        return source.get_restricted_release_detail(start_date, end_date)

    # ========================================================================
    # Shareholder Changes (股本变动/质押/冻结)
    # ========================================================================

    def get_shareholder_changes(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch shareholder holding changes."""
        source = self._get_source(source)
        return source.get_shareholder_changes(symbol)

    def get_insider_trading(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch insider (executive) trading changes."""
        source = self._get_source(source)
        return source.get_insider_trading(symbol)

    def get_equity_freeze(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch equity freeze/pledge info."""
        source = self._get_source(source)
        return source.get_equity_freeze(symbol)

    def get_capital_change(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch capital/share structure changes."""
        source = self._get_source(source)
        return source.get_capital_change(symbol)

    # ========================================================================
    # Earnings Forecast (业绩预告)
    # ========================================================================

    def get_earnings_forecast(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch earnings forecast/preview data."""
        source = self._get_source(source)
        return source.get_earnings_forecast(symbol)

    # ========================================================================
    # Open-end Fund Data (FOF基金)
    # ========================================================================

    def get_fund_open_daily(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch all open-end fund daily NAV list."""
        source = self._get_source(source)
        return source.get_fund_open_daily()

    def get_fund_open_nav(
        self,
        fund_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch open-end fund historical NAV."""
        source = self._get_source(source)
        return source.get_fund_open_nav(fund_code, start_date, end_date)

    def get_fund_open_info(
        self, fund_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """Fetch open-end fund basic info."""
        source = self._get_source(source)
        return source.get_fund_open_info(fund_code)

    # ========================================================================
    # Extended Corporate Events & Depth Data
    # ========================================================================

    def get_equity_pledge(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch equity pledge data."""
        sym = normalize_symbol(symbol) if symbol else None
        return self.cached_fetch(
            table="equity_pledge",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_equity_pledge(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def get_equity_pledge_rank(
        self,
        date: Optional[str] = None,
        top_n: int = 50,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch equity pledge ranking."""
        source = self._get_source(source)
        return source.get_equity_pledge_rank(date, top_n)

    def get_restricted_release(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch restricted release data."""
        sym = normalize_symbol(symbol) if symbol else None
        return self.cached_fetch(
            table="restricted_release",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_restricted_release(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def get_restricted_release_calendar(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch restricted release calendar."""
        source = self._get_source(source)
        return source.get_restricted_release_calendar(start_date, end_date)

    def get_goodwill_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch goodwill data."""
        sym = normalize_symbol(symbol) if symbol else None
        return self.cached_fetch(
            table="goodwill",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_goodwill_data(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def get_goodwill_impairment(
        self, date: Optional[str] = None, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch goodwill impairment."""
        source = self._get_source(source)
        return source.get_goodwill_impairment(date)

    def get_goodwill_by_industry(
        self, date: Optional[str] = None, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch goodwill by industry."""
        source = self._get_source(source)
        return source.get_goodwill_by_industry(date)

    def get_repurchase_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch repurchase data."""
        sym = normalize_symbol(symbol) if symbol else None
        return self.cached_fetch(
            table="repurchase",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_repurchase_data(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def get_esg_rating(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch ESG rating data."""
        sym = normalize_symbol(symbol) if symbol else None
        return self.cached_fetch(
            table="esg_rating",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_esg_rating(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    def get_esg_rank(
        self,
        date: Optional[str] = None,
        top_n: int = 50,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch ESG ranking."""
        source = self._get_source(source)
        return source.get_esg_rank(date, top_n)

    def get_performance_forecast(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch performance forecast."""
        source = self._get_source(source)
        return source.get_performance_forecast(symbol, start_date, end_date)

    def get_performance_express(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch performance express."""
        source = self._get_source(source)
        return source.get_performance_express(symbol, start_date, end_date)

    def get_analyst_rank(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch analyst rank."""
        source = self._get_source(source)
        return source.get_analyst_rank(start_date, end_date)

    def get_research_report(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch research report."""
        source = self._get_source(source)
        return source.get_research_report(symbol, start_date, end_date)

    def get_chip_distribution(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch chip distribution."""
        symbol = normalize_symbol(symbol)
        source = self._get_source(source)
        return source.get_chip_distribution(symbol)

    def get_stock_bonus(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch stock bonus."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="stock_bonus",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_stock_bonus(sym),
            symbol=sym,
        )

    def get_rights_issue(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch rights issue."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="rights_issue",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_rights_issue(sym),
            symbol=sym,
        )

    def get_dividend_by_date(
        self, date: Optional[str] = None, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch dividend by date."""
        source = self._get_source(source)
        return source.get_dividend_by_date(date)

    def get_management_info(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch management info."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="company_management",
            storage_layer="meta",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_management_info(sym),
            symbol=sym,
        )

    def get_name_history(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch name history."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="name_history",
            storage_layer="meta",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_name_history(sym),
            symbol=sym,
        )

    def get_shibor_rate(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch Shibor rate."""
        return self.cached_fetch(
            table="shibor_rate",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_shibor_rate(
                start_date, end_date
            ),
            start_date=start_date,
            end_date=end_date,
        )

    def get_social_financing(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch social financing."""
        return self.cached_fetch(
            table="social_financing",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_social_financing(
                start_date, end_date
            ),
            start_date=start_date,
            end_date=end_date,
        )

    def get_macro_gdp(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch macro GDP."""
        return self.cached_fetch(
            table="macro_gdp",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_macro_gdp(start_date, end_date),
            start_date=start_date,
            end_date=end_date,
        )

    def get_macro_exchange_rate(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch macro exchange rate."""
        return self.cached_fetch(
            table="macro_exchange_rate",
            storage_layer="daily",
            fetch_fn=lambda: self._get_source(source).get_macro_exchange_rate(
                start_date, end_date
            ),
            start_date=start_date,
            end_date=end_date,
        )

    def get_fof_list(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch FOF fund list."""
        return self.cached_fetch(
            table="fof_fund",
            storage_layer="meta",
            fetch_fn=lambda: self._get_source(source).get_fof_list(),
        )

    def get_fof_nav(
        self,
        fund_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch FOF NAV."""
        source = self._get_source(source)
        return source.get_fof_nav(fund_code, start_date=start_date, end_date=end_date)

    def get_lof_spot(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch LOF spot quotes."""
        return self.cached_fetch(
            table="lof_fund",
            storage_layer="meta",
            fetch_fn=lambda: self._get_source(source).get_lof_spot(),
        )

    def get_lof_nav(
        self, fund_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch LOF NAV."""
        source = self._get_source(source)
        return source.get_lof_nav(fund_code)

    def get_convert_bond_premium(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch convertible bond premium."""
        return self.cached_fetch(
            table="convert_bond_premium",
            storage_layer="snapshot",
            fetch_fn=lambda: self._get_source(source).get_convert_bond_premium(),
        )

    def get_convert_bond_spot(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch convertible bond spot."""
        source = self._get_source(source)
        return source.get_convert_bond_spot()

    def get_industry_performance(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "日k",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch industry performance."""
        return self.cached_fetch(
            table="industry_performance",
            storage_layer="snapshot",
            partition_by="symbol",
            partition_value=symbol,
            fetch_fn=lambda: self._get_source(source).get_industry_performance(
                symbol, start_date, end_date, period
            ),
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period=period,
        )

    def get_concept_performance(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "daily",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch concept performance."""
        return self.cached_fetch(
            table="concept_performance",
            storage_layer="snapshot",
            partition_by="symbol",
            partition_value=symbol,
            fetch_fn=lambda: self._get_source(source).get_concept_performance(
                symbol, start_date, end_date, period
            ),
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period=period,
        )

    def get_stock_industry(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch stock industry."""
        symbol = normalize_symbol(symbol)
        source = self._get_source(source)
        return source.get_stock_industry(symbol)

    def get_hot_rank(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch hot rank."""
        return self.cached_fetch(
            table="hot_rank",
            storage_layer="snapshot",
            fetch_fn=lambda: self._get_source(source).get_hot_rank(),
        )

    # ========================================================================
    # Convertible Bond (可转债)
    # ========================================================================

    def get_conversion_bond_list(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch convertible bond list."""
        source = self._get_source(source)
        return source.get_conversion_bond_list()

    def get_conversion_bond_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch convertible bond daily data."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="conversion_bond_daily",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_conversion_bond_daily(
                sym, start_date, end_date
            ),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    # ========================================================================
    # Option (期权)
    # ========================================================================

    def get_option_list(
        self,
        source: Optional[Union[str, List[str]]] = None,
        market: str = "sse",
    ) -> pd.DataFrame:
        """Fetch option list. market: 'sse' | 'szse' | 'cffex'"""
        source = self._get_source(source)
        return source.get_option_list(market=market)

    def get_option_daily(
        self,
        symbol: str,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch option daily data."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="option_daily",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_option_daily(sym),
            symbol=sym,
        )

    # ========================================================================
    # LOF Fund (LOF基金)
    # ========================================================================

    def get_lof_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch LOF fund daily data."""
        sym = normalize_symbol(symbol)
        return self.cached_fetch(
            table="lof_fund",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=sym,
            fetch_fn=lambda: self._get_source(source).get_lof_hist(sym),
            symbol=sym,
            start_date=start_date,
            end_date=end_date,
        )

    # ========================================================================
    # Futures (期货)
    # ========================================================================

    def get_futures_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch futures daily data."""
        return self.cached_fetch(
            table="futures_daily",
            storage_layer="daily",
            partition_by="symbol",
            partition_value=symbol,
            fetch_fn=lambda: self._get_source(source).get_futures_daily(
                symbol, start_date, end_date
            ),
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def get_futures_spot(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch futures spot quotes."""
        source = self._get_source(source)
        return source.get_futures_spot()

    # ========================================================================
    # Spot / Real-time Quotes (全市场实时行情)
    # ========================================================================

    def get_spot_em(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch all A-share real-time quotes from Eastmoney."""
        source = self._get_source(source)
        return source.get_spot_em()

    def get_stock_hist(
        self,
        symbol: str,
        period: str = "daily",
        start_date: str = "",
        end_date: str = "",
        adjust: str = "",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch stock historical data (wrapper for get_daily/get_minute)."""
        sym = normalize_symbol(symbol)
        if period == "daily":
            return self.get_daily(
                sym, start_date, end_date, adjust or "qfq", source
            )
        return self.cn.stock.quote.minute(sym, freq=period, start_date=start_date, end_date=end_date, source=source)

    # ========================================================================
    # Shenwan Industry (申万行业)
    # ========================================================================

    def get_sw_industry_list(
        self,
        level: str = "1",
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch Shenwan industry list. level: '1' | '2' | '3'"""
        source = self._get_source(source)
        return source.get_sw_industry(level=level)

    def get_sw_industry_daily(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch Shenwan industry index daily data."""
        return self.cached_fetch(
            table="sw_industry_daily",
            storage_layer="daily",
            partition_by="index_code",
            partition_value=index_code,
            fetch_fn=lambda: self._get_source(source).get_sw_index_daily(index_code),
            index_code=index_code,
            start_date=start_date,
            end_date=end_date,
        )

    # ========================================================================
    # Concept Board (概念板块)
    # ========================================================================

    def get_concept_list(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch concept board list."""
        source = self._get_source(source)
        return source.get_concept_list()

    def get_concept_stocks(
        self, concept_code: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch concept board constituent stocks."""
        return self.cached_fetch(
            table="concept_components",
            storage_layer="meta",
            partition_by="concept_code",
            partition_value=concept_code,
            fetch_fn=lambda: self._get_source(source).get_concept_components(concept_code),
            concept_code=concept_code,
        )

    def get_stock_concepts(
        self, symbol: str, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        """Fetch concepts that a stock belongs to."""
        sym = normalize_symbol(symbol)
        source = self._get_source(source)
        return source.get_stock_concepts(sym)

    # ========================================================================
    # Call Auction (集合竞价)
    # ========================================================================

    def get_call_auction(
        self,
        symbol: str,
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Fetch call auction data."""
        return self.cn.stock.quote.call_auction(symbol, date, source)


_default_service: Optional[DataService] = None


def get_service() -> DataService:
    """Get global DataService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = DataService()
    return _default_service
