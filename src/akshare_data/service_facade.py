"""Read-only online service facade.

This facade only supports read/query/backfill delegation to
`akshare_data.service.data_service.DataService`.
Online API does not synchronously fetch from source adapters and does not write
cache data.
"""

from __future__ import annotations

import logging
import warnings
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from akshare_data.core.symbols import normalize_symbol
from akshare_data.legacy_adapter import _DeprecatedSourceAdapterHandle, SourceProxy
from akshare_data.namespace_assembly import (
    CNMarketAPI,
    HKMarketAPI,
    MacroAPI,
    USMarketAPI,
)
from akshare_data.service.data_service import (
    DataService as ServedDataService,
    QueryResult,
)
from akshare_data.service.missing_data_policy import MissingAction

logger = logging.getLogger("akshare_data")


class DataService:
    """Unified data service (Read-Only Served strategy).

    This facade delegates to the internal ServedDataService for all queries.
    It NEVER synchronously fetches from source adapters.

    Backward compatibility:
    - All existing method signatures are preserved
    - Namespace API (cn, hk, us, macro) still works
    - When Served has no data, returns empty DataFrame
    - Legacy source adapters accessible via ``self._legacy`` (deprecated)
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
        with warnings.catch_warnings():
            warnings.simplefilter("always", DeprecationWarning)
            self._legacy = _DeprecatedSourceAdapterHandle(
                router=router, access_logger=access_logger, source=source
            )

        self.cn = CNMarketAPI(self)
        self.hk = HKMarketAPI(self)
        self.us = USMarketAPI(self)
        self.macro = MacroAPI(self)

    # --- Legacy source adapter properties (deprecated) ---

    @property
    def akshare(self):
        """Deprecated: access legacy akshare adapter via self._legacy."""
        return self._legacy.akshare

    @property
    def lixinger(self):
        """Deprecated: access legacy lixinger adapter via self._legacy."""
        return self._legacy.lixinger

    @property
    def adapters(self):
        """Deprecated: access legacy adapters dict via self._legacy."""
        return self._legacy.adapters

    @property
    def router(self):
        """Deprecated: access legacy router via self._legacy."""
        return self._legacy.router

    @router.setter
    def router(self, value):
        self._legacy.router = value

    def _get_source(self, requested_source=None):
        """Deprecated: return a SourceProxy for backward-compatible source dispatch."""
        logger.warning(
            "_get_source called but source adapters are disabled in read-only mode. "
            "Use offline downloader to populate data first."
        )
        return SourceProxy(self, requested_source)

    def _execute_source_method(self, method_name, requested_source, *args, **kwargs):
        """Deprecated: delegate source method execution to the legacy handle."""
        return self._legacy._execute_source_method(
            method_name, requested_source, *args, **kwargs
        )

    def _resolve_sources(self, requested_source, method_name):
        """Deprecated: delegate source resolution to the legacy handle."""
        return self._legacy._resolve_sources(requested_source, method_name)

    @property
    def cache(self):
        """Backward-compatible access to underlying CacheManager."""
        return self._served._reader._cache

    @staticmethod
    def _result_to_df(result: QueryResult | None) -> pd.DataFrame:
        if result is None or result.data is None:
            return pd.DataFrame()
        return result.data.copy()

    @staticmethod
    def _ensure_alias_column(df: pd.DataFrame, target: str, *sources: str) -> None:
        if target in df.columns:
            return
        for source in sources:
            if source in df.columns:
                df[target] = df[source]
                return

    def _with_legacy_security_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        if df.empty:
            return df.copy()

        result = df.copy()
        self._ensure_alias_column(
            result, "code", "symbol", "stock_code", "fund_code", "bond_code"
        )
        self._ensure_alias_column(
            result,
            "display_name",
            "name",
            "stock_name",
            "fund_name",
            "bond_name",
        )
        self._ensure_alias_column(
            result, "name", "display_name", "stock_name", "fund_name", "bond_name"
        )
        self._ensure_alias_column(result, "start_date", "list_date", "nav_date")
        self._ensure_alias_column(result, "end_date", "delist_date")

        if "type" not in result.columns and "security_type" in result.columns:
            result["type"] = result["security_type"]
        if "security_type" not in result.columns and "type" in result.columns:
            result["security_type"] = result["type"]
        if "display_name" not in result.columns and "code" in result.columns:
            result["display_name"] = result["code"]

        return result

    def _with_index_component_aliases(self, df: pd.DataFrame) -> pd.DataFrame:
        result = self._with_legacy_security_columns(df)
        if result.empty:
            return result

        self._ensure_alias_column(result, "stock_name", "name", "display_name", "code")
        self._ensure_alias_column(result, "name", "stock_name", "display_name")
        if "display_name" not in result.columns and "stock_name" in result.columns:
            result["display_name"] = result["stock_name"]

        return result

    def _extract_codes(self, df: pd.DataFrame) -> List[str]:
        result = self._with_legacy_security_columns(df)
        if result.empty or "code" not in result.columns:
            return []
        return [str(value) for value in result["code"].dropna().tolist()]

    @staticmethod
    def _with_legacy_security_info(info: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(info)

        def set_alias(target: str, *sources: str) -> None:
            if target in result:
                return
            for source in sources:
                if source in result:
                    result[target] = result[source]
                    return

        set_alias("code", "symbol", "stock_code", "fund_code", "bond_code")
        set_alias("display_name", "name", "stock_name", "fund_name", "bond_name")
        set_alias("name", "display_name", "stock_name")
        set_alias("start_date", "list_date", "nav_date")
        set_alias("end_date", "delist_date")
        if "type" not in result and "security_type" in result:
            result["type"] = result["security_type"]

        return result

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
        """Read-only fetch from Served cache.

        `fetch_fn` is kept only for backward-compatible signatures and is ignored.
        Online API never performs synchronous source pull or cache writes.

        .. deprecated:: 0.3.0
            Use ``DataService.query()`` instead. Will be removed in 0.4.0.
        """
        warnings.warn(
            "cached_fetch() is deprecated since 0.3.0 and will be removed in 0.4.0. "
            "Use DataService.query() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        where = {}
        for k, v in params.items():
            if k in ("start_date", "end_date") and v:
                if "start_date" in params and "end_date" in params:
                    where[date_col] = (params.get("start_date"), params.get("end_date"))
                    break

        if fetch_fn is not None:
            logger.warning(
                "cached_fetch(fetch_fn=...) is ignored in online read-only API; "
                "use ingestion/offline jobs to populate data"
            )

        result = self._served.query(
            table=table,
            where=where or None,
            partition_by=partition_by,
            partition_value=partition_value,
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
        df = self.get_index_components(index_code, include_weights=False, source=source)
        return self._extract_codes(df)

    def get_index_components(
        self,
        index_code: str,
        include_weights: bool = True,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        code = normalize_symbol(index_code)
        result = self._served.query(
            table="index_components",
            where={"index_code": code},
        )
        df = self._with_index_component_aliases(self._result_to_df(result))
        if not include_weights and "weight" in df.columns:
            df = df.drop(columns=["weight"])
        return df

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[str] = None,
        source: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        result = self._served.query(table="securities")
        df = self._with_legacy_security_columns(self._result_to_df(result))

        filter_col = next(
            (col for col in ("security_type", "type") if col in df.columns),
            None,
        )
        if filter_col is not None:
            df = df[df[filter_col].astype(str) == str(security_type)].copy()
        elif security_type != "stock" and not df.empty:
            return pd.DataFrame(columns=df.columns)

        return df

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
        return self._extract_codes(self._result_to_df(result))

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
        return self._with_legacy_security_columns(self._result_to_df(result))

    def get_st_stocks(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="st_stocks")
        return self._with_legacy_security_columns(self._result_to_df(result))

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
            where={"symbol": sym},
        )
        if result.data is not None and not result.data.empty:
            return self._with_legacy_security_info(result.data.iloc[0].to_dict())
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
            where={"symbol": sym},
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
            where={"index_code": code},
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
        where["symbol"] = sym
        result = self._served.query(
            table="conversion_bond_daily",
            where=where or None,
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
            where={"symbol": sym},
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

    def get_futures_main_contracts(
        self, source: Optional[Union[str, List[str]]] = None
    ) -> pd.DataFrame:
        result = self._served.query(table="futures_main_contracts")
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
        _default_service = DataService()
    return _default_service
