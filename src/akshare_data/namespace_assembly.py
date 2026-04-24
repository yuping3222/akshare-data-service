"""Namespace assembly for read-only online API.

Online API is strictly read/query only. It does not synchronously pull from
source adapters and does not write cache data.
"""

from __future__ import annotations

import logging
from datetime import datetime
from types import SimpleNamespace
from typing import List, Optional, Union

import pandas as pd

from akshare_data.core.symbols import normalize_symbol

logger = logging.getLogger("akshare_data")

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
        where = {"symbol": sym}
        if date:
            where["date"] = date
        result = self.service._served.query(
            table="call_auction",
            where=where,
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
            where={"date": (start_date, end_date)},
            partition_by="symbol",
            partition_value=sym,
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
            where={"date": (start_date, end_date)},
            partition_by="symbol",
            partition_value=sym,
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


class MacroAPI:
    def __init__(self, service):
        self.service = service
        self.china = MacroChinaAPI(service)
