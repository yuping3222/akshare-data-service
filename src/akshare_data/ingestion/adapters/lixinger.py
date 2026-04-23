"""Lixinger adapter for the ingestion layer.

Implements ``ingestion.base.DataSource`` using the Lixinger OpenAPI.
Delegates HTTP calls to ``sources.lixinger_client.LixingerClient``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests

from akshare_data.ingestion.base import DataSource
from akshare_data.core.errors import (
    ErrorCode,
    SourceUnavailableError,
)
from akshare_data.core.symbols import format_stock_symbol
from akshare_data.core.tokens import get_token as _get_token
from akshare_data.sources.lixinger_client import LixingerClient, get_lixinger_client

logger = logging.getLogger(__name__)


class LixingerAdapter(DataSource):
    """Lixinger data source adapter.

    Uses ``LixingerClient`` for all HTTP calls.  Symbol formatting and
    response normalisation are handled internally.
    """

    name = "lixinger"
    source_type = "partial"

    def __init__(self, token: Optional[str] = None, **kwargs):
        self._token = token
        self._client: Optional[LixingerClient] = None

    @property
    def client(self) -> LixingerClient:
        if self._client is None:
            effective_token = self._token or _get_token("lixinger")
            self._client = get_lixinger_client(token=effective_token or None)
        return self._client

    def is_configured(self) -> bool:
        if self._client is not None:
            return self._client.is_configured()
        return bool(self._token or _get_token("lixinger"))

    def _ensure_configured(self) -> None:
        if not self.client.is_configured():
            raise SourceUnavailableError(
                "Lixinger token not configured. "
                "Set LIXINGER_TOKEN environment variable."
            )

    def _normalize_date(self, dt: Union[str, date, datetime]) -> str:
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d")
        if isinstance(dt, date):
            return dt.strftime("%Y-%m-%d")
        return str(dt)

    def _format_stock_code(self, symbol: str) -> str:
        return format_stock_symbol(symbol)

    def _format_index_code(self, index_code: str) -> str:
        return format_stock_symbol(index_code)

    def _normalize_daily_df(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        df = df.copy()
        date_col = None
        for col in ["date", "日期", "datetime"]:
            if col in df.columns:
                date_col = col
                break
        if date_col is None:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        rename_map = {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
            "收盘": "close",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
        df = df.rename(columns=rename_map)
        standard_cols = ["date", "open", "high", "low", "close", "volume", "amount"]
        for col in standard_cols:
            if col not in df.columns:
                df[col] = None
        return df[[c for c in standard_cols if c in df.columns]]

    # -- DataSource abstract methods ---------------------------------------

    def get_daily_data(
        self,
        symbol: str,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date) if start_date else "2000-01-01"
            end_str = self._normalize_date(end_date) if end_date else "2099-12-31"
            df = self.client.get_company_candlestick(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
            )
            if df.empty:
                logger.warning("[Lixinger] No data for %s", symbol)
                return pd.DataFrame()
            return self._normalize_daily_df(df, symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        index_code = self._format_index_code(index_code)
        self._ensure_configured()
        try:
            if include_weights:
                df = self.client.get_index_constituent_weightings(
                    symbol=index_code,
                    start_date=kwargs.get("start_date", "2020-01-01"),
                    end_date=kwargs.get(
                        "end_date", datetime.now().strftime("%Y-%m-%d")
                    ),
                )
            else:
                df = self.client.get_index_constituents(symbol=index_code)
            if not isinstance(df, pd.DataFrame) or df.empty:
                return pd.DataFrame()
            result = pd.DataFrame()
            result["index_code"] = index_code
            for col in ["stockCode", "code", "constituent"]:
                if col in df.columns:
                    result["code"] = df[col].astype(str).str.zfill(6)
                    break
            if "stockName" in df.columns:
                result["stock_name"] = df["stockName"]
            if "weight" in df.columns:
                result["weight"] = pd.to_numeric(df["weight"], errors="coerce")
            if "date" in df.columns:
                result["effective_date"] = pd.to_datetime(df["date"], errors="coerce")
            return result
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> List[str]:
        raise NotImplementedError(f"{self.name} does not support get_trading_days")

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            if security_type == "stock":
                return self.client.get_company_list()
            elif security_type == "index":
                return self.client.get_index_list()
            elif security_type in ("fund", "etf"):
                return self.client.get_fund_list()
            return pd.DataFrame()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self.client.get_company_profile(symbol=symbol)
            if df.empty:
                return {}
            return df.iloc[0].to_dict()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_minute_data(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_minute_data")

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_money_flow")

    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_north_money_flow")

    def get_industry_stocks(
        self, industry_code: str, level: int = 1, **kwargs
    ) -> List[str]:
        self._ensure_configured()
        try:
            df = self.client.get_industry_constituents(industry_code=industry_code)
            if df.empty:
                return []
            stocks = []
            for col in ["stockCode", "code"]:
                if col in df.columns:
                    stocks = df[col].tolist()
                    break
            return [
                f"{s.zfill(6)}.XSHG"
                if str(s).startswith(("6", "9"))
                else f"{s.zfill(6)}.XSHE"
                for s in stocks
                if pd.notna(s)
            ]
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_industry_mapping(self, symbol: str, level: int = 1, **kwargs) -> str:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self.client.get_company_industries(symbol=symbol)
            if df.empty:
                return ""
            for col in ["industryCode", "code", "industry"]:
                if col in df.columns:
                    return str(df[col].iloc[0])
            return ""
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    # -- Additional Lixinger-specific methods ------------------------------

    def get_index_stocks(self, index_code: str, **kwargs) -> List[str]:
        index_code = self._format_index_code(index_code)
        self._ensure_configured()
        try:
            df = self.client.get_index_constituents(symbol=index_code)
            if not isinstance(df, pd.DataFrame) or df.empty:
                return []
            stocks = []
            for col in ["constituents", "stockCode", "code", "成分股"]:
                if col in df.columns:
                    stocks = df[col].tolist()
                    break
            import numpy as np

            return [
                f"{s.zfill(6)}.XSHG"
                if str(s).startswith(("6", "9"))
                else f"{s.zfill(6)}.XSHE"
                for s in stocks
                if isinstance(s, (list, np.ndarray))
                and len(s) > 0
                or (not isinstance(s, (list, np.ndarray)) and pd.notna(s))
            ]
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            metrics = fields or ["pe_ttm.mcw", "pb.mcw", "ps_ttm.mcw", "dyr.mcw"]
            start_str = self._normalize_date(start_date) if start_date else None
            end_str = self._normalize_date(end_date) if end_date else None
            return self.client.get_stock_financial(
                symbol=symbol,
                metrics=metrics,
                start_date=start_str,
                end_date=end_str,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_etf_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date) if start_date else "2000-01-01"
            end_str = self._normalize_date(end_date) if end_date else "2099-12-31"
            return self.client.get_fund_candlestick(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_index_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_index_code(symbol)
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date) if start_date else "2000-01-01"
            end_str = self._normalize_date(end_date) if end_date else "2099-12-31"
            df = self.client.get_index_candlestick(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
            )
            return self._normalize_daily_df(df, symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self.client.get_company_fs_non_financial(
                symbol=symbol,
                start_date="2000-01-01",
                end_date="2099-12-31",
            )
            return self._filter_by_report_type(
                df, ["bs", "balance_sheet", "资产负债表"]
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self.client.get_company_fs_non_financial(
                symbol=symbol,
                start_date="2000-01-01",
                end_date="2099-12-31",
            )
            return self._filter_by_report_type(df, ["is", "income_statement", "利润表"])
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self.client.get_company_fs_non_financial(
                symbol=symbol,
                start_date="2000-01-01",
                end_date="2099-12-31",
            )
            return self._filter_by_report_type(df, ["cf", "cash_flow", "现金流量表"])
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def _filter_by_report_type(
        self, df: pd.DataFrame, report_type_values: list[str]
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        type_col = None
        for candidate in ["type", "report_type", "stmt_type", "statement_type"]:
            if candidate in df.columns:
                type_col = candidate
                break
        if type_col is None:
            logger.warning(
                "[Lixinger] No report_type column found in financial statements."
            )
            return pd.DataFrame()
        return df[df[type_col].isin(report_type_values)].reset_index(drop=True)

    def health_check(self) -> Dict[str, Any]:
        try:
            self._ensure_configured()
            return {"status": "ok", "message": "Lixinger API reachable"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_source_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.source_type,
            "description": "Lixinger OpenAPI financial data",
            "requires_auth": True,
        }
