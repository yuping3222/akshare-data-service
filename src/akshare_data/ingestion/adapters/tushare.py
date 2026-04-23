"""Tushare adapter for the ingestion layer.

Implements ``ingestion.base.DataSource`` using the Tushare Pro API.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from akshare_data.ingestion.base import DataSource
from akshare_data.core.errors import SourceUnavailableError, ErrorCode
from akshare_data.core.symbols import format_stock_symbol, ak_code_to_jq
from akshare_data.core.tokens import get_token as _get_token, set_token as _set_token

logger = logging.getLogger(__name__)

try:
    import tushare as ts
except ImportError:
    ts = None


def set_tushare_token(token: str) -> None:
    """Set Tushare API token (backward-compatible; delegates to TokenManager)."""
    _set_token("tushare", token)


class TushareAdapter(DataSource):
    """Tushare Pro data source adapter.

    Requires a Tushare API token for authentication.
    """

    name = "tushare"
    source_type = "real"

    def __init__(self, token: Optional[str] = None, **kwargs):
        self._token = token
        self._pro = None
        self._initialized = False

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        token = self._token or _get_token("tushare")
        if not token:
            self._initialized = True
            return
        try:
            ts_local = ts
            if ts_local is None:
                import tushare as _ts

                ts_local = _ts
            ts_local.set_token(token)
            self._pro = ts_local.pro_api()
            self._initialized = True
            logger.info("Tushare Pro API initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize Tushare Pro API: %s", e)
            self._pro = None
            self._initialized = True

    def is_configured(self) -> bool:
        if self._initialized:
            return self._pro is not None
        return bool(self._token or _get_token("tushare"))

    def _ensure_configured(self) -> None:
        self._ensure_initialized()
        if self._pro is None:
            raise SourceUnavailableError(
                "Tushare token not configured. Set TUSHARE_TOKEN environment variable."
            )

    def _to_ts_code(self, symbol: str) -> str:
        symbol = format_stock_symbol(symbol)
        if symbol.startswith("6") or symbol.startswith("9"):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"

    def _from_ts_code(self, ts_code: str) -> str:
        if ts_code.endswith(".SH"):
            return ts_code.replace(".SH", "")
        if ts_code.endswith(".SZ"):
            return ts_code.replace(".SZ", "")
        return ts_code

    def _normalize_date(self, dt: Union[str, date, datetime]) -> str:
        if isinstance(dt, datetime):
            return dt.strftime("%Y%m%d")
        if isinstance(dt, date):
            return dt.strftime("%Y%m%d")
        return str(dt).replace("-", "")

    def _normalize_daily_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        if "trade_date" in df.columns:
            df["datetime"] = pd.to_datetime(df["trade_date"], errors="coerce")
        elif "date" in df.columns:
            df["datetime"] = pd.to_datetime(df["date"], errors="coerce")
        rename_map = {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
            "amount": "amount",
        }
        df = df.rename(columns=rename_map)
        standard_cols = ["datetime", "open", "high", "low", "close", "volume", "amount"]
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
        self._ensure_configured()
        try:
            ts_code = self._to_ts_code(symbol)
            start_str = self._normalize_date(start_date) if start_date else None
            end_str = self._normalize_date(end_date) if end_date else None
            df = self._pro.daily(
                ts_code=ts_code, start_date=start_str, end_date=end_str
            )
            if df.empty:
                logger.warning("[Tushare] No data for %s", symbol)
                return pd.DataFrame()
            return self._normalize_daily_df(df)
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            index_code = format_stock_symbol(index_code)
            index_ts_code = f"{index_code}.SH"
            df = self._pro.index_weight(index_code=index_ts_code)
            if df.empty:
                return pd.DataFrame()
            result = pd.DataFrame()
            result["index_code"] = index_code
            if "con_code" in df.columns:
                result["code"] = (
                    df["con_code"].str.replace(r"\.\w+", "", regex=True).str.zfill(6)
                )
            elif "code" in df.columns:
                result["code"] = df["code"].astype(str).str.zfill(6)
            if "con_name" in df.columns:
                result["stock_name"] = df["con_name"]
            if include_weights and "weight" in df.columns:
                result["weight"] = pd.to_numeric(df["weight"], errors="coerce")
            if "in_date" in df.columns:
                result["effective_date"] = pd.to_datetime(
                    df["in_date"], errors="coerce"
                )
            return result
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> List[str]:
        self._ensure_configured()
        try:
            exchange = kwargs.get("exchange", "SSE")
            df = self._pro.trade_cal(
                exchange=exchange, start_date=start_date, end_date=end_date
            )
            if df.empty:
                return []
            if "cal_date" in df.columns:
                df["cal_date"] = pd.to_datetime(df["cal_date"], errors="coerce")
                is_open = df["is_open"] == 1 if "is_open" in df.columns else True
                return df[is_open]["cal_date"].dt.strftime("%Y-%m-%d").tolist()
            return []
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            if security_type == "stock":
                df = self._pro.stock_basic(list_status="L")
            elif security_type == "index":
                df = self._pro.index_basic()
            else:
                return pd.DataFrame()
            if df.empty:
                return pd.DataFrame()
            result = pd.DataFrame()
            if "ts_code" in df.columns:
                result["code"] = df["ts_code"].apply(self._from_ts_code)
            elif "code" in df.columns:
                result["code"] = df["code"].astype(str).str.zfill(6)
            if "name" in df.columns:
                result["display_name"] = df["name"]
            result["type"] = security_type
            if "list_date" in df.columns:
                result["start_date"] = df["list_date"]
            return result
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        self._ensure_configured()
        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.stock_basic(ts_code=ts_code)
            if df.empty:
                return {}
            row = df.iloc[0]
            return {
                "code": self._from_ts_code(row.get("ts_code", "")),
                "display_name": row.get("name", ""),
                "type": row.get("type", ""),
                "start_date": row.get("list_date", ""),
                "end_date": row.get("delist_date", ""),
                "industry": row.get("industry", ""),
            }
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_minute_data(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            df = self._pro.moneyflow_hsgt(trade_date=kwargs.get("trade_date"))
            if df.empty:
                return pd.DataFrame()
            return df
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date) if start_date else None
            end_str = self._normalize_date(end_date) if end_date else None
            df = self._pro.moneyflow_hkctl(start_date=start_str, end_date=end_str)
            if df.empty:
                return pd.DataFrame()
            return df
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_industry_stocks(
        self, industry_code: str, level: int = 1, **kwargs
    ) -> List[str]:
        return []

    def get_industry_mapping(self, symbol: str, level: int = 1, **kwargs) -> str:
        self._ensure_configured()
        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.stock_basic(ts_code=ts_code)
            if df.empty:
                return ""
            return df.iloc[0].get("industry", "")
        except Exception:
            return ""

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            ts_code = self._to_ts_code(symbol)
            start_str = self._normalize_date(start_date) if start_date else None
            end_str = self._normalize_date(end_date) if end_date else None
            df = self._pro.fina_indicator(
                ts_code=ts_code, start_date=start_str, end_date=end_str
            )
            if df.empty:
                return pd.DataFrame()
            if fields:
                available = [f for f in fields if f in df.columns]
                if available:
                    df = df[available]
            return df
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_index_stocks(self, index_code: str, **kwargs) -> List[str]:
        self._ensure_configured()
        try:
            index_code = format_stock_symbol(index_code)
            index_ts_code = f"{index_code}.SH"
            df = self._pro.index_weight(index_code=index_ts_code)
            if df.empty:
                return []
            stocks = []
            if "con_code" in df.columns:
                stocks = df["con_code"].tolist()
            elif "code" in df.columns:
                stocks = df["code"].tolist()
            result = []
            for s in stocks:
                s = str(s).replace(".SH", "").replace(".SZ", "")
                result.append(ak_code_to_jq(s))
            return result
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def health_check(self) -> Dict[str, Any]:
        try:
            import time

            self._ensure_configured()
            start = time.time()
            df = self._pro.daily(
                ts_code="000001.SZ", start_date="20260420", end_date="20260421"
            )
            latency = (time.time() - start) * 1000
            return {
                "status": "ok" if not df.empty else "error",
                "message": f"Tushare API reachable, got {len(df)} rows",
                "latency_ms": round(latency, 2),
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "latency_ms": None}

    def get_source_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.source_type,
            "description": "Tushare Pro financial data API",
            "requires_auth": True,
        }
