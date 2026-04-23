"""
Tushare data source adapter.

This module implements the DataSource interface using Tushare Pro API.
Tushare provides comprehensive financial data including:
- Daily price data
- Financial statements (income, balance sheet, cash flow)
- Financial indicators
- Money flow data
- Index data
- And much more
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from akshare_data.core.base import DataSource
from akshare_data.core.errors import SourceUnavailableError, ErrorCode
from akshare_data.core.symbols import format_stock_symbol, ak_code_to_jq
from akshare_data.core.tokens import get_token as _get_token, set_token as _set_token

logger = logging.getLogger(__name__)

try:
    import tushare as ts
except ImportError:
    ts = None


# Backward-compat alias — delegates to TokenManager
def set_tushare_token(token: str):
    """Set Tushare API Token. (Backward-compatible; delegates to TokenManager.)"""
    _set_token("tushare", token)


class TushareAdapter(DataSource):
    """Tushare Pro data source adapter.

    Implements DataSource interface using Tushare Pro API.
    Requires a Tushare API token for authentication.
    """

    name = "tushare"
    source_type = "real"

    def __init__(self, token: Optional[str] = None, **kwargs):
        """Initialize Tushare adapter.

        Args:
            token: Tushare API token. If not provided, tries to load from:
                   1. TUSHARE_TOKEN environment variable
                   2. token.cfg file
        """
        self._token = token
        self._pro = None
        self._initialized = False

    def _ensure_initialized(self):
        """Ensure Tushare API is initialized."""
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
            logger.error(f"Failed to initialize Tushare Pro API: {e}")
            self._pro = None
            self._initialized = True

    def is_configured(self) -> bool:
        """Check if the Tushare token is configured."""
        if self._initialized:
            return self._pro is not None
        return bool(self._token or _get_token("tushare"))

    def _ensure_configured(self):
        """Ensure client is configured with a token."""
        self._ensure_initialized()
        if self._pro is None:
            raise SourceUnavailableError(
                "Tushare token not configured. "
                "Set TUSHARE_TOKEN environment variable or call set_tushare_token()."
            )

    def _to_ts_code(self, symbol: str) -> str:
        """Convert symbol to Tushare format (e.g., '600519.SH')."""
        symbol = format_stock_symbol(symbol)
        if symbol.startswith("6") or symbol.startswith("9"):
            return f"{symbol}.SH"
        else:
            return f"{symbol}.SZ"

    def _from_ts_code(self, ts_code: str) -> str:
        """Convert Tushare code to standard format."""
        if ts_code.endswith(".SH"):
            return ts_code.replace(".SH", "")
        elif ts_code.endswith(".SZ"):
            return ts_code.replace(".SZ", "")
        return ts_code

    def _normalize_date(self, dt: Union[str, date, datetime]) -> str:
        """Convert datetime to YYYYMMDD format for Tushare."""
        if isinstance(dt, datetime):
            return dt.strftime("%Y%m%d")
        elif isinstance(dt, date):
            return dt.strftime("%Y%m%d")
        return str(dt).replace("-", "")

    def _normalize_daily_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize Tushare daily data to standard schema."""
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

        return df[[col for col in standard_cols if col in df.columns]]

    def get_daily_data(
        self,
        symbol: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        """Get daily data from Tushare.

        Args:
            symbol: Stock code
            start_date: Start date
            end_date: End date
            adjust: Adjustment type ('qfq', 'hfq', 'none')

        Returns:
            DataFrame with standard columns: datetime, open, high, low, close, volume
        """
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)
            start_str = self._normalize_date(start_date)
            end_str = self._normalize_date(end_date)

            df = self._pro.daily(
                ts_code=ts_code, start_date=start_str, end_date=end_str
            )

            if df.empty:
                logger.warning(f"[Tushare] No data for {symbol}")
                return pd.DataFrame()

            return self._normalize_daily_df(df)

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_daily_data for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get daily data for {symbol}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error in get_daily_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_index_stocks(self, index_code: str, **kwargs) -> List[str]:
        """Get index constituent stocks.

        Args:
            index_code: Index code (e.g., '000300.XSHG')

        Returns:
            List of stock codes in jq format (e.g., '600519.XSHG')
        """
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
            logger.error(
                f"[Tushare] Auth or parsing error in get_index_stocks for {index_code}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get index stocks for {index_code}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        """Get index constituent details with weights.

        Args:
            index_code: Index code
            include_weights: Whether to include weight information

        Returns:
            DataFrame with columns: index_code, code, stock_name, weight, effective_date
        """
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
            elif "name" in df.columns:
                result["stock_name"] = df["name"]

            if include_weights and "weight" in df.columns:
                result["weight"] = pd.to_numeric(df["weight"], errors="coerce")

            if "in_date" in df.columns:
                result["effective_date"] = pd.to_datetime(
                    df["in_date"], errors="coerce"
                )

            return result

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_index_components for {index_code}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(
                f"[Tushare] Failed to get index components for {index_code}: {e}"
            )
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
        """Get trading days from Tushare.

        Args:
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            List of trading dates in YYYY-MM-DD format
        """
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
            logger.error(f"[Tushare] Auth or parsing error in get_trading_days: {e}")
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get trading days: {e}")
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
        """Get securities list from Tushare.

        Args:
            security_type: Type of securities ('stock', 'index', 'etf', 'fund')
            date: Optional date

        Returns:
            DataFrame with columns: code, display_name, type, start_date
        """
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
            logger.error(f"[Tushare] Auth or parsing error in get_securities_list: {e}")
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get securities list: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """Get security info from Tushare."""
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
            logger.error(
                f"[Tushare] Auth or parsing error in get_security_info for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get security info for {symbol}: {e}")
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
        """Get minute data - Tushare does not support minute data for stocks."""
        return pd.DataFrame()

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Get money flow data from Tushare."""
        self._ensure_configured()

        try:
            self._to_ts_code(symbol)
            self._normalize_date(start_date) if start_date else None
            self._normalize_date(end_date) if end_date else None

            df = self._pro.moneyflow_hsgt(trade_date=kwargs.get("trade_date"))

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_money_flow for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get money flow for {symbol}: {e}")
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
        """Get north money flow (northbound) data from Tushare."""
        self._ensure_configured()

        try:
            start_str = self._normalize_date(start_date) if start_date else None
            end_str = self._normalize_date(end_date) if end_date else None

            df = self._pro.moneyflow_hkctl(start_date=start_str, end_date=end_str)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_north_money_flow: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get north money flow: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_industry_stocks(
        self, industry_code: str = "", level: int = 1, **kwargs
    ) -> List[str]:
        """Get industry stocks - use concept components instead."""
        return []

    def get_industry_mapping(self, symbol: str, level: int = 1, **kwargs) -> str:
        """Get industry mapping for a stock."""
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.stock_basic(ts_code=ts_code)

            if df.empty:
                return ""

            row = df.iloc[0]
            return row.get("industry", "")

        except Exception as e:
            logger.error(f"[Tushare] Failed to get industry mapping for {symbol}: {e}")
            return ""

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Get financial indicator data from Tushare."""
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
            logger.error(
                f"[Tushare] Auth or parsing error in get_finance_indicator for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get finance indicator for {symbol}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_call_auction(
        self, symbol: str, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        """Get call auction - not supported by Tushare."""
        return pd.DataFrame()

    def health_check(self) -> Dict[str, Any]:
        """Health check for Tushare API."""
        try:
            import time

            self._ensure_configured()

            start = time.time()
            # Use daily — available to all users with higher rate limit than stock_basic
            df = self._pro.daily(
                ts_code="000001.SZ",
                start_date="20260420",
                end_date="20260421",
            )
            latency = (time.time() - start) * 1000

            return {
                "status": "ok" if not df.empty else "error",
                "message": f"Tushare API reachable, got {len(df)} rows",
                "latency_ms": round(latency, 2),
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "latency_ms": None,
            }

    def get_source_info(self) -> Dict[str, Any]:
        """Get source information."""
        return {
            "name": self.name,
            "type": self.source_type,
            "description": "Tushare Pro financial data API",
            "requires_auth": True,
        }

    def get_stock_pe_pb(self, symbol: str) -> pd.DataFrame:
        """Get stock PE/PB data from Tushare."""
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.daily_basic(ts_code=ts_code)

            if df.empty:
                return pd.DataFrame()

            return (
                df[["trade_date", "close", "pe", "pb"]]
                if "trade_date" in df.columns
                else pd.DataFrame()
            )

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_stock_pe_pb for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get PE/PB for {symbol}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_financial_report(self, symbol: str, report_type: str) -> pd.DataFrame:
        """Get financial report from Tushare.

        Args:
            symbol: Stock code
            report_type: 'income' | 'balancesheet' | 'cashflow'

        Returns:
            DataFrame with financial report data
        """
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)

            api_map = {
                "income": "income",
                "balancesheet": "balancesheet",
                "cashflow": "cashflow",
            }

            api_name = api_map.get(report_type)
            if not api_name:
                return pd.DataFrame()

            df = getattr(self._pro, api_name)(ts_code=ts_code)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_financial_report for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get financial report for {symbol}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_dividend(self, symbol: str) -> pd.DataFrame:
        """Get dividend data from Tushare."""
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.dividend(ts_code=ts_code)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_dividend for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get dividend for {symbol}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_top10_holders(self, symbol: str) -> pd.DataFrame:
        """Get top 10 holders data from Tushare."""
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.top10_holders(ts_code=ts_code)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_top10_holders for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get top 10 holders for {symbol}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_top10_float_holders(self, symbol: str) -> pd.DataFrame:
        """Get top 10 float holders data from Tushare."""
        self._ensure_configured()

        try:
            ts_code = self._to_ts_code(symbol)
            df = self._pro.top10_floatholders(ts_code=ts_code)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_top10_float_holders for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(
                f"[Tushare] Failed to get top 10 float holders for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_margin_detail(self, market: str, date: str) -> pd.DataFrame:
        """Get margin detail data from Tushare."""
        self._ensure_configured()

        try:
            trade_date = date.replace("-", "")
            df = self._pro.margin_detail(trade_date=trade_date)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"[Tushare] Auth or parsing error in get_margin_detail: {e}")
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get margin detail: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_macro_raw(self, indicator: str) -> pd.DataFrame:
        """Get macro data from Tushare.

        Args:
            indicator: 'cpi' | 'ppi' | 'gdp' | 'pmi' | 'shibor' | 'lpr'

        Returns:
            DataFrame with macro data
        """
        self._ensure_configured()

        try:
            api_map = {
                "cpi": "cpi_monthly",
                "ppi": "ppi_monthly",
                "gdp": "gdp_monthly",
                "pmi": "pmi_monthly",
                "shibor": "shibor",
                "lpr": "lpr_data",
            }

            api_name = api_map.get(indicator)
            if not api_name:
                return pd.DataFrame()

            df = getattr(self._pro, api_name)()

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(
                f"[Tushare] Auth or parsing error in get_macro_raw for {indicator}: {e}"
            )
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get macro data for {indicator}: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e

    def get_billboard_list(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """Get dragon tiger list (billboard) data from Tushare."""
        self._ensure_configured()

        try:
            start_str = start_date.replace("-", "")
            end_str = end_date.replace("-", "") if end_date else None

            df = self._pro.top_list(start_date=start_str, end_date=end_str)

            if df.empty:
                return pd.DataFrame()

            return df

        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"[Tushare] Auth or parsing error in get_billboard_list: {e}")
            raise SourceUnavailableError(
                f"Tushare auth or parsing error: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
            ) from e
        except Exception as e:
            logger.error(f"[Tushare] Failed to get billboard list: {e}")
            raise SourceUnavailableError(
                f"Tushare unexpected error: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ) from e


__all__ = ["TushareAdapter", "set_tushare_token"]
