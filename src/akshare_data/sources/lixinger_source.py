"""
Lixinger data source adapter.

Implements the DataSource interface using Lixinger OpenAPI.
"""
# ruff: noqa: F811

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Union


def _default_lookback_window(years: int = 10) -> tuple[str, str]:
    """Return (start_date, end_date) covering the last ``years`` years.

    Lixinger endpoints enforce a max span of 10 years; this helper replaces
    the previously hard-coded ``2000-01-01 → 2099-12-31`` span used by
    several adapter methods that kicked back 403 ForbiddenError responses.
    """
    today = date.today()
    start = today - timedelta(days=365 * years)
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

import pandas as pd
import requests

from akshare_data.core.base import DataSource
from akshare_data.core.errors import (
    DataSourceError,
    ErrorCode,
    SourceUnavailableError,
)
from akshare_data.core.symbols import format_stock_symbol
from akshare_data.core.tokens import get_token as _get_token
from akshare_data.sources.lixinger_client import LixingerClient, get_lixinger_client

logger = logging.getLogger(__name__)


class LixingerAdapter(DataSource):
    """Lixinger data source adapter."""

    name = "lixinger"
    source_type = "partial"

    def __init__(self, token: Optional[str] = None, **kwargs):
        self._token = token
        self._client: Optional[LixingerClient] = None

    @property
    def client(self) -> LixingerClient:
        if self._client is None:
            # Use injected token, or fall back to TokenManager resolution
            effective_token = self._token or _get_token("lixinger")
            self._client = get_lixinger_client(token=effective_token or None)
        return self._client

    def is_configured(self) -> bool:
        """Check if the Lixinger token is configured."""
        if self._client is not None:
            return self._client.is_configured()
        return bool(self._token or _get_token("lixinger"))

    def _ensure_configured(self):
        if not self.client.is_configured():
            raise SourceUnavailableError(
                "Lixinger token not configured. "
                "Set LIXINGER_TOKEN environment variable or create token.cfg file."
            )

    def _normalize_date(self, dt: Union[str, date, datetime]) -> str:
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d")
        elif isinstance(dt, date):
            return dt.strftime("%Y-%m-%d")
        return str(dt)

    def _format_index_code(self, index_code: str) -> str:
        return format_stock_symbol(index_code)

    def _format_stock_code(self, symbol: str) -> str:
        return format_stock_symbol(symbol)

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
        return df[[col for col in standard_cols if col in df.columns]]

    def get_daily_data(
        self,
        symbol: str,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date)
            end_str = self._normalize_date(end_date)
            df = self.client.get_company_candlestick(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
            )
            if df.empty:
                logger.warning(f"[Lixinger] No data for {symbol}")
                return pd.DataFrame()
            return self._normalize_daily_df(df, symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_daily_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_daily_data: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_daily_data: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_daily_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

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
            if not stocks and "stockCode" in df.columns:
                stocks = df["stockCode"].tolist()
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
                f"Lixinger API call failed for get_index_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_index_stocks: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_index_stocks: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
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
            elif "name" in df.columns:
                result["stock_name"] = df["name"]
            if "weight" in df.columns:
                result["weight"] = pd.to_numeric(df["weight"], errors="coerce")
            if "date" in df.columns:
                result["effective_date"] = pd.to_datetime(df["date"], errors="coerce")
            return result
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_components: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_index_components: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_index_components: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_components: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> List[str]:
        raise NotImplementedError(f"{self.name} 不支持 get_trading_days")

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
            elif security_type == "fund" or security_type == "etf":
                return self.client.get_fund_list()
            return pd.DataFrame()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_securities_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_securities_list: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_securities_list: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_securities_list: {e}",
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
                f"Lixinger API call failed for get_security_info: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_security_info: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(
                f"[Lixinger] Unexpected error in get_security_info for {symbol}: {e}"
            )
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_security_info: {e}",
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
        raise NotImplementedError(f"{self.name} 不支持 get_minute_data")

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_money_flow")

    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_north_money_flow")

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
                f"Lixinger API call failed for get_industry_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_industry_stocks: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_industry_stocks: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_industry_stocks: {e}",
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
                f"Lixinger API call failed for get_industry_mapping: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError) as e:
            raise SourceUnavailableError(
                f"Lixinger response data malformed for get_industry_mapping: {e}",
                error_code=ErrorCode.SOURCE_RESPONSE_EMPTY,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_industry_mapping: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_industry_mapping: {e}",
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
            df = self.client.get_stock_financial(
                symbol=symbol,
                metrics=metrics,
                start_date=start_str,
                end_date=end_str,
            )
            return df
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_finance_indicator: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_finance_indicator: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_finance_indicator: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_finance_indicator: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_call_auction(
        self, symbol: str, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_call_auction")

    def get_st_stocks(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_st_stocks")

    def get_suspended_stocks(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_suspended_stocks")

    def get_realtime_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_realtime_data")

    def get_etf_daily(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date)
            end_str = self._normalize_date(end_date)
            return self.client.get_fund_candlestick(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_etf_daily: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_etf_daily: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_etf_daily: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_etf_daily: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_index_daily(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
        **kwargs,
    ) -> pd.DataFrame:
        symbol = self._format_index_code(symbol)
        self._ensure_configured()
        try:
            start_str = self._normalize_date(start_date)
            end_str = self._normalize_date(end_date)
            df = self.client.get_index_candlestick(
                symbol=symbol,
                start_date=start_str,
                end_date=end_str,
            )
            return self._normalize_daily_df(df, symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_daily: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_index_daily: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_index_daily: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_daily: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_index_list(self) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_index_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_index_list: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_index_list: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_etf_list(self) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_fund_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_etf_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_etf_list: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_etf_list: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_etf_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_fund_manager_info(self, fund_code: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_fund_manager(symbol=fund_code)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fund_manager_info: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_fund_manager_info: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_fund_manager_info: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fund_manager_info: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_fund_net_value(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_fund_net_value(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fund_net_value: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_fund_net_value: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_fund_net_value: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fund_net_value: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_lof_list(self) -> pd.DataFrame:
        return self.get_etf_list()

    def get_futures_hist_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_futures_hist_data")

    def get_futures_realtime_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_futures_realtime_data")

    def get_futures_main_contracts(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_futures_main_contracts")

    def get_news_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_news_data")

    def get_block_deal(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_block_deal(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_block_deal: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_block_deal: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_block_deal: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_block_deal: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_block_deal_summary(
        self, start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_block_deal_summary")

    def get_margin_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_margin_trading(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_margin_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_margin_data: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_margin_data: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_margin_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_margin_summary(
        self, start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_margin_summary")

    def get_lpr_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_interest_rates(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_lpr_rate: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_lpr_rate: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_lpr_rate: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_lpr_rate: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_pmi_index(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_pmi(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_pmi_index: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_pmi_index: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_pmi_index: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_pmi_index: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_cpi_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_cpi(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_cpi_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_cpi_data: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_cpi_data: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_cpi_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_ppi_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_ppi(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_ppi_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_ppi_data: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_ppi_data: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_ppi_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_m2_supply(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_money_supply(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_m2_supply: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_m2_supply: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_m2_supply: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_m2_supply: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_options_chain(self, underlying_symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_options_chain")

    def get_options_realtime_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_options_realtime_data")

    def get_options_expirations(self, underlying_symbol: str) -> List[str]:
        raise NotImplementedError(f"{self.name} 不支持 get_options_expirations")

    def get_options_hist_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_options_hist_data")

    def get_option_greeks(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_option_greeks")

    def calculate_option_implied_vol(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_option_implied_vol")

    def black_scholes_price(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 black_scholes_price")

    def get_basic_info(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_profile(symbol=symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_basic_info: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_basic_info: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_basic_info: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_basic_info: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def _get_combined_financial_statements(self, symbol: str) -> pd.DataFrame:
        """Fetch combined financial statements from Lixinger API.

        The cn/company/fs/non_financial endpoint returns balance sheet, income
        statement, and cash flow data in a single response, distinguished by
        a report_type column.

        Lixinger enforces a 10-year maximum span per request, so we use a
        rolling 10-year window ending today. Callers that need older history
        should page their own window through multiple calls.
        """
        start_date, end_date = _default_lookback_window()
        return self.client.get_company_fs_non_financial(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def _filter_by_report_type(
        self, df: pd.DataFrame, report_type_values: list[str]
    ) -> pd.DataFrame:
        """Filter combined financial statements by report_type.

        Tries several possible column names used by the Lixinger API
        to identify the statement type.
        """
        if df.empty:
            return pd.DataFrame()

        # Possible column names for statement type in Lixinger responses
        type_col = None
        for candidate in ["type", "report_type", "stmt_type", "statement_type"]:
            if candidate in df.columns:
                type_col = candidate
                break

        if type_col is None:
            logger.warning(
                "[Lixinger] No report_type column found in financial statements. "
                "Cannot separate income statement / cash flow from balance sheet."
            )
            return pd.DataFrame()

        return df[df[type_col].isin(report_type_values)].reset_index(drop=True)

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self._get_combined_financial_statements(symbol)
            return self._filter_by_report_type(
                df, ["bs", "balance_sheet", "资产负债表"]
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_balance_sheet: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, pd.errors.EmptyDataError) as e:
            raise SourceUnavailableError(
                f"Lixinger financial data malformed for get_balance_sheet: {e}",
                error_code=ErrorCode.SOURCE_RESPONSE_EMPTY,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_balance_sheet: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_balance_sheet: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self._get_combined_financial_statements(symbol)
            return self._filter_by_report_type(df, ["is", "income_statement", "利润表"])
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_income_statement: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, pd.errors.EmptyDataError) as e:
            raise SourceUnavailableError(
                f"Lixinger financial data malformed for get_income_statement: {e}",
                error_code=ErrorCode.SOURCE_RESPONSE_EMPTY,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_income_statement: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_income_statement: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            df = self._get_combined_financial_statements(symbol)
            return self._filter_by_report_type(df, ["cf", "cash_flow", "现金流量表"])
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_cash_flow: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, pd.errors.EmptyDataError) as e:
            raise SourceUnavailableError(
                f"Lixinger financial data malformed for get_cash_flow: {e}",
                error_code=ErrorCode.SOURCE_RESPONSE_EMPTY,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_cash_flow: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_cash_flow: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_financial_metrics(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            metrics = [
                "pe_ttm.mcw",
                "pb.mcw",
                "ps_ttm.mcw",
                "dyr.mcw",
                "roe.avg",
                "roa.avg",
            ]
            return self.client.get_stock_financial(
                symbol=symbol,
                metrics=metrics,
                date="latest",
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_financial_metrics: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_financial_metrics: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_financial_metrics: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_financial_metrics: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_stock_valuation(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            metrics = ["pe_ttm.mcw", "pb.mcw", "ps_ttm.mcw", "dyr.mcw"]
            return self.client.get_stock_financial(
                symbol=symbol,
                metrics=metrics,
                date="latest",
            )
        except Exception as e:
            logger.error(f"[Lixinger] Failed to get stock valuation: {e}")
            raise DataSourceError(f"Failed to get stock valuation: {e}") from e

    def get_stock_pe_pb(self, symbol: str) -> pd.DataFrame:
        return self.get_stock_valuation(symbol)

    def get_index_valuation(self, index_code: str) -> pd.DataFrame:
        index_code = self._format_index_code(index_code)
        self._ensure_configured()
        try:
            metrics = ["pe_ttm.mcw", "pb.mcw"]
            return self.client.get_index_fundamental(
                symbols=[index_code],
                metrics=metrics,
            )
        except Exception as e:
            logger.error(f"[Lixinger] Failed to get index valuation: {e}")
            raise DataSourceError(f"Failed to get index valuation: {e}") from e

    def get_shareholder_changes(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_equity_change(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_shareholder_changes: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_shareholder_changes: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_shareholder_changes: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_shareholder_changes: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_top_shareholders(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_majority_shareholders(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_top_shareholders: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_top_shareholders: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_top_shareholders: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_top_shareholders: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_institution_holdings(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_fund_shareholders(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_institution_holdings: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_institution_holdings: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(
                f"[Lixinger] Unexpected error in get_institution_holdings: {e}"
            )
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_institution_holdings: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_top10_stock_holder_info(self, symbol: str) -> pd.DataFrame:
        return self.get_top_shareholders(symbol)

    def get_latest_holder_number(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_shareholders_num(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_latest_holder_number: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_latest_holder_number: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(
                f"[Lixinger] Unexpected error in get_latest_holder_number: {e}"
            )
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_latest_holder_number: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_performance_forecast(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_performance_forecast(symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_performance_forecast: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_performance_forecast: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(
                f"[Lixinger] Unexpected error in get_performance_forecast: {e}"
            )
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_performance_forecast: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_performance_express(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_performance_express(symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_performance_express: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_performance_express: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_performance_express: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_performance_express: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_analyst_rank(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_analyst_rank()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_analyst_rank: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_analyst_rank: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_analyst_rank: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_analyst_rank: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_research_report(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_research_report(symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_research_report: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_research_report: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_research_report: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_research_report: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_hot_rank(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_hot_rank")

    def get_stock_sentiment(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_stock_sentiment")

    def get_concept_list(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_concept_list")

    def get_concept_stocks(self, concept: str) -> List[str]:
        raise NotImplementedError(f"{self.name} 不支持 get_concept_stocks")

    def get_industry_list(self, source: str = "em") -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_industry_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_industry_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_industry_list: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_industry_list: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_industry_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_industry_performance(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_industry_performance")

    def get_concept_performance(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_concept_performance")

    def search_concept(self, keyword: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 search_concept")

    def get_all_concept_stocks(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_all_concept_stocks")

    def get_all_industries(self) -> pd.DataFrame:
        return self.get_industry_list()

    def filter_stocks_by_industry(self, industry: str) -> List[str]:
        raise NotImplementedError(f"{self.name} 不支持 filter_stocks_by_industry")

    def query_industry_sw(self, level: str = "1") -> pd.DataFrame:
        return self.get_industry_list()

    def get_hk_stocks(self) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_hk_company_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_hk_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_hk_stocks: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_hk_stocks: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_hk_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_us_stocks(self) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_us_index_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_us_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_us_stocks: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_us_stocks: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_us_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_new_stocks(self) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_company_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_new_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_new_stocks: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_new_stocks: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_new_stocks: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_ipo_info(self) -> pd.DataFrame:
        return self.get_new_stocks()

    def get_kcb_stocks(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_kcb_stocks")

    def get_cyb_stocks(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_cyb_stocks")

    def get_northbound_flow(
        self, start_date: str, end_date: str, market: str = "all"
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_northbound_flow")

    def get_northbound_holdings(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_northbound_holdings")

    def get_northbound_top_stocks(self, date: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_northbound_top_stocks")

    def get_north_stock_detail(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_north_stock_detail")

    def get_north_quota_info(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_north_quota_info")

    def get_north_calendar(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_north_calendar")

    def compute_north_money_signal(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 compute_north_money_signal")

    def get_stock_fund_flow(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_stock_fund_flow")

    def get_sector_fund_flow(self, sector_type: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_sector_fund_flow")

    def get_main_fund_flow_rank(self, date: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_main_fund_flow_rank")

    def get_dragon_tiger_list(self, date: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_dragon_tiger_list")

    def get_dragon_tiger_summary(
        self, start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_dragon_tiger_summary")

    def get_dragon_tiger_broker_stats(
        self, start_date: str, end_date: str, **kwargs
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_dragon_tiger_broker_stats")

    def get_limit_up_pool(self, date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_limit_up_pool")

    def get_limit_down_pool(self, date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_limit_down_pool")

    def get_limit_up_stats(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_limit_up_stats")

    def get_disclosure_news(self, symbol: str, **kwargs) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_announcement(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_disclosure_news: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_disclosure_news: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_disclosure_news: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_disclosure_news: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_dividend_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_dividend(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_dividend_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_dividend_data: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_dividend_data: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_dividend_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_repurchase_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_repurchase_data")

    def get_st_delist_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_st_delist_data")

    def get_shibor_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_interest_rates(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_shibor_rate: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_shibor_rate: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_shibor_rate: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_shibor_rate: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_social_financing(self, start_date: str, end_date: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_macro_social_financing(start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_social_financing: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_social_financing: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_social_financing: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_social_financing: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_deal_detail(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_deal_detail")

    def get_equity_pledge(self, symbol: str, **kwargs) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_pledge(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_equity_pledge: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_equity_pledge: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_equity_pledge: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_equity_pledge: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_equity_pledge_ratio_rank(self, date: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_equity_pledge_ratio_rank")

    def get_restricted_release(self, symbol: str, **kwargs) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_restricted_release(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_restricted_release: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_restricted_release: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_restricted_release: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_restricted_release: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_restricted_release_calendar(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_restricted_release_calendar")

    def get_goodwill_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_goodwill_data")

    def get_goodwill_impairment(self, date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_goodwill_impairment")

    def get_goodwill_by_industry(self, date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_goodwill_by_industry")

    def get_esg_rating(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_esg_rating")

    def get_esg_rating_rank(self, date: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_esg_rating_rank")

    def get_chip_distribution(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_chip_distribution")

    def get_broker_forecast(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_broker_forecast")

    def get_institutional_research(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_institutional_research")

    def get_call_auction_batch(self, symbols: List[str], date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_call_auction_batch")

    def calculate_implied_vol(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_implied_vol")

    def calculate_greeks(self, **kwargs) -> dict:
        raise NotImplementedError(f"{self.name} 不支持 calculate_greeks")

    def get_index_weights(self, index_code: str, date: str = "latest") -> pd.DataFrame:
        index_code = self._format_index_code(index_code)
        self._ensure_configured()
        try:
            return self.client.get_index_constituent_weightings(
                symbol=index_code,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_weights: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_index_weights: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_index_weights: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_weights: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_index_weights_history(
        self, index_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        index_code = self._format_index_code(index_code)
        self._ensure_configured()
        try:
            return self.client.get_index_constituent_weightings(
                symbol=index_code,
                start_date=start_date,
                end_date=end_date,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_weights_history: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_index_weights_history: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(
                f"[Lixinger] Unexpected error in get_index_weights_history: {e}"
            )
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_index_weights_history: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_index_info(self, index_code: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_index_info")

    def get_security_status(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_security_status")

    def get_name_history(self, symbol: str) -> pd.DataFrame:
        self._ensure_configured()
        try:
            symbol = self._format_stock_code(symbol)
            return self.client.get_company_profile(symbol)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_name_history: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_name_history: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_name_history: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_name_history: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_management_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_management_info")

    def get_employee_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_employee_info")

    def get_listing_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_listing_info")

    def get_industry_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_industry_info")

    def calculate_ex_rights_price(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_ex_rights_price")

    def calculate_adjust_price(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_adjust_price")

    def get_stock_bonus(self, symbol: str) -> pd.DataFrame:
        return self.get_dividend_data(symbol)

    def get_rights_issue(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_allotment(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_rights_issue: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_rights_issue: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_rights_issue: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_rights_issue: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_dividend_by_date(self, date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_dividend_by_date")

    def get_freeze_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_freeze_info")

    def get_capital_change(self, symbol: str) -> pd.DataFrame:
        return self.get_shareholder_changes(symbol)

    def get_topholder_change(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_major_shareholders_shares_change(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_topholder_change: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_topholder_change: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_topholder_change: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_topholder_change: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_major_holder_trade(self, symbol: str) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            return self.client.get_company_senior_executive_shares_change(
                symbol=symbol,

                start_date=_default_lookback_window()[0],

                end_date=_default_lookback_window()[1],
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_major_holder_trade: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_major_holder_trade: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_major_holder_trade: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_major_holder_trade: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_contract_multiplier(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_contract_multiplier")

    def get_margin_rate_for_contract(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_margin_rate_for_contract")

    def calculate_position_value(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_position_value")

    def calculate_required_margin(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_required_margin")

    def get_contract_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_contract_info")

    def get_margin_rate(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_margin_rate")

    def get_shareholder_structure(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_shareholder_structure")

    def get_shareholder_concentration(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_shareholder_concentration")

    def get_convert_bond_list(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_list")

    def get_convert_bond_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_info")

    def get_convert_bond_hist(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_hist")

    def get_convert_bond_spot(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_spot")

    def get_convert_bond_premium(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_premium")

    def get_convert_bond_by_stock(self, stock: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_by_stock")

    def get_convert_bond_quote(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_quote")

    def calculate_conversion_value(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_conversion_value")

    def calculate_premium_rate(self, **kwargs) -> float:
        raise NotImplementedError(f"{self.name} 不支持 calculate_premium_rate")

    def get_convert_bond_daily(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_convert_bond_daily")

    def get_lof_hist_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_lof_hist_data")

    def get_lof_spot(self, symbol: Optional[str] = None) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_fund_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_lof_spot: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_lof_spot: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_lof_spot: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_lof_spot: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_lof_nav(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_lof_nav")

    def get_fof_list(self) -> pd.DataFrame:
        self._ensure_configured()
        try:
            return self.client.get_fund_list()
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fof_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_fof_list: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_fof_list: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fof_list: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_fof_nav(self, symbol: str, **kwargs) -> pd.DataFrame:
        self._ensure_configured()
        try:
            start_date, end_date = _default_lookback_window()
            start_date = kwargs.get("start_date", start_date)
            end_date = kwargs.get("end_date", end_date)
            return self.client.get_fund_net_value(symbol, start_date, end_date)
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fof_nav: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_fof_nav: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_fof_nav: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_fof_nav: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_fof_info(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_fof_info")

    def get_bond_list(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_bond_list")

    def get_bond_hist_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_bond_hist_data")

    def get_bond_realtime_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_bond_realtime_data")

    def get_etf_hist_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        symbol = self._format_stock_code(symbol)
        self._ensure_configured()
        try:
            start_date, end_date = _default_lookback_window()
            start_date = kwargs.get("start_date", start_date)
            end_date = kwargs.get("end_date", end_date)
            return self.client.get_fund_candlestick(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
        except (requests.RequestException, ConnectionError, OSError) as e:
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_etf_hist_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e
        except (KeyError, IndexError, ValueError) as e:
            raise SourceUnavailableError(
                f"Lixinger auth token invalid or response malformed for get_etf_hist_data: {e}",
                error_code=ErrorCode.SOURCE_AUTH_FAILED,
                source="lixinger",
            ) from e
        except Exception as e:
            logger.error(f"[Lixinger] Unexpected error in get_etf_hist_data: {e}")
            raise SourceUnavailableError(
                f"Lixinger API call failed for get_etf_hist_data: {e}",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="lixinger",
            ) from e

    def get_etf_realtime_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_etf_realtime_data")

    def get_fund_rating_data(self) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_fund_rating_data")

    def get_inner_trade_data(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} 不支持 get_inner_trade_data")

    def health_check(self) -> Dict[str, Any]:
        try:
            import time

            self._ensure_configured()
            start = time.time()
            df = self.client.get_index_constituents(symbol="000300")
            latency = (time.time() - start) * 1000
            return {
                "status": "ok" if not df.empty else "error",
                "message": f"Lixinger API reachable, got {len(df)} rows",
                "latency_ms": round(latency, 2),
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "latency_ms": None,
            }

    def get_source_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.source_type,
            "description": "Lixinger financial data API",
            "requires_auth": True,
        }


__all__ = ["LixingerAdapter"]
