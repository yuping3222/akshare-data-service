"""
Lixinger API client for making requests to Lixinger OpenAPI.

This module provides a singleton client for managing Lixinger API connections,
token management, and request handling.
"""

import json
import os
import threading
import time
from typing import Any, ClassVar

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from akshare_data.core.logging import get_logger
from akshare_data.core.tokens import get_token as _get_token, set_token as _set_token


class LixingerClient:
    """Lixinger API client with token management and retry mechanism."""

    _instance: ClassVar["LixingerClient | None"] = None
    _instance_lock: ClassVar[threading.Lock] = threading.Lock()
    _DEFAULT_BASE_URL = "https://open.lixinger.com/api/"
    # Public class attribute preserved for backward compat with tests and
    # callers that read ``LixingerClient.BASE_URL`` directly.
    BASE_URL: ClassVar[str] = _DEFAULT_BASE_URL

    @property
    def base_url(self) -> str:
        return os.environ.get("LIXINGER_BASE_URL", self._DEFAULT_BASE_URL)

    def __new__(cls, token: str | None = None) -> "LixingerClient":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    try:
                        resolved_token = token or cls._load_token()
                        object.__setattr__(instance, "_token", resolved_token)
                        object.__setattr__(instance, "_session", cls._create_session())
                        object.__setattr__(instance, "logger", get_logger(__name__))
                        object.__setattr__(instance, "_initialized", True)
                    except Exception:
                        object.__setattr__(instance, "_initialized", False)
                        raise
                    cls._instance = instance
        return cls._instance

    @classmethod
    def _load_token(cls) -> str:
        """Resolve the Lixinger token via TokenManager."""
        return _get_token("lixinger") or ""

    @classmethod
    def _create_session(cls) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            connect=3,
            read=3,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def query_api(
        self, api_suffix: str, params: dict[str, Any], timeout: int = 30
    ) -> dict[str, Any]:
        if not self._token:
            raise RuntimeError(
                "Lixinger token not configured. Please set LIXINGER_TOKEN or create token.cfg"
            )

        params["token"] = self._token
        headers = {"Content-Type": "application/json"}

        suffix = api_suffix.replace(".", "/")
        if suffix.startswith("/"):
            suffix = suffix[1:]
        url = self.base_url + suffix

        start_time = time.time()

        try:
            response = self._session.post(
                url=url, data=json.dumps(params), headers=headers, timeout=timeout
            )
            duration_ms = (time.time() - start_time) * 1000

            if not response.ok:
                try:
                    error_body = response.json()
                    error_msg = error_body.get("msg", response.text[:200])
                except Exception:
                    error_body = {}
                    error_msg = response.text[:200]
                self.logger.error(
                    f"API HTTP error: {api_suffix} status={response.status_code}",
                    extra={
                        "context": {
                            "log_type": "api_request",
                            "provider": "lixinger",
                            "api_suffix": api_suffix,
                            "duration_ms": round(duration_ms, 2),
                            "status": "error",
                            "http_status": response.status_code,
                            "error_msg": error_msg,
                        }
                    },
                )
                # Lixinger returns structured 400/403/404 payloads when the
                # request parameters are accepted but do not map to any data
                # (span limits, obsolete parameter names, deprecated
                # endpoints). Return an empty response in those cases so
                # callers receive an empty DataFrame instead of a raised
                # exception; genuine 5xx errors still raise. 401/403 auth
                # failures (distinguished by missing "error.name" payload
                # body) continue to raise so callers can detect mis-config.
                if response.status_code in (400, 403, 404):
                    err = error_body.get("error") or {}
                    err_name = err.get("name", "")
                    err_message = err.get("message", "")
                    if err_name in ("ValidationError", "ForbiddenError") or (
                        response.status_code == 404 and "not found" in err_message
                    ):
                        return {"code": 0, "data": []}
                raise RuntimeError(
                    f"API request failed with HTTP {response.status_code}: {error_msg}"
                )

            try:
                result = response.json()
            except Exception as e:
                self.logger.error(
                    f"API response parse error: {api_suffix}",
                    extra={
                        "context": {
                            "log_type": "api_request",
                            "provider": "lixinger",
                            "api_suffix": api_suffix,
                            "duration_ms": round(duration_ms, 2),
                            "status": "parse_error",
                            "error_msg": str(e),
                        }
                    },
                )
                raise RuntimeError(f"API response parse error: {e}")

            if result.get("code") == 1:
                self.logger.info(
                    f"API request successful: {api_suffix}",
                    extra={
                        "context": {
                            "log_type": "api_request",
                            "provider": "lixinger",
                            "api_suffix": api_suffix,
                            "duration_ms": round(duration_ms, 2),
                            "status": "success",
                        }
                    },
                )
            else:
                self.logger.warning(
                    f"API returned error: {api_suffix}",
                    extra={
                        "context": {
                            "log_type": "api_request",
                            "provider": "lixinger",
                            "api_suffix": api_suffix,
                            "duration_ms": round(duration_ms, 2),
                            "status": "error",
                            "error_msg": result.get("msg"),
                            "error_code": result.get("code"),
                        }
                    },
                )
            return result

        except requests.exceptions.Timeout:
            duration_ms = (time.time() - start_time) * 1000
            self.logger.error(
                f"API request timeout: {api_suffix}",
                extra={
                    "context": {
                        "log_type": "api_request",
                        "provider": "lixinger",
                        "api_suffix": api_suffix,
                        "duration_ms": round(duration_ms, 2),
                        "status": "timeout",
                    }
                },
            )
            raise RuntimeError(f"API request timeout after {timeout}s")

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.logger.error(
                f"API request failed: {api_suffix}",
                extra={
                    "context": {
                        "log_type": "api_request",
                        "provider": "lixinger",
                        "api_suffix": api_suffix,
                        "duration_ms": round(duration_ms, 2),
                        "status": "error",
                        "error_type": type(e).__name__,
                        "error_msg": str(e),
                    }
                },
                exc_info=True,
            )
            raise RuntimeError(f"API request failed: {e}")

    def _to_df(self, response: dict) -> pd.DataFrame:
        if response.get("code") != 1:
            return pd.DataFrame()
        data = response.get("data", [])
        if not data:
            return pd.DataFrame()
        return pd.json_normalize(data)

    # ==================== Index APIs ====================

    def get_index_list(self) -> pd.DataFrame:
        params = {}
        return self._to_df(self.query_api("cn/index", params))

    def get_index_candlestick(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        candlestick_type: str = "normal",
    ) -> pd.DataFrame:
        # cn/index/candlestick takes the singular stockCode parameter.
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
            "type": candlestick_type,
        }
        return self._to_df(self.query_api("cn/index/candlestick", params))

    def get_index_constituents(self, symbol: str, date: str = "latest") -> pd.DataFrame:
        params = {"stockCodes": [symbol], "date": date}
        return self._to_df(self.query_api("cn/index/constituents", params))

    def get_index_constituent_weightings(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {"stockCodes": [symbol], "startDate": start_date, "endDate": end_date}
        return self._to_df(self.query_api("cn/index/constituent-weightings", params))

    def get_index_fundamental(
        self, symbols: list[str], metrics: list[str], date: str | None = None
    ) -> pd.DataFrame:
        params = {"stockCodes": symbols, "metricsList": metrics}
        if date:
            params["date"] = date
        return self._to_df(self.query_api("cn/index/fundamental", params))

    def get_index_drawdown(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/index/drawdown", params))

    def get_index_fs_hybrid(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/index/fs/hybrid", params))

    def get_index_tracking_fund(self, symbol: str) -> pd.DataFrame:
        params = {"stockCodes": [symbol]}
        return self._to_df(self.query_api("cn/index/tracking-fund", params))

    def get_index_mutual_market(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/index/mutual-market", params))

    def get_index_margin_trading(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/index/margin-trading-and-securities-lending", params)
        )

    # ==================== Company APIs ====================

    def get_company_list(
        self,
        stock_codes: list[str] | None = None,
        industries: list[str] | None = None,
        provinces: list[str] | None = None,
    ) -> pd.DataFrame:
        params = {}
        if stock_codes:
            params["stockCodes"] = stock_codes
        if industries:
            params["industries"] = industries
        if provinces:
            params["provinces"] = provinces
        return self._to_df(self.query_api("cn/company", params))

    def get_company_profile(self, symbol: str) -> pd.DataFrame:
        params = {"stockCode": symbol}
        return self._to_df(self.query_api("cn/company/profile", params))

    def get_company_candlestick(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        candlestick_type: str = "normal",
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
            "type": candlestick_type,
        }
        return self._to_df(self.query_api("cn/company/candlestick", params))

    def get_company_dividend(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/dividend", params))

    def get_company_announcement(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/announcement", params))

    def get_company_allotment(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/allotment", params))

    def get_company_split(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/split", params))

    def get_company_indices(self, symbol: str) -> pd.DataFrame:
        params = {"stockCode": symbol}
        return self._to_df(self.query_api("cn/company/indices", params))

    def get_company_industries(self, symbol: str) -> pd.DataFrame:
        params = {"stockCode": symbol}
        return self._to_df(self.query_api("cn/company/industries", params))

    def get_company_equity_change(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/equity-change", params))

    def get_company_fundamental_financial(
        self, symbol: str, metrics: list[str], date: str | None = None
    ) -> pd.DataFrame:
        params = {"stockCodes": [symbol], "metricsList": metrics}
        if date:
            params["date"] = date
        return self._to_df(self.query_api("cn/company/fundamental/financial", params))

    def get_company_fundamental_non_financial(
        self, symbol: str, metrics: list[str], date: str | None = None
    ) -> pd.DataFrame:
        params = {"stockCodes": [symbol], "metricsList": metrics}
        if date:
            params["date"] = date
        return self._to_df(
            self.query_api("cn/company/fundamental/non_financial", params)
        )

    # Default metricsList used by fs/* endpoints when the caller does not
    # provide a specific metric set. Lixinger requires a non-empty list.
    _DEFAULT_FS_METRICS = [
        "bs.ta",  # total assets
        "bs.tl",  # total liabilities
        "bs.se",  # shareholders' equity
        "is.or",  # operating revenue
        "is.np",  # net profit
        "cf.nccfofa",  # net cash flow from operating activities
    ]

    def get_company_fs_non_financial(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        metrics: list[str] | None = None,
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
            "metricsList": metrics or self._DEFAULT_FS_METRICS,
        }
        return self._to_df(self.query_api("cn/company/fs/non_financial", params))

    def get_company_fs_hybrid(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        metrics: list[str] | None = None,
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
            "metricsList": metrics or self._DEFAULT_FS_METRICS,
        }
        return self._to_df(self.query_api("cn/company/fs/hybrid", params))

    def get_company_customers(self, symbol: str) -> pd.DataFrame:
        params = {"stockCode": symbol}
        return self._to_df(self.query_api("cn/company/customers", params))

    def get_company_suppliers(self, symbol: str) -> pd.DataFrame:
        params = {"stockCode": symbol}
        return self._to_df(self.query_api("cn/company/suppliers", params))

    def get_company_operating_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/operating-data", params))

    def get_company_revenue_constitution(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/company/operation-revenue-constitution", params)
        )

    def get_company_hot_tr_dri(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/hot/tr_dri", params))

    def get_company_inquiry(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/inquiry", params))

    def get_company_measures(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/measures", params))

    def get_company_mutual_market(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/mutual-market", params))

    # ==================== Shareholder APIs ====================

    def get_company_fund_shareholders(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/fund-shareholders", params))

    def get_company_fund_collection_shareholders(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/company/fund-collection-shareholders", params)
        )

    def get_company_majority_shareholders(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/majority-shareholders", params))

    def get_company_nolimit_shareholders(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/nolimit-shareholders", params))

    def get_company_shareholders_num(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/shareholders-num", params))

    def get_company_major_shareholders_shares_change(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/company/major-shareholders-shares-change", params)
        )

    def get_company_senior_executive_shares_change(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/company/senior-executive-shares-change", params)
        )

    # ==================== Trading APIs ====================

    def get_company_pledge(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/pledge", params))

    def get_company_restricted_release(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/restricted-release", params))

    def get_company_block_deal(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/block-deal", params))

    def get_company_trading_abnormal(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/company/trading-abnormal", params))

    def get_company_margin_trading(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/company/margin-trading-and-securities-lending", params)
        )

    # ==================== Industry APIs ====================

    def get_industry_list(self) -> pd.DataFrame:
        return self._to_df(self.query_api("cn/industry", {}))

    def get_industry_constituents(
        self, industry_code: str, source: str = "sw"
    ) -> pd.DataFrame:
        params = {}
        return self._to_df(self.query_api(f"cn/industry/constituents/{source}", params))

    def get_industry_fundamental(
        self, industry_code: str, metrics: list[str], source: str = "sw"
    ) -> pd.DataFrame:
        params = {"metricsList": metrics}
        return self._to_df(self.query_api(f"cn/industry/fundamental/{source}", params))

    def get_industry_fs_hybrid(
        self, industry_code: str, start_date: str, end_date: str, source: str = "sw"
    ) -> pd.DataFrame:
        params = {
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api(f"cn/industry/fs/{source}/hybrid", params))

    def get_industry_mutual_market(
        self, industry_code: str, start_date: str, end_date: str, source: str = "sw"
    ) -> pd.DataFrame:
        params = {
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api(f"cn/industry/mutual-market/{source}", params)
        )

    # ==================== Fund APIs ====================

    def get_fund_list(self) -> pd.DataFrame:
        return self._to_df(self.query_api("cn/fund", {}))

    def get_fund_candlestick(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/candlestick", params))

    def get_fund_net_value(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/net-value", params))

    def get_fund_total_net_value(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/total-net-value", params))

    def get_fund_shareholdings(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/shareholdings", params))

    def get_fund_dividend(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/dividend", params))

    def get_fund_split(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/split", params))

    def get_fund_profile(self, symbol: str) -> pd.DataFrame:
        params = {"stockCodes": [symbol]}
        return self._to_df(self.query_api("cn/fund/profile", params))

    def get_fund_manager(self, symbol: str) -> pd.DataFrame:
        # Lixinger API expects ``stockCodes`` (plural array).
        params = {"stockCodes": [symbol]}
        return self._to_df(self.query_api("cn/fund/manager", params))

    def get_fund_drawdown(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/drawdown", params))

    def get_fund_turnover_rate(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund/turnover-rate", params))

    def get_fund_exchange_traded_close_price(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(
            self.query_api("cn/fund/exchange-traded-close-price", params)
        )

    # ==================== Fund Manager APIs ====================

    def get_fund_manager_list(self) -> pd.DataFrame:
        return self._to_df(self.query_api("cn/fund-manager", {}))

    def get_fund_manager_management_funds(self, manager_code: str) -> pd.DataFrame:
        params = {"managerCode": manager_code}
        return self._to_df(self.query_api("cn/fund-manager/management-funds", params))

    def get_fund_manager_profit_ratio(
        self, manager_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "managerCode": manager_code,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund-manager/profit-ratio", params))

    def get_fund_manager_shareholdings(
        self, manager_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "managerCode": manager_code,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("cn/fund-manager/shareholdings", params))

    # ==================== Macro APIs ====================

    def get_macro_data(
        self, indicator: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api(f"macro/{indicator}", params))

    def get_macro_gdp(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("gdp", start_date, end_date)

    def get_macro_cpi(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("price-index", start_date, end_date)

    def get_macro_ppi(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("price-index", start_date, end_date)

    def get_macro_pmi(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("price-index", start_date, end_date)

    def get_macro_money_supply(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("money-supply", start_date, end_date)

    def get_macro_social_financing(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return self.get_macro_data("social-financing", start_date, end_date)

    def get_macro_interest_rates(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("interest-rates", start_date, end_date)

    def get_macro_exchange_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("currency-exchange-rate", start_date, end_date)

    def get_macro_foreign_trade(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("foreign-trade", start_date, end_date)

    def get_macro_gold_price(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("gold-price", start_date, end_date)

    def get_macro_crude_oil(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("crude-oil", start_date, end_date)

    def get_macro_national_debt(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("national-debt", start_date, end_date)

    def get_macro_real_estate(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("real-estate", start_date, end_date)

    def get_macro_rmb_deposits(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("rmb-deposits", start_date, end_date)

    def get_macro_rmb_loans(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("rmb-loans", start_date, end_date)

    def get_macro_usdx(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("usdx", start_date, end_date)

    def get_macro_rmbidx(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.get_macro_data("rmbidx", start_date, end_date)

    # ==================== HK APIs ====================

    def get_hk_company_list(self) -> pd.DataFrame:
        return self._to_df(self.query_api("hk/company", {}))

    def get_hk_company_profile(self, symbol: str) -> pd.DataFrame:
        params = {"stockCode": symbol}
        return self._to_df(self.query_api("hk/company/profile", params))

    def get_hk_company_candlestick(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("hk/company/candlestick", params))

    def get_hk_company_dividend(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCode": symbol,
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("hk/company/dividend", params))

    def get_hk_index_list(self) -> pd.DataFrame:
        return self._to_df(self.query_api("hk/index", {}))

    def get_hk_index_candlestick(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("hk/index/candlestick", params))

    def get_hk_index_constituents(self, symbol: str) -> pd.DataFrame:
        params = {"stockCodes": [symbol]}
        return self._to_df(self.query_api("hk/index/constituents", params))

    def get_hk_index_fundamental(
        self, symbols: list[str], metrics: list[str]
    ) -> pd.DataFrame:
        params = {"stockCodes": symbols, "metricsList": metrics}
        return self._to_df(self.query_api("hk/index/fundamental", params))

    # ==================== US APIs ====================

    def get_us_index_list(self) -> pd.DataFrame:
        return self._to_df(self.query_api("us/index", {}))

    def get_us_index_candlestick(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "stockCodes": [symbol],
            "startDate": start_date,
            "endDate": end_date,
        }
        return self._to_df(self.query_api("us/index/candlestick", params))

    def get_us_index_constituents(self, symbol: str) -> pd.DataFrame:
        params = {"stockCodes": [symbol]}
        return self._to_df(self.query_api("us/index/constituents", params))

    def get_us_index_fundamental(
        self, symbols: list[str], metrics: list[str]
    ) -> pd.DataFrame:
        params = {"stockCodes": symbols, "metricsList": metrics}
        return self._to_df(self.query_api("us/index/fundamental", params))

    # ==================== Stock Fundamental (alias) ====================

    def get_stock_financial(
        self,
        symbol: str,
        metrics: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        date: str | None = None,
    ) -> pd.DataFrame:
        params = {"stockCodes": [symbol], "metricsList": metrics}
        if date:
            params["date"] = date
        elif start_date:
            params["startDate"] = start_date
            if end_date:
                params["endDate"] = end_date
        return self._to_df(self.query_api("cn/stock/fundamental", params))

    @property
    def token(self) -> str:
        return self._token

    @property
    def session(self) -> requests.Session:
        return self._session

    def is_configured(self) -> bool:
        return bool(self._token)

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None


def get_lixinger_client(token: str | None = None) -> LixingerClient:
    """Get or create the LixingerClient singleton.

    Args:
        token: Optional token to use (only takes effect on first call).
    """
    return LixingerClient(token=token)


def set_lixinger_token(token: str) -> None:
    """Set Lixinger API token. (Backward-compatible; delegates to TokenManager.)"""
    _set_token("lixinger", token)
