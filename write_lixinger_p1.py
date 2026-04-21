#!/usr/bin/env python3
"""Script to generate the refactored lixinger_source.py file."""

content = '''\
"""
Lixinger data source adapter.

Implements the DataSource interface using Lixinger OpenAPI.
Refactored: domain-specific sub-adapters composed by LixingerAdapter.
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests

from akshare_data.core.base import DataSource
from akshare_data.core.errors import (
    DataSourceError,
    ErrorCode,
    SourceUnavailableError,
)
from akshare_data.core.symbols import format_stock_symbol, jq_code_to_ak
from akshare_data.core.tokens import get_token as _get_token
from akshare_data.sources.lixinger_client import LixingerClient, get_lixinger_client

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# Base adapter — shared infrastructure
# ──────────────────────────────────────────────────────────

class _LixingerBaseAdapter:
    """Shared infrastructure for all Lixinger sub-adapters."""

    def __init__(self, token: Optional[str] = None):
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

    def _ensure_configured(self):
        if not self.client.is_configured():
            raise SourceUnavailableError(
                "Lixinger token not configured. "
                "Set LIXINGER_TOKEN environment variable or create token.cfg file."
            )

    @staticmethod
    def _normalize_date(dt: Union[str, date, datetime]) -> str:
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d")
        elif isinstance(dt, date):
            return dt.strftime("%Y-%m-%d")
        return str(dt)

    @staticmethod
    def _format_index_code(index_code: str) -> str:
        return format_stock_symbol(index_code)

    @staticmethod
    def _format_stock_code(symbol: str) -> str:
        return format_stock_symbol(symbol)

    @staticmethod
    def _normalize_daily_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        df = df.copy()
        date_col = None
        for col in ["date", "\\u65e5\\u671f", "datetime"]:
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
            "\\u6536\\u76d8": "close",
            "\\u5f00\\u76d8": "open",
            "\\u6700\\u9ad8": "high",
            "\\u6700\\u4f4e": "low",
            "\\u6210\\u4ea4\\u91cf": "volume",
            "\\u6210\\u4ea4\\u989d": "amount",
        }
        df = df.rename(columns=rename_map)
        standard_cols = ["date", "open", "high", "low", "close", "volume", "amount"]
        for col in standard_cols:
            if col not in df.columns:
                df[col] = None
        return df[[col for col in standard_cols if col in df.columns]]

    @staticmethod
    def _get_combined_financial_statements(client, symbol: str) -> pd.DataFrame:
        return client.get_company_fs_non_financial(
            symbol=symbol,
            start_date="2000-01-01",
            end_date="2099-12-31",
        )

    @staticmethod
    def _filter_by_report_type(
        df: pd.DataFrame, report_type_values: list
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
                "[Lixinger] No report_type column found in financial statements. "
                "Cannot separate income statement / cash flow from balance sheet."
            )
            return pd.DataFrame()
        return df[df[type_col].isin(report_type_values)].reset_index(drop=True)
'''

with open('/Users/fengzhi/Downloads/git/akshare-data-service/src/akshare_data/sources/lixinger_source.py', 'w') as f:
    f.write(content)
print("Part 1 written OK")