"""Normalizer for the ``market_quote_daily`` standard entity.

Standard fields (see docs/design/30-standard-entities.md):
    security_id, exchange, adjust_type, trade_date,
    open_price, high_price, low_price, close_price,
    volume, turnover_amount, change_pct, turnover_rate

System fields (injected by base):
    batch_id, source_name, interface_name, ingest_time,
    normalize_version, schema_version
"""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from akshare_data.standardized.normalizer.base import (
    NormalizerBase,
    load_field_mapping,
)
from akshare_data.core.symbols import format_stock_symbol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Temporary inline source mappings
#
# TODO: migrate to config/mappings/sources/market_quote_daily/<source>.yaml
# once the unified mapping config is merged (task 15).
# ---------------------------------------------------------------------------

_AKSHARE_EASTMONEY_MAP: Dict[str, str] = {
    "日期": "trade_date",
    "date": "trade_date",
    "开盘": "open_price",
    "open": "open_price",
    "最高": "high_price",
    "high": "high_price",
    "最低": "low_price",
    "low": "low_price",
    "收盘": "close_price",
    "close": "close_price",
    "成交量": "volume",
    "成交额": "turnover_amount",
    "amount": "turnover_amount",
    "涨跌幅": "change_pct",
    "换手率": "turnover_rate",
    "振幅": "amplitude",
    "涨跌额": "change",
    "代码": "security_id",
    "symbol": "security_id",
}

_AKSHARE_SINA_MAP: Dict[str, str] = {
    "date": "trade_date",
    "day": "trade_date",
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price",
    "volume": "volume",
    "amount": "turnover_amount",
    "symbol": "security_id",
    "code": "security_id",
}

_TUSHARE_MAP: Dict[str, str] = {
    "trade_date": "trade_date",
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price",
    "vol": "volume",
    "amount": "turnover_amount",
    "pct_chg": "change_pct",
    "turn": "turnover_rate",
    "ts_code": "security_id",
    "symbol": "security_id",
}

_BAOSTOCK_MAP: Dict[str, str] = {
    "date": "trade_date",
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price",
    "volume": "volume",
    "amount": "turnover_amount",
    "adjustflag": "adjust_type",
    "turn": "turnover_rate",
    "pctChg": "change_pct",
    "code": "security_id",
    "symbol": "security_id",
}

_LIXINGER_MAP: Dict[str, str] = {
    "date": "trade_date",
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price",
    "volume": "volume",
    "amount": "turnover_amount",
    "pct_change": "change_pct",
    "turnover_rate": "turnover_rate",
    "symbol": "security_id",
    "code": "security_id",
}

_SOURCE_MAPS: Dict[str, Dict[str, str]] = {
    "akshare": _AKSHARE_EASTMONEY_MAP,
    "akshare_eastmoney": _AKSHARE_EASTMONEY_MAP,
    "akshare_sina": _AKSHARE_SINA_MAP,
    "tushare": _TUSHARE_MAP,
    "baostock": _BAOSTOCK_MAP,
    "lixinger": _LIXINGER_MAP,
}

_ADJUST_TYPE_MAP: Dict[str, str] = {
    "qfq": "forward",
    "hfq": "backward",
    "": "none",
    "none": "none",
    "1": "forward",
    "2": "backward",
    "3": "none",
}

# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


class MarketQuoteDailyNormalizer(NormalizerBase):
    """Convert raw daily-quote DataFrames into the ``market_quote_daily`` entity."""

    dataset_name = "market_quote_daily"
    normalize_version = "v1"
    schema_version = "v1"

    _required_standard_fields = {
        "security_id",
        "exchange",
        "adjust_type",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "turnover_amount",
    }

    # ------------------------------------------------------------------
    # Hook implementations
    # ------------------------------------------------------------------

    def _field_mapping(self, source_name: str) -> Dict[str, str]:
        # 1. Try unified config loader first
        cfg_map = load_field_mapping(self.dataset_name, source_name)
        if cfg_map:
            return cfg_map
        # 2. Fallback to inline constants
        return _SOURCE_MAPS.get(source_name, _AKSHARE_EASTMONEY_MAP)

    def _coerce_types(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        numeric_cols = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover_amount",
            "change_pct",
            "turnover_rate",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

        return df

    def _derive_fields(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        df = self._normalize_security_id(df)
        df = self._derive_exchange(df)
        df = self._normalize_adjust_type(df, source_name)
        return df

    def _validate_record(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows missing mandatory keys (security_id or trade_date)."""
        before = len(df)
        mask = pd.Series(True, index=df.index)
        if "security_id" in df.columns:
            mask &= df["security_id"].notna()
        if "trade_date" in df.columns:
            mask &= df["trade_date"].notna()
        df = df.loc[mask].copy()
        dropped = before - len(df)
        if dropped:
            logger.debug(
                "%s: dropped %d rows with missing security_id or trade_date",
                self.dataset_name,
                dropped,
            )
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_security_id(df: pd.DataFrame) -> pd.DataFrame:
        """Convert various symbol formats to 6-digit security_id."""
        if "security_id" not in df.columns:
            return df

        df = df.copy()

        def _fmt(sid) -> str:
            if pd.isna(sid) or sid is None:
                return None
            return format_stock_symbol(str(sid))

        df["security_id"] = df["security_id"].apply(_fmt)
        return df

    @staticmethod
    def _derive_exchange(df: pd.DataFrame) -> pd.DataFrame:
        """Derive exchange from the first digit of security_id."""
        if "security_id" not in df.columns:
            df["exchange"] = None
            return df

        def _exchange(sid: str) -> str:
            if not sid or not sid[0].isdigit():
                return "UNKNOWN"
            first = sid[0]
            if first == "6":
                return "SSE"
            if first in ("0", "3"):
                return "SZSE"
            if first in ("4", "8"):
                return "BSE"
            return "UNKNOWN"

        df = df.copy()
        df["exchange"] = df["security_id"].apply(_exchange)
        return df

    @staticmethod
    def _normalize_adjust_type(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Ensure adjust_type uses standard values: forward / backward / none."""
        if "adjust_type" not in df.columns:
            df["adjust_type"] = "none"
            return df

        df = df.copy()
        df["adjust_type"] = (
            df["adjust_type"]
            .astype(str)
            .str.lower()
            .map(_ADJUST_TYPE_MAP)
            .fillna("none")
        )
        return df
