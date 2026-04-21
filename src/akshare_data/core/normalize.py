"""
数据标准化模块 - 统一各数据源的 DataFrame 格式

合并来源:
- jk2bt akshare.py: _standardize_ohlcv
- jk2bt data_source_backup.py: _normalize_* 系列函数
- stock-bt akshare_wrapper_v2.py: _normalize_dataframe_for_parquet
"""

from typing import Optional

import pandas as pd

from akshare_data.core.fields import FIELD_MAPS, FLOAT_FIELDS, INT_FIELDS


# ============================================================================
# 通用标准化函数
# ============================================================================


def normalize(
    df: pd.DataFrame,
    source: str,
    select_cols: Optional[list[str]] = None,
    coerce_numeric: bool = False,
    coerce_fields: Optional[set[str]] = None,
) -> pd.DataFrame:
    """Generic DataFrame normalizer using FIELD_MAPS[source].

    Applies column renaming via FIELD_MAPS[source], converts the datetime
    column, optionally coerces numeric types, and selects output columns.

    Args:
        df: Input DataFrame
        source: Key in FIELD_MAPS for column mapping
        select_cols: Ordered list of columns to keep. If None, keeps all.
        coerce_numeric: Whether to coerce FLOAT_FIELDS/INT_FIELDS to numeric.
        coerce_fields: Override set of fields to coerce (default: FLOAT_FIELDS + INT_FIELDS).

    Returns:
        Normalized DataFrame
    """
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    result = df.copy()

    # Apply column mapping
    if source in FIELD_MAPS:
        for old_col, new_col in FIELD_MAPS[source].items():
            if old_col in result.columns:
                result[new_col] = result[old_col]

    # Convert datetime column
    if "datetime" in result.columns:
        result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")

    # Coerce numeric types
    if coerce_numeric:
        fields_to_coerce = (
            coerce_fields if coerce_fields is not None else (FLOAT_FIELDS | INT_FIELDS)
        )
        for col in fields_to_coerce:
            if col in result.columns:
                if col in INT_FIELDS:
                    result[col] = pd.to_numeric(result[col], errors="coerce")
                    result[col] = result[col].astype("Int64")
                else:
                    result[col] = pd.to_numeric(result[col], errors="coerce")

    # Select columns
    if select_cols is not None:
        available = [c for c in select_cols if c in result.columns]
        return result[available].copy()

    return result


# ============================================================================
# OHLCV 标准字段映射 (来自 akshare.py _standardize_ohlcv)
# ============================================================================


def standardize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化 OHLCV 字段 (来自 akshare.py _standardize_ohlcv)

    将各数据源的中文/英文字段统一为标准英文名:
    datetime, open, high, low, close, volume, amount
    """
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    return normalize(
        df,
        "ohlcv",
        select_cols=["datetime", "open", "high", "low", "close", "volume", "amount"],
    )


# ============================================================================
# 股票日线标准化 (来自 data_source_backup.py _normalize_stock_daily)
# ============================================================================


def normalize_stock_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化股票日线数据"""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    result = normalize(df, "eastmoney")

    select_cols = ["datetime", "open", "high", "low", "close", "volume"]
    if "amount" in result.columns:
        select_cols.append("amount")

    available = [c for c in select_cols if c in result.columns]
    return result[available].copy()


# ============================================================================
# 新浪日线标准化 (来自 data_source_backup.py _normalize_sina_daily)
# ============================================================================


def normalize_sina_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化新浪日线数据"""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    result = normalize(df, "sina")

    select_cols = ["datetime", "open", "high", "low", "close", "volume", "amount"]
    available = [c for c in select_cols if c in result.columns]
    return result[available].copy()


# ============================================================================
# Tushare 日线标准化 (来自 data_source_backup.py _normalize_tushare_daily)
# ============================================================================


def normalize_tushare_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化 Tushare 日线数据"""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    result = normalize(df, "tushare")

    select_cols = ["datetime", "open", "high", "low", "close", "volume", "amount"]
    available = [c for c in select_cols if c in result.columns]
    return result[available].copy()


# ============================================================================
# Baostock 日线标准化 (来自 data_source_backup.py _normalize_baostock_daily)
# ============================================================================


def normalize_baostock_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化 Baostock 日线数据"""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    return normalize(
        df,
        "baostock",
        select_cols=["datetime", "open", "high", "low", "close", "volume", "amount"],
        coerce_numeric=True,
    )


# ============================================================================
# ETF 日线标准化 (来自 data_source_backup.py _normalize_etf_daily)
# ============================================================================


def normalize_etf_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化 ETF 日线数据"""
    return normalize_stock_daily(df)


# ============================================================================
# 分钟数据标准化 (来自 data_source_backup.py _normalize_minute_data)
# ============================================================================


def normalize_minute_data(df: pd.DataFrame) -> pd.DataFrame:
    """标准化分钟数据"""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    result = normalize(df, "minute")

    select_cols = ["datetime", "open", "high", "low", "close", "volume", "money"]
    available = [c for c in select_cols if c in result.columns]
    return result[available].copy()


# ============================================================================
# 期货日线标准化 (来自 data_source_backup.py _normalize_futures_daily)
# ============================================================================


def normalize_futures_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化期货日线数据"""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return pd.DataFrame()

    df = df.copy()

    # Special date handling: check for date/日期 before generic normalize
    if "date" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"])
    elif "日期" in df.columns:
        df["datetime"] = pd.to_datetime(df["日期"])

    col_map = {
        "open": "open",
        "开盘": "open",
        "high": "high",
        "最高": "high",
        "low": "low",
        "最低": "low",
        "close": "close",
        "收盘": "close",
        "volume": "volume",
        "成交量": "volume",
        "openinterest": "openinterest",
        "持仓量": "openinterest",
        "settle": "settle",
        "结算价": "settle",
    }

    for old_col, new_col in col_map.items():
        if old_col in df.columns and new_col not in df.columns:
            df[new_col] = df[old_col]

    default_cols = [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "openinterest",
        "settle",
    ]
    available = [c for c in default_cols if c in df.columns]
    return df[available].copy()


# ============================================================================
# 期权日线标准化 (来自 data_source_backup.py _normalize_option_daily)
# ============================================================================


def normalize_option_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化期权日线数据"""
    return normalize_futures_daily(df)


# ============================================================================
# Parquet 兼容标准化 (来自 akshare_wrapper_v2.py _normalize_dataframe_for_parquet)
# ============================================================================


def normalize_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    规范化 DataFrame 以确保兼容 Parquet 格式

    处理混合类型列（特别是 object 类型列包含多种数据类型的情况）
    """
    if df.empty:
        return df

    df_normalized = df.copy()

    for col in df_normalized.columns:
        if df_normalized[col].dtype == "object":
            unique_types = set(type(x).__name__ for x in df_normalized[col].dropna())
            if len(unique_types) > 1:
                df_normalized[col] = df_normalized[col].astype(str)
            elif unique_types == {"int"}:
                try:
                    df_normalized[col] = df_normalized[col].astype("int64")
                except (ValueError, TypeError):
                    df_normalized[col] = df_normalized[col].astype(str)
            elif unique_types == {"float"}:
                try:
                    df_normalized[col] = df_normalized[col].astype("float64")
                except (ValueError, TypeError):
                    df_normalized[col] = df_normalized[col].astype(str)
            elif unique_types == {"str"}:
                df_normalized[col] = df_normalized[col].astype(str)

    return df_normalized
