"""Tests for akshare_data.core.normalize module.

Covers:
- standardize_ohlcv() function
- normalize_stock_daily() function
- normalize_sina_daily() function
- normalize_tushare_daily() function
- normalize_baostock_daily() function
- normalize_etf_daily() function
- normalize_minute_data() function
- normalize_futures_daily() function
- normalize_option_daily() function
- normalize_dataframe_for_parquet() function
"""

import pytest
import pandas as pd
import numpy as np
from akshare_data.core.normalize import (
    standardize_ohlcv,
    normalize_stock_daily,
    normalize_sina_daily,
    normalize_tushare_daily,
    normalize_baostock_daily,
    normalize_etf_daily,
    normalize_minute_data,
    normalize_futures_daily,
    normalize_option_daily,
    normalize_dataframe_for_parquet,
)


class TestStandardizeOhlcv:
    """Test standardize_ohlcv() function."""

    def test_with_chinese_columns(self):
        """Should standardize Chinese column names."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01", "2023-01-02"],
                "开盘": [100.0, 101.0],
                "最高": [105.0, 106.0],
                "最低": [99.0, 100.0],
                "收盘": [102.0, 103.0],
                "成交量": [1000, 1100],
                "成交额": [100000.0, 110000.0],
            }
        )
        result = standardize_ohlcv(df)
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns

    def test_with_english_columns(self):
        """Should work with English column names."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-01-02"],
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [102.0, 103.0],
                "volume": [1000, 1100],
                "amount": [100000.0, 110000.0],
            }
        )
        result = standardize_ohlcv(df)
        assert "datetime" in result.columns

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        df = pd.DataFrame()
        result = standardize_ohlcv(df)
        assert result.empty

    def test_none_dataframe(self):
        """Should return empty DataFrame for None."""
        result = standardize_ohlcv(None)
        assert result.empty

    def test_converts_datetime_column(self):
        """Should convert datetime column to datetime type."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01", "2023-01-02"],
                "开盘": [100.0, 101.0],
                "最高": [105.0, 106.0],
                "最低": [99.0, 100.0],
                "收盘": [102.0, 103.0],
                "成交量": [1000, 1100],
            }
        )
        result = standardize_ohlcv(df)
        assert pd.api.types.is_datetime64_any_dtype(result["datetime"])

    def test_includes_amount_if_present(self):
        """Should include amount column when present."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01"],
                "开盘": [100.0],
                "最高": [105.0],
                "最低": [99.0],
                "收盘": [102.0],
                "成交量": [1000],
                "成交额": [100000.0],
            }
        )
        result = standardize_ohlcv(df)
        assert "amount" in result.columns


class TestNormalizeStockDaily:
    """Test normalize_stock_daily() function."""

    def test_with_chinese_columns(self):
        """Should standardize Chinese column names."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01"],
                "开盘": [100.0],
                "最高": [105.0],
                "最低": [99.0],
                "收盘": [102.0],
                "成交量": [1000],
                "成交额": [100000.0],
            }
        )
        result = normalize_stock_daily(df)
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "close" in result.columns

    def test_with_english_columns(self):
        """Should work with English column names."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
                "amount": [100000.0],
            }
        )
        result = normalize_stock_daily(df)
        assert "datetime" in result.columns

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        result = normalize_stock_daily(pd.DataFrame())
        assert result.empty

    def test_none_dataframe(self):
        """Should return empty DataFrame for None."""
        result = normalize_stock_daily(None)
        assert result.empty


class TestNormalizeSinaDaily:
    """Test normalize_sina_daily() function."""

    def test_standardizes_columns(self):
        """Should standardize Sina daily columns."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
                "amount": [100000.0],
            }
        )
        result = normalize_sina_daily(df)
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        result = normalize_sina_daily(pd.DataFrame())
        assert result.empty

    def test_none_dataframe(self):
        """Should return empty DataFrame for None."""
        result = normalize_sina_daily(None)
        assert result.empty


class TestNormalizeTushareDaily:
    """Test normalize_tushare_daily() function."""

    def test_standardizes_columns(self):
        """Should standardize Tushare daily columns."""
        df = pd.DataFrame(
            {
                "trade_date": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "vol": [1000],
                "amount": [100000.0],
            }
        )
        result = normalize_tushare_daily(df)
        assert "datetime" in result.columns
        assert "volume" in result.columns
        assert "vol" not in result.columns

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        result = normalize_tushare_daily(pd.DataFrame())
        assert result.empty


class TestNormalizeBaostockDaily:
    """Test normalize_baostock_daily() function."""

    def test_standardizes_columns(self):
        """Should standardize Baostock daily columns."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
                "amount": [100000.0],
            }
        )
        result = normalize_baostock_daily(df)
        assert "datetime" in result.columns
        assert "open" in result.columns

    def test_converts_to_numeric(self):
        """Should convert columns to numeric types."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "open": ["100.0"],
                "high": ["105.0"],
                "low": ["99.0"],
                "close": ["102.0"],
                "volume": ["1000"],
                "amount": ["100000.0"],
            }
        )
        result = normalize_baostock_daily(df)
        assert pd.api.types.is_numeric_dtype(result["open"])

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        result = normalize_baostock_daily(pd.DataFrame())
        assert result.empty


class TestNormalizeEtfDaily:
    """Test normalize_etf_daily() function."""

    def test_uses_stock_daily_logic(self):
        """Should use same logic as normalize_stock_daily."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01"],
                "开盘": [100.0],
                "最高": [105.0],
                "最低": [99.0],
                "收盘": [102.0],
                "成交量": [1000],
                "成交额": [100000.0],
            }
        )
        result = normalize_etf_daily(df)
        assert "datetime" in result.columns
        assert "open" in result.columns


class TestNormalizeMinuteData:
    """Test normalize_minute_data() function."""

    def test_standardizes_columns(self):
        """Should standardize minute data columns."""
        df = pd.DataFrame(
            {
                "时间": ["2023-01-01 09:30:00"],
                "开盘": [100.0],
                "最高": [105.0],
                "最低": [99.0],
                "收盘": [102.0],
                "成交量": [1000],
                "成交额": [100000.0],
            }
        )
        result = normalize_minute_data(df)
        assert "datetime" in result.columns
        assert "volume" in result.columns

    def test_maps_money_to_money(self):
        """Should map 成交额 to money."""
        df = pd.DataFrame(
            {
                "时间": ["2023-01-01 09:30:00"],
                "开盘": [100.0],
                "最高": [105.0],
                "最低": [99.0],
                "收盘": [102.0],
                "成交量": [1000],
                "成交额": [100000.0],
            }
        )
        result = normalize_minute_data(df)
        assert "money" in result.columns

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        result = normalize_minute_data(pd.DataFrame())
        assert result.empty


class TestNormalizeFuturesDaily:
    """Test normalize_futures_daily() function."""

    def test_standardizes_columns(self):
        """Should standardize futures daily columns."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
                "openinterest": [5000],
                "settle": [101.0],
            }
        )
        result = normalize_futures_daily(df)
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "openinterest" in result.columns

    def test_handles_chinese_date_column(self):
        """Should handle Chinese 日期 column."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01"],
                "开盘": [100.0],
                "最高": [105.0],
                "最低": [99.0],
                "收盘": [102.0],
                "成交量": [1000],
            }
        )
        result = normalize_futures_daily(df)
        assert "datetime" in result.columns

    def test_empty_dataframe(self):
        """Should return empty DataFrame."""
        result = normalize_futures_daily(pd.DataFrame())
        assert result.empty


class TestNormalizeOptionDaily:
    """Test normalize_option_daily() function."""

    def test_uses_futures_daily_logic(self):
        """Should use same logic as normalize_futures_daily."""
        df = pd.DataFrame(
            {
                "date": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
            }
        )
        result = normalize_option_daily(df)
        assert "datetime" in result.columns


class TestNormalizeDataframeForParquet:
    """Test normalize_dataframe_for_parquet() function."""

    def test_returns_empty_for_empty_dataframe(self):
        """Should return empty DataFrame unchanged."""
        df = pd.DataFrame()
        result = normalize_dataframe_for_parquet(df)
        assert result.empty

    def test_converts_mixed_object_column_to_str(self):
        """Should convert mixed-type object columns to string."""
        df = pd.DataFrame(
            {
                "col1": [1, "text", 3.0],
                "numeric": [1.0, 2.0, 3.0],
            }
        )
        result = normalize_dataframe_for_parquet(df)
        assert result["col1"].dtype == object or result["col1"].dtype == np.object_

    def test_converts_int_objects_to_int64(self):
        """Should convert object columns with only ints to int64."""
        df = pd.DataFrame(
            {
                "int_col": pd.Series([1, 2, 3], dtype=object),
            }
        )
        result = normalize_dataframe_for_parquet(df)
        assert result["int_col"].dtype in [np.int64, np.float64]

    def test_converts_float_objects_to_float64(self):
        """Should convert object columns with only floats to float64."""
        df = pd.DataFrame(
            {
                "float_col": pd.Series([1.0, 2.0, 3.0], dtype=object),
            }
        )
        result = normalize_dataframe_for_parquet(df)
        assert result["float_col"].dtype in [np.float64, np.int64]

    def test_keeps_numeric_columns_unchanged(self):
        """Should keep properly typed numeric columns unchanged."""
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.0, 2.0, 3.0],
            }
        )
        result = normalize_dataframe_for_parquet(df)
        assert (
            result["int_col"].dtype == np.int64 or result["int_col"].dtype == np.float64
        )
        assert result["float_col"].dtype == np.float64


class TestNormalizeFunctionsWithNone:
    """Test that all normalize functions handle None gracefully."""

    @pytest.mark.parametrize(
        "func",
        [
            standardize_ohlcv,
            normalize_stock_daily,
            normalize_sina_daily,
            normalize_tushare_daily,
            normalize_baostock_daily,
            normalize_etf_daily,
            normalize_minute_data,
            normalize_futures_daily,
            normalize_option_daily,
        ],
    )
    def test_handles_none(self, func):
        """Should handle None input gracefully."""
        result = func(None)
        assert result is None or result.empty
