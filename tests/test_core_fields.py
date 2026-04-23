"""Tests for akshare_data.core.fields module.

Covers:
- Field type constants (FLOAT_FIELDS, INT_FIELDS, STR_FIELDS, DATE_FIELDS)
- get_field_type() function
- validate_field_types() function
- CN_TO_EN mapping
- FIELD_MAPS for different sources
- Symbol width constant
- Code-to-name mapping functions
- Column standardization functions
"""

import pandas as pd
from akshare_data.core.fields import (
    FLOAT_FIELDS,
    INT_FIELDS,
    STR_FIELDS,
    DATE_FIELDS,
    get_field_type,
    validate_field_types,
    CN_TO_EN,
    FIELD_MAPS,
    SYMBOL_ZFILL_WIDTH,
    get_name_by_code,
    get_stock_name,
    get_index_name,
    get_etf_name,
    get_industry_name,
    get_option_name,
    get_all_codes,
    get_all_names,
    search_by_name,
    get_option_underlying_patterns,
    preload_mappings,
    standardize_columns,
    standardize_columns_generic,
    select_ohlcv_columns,
)


class TestFieldTypeConstants:
    """Test field type constant sets."""

    def test_float_fields_contain_expected(self):
        """FLOAT_FIELDS should contain OHLCV fields."""
        expected = {"open", "high", "low", "close", "volume", "amount"}
        assert expected.issubset(FLOAT_FIELDS)

    def test_int_fields_contain_expected(self):
        """INT_FIELDS should contain symbol."""
        assert "symbol" in INT_FIELDS

    def test_str_fields_contain_expected(self):
        """STR_FIELDS should contain name and date fields."""
        assert "name" in STR_FIELDS
        assert "datetime" in STR_FIELDS
        assert "date" in STR_FIELDS

    def test_date_fields_contain_expected(self):
        """DATE_FIELDS should contain datetime and date."""
        assert "datetime" in DATE_FIELDS
        assert "date" in DATE_FIELDS
        assert "trade_date" in DATE_FIELDS


class TestGetFieldType:
    """Test get_field_type() function."""

    def test_float_field_types(self):
        """Known float fields should return 'float'."""
        assert get_field_type("open") == "float"
        assert get_field_type("high") == "float"
        assert get_field_type("close") == "float"
        assert get_field_type("volume") == "float"

    def test_int_field_types(self):
        """Known int fields should return 'int'."""
        assert get_field_type("symbol") == "int"

    def test_str_field_types(self):
        """Known str fields should return 'str'."""
        assert get_field_type("name") == "str"
        assert get_field_type("stock_name") == "str"

    def test_date_field_types(self):
        """Date fields should return 'date'."""
        assert get_field_type("datetime") == "date"
        assert get_field_type("date") == "date"
        assert get_field_type("trade_date") == "date"

    def test_case_insensitive(self):
        """Field type lookup should be case insensitive."""
        assert get_field_type("OPEN") == "float"
        assert get_field_type("Close") == "float"

    def test_unknown_field_returns_str(self):
        """Unknown fields should default to 'str'."""
        assert get_field_type("unknown_field") == "str"
        assert get_field_type("random") == "str"


class TestValidateFieldTypes:
    """Test validate_field_types() function."""

    def test_valid_dataframe(self):
        """DataFrame with correct types should be valid."""
        df = pd.DataFrame(
            {
                "open": [1.0, 2.0],
                "high": [3.0, 4.0],
                "low": [1.5, 2.5],
                "close": [2.0, 3.0],
                "volume": [100, 200],
            }
        )
        is_valid, errors = validate_field_types(df)
        assert is_valid is True
        assert len(errors) == 0

    def test_invalid_float_field(self):
        """DataFrame with wrong float type should return errors."""
        df = pd.DataFrame(
            {
                "open": ["a", "b"],
                "high": [3.0, 4.0],
                "low": [1.5, 2.5],
                "close": [2.0, 3.0],
                "volume": [100, 200],
            }
        )
        is_valid, errors = validate_field_types(df)
        assert is_valid is False
        assert len(errors) > 0

    def test_empty_dataframe(self):
        """Empty DataFrame should be valid."""
        df = pd.DataFrame()
        is_valid, errors = validate_field_types(df)
        assert is_valid is True


class TestCNToENMapping:
    """Test Chinese to English field mapping."""

    def test_mapping_contains_common_fields(self):
        """CN_TO_EN should contain common Chinese field mappings."""
        assert "日期" in CN_TO_EN
        assert CN_TO_EN["日期"] == "datetime"
        assert "开盘" in CN_TO_EN
        assert CN_TO_EN["开盘"] == "open"
        assert "收盘" in CN_TO_EN
        assert CN_TO_EN["收盘"] == "close"

    def test_multiple_codes_map_to_same_field(self):
        """Multiple codes can map to the same field."""
        assert CN_TO_EN["最高价"] == "high"
        assert CN_TO_EN["最高"] == "high"


class TestFieldMaps:
    """Test FIELD_MAPS for different sources."""

    def test_eastmoney_mapping(self):
        """Eastmoney mapping should have expected fields."""
        em_map = FIELD_MAPS["eastmoney"]
        assert "日期" in em_map
        assert "开盘" in em_map
        assert "收盘" in em_map

    def test_sina_mapping(self):
        """Sina mapping should have expected fields."""
        sina_map = FIELD_MAPS["sina"]
        assert "date" in sina_map
        assert "open" in sina_map
        assert "volume" in sina_map

    def test_tushare_mapping(self):
        """Tushare mapping should have expected fields."""
        ts_map = FIELD_MAPS["tushare"]
        assert "trade_date" in ts_map
        assert "ts_code" in ts_map
        assert "vol" in ts_map

    def test_baostock_mapping(self):
        """Baostock mapping should have expected fields."""
        bs_map = FIELD_MAPS["baostock"]
        assert "date" in bs_map
        assert "code" in bs_map
        assert "preclose" in bs_map

    def test_all_sources_have_required_fields(self):
        """Time-series source maps should map to datetime/date fields."""
        # Non-time-series sources (options_chain, options_realtime) don't require datetime
        time_series_sources = {
            "eastmoney",
            "sina",
            "tushare",
            "baostock",
            "ohlcv",
            "options_hist",
            "minute",
            "futures",
            "realtime",
        }
        for source, mapping in FIELD_MAPS.items():
            if source in time_series_sources:
                assert "datetime" in mapping.values() or "date" in mapping.values()


class TestSymbolZfillWidth:
    """Test SYMBOL_ZFILL_WIDTH constant."""

    def test_value_is_six(self):
        """Symbol zfill width should be 6."""
        assert SYMBOL_ZFILL_WIDTH == 6


class TestGetNameByCode:
    """Test get_name_by_code() function."""

    def test_returns_none_for_nonexistent(self):
        """Should return None for nonexistent code."""
        result = get_name_by_code("stock_code_to_name", "000000")
        assert result is None or isinstance(result, str)

    def test_handles_invalid_table(self):
        """Should handle invalid table names gracefully."""
        result = get_name_by_code("nonexistent_table", "600519")
        assert result is None


class TestStockNameFunctions:
    """Test stock name retrieval functions."""

    def test_get_stock_name_handles_invalid(self):
        """Should handle invalid stock codes."""
        result = get_stock_name("000000")
        assert result is None or isinstance(result, str)

    def test_get_index_name_handles_invalid(self):
        """Should handle invalid index codes."""
        result = get_index_name("000000")
        assert result is None or isinstance(result, str)

    def test_get_etf_name_handles_invalid(self):
        """Should handle invalid ETF codes."""
        result = get_etf_name("000000")
        assert result is None or isinstance(result, str)

    def test_get_industry_name_handles_invalid(self):
        """Should handle invalid industry codes."""
        result = get_industry_name("000000")
        assert result is None or isinstance(result, str)

    def test_get_option_name_handles_invalid(self):
        """Should handle invalid option symbols."""
        result = get_option_name("000000")
        assert result is None or isinstance(result, str)


class TestGetAllCodesAndNames:
    """Test get_all_codes() and get_all_names() functions."""

    def test_get_all_codes_returns_list(self):
        """Should return a list."""
        result = get_all_codes("stock_code_to_name")
        assert isinstance(result, list)

    def test_get_all_names_returns_list(self):
        """Should return a list."""
        result = get_all_names("stock_code_to_name")
        assert isinstance(result, list)

    def test_get_all_codes_handles_invalid_table(self):
        """Should handle invalid table names."""
        result = get_all_codes("nonexistent_table")
        assert result == []


class TestSearchByName:
    """Test search_by_name() function."""

    def test_returns_dict(self):
        """Should return a dictionary."""
        result = search_by_name("stock_code_to_name", "test")
        assert isinstance(result, dict)

    def test_handles_invalid_table(self):
        """Should handle invalid table names."""
        result = search_by_name("nonexistent_table", "test")
        assert result == {}


class TestGetOptionUnderlyingPatterns:
    """Test get_option_underlying_patterns() function."""

    def test_returns_list(self):
        """Should return a list."""
        result = get_option_underlying_patterns("some_code")
        assert isinstance(result, list)


class TestPreloadMappings:
    """Test preload_mappings() function."""

    def test_runs_without_error(self):
        """Should run without raising errors."""
        preload_mappings()


class TestStandardizeColumns:
    """Test column standardization functions."""

    def test_standardize_columns_with_valid_source(self):
        """Should rename columns according to source map."""
        df = pd.DataFrame(
            {
                "日期": ["2023-01-01"],
                "开盘": [100.0],
                "收盘": [105.0],
            }
        )
        result = standardize_columns(df, "eastmoney")
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "close" in result.columns

    def test_standardize_columns_with_invalid_source(self):
        """Should return original DataFrame for unknown source."""
        df = pd.DataFrame({"date": ["2023-01-01"]})
        result = standardize_columns(df, "unknown_source")
        assert "date" in result.columns

    def test_standardize_columns_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()
        result = standardize_columns(df, "eastmoney")
        assert result.empty

    def test_standardize_columns_none_dataframe(self):
        """Should handle None DataFrame."""
        result = standardize_columns(None, "eastmoney")
        assert result is None


class TestStandardizeColumnsGeneric:
    """Test standardize_columns_generic() function."""

    def test_with_custom_mapping(self):
        """Should rename columns using custom mapping."""
        df = pd.DataFrame(
            {
                "old_name": [1.0],
                "another": [2.0],
            }
        )
        mapping = {"old_name": "new_name"}
        result = standardize_columns_generic(df, mapping)
        assert "new_name" in result.columns

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()
        result = standardize_columns_generic(df, {"a": "b"})
        assert result.empty

    def test_none_dataframe(self):
        """Should handle None DataFrame."""
        result = standardize_columns_generic(None, {"a": "b"})
        assert result is None


class TestSelectOhlcvColumns:
    """Test select_ohlcv_columns() function."""

    def test_selects_required_columns(self):
        """Should select datetime, OHLC, V columns."""
        df = pd.DataFrame(
            {
                "datetime": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
                "amount": [100000.0],
            }
        )
        result = select_ohlcv_columns(df)
        assert "datetime" in result.columns
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns

    def test_without_amount(self):
        """Should work without amount column."""
        df = pd.DataFrame(
            {
                "datetime": ["2023-01-01"],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [102.0],
                "volume": [1000],
            }
        )
        result = select_ohlcv_columns(df, include_amount=False)
        assert "amount" not in result.columns

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()
        result = select_ohlcv_columns(df)
        assert result.empty

    def test_none_dataframe(self):
        """Should handle None DataFrame."""
        result = select_ohlcv_columns(None)
        assert result is None
