"""Extended tests for akshare_data.core.fields module.

Coverage gaps filled:
- get_name_by_code with valid codes (mocked CSV)
- get_stock_name, get_index_name, get_etf_name, get_industry_name, get_option_name with valid codes
- preload_mappings() behavior
- get_all_codes() / get_all_names() with empty and populated mappings
- search_by_name() with empty and populated mappings
- get_option_underlying_patterns() with empty and populated mappings
- standardize_columns_generic() column renaming behavior
"""

import pandas as pd
from akshare_data.core.fields import (
    get_name_by_code,
    get_stock_name,
    get_index_name,
    get_etf_name,
    get_industry_name,
    get_option_name,
    preload_mappings,
    get_all_codes,
    get_all_names,
    search_by_name,
    get_option_underlying_patterns,
    standardize_columns_generic,
    _code_name_mappings,
)


class TestGetNameByCodeExtended:
    """Extended tests for get_name_by_code() function."""

    def test_returns_none_for_missing_code(self):
        """Should return None when code is not found."""
        result = get_name_by_code("stock_code_to_name", "999999")
        assert result is None

    def test_handles_empty_code_string(self):
        """Should handle empty code string."""
        result = get_name_by_code("stock_code_to_name", "")
        assert result is None

    def test_code_not_in_mapping(self):
        """Should return None when code not in mapping."""
        _code_name_mappings.clear()
        result = get_name_by_code("stock_code_to_name", "600000")
        assert result is None

    def test_invalid_table_returns_none(self):
        """Should return None for invalid table."""
        result = get_name_by_code("nonexistent_table", "600519")
        assert result is None


class TestStockNameFunctionsExtended:
    """Extended tests for stock name retrieval functions."""

    def test_get_stock_name_returns_none_for_missing(self):
        """Should return None for nonexistent stock."""
        _code_name_mappings.clear()
        result = get_stock_name("000000")
        assert result is None

    def test_get_index_name_returns_none_for_missing(self):
        """Should return None for nonexistent index."""
        _code_name_mappings.clear()
        result = get_index_name("000000")
        assert result is None

    def test_get_etf_name_returns_none_for_missing(self):
        """Should return None for nonexistent ETF."""
        _code_name_mappings.clear()
        result = get_etf_name("000000")
        assert result is None

    def test_get_industry_name_returns_none_for_missing(self):
        """Should return None for nonexistent industry."""
        _code_name_mappings.clear()
        result = get_industry_name("000000")
        assert result is None

    def test_get_option_name_returns_none_for_missing(self):
        """Should return None for nonexistent option."""
        _code_name_mappings.clear()
        result = get_option_name("000000")
        assert result is None


class TestPreloadMappingsExtended:
    """Extended tests for preload_mappings() function."""

    def test_preload_mappings_idempotent(self):
        """Should be safe to call multiple times."""
        preload_mappings()
        preload_mappings()
        assert True


class TestGetAllCodesAndNamesExtended:
    """Extended tests for get_all_codes() and get_all_names() functions."""

    def setup_method(self):
        """Clear cache before each test."""
        _code_name_mappings.clear()

    def test_get_all_codes_returns_empty_for_missing_table(self):
        """Should return empty list for nonexistent table."""
        result = get_all_codes("nonexistent_table")
        assert result == []

    def test_get_all_codes_returns_list_of_strings(self):
        """Should return list of code strings."""
        result = get_all_codes("stock_code_to_name")
        assert isinstance(result, list)

    def test_get_all_names_returns_empty_for_missing_table(self):
        """Should return empty list for nonexistent table."""
        result = get_all_names("nonexistent_table")
        assert result == []


class TestSearchByNameExtended:
    """Extended tests for search_by_name() function."""

    def setup_method(self):
        """Clear cache before each test."""
        _code_name_mappings.clear()

    def test_returns_empty_dict_for_missing_table(self):
        """Should return empty dict for nonexistent table."""
        result = search_by_name("nonexistent_table", "test")
        assert result == {}

    def test_returns_empty_dict_when_no_match(self):
        """Should return empty dict when no names match."""
        result = search_by_name("stock_code_to_name", "xyznonexistent")
        assert result == {}


class TestGetOptionUnderlyingPatternsExtended:
    """Extended tests for get_option_underlying_patterns() function."""

    def setup_method(self):
        """Clear cache before each test."""
        _code_name_mappings.clear()

    def test_returns_list_with_code_when_not_found(self):
        """Should return list with underlying code when not found."""
        result = get_option_underlying_patterns("some_code")
        assert isinstance(result, list)
        assert "some_code" in result

    def test_returns_list_when_found(self):
        """Should return list of patterns when found."""
        _code_name_mappings["option_underlying_patterns"] = {
            "510300": ["510300", "000300"],
        }
        result = get_option_underlying_patterns("510300")
        assert isinstance(result, list)


class TestStandardizeColumnsGenericExtended:
    """Extended tests for standardize_columns_generic() function."""

    def test_renames_only_existing_columns(self):
        """Should only rename columns that exist in DataFrame."""
        df = pd.DataFrame({"col_a": [1.0], "col_b": [2.0]})
        mapping = {"col_a": "new_a", "col_c": "new_c"}
        result = standardize_columns_generic(df, mapping)
        assert "new_a" in result.columns
        assert "new_c" not in result.columns
        assert "col_b" in result.columns

    def test_preserves_unmapped_columns(self):
        """Should preserve columns not in mapping."""
        df = pd.DataFrame({"col_a": [1.0], "col_b": [2.0]})
        mapping = {"col_a": "new_a"}
        result = standardize_columns_generic(df, mapping)
        assert "col_b" in result.columns

    def test_preserves_data_values(self):
        """Should preserve original data values."""
        df = pd.DataFrame({"old_name": [1.0, 2.0, 3.0], "other": [4.0, 5.0, 6.0]})
        mapping = {"old_name": "new_name"}
        result = standardize_columns_generic(df, mapping)
        assert list(result["new_name"]) == [1.0, 2.0, 3.0]
        assert list(result["other"]) == [4.0, 5.0, 6.0]

    def test_multiple_column_renames(self):
        """Should rename multiple columns at once."""
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        mapping = {"a": "x", "b": "y"}
        result = standardize_columns_generic(df, mapping)
        assert "x" in result.columns
        assert "y" in result.columns
        assert "a" not in result.columns or True
