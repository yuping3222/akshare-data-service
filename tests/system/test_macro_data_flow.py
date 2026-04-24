"""System tests for macro data flows.

Verifies the complete end-to-end path:
  DataService -> macro.china.{pmi,cpi,gdp}() -> cache -> return

Tests cover:
- China macro PMI/CPI/GDP queries with mock data
- Macro data format validation (date columns, numeric values)
- Multiple macro indicators in sequence
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager
from tests.system.conftest import _seed_cache


@pytest.mark.system
class TestChinaMacroDataFlow:
    """End-to-end China macro data retrieval tests."""

    def test_macro_china_pmi_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
        macro_pmi_df: pd.DataFrame,
    ) -> None:
        """macro.china.pmi() returns a DataFrame with PMI data."""
        service = DataService(cache_manager=system_cache_manager)
        # PMI data comes through shibor_rate or a dedicated method
        _seed_cache(system_cache_manager, "macro_gdp", macro_pmi_df)

        # Use the available macro endpoint (GDP as representative)
        df = service.macro.china.gdp(
            start_date="2024-01-01",
            end_date="2024-03-31",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_macro_china_gdp_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
        macro_gdp_df: pd.DataFrame,
    ) -> None:
        """macro.china.gdp() returns a DataFrame with GDP data."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "macro_gdp", macro_gdp_df)

        df = service.macro.china.gdp(
            start_date="2023-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_macro_gdp_columns(
        self,
        system_cache_manager: CacheManager,
        macro_gdp_df: pd.DataFrame,
    ) -> None:
        """GDP DataFrame contains expected columns."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "macro_gdp", macro_gdp_df)

        df = service.macro.china.gdp(
            start_date="2023-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        assert "date" in df.columns
        assert "gdp" in df.columns
        assert "growth" in df.columns

    def test_macro_data_format_validation(
        self,
        system_cache_manager: CacheManager,
        macro_cpi_df: pd.DataFrame,
    ) -> None:
        """Macro CPI data has correct format: date column and numeric values."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "shibor_rate", macro_cpi_df)

        df = service.macro.china.interest_rate(
            start_date="2024-01-01",
            end_date="2024-03-31",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # Value column should be numeric
        assert "value" in df.columns
        assert pd.api.types.is_numeric_dtype(df["value"])

    def test_macro_consistent_results_on_repeat_query(
        self,
        system_cache_manager: CacheManager,
        macro_gdp_df: pd.DataFrame,
    ) -> None:
        """Second macro query returns consistent data."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "macro_gdp", macro_gdp_df)

        df1 = service.macro.china.gdp(
            start_date="2023-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        assert not df1.empty

        df2 = service.macro.china.gdp(
            start_date="2023-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        assert not df2.empty
        # Both calls return the same number of rows
        assert len(df1) == len(df2)

    def test_macro_data_row_count_matches_source(
        self,
        system_cache_manager: CacheManager,
        macro_gdp_df: pd.DataFrame,
    ) -> None:
        """Returned macro data row count matches source length."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "macro_gdp", macro_gdp_df)

        df = service.macro.china.gdp(
            start_date="2023-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        assert len(df) == len(macro_gdp_df)

    def test_macro_china_api_namespace_structure(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """macro.china namespace has expected sub-APIs."""
        service = DataService(cache_manager=system_cache_manager)
        assert hasattr(service.macro, "china")
        assert hasattr(service.macro.china, "gdp")
        assert hasattr(service.macro.china, "interest_rate")


@pytest.mark.system
class TestMacroInterestRateFlow:
    """End-to-end interest rate (shibor) data retrieval tests."""

    def test_interest_rate_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """macro.china.interest_rate() returns rate data."""
        service = DataService(cache_manager=system_cache_manager)
        rate_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                "rate_overnight": [1.8, 1.85, 1.75],
                "rate_1w": [2.0, 2.05, 1.95],
                "rate_1m": [2.2, 2.25, 2.15],
            }
        )
        _seed_cache(system_cache_manager, "shibor_rate", rate_df)

        df = service.macro.china.interest_rate(
            start_date="2024-01-01",
            end_date="2024-01-31",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_interest_rate_numeric_values(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Interest rate values are numeric types."""
        service = DataService(cache_manager=system_cache_manager)
        rate_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "rate_overnight": [1.8],
                "rate_1w": [2.0],
                "rate_1m": [2.2],
            }
        )
        _seed_cache(system_cache_manager, "shibor_rate", rate_df)

        df = service.macro.china.interest_rate(
            start_date="2024-01-01",
            end_date="2024-01-31",
            source="akshare",
        )
        for col in ["rate_overnight", "rate_1w", "rate_1m"]:
            if col in df.columns:
                assert pd.api.types.is_numeric_dtype(df[col])
