"""Tests for MockSource — a first-class test double for the DataSource interface.

MockSource generates deterministic (seedable) synthetic data without any
network calls, making it suitable for unit tests, integration tests, and
development workflows.
"""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from akshare_data.sources.mock import MockSource
from akshare_data.core.base import DataSource


class TestMockSource:
    """Test suite for MockSource class."""

    @pytest.fixture
    def source(self):
        """Create MockSource instance."""
        return MockSource()

    class TestInitialization:
        """Test MockSource initialization."""

        def test_source_has_name(self, source):
            """Test source has name property."""
            assert source.name == "mock"

        def test_source_type_is_mock(self, source):
            """Test source_type returns 'mock'."""
            assert source.source_type == "mock"

        def test_is_instance_of_datasource(self, source):
            """MockSource is a proper DataSource subclass."""
            assert isinstance(source, DataSource)

    class TestGetDailyData:
        """Test get_daily_data method."""

        def test_returns_dataframe(self, source):
            """Test returns a DataFrame."""
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-10")
            assert isinstance(result, pd.DataFrame)

        def test_returns_empty_for_invalid_date_range(self, source):
            """Test returns empty DataFrame for invalid date range."""
            result = source.get_daily_data("000001", "2024-01-10", "2024-01-01")
            assert isinstance(result, pd.DataFrame)
            assert result.empty

        def test_generates_weekday_data_only(self, source):
            """Test generates data only for weekdays."""
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-07")
            if not result.empty:
                dates = pd.to_datetime(result["date"])
                weekdays = dates.dt.weekday < 5
                assert all(weekdays), "All dates should be weekdays"

        def test_includes_required_columns(self, source):
            """Test result includes required columns."""
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-05")
            if not result.empty:
                required_cols = ["date", "open", "high", "low", "close", "volume"]
                for col in required_cols:
                    assert col in result.columns

        def test_includes_symbol_column(self, source):
            """Test result includes symbol column."""
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-05")
            if not result.empty:
                assert "symbol" in result.columns
                assert all(result["symbol"] == "000001")

        def test_high_greater_than_open_and_close(self, source):
            """Test high price is >= open and close."""
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-05")
            if not result.empty:
                assert all(result["high"] >= result["open"])
                assert all(result["high"] >= result["close"])

        def test_low_less_than_open_and_close(self, source):
            """Test low price is <= open and close."""
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-05")
            if not result.empty:
                assert all(result["low"] <= result["open"])
                assert all(result["low"] <= result["close"])

        def test_row_count_matches_trading_days(self, source):
            """Test that the number of rows equals the number of weekdays."""
            start = datetime(2024, 1, 1)
            end = datetime(2024, 1, 31)
            expected_days = sum(
                1
                for d in range((end - start).days + 1)
                if (start + pd.Timedelta(days=d)).weekday() < 5
            )
            result = source.get_daily_data("000001", "2024-01-01", "2024-01-31")
            assert len(result) == expected_days

        def test_different_symbols_independent(self, source):
            """Different symbols get their own rows (symbol column differs)."""
            r1 = source.get_daily_data("000001", "2024-01-01", "2024-01-05")
            r2 = source.get_daily_data("000002", "2024-01-01", "2024-01-05")
            if not r1.empty and not r2.empty:
                assert set(r1["symbol"].unique()) == {"000001"}
                assert set(r2["symbol"].unique()) == {"000002"}

    class TestGetMinuteData:
        """Test get_minute_data method."""

        def test_delegates_to_get_daily_data(self, source):
            """Test get_minute_data delegates to get_daily_data."""
            result = source.get_minute_data(
                "000001", "5min", "2024-01-01", "2024-01-05"
            )
            assert isinstance(result, pd.DataFrame)

        def test_uses_default_dates_when_none(self, source):
            """When start/end dates are None, defaults are used."""
            result = source.get_minute_data("000001", "1min")
            assert isinstance(result, pd.DataFrame)
            # Defaults are 2024-01-01 to 2024-01-05
            assert not result.empty

    class TestGetIndexStocks:
        """Test get_index_stocks method."""

        def test_returns_list_of_codes(self, source):
            """Test returns a list of stock codes."""
            result = source.get_index_stocks("000300")
            assert isinstance(result, list)
            assert len(result) == 5000

        def test_returns_6_digit_codes(self, source):
            """Test returns 6-digit codes."""
            result = source.get_index_stocks("000300")
            for code in result[:5]:
                assert len(code) == 6
                assert code.isdigit()

        def test_starts_from_1(self, source):
            """Test codes start from 1."""
            result = source.get_index_stocks("000300")
            assert result[0] == "000001"

    class TestGetIndexComponents:
        """Test get_index_components method."""

        def test_returns_dataframe_with_weights(self, source):
            """Returns DataFrame with index_code, code, stock_name, weight."""
            result = source.get_index_components("000300", include_weights=True)
            assert isinstance(result, pd.DataFrame)
            assert "index_code" in result.columns
            assert "code" in result.columns
            assert "stock_name" in result.columns
            assert "weight" in result.columns
            assert len(result) == 5000

        def test_returns_dataframe_without_weights(self, source):
            """Returns DataFrame without weight column."""
            result = source.get_index_components("000300", include_weights=False)
            assert isinstance(result, pd.DataFrame)
            assert "weight" not in result.columns

    class TestGetTradingDays:
        """Test get_trading_days method."""

        def test_returns_list_of_dates(self, source):
            """Test returns a list of date strings."""
            result = source.get_trading_days("2024-01-01", "2024-01-10")
            assert isinstance(result, list)
            assert len(result) > 0

        def test_all_returns_weekdays(self, source):
            """Test all returned dates are weekdays."""
            result = source.get_trading_days("2024-01-01", "2024-01-14")
            for date_str in result:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                assert dt.weekday() < 5

        def test_dates_in_range(self, source):
            """Test dates are within the specified range."""
            result = source.get_trading_days("2024-01-01", "2024-01-10")
            if result:
                first_date = datetime.strptime(result[0], "%Y-%m-%d")
                last_date = datetime.strptime(result[-1], "%Y-%m-%d")
                assert first_date >= datetime(2024, 1, 1)
                assert last_date <= datetime(2024, 1, 10)

        def test_includes_start_and_end_dates(self, source):
            """Test range includes start and end dates if weekdays."""
            result = source.get_trading_days("2024-01-02", "2024-01-05")
            date_strings = [datetime.strptime(d, "%Y-%m-%d") for d in result]
            assert any(
                d.year == 2024 and d.month == 1 and d.day == 2 for d in date_strings
            )
            assert any(
                d.year == 2024 and d.month == 1 and d.day == 5 for d in date_strings
            )

        def test_uses_defaults_when_none(self, source):
            """When start/end dates are None, defaults (2020-01) are used."""
            result = source.get_trading_days()
            assert len(result) > 0

    class TestGetSecuritiesList:
        """Test get_securities_list method."""

        def test_returns_dataframe(self, source):
            result = source.get_securities_list("stock")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 100

        def test_required_columns(self, source):
            result = source.get_securities_list("etf")
            for col in ["code", "display_name", "type"]:
                assert col in result.columns

        def test_type_reflected_in_result(self, source):
            result = source.get_securities_list("etf")
            assert all(result["type"] == "etf")

    class TestGetSecurityInfo:
        """Test get_security_info method."""

        def test_returns_dict(self, source):
            result = source.get_security_info("000001")
            assert isinstance(result, dict)

        def test_contains_expected_keys(self, source):
            result = source.get_security_info("000001")
            for key in ["code", "display_name", "type"]:
                assert key in result

    class TestGetMoneyFlow:
        """Test get_money_flow method."""

        def test_returns_dataframe(self, source):
            result = source.get_money_flow("000001")
            assert isinstance(result, pd.DataFrame)

        def test_required_columns(self, source):
            result = source.get_money_flow("000001")
            for col in ["date", "main_net", "retail_net"]:
                assert col in result.columns

    class TestGetNorthMoneyFlow:
        """Test get_north_money_flow method."""

        def test_returns_dataframe(self, source):
            result = source.get_north_money_flow()
            assert isinstance(result, pd.DataFrame)

        def test_required_columns(self, source):
            result = source.get_north_money_flow()
            for col in ["date", "north_buy", "north_sell", "north_net"]:
                assert col in result.columns

    class TestGetIndustryStocks:
        """Test get_industry_stocks method."""

        def test_returns_list(self, source):
            result = source.get_industry_stocks("801010")
            assert isinstance(result, list)
            assert len(result) == 100

    class TestGetIndustryMapping:
        """Test get_industry_mapping method."""

        def test_returns_string(self, source):
            result = source.get_industry_mapping("000001")
            assert isinstance(result, str)

    class TestGetFinanceIndicator:
        """Test get_finance_indicator method."""

        def test_returns_dataframe(self, source):
            result = source.get_finance_indicator("000001")
            assert isinstance(result, pd.DataFrame)

        def test_contains_expected_columns(self, source):
            result = source.get_finance_indicator("000001")
            for col in ["date", "symbol", "pe_ttm", "pb"]:
                assert col in result.columns

    class TestGetCallAuction:
        """Test get_call_auction method."""

        def test_returns_dataframe(self, source):
            result = source.get_call_auction("000001")
            assert isinstance(result, pd.DataFrame)

        def test_contains_expected_columns(self, source):
            result = source.get_call_auction("000001")
            for col in ["time", "open", "high", "low", "close", "volume"]:
                assert col in result.columns

    class TestMockDataDeterminism:
        """Test that mock data is reproducible."""

        def test_same_date_range_gives_same_length(self, source):
            """Test same date range gives same number of records."""
            result1 = source.get_daily_data("000001", "2024-06-01", "2024-06-30")
            result2 = source.get_daily_data("000001", "2024-06-01", "2024-06-30")
            assert len(result1) == len(result2)

    class TestMockSourceInterface:
        """Test MockSource implements DataSource interface."""

        def test_has_name_attribute(self, source):
            assert hasattr(source, "name")
            assert isinstance(source.name, str)

        def test_has_get_daily_data(self, source):
            assert hasattr(source, "get_daily_data")
            assert callable(source.get_daily_data)

        def test_has_get_minute_data(self, source):
            assert hasattr(source, "get_minute_data")
            assert callable(source.get_minute_data)

        def test_has_get_index_stocks(self, source):
            assert hasattr(source, "get_index_stocks")
            assert callable(source.get_index_stocks)

        def test_has_get_trading_days(self, source):
            assert hasattr(source, "get_trading_days")
            assert callable(source.get_trading_days)

        def test_has_health_check(self, source):
            """DataSource provides health_check."""
            assert hasattr(source, "health_check")
            result = source.health_check()
            assert result["status"] == "ok"

    class TestOptionalMethodsNotImplemented:
        """Test that optional methods not implemented by MockSource raise
        NotImplementedError."""

        def test_get_spot_em_raises(self, source):
            with pytest.raises(NotImplementedError, match="mock"):
                source.get_spot_em()

        def test_get_securities_code_name_raises(self, source):
            with pytest.raises(NotImplementedError, match="mock"):
                source.get_securities_code_name()

        def test_get_bond_yield_raises(self, source):
            with pytest.raises(NotImplementedError, match="mock"):
                source.get_bond_yield("000001")

    class TestDeterministicDataWithSeed:
        """Test that using numpy seed makes data fully reproducible."""

        def test_seeded_data_is_reproducible(self):
            """Two calls with same seed produce identical values."""
            np.random.seed(42)
            r1 = MockSource().get_daily_data("000001", "2024-01-01", "2024-01-10")

            np.random.seed(42)
            r2 = MockSource().get_daily_data("000001", "2024-01-01", "2024-01-10")

            pd.testing.assert_frame_equal(r1, r2)
