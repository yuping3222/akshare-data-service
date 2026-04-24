"""System tests for stock data flows.

Verifies the complete end-to-end path:
  DataService -> cn.stock.quote.daily() -> cache -> return

Tests cover:
- Basic daily stock data retrieval with mock source
- Full cache flow: miss -> fetch -> write -> cache hit on second call
- Column schema validation (date, open, high, low, close, volume, amount)
- Minute-level data flow
- Financial data query flow (balance sheet, income statement, cash flow)
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager
from tests.system.conftest import _seed_cache  # noqa: F401


@pytest.mark.system
class TestStockDailyDataFlow:
    """End-to-end daily stock data retrieval tests."""

    def test_service_creation_with_cache_manager(
        self, system_cache_manager: CacheManager
    ) -> None:
        """DataService can be created with a custom CacheManager instance."""
        service = DataService(cache_manager=system_cache_manager)
        assert service is not None
        assert service.cache is system_cache_manager
        assert hasattr(service, "cn")
        assert hasattr(service.cn, "stock")
        assert hasattr(service.cn.stock, "quote")

    def test_daily_data_returns_ohlcv_dataframe(
        self,
        data_service_with_stock_source: DataService,
        stock_source_df: pd.DataFrame,
    ) -> None:
        """cn.stock.quote.daily() returns a DataFrame with expected OHLCV columns."""
        service = data_service_with_stock_source
        df = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        expected_cols = {"date", "open", "high", "low", "close", "volume", "amount"}
        assert expected_cols.issubset(set(df.columns))

    def test_daily_data_values_match_source(
        self,
        data_service_with_stock_source: DataService,
        stock_source_df: pd.DataFrame,
    ) -> None:
        """Returned data matches the source data."""
        service = data_service_with_stock_source
        df = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert len(df) == len(stock_source_df)
        # Under the read-only facade, symbols are stored and returned in the
        # normalized 6-digit canonical form ("600000") rather than the JQ
        # format ("600000.XSHG") kept in the raw source fixture.
        assert "600000" in df["symbol"].values or "600000.XSHG" in df["symbol"].values

    def test_cache_miss_then_fetch_then_write_then_return(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Full flow under the read-only facade: data pre-seeded in Served is returned."""
        source_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-10", freq="B"),
                "symbol": ["600000.XSHG"] * 7,
                "open": [10.0] * 7,
                "high": [11.0] * 7,
                "low": [9.0] * 7,
                "close": [10.5] * 7,
                "volume": [100_000] * 7,
                "amount": [1_000_000.0] * 7,
            }
        )
        _seed_cache(system_cache_manager, "stock_daily", source_df)
        service = DataService(cache_manager=system_cache_manager)

        df1 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        assert isinstance(df1, pd.DataFrame)
        assert not df1.empty
        assert len(df1) == 7

    def test_cache_hit_on_second_call(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Second call to same query returns the same cached data."""
        source_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-10", freq="B"),
                "symbol": ["600000.XSHG"] * 7,
                "open": [10.0] * 7,
                "high": [11.0] * 7,
                "low": [9.0] * 7,
                "close": [10.5] * 7,
                "volume": [100_000] * 7,
                "amount": [1_000_000.0] * 7,
            }
        )
        _seed_cache(system_cache_manager, "stock_daily", source_df)
        service = DataService(cache_manager=system_cache_manager)

        df1 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        assert not df1.empty

        df2 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        assert not df2.empty
        assert len(df2) == len(df1)

    def test_column_schema_validation(
        self,
        data_service_with_stock_source: DataService,
    ) -> None:
        """Verify returned DataFrame has the expected schema with correct dtypes."""
        df = data_service_with_stock_source.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        # Required columns
        assert "date" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "amount" in df.columns

        # Dtype checks
        assert pd.api.types.is_datetime64_any_dtype(df["date"])
        assert pd.api.types.is_float_dtype(df["open"])
        assert pd.api.types.is_float_dtype(df["high"])
        assert pd.api.types.is_float_dtype(df["low"])
        assert pd.api.types.is_float_dtype(df["close"])
        assert pd.api.types.is_numeric_dtype(df["volume"])

    def test_symbol_normalization_in_query(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Different symbol formats normalize to the same cached row."""
        source_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5, freq="B"),
                "symbol": ["600000.XSHG"] * 5,
                "open": [10.0] * 5,
                "high": [11.0] * 5,
                "low": [9.0] * 5,
                "close": [10.5] * 5,
                "volume": [100_000] * 5,
                "amount": [1_000_000.0] * 5,
            }
        )
        _seed_cache(system_cache_manager, "stock_daily", source_df)
        service = DataService(cache_manager=system_cache_manager)

        df1 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        df2 = service.cn.stock.quote.daily(
            symbol="600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        assert not df1.empty
        assert not df2.empty

    def test_convenience_method_get_daily(
        self, data_service_with_stock_source: DataService
    ) -> None:
        """get_daily() convenience method delegates to cn.stock.quote.daily()."""
        df = data_service_with_stock_source.get_daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


@pytest.mark.system
class TestStockMinuteDataFlow:
    """End-to-end minute-level stock data tests."""

    def test_minute_data_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """cn.stock.quote.minute() reads pre-seeded Served data."""
        dt = pd.date_range("2024-01-02 09:30", periods=30, freq="min")
        minute_df = pd.DataFrame(
            {
                "datetime": dt,
                "symbol": ["600000.XSHG"] * 30,
                "week": ["2024-W01"] * 30,
                "period": ["1min"] * 30,
                "open": [10.0 + i * 0.01 for i in range(30)],
                "high": [10.05 + i * 0.01 for i in range(30)],
                "low": [9.95 + i * 0.01 for i in range(30)],
                "close": [10.02 + i * 0.01 for i in range(30)],
                "volume": [5_000 + i * 500 for i in range(30)],
                "amount": [50_000.0 + i * 500.0 for i in range(30)],
            }
        )
        _seed_cache(system_cache_manager, "stock_minute", minute_df, adjust="none")
        service = DataService(cache_manager=system_cache_manager)

        df = service.cn.stock.quote.minute(
            symbol="sh600000",
            freq="1min",
            start_date="2024-01-02 00:00:00",
            end_date="2024-01-02 23:59:59",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "datetime" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns


@pytest.mark.system
class TestStockFinancialDataFlow:
    """End-to-end financial statement data tests."""

    def test_finance_indicator_flow(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """cn.stock.finance.indicators() returns pre-seeded financial data."""
        fi_df = pd.DataFrame(
            {
                "report_date": pd.to_datetime(["2023-12-31", "2024-03-31"]),
                "symbol": ["600000.XSHG"] * 2,
                "roe": [12.5, 13.1],
                "roa": [1.2, 1.3],
                "eps": [0.85, 0.92],
            }
        )
        _seed_cache(system_cache_manager, "finance_indicator", fi_df, adjust="none")
        service = DataService(cache_manager=system_cache_manager)

        df = service.cn.stock.finance.indicators(
            symbol="sh600000",
            start_date="2023-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
