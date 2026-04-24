"""System tests for ETF data flows.

Verifies the complete end-to-end path:
  DataService -> cn.fund.quote.daily() -> cache -> return

Tests cover:
- Daily ETF data retrieval with mock data
- Minute-level ETF data flow
- ETF-specific field validation
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager
from tests.system.conftest import _seed_cache


@pytest.mark.system
class TestETFDailyDataFlow:
    """End-to-end ETF daily data retrieval tests."""

    def test_etf_daily_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
        etf_source_df: pd.DataFrame,
    ) -> None:
        """cn.fund.quote.daily() returns a DataFrame for the requested ETF."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "etf_daily", etf_source_df)

        df = service.cn.fund.quote.daily(
            symbol="sh510300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_etf_daily_columns_match_schema(
        self,
        system_cache_manager: CacheManager,
        etf_source_df: pd.DataFrame,
    ) -> None:
        """ETF DataFrame contains expected OHLCV columns."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "etf_daily", etf_source_df)

        df = service.cn.fund.quote.daily(
            symbol="sh510300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        expected_cols = {"date", "open", "high", "low", "close", "volume", "amount"}
        assert expected_cols.issubset(set(df.columns))

    def test_etf_daily_consistent_on_repeat_query(
        self,
        system_cache_manager: CacheManager,
        etf_source_df: pd.DataFrame,
    ) -> None:
        """Repeat queries against pre-seeded Served return the same data."""
        _seed_cache(system_cache_manager, "etf_daily", etf_source_df)
        service = DataService(cache_manager=system_cache_manager)

        df1 = service.cn.fund.quote.daily(
            symbol="sh510300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df1.empty

        df2 = service.cn.fund.quote.daily(
            symbol="sh510300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df2.empty
        assert len(df1) == len(df2)

    def test_etf_daily_convenience_method(
        self,
        system_cache_manager: CacheManager,
        etf_source_df: pd.DataFrame,
    ) -> None:
        """get_etf() convenience method delegates to cn.fund.quote.daily()."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "etf_daily", etf_source_df)

        df = service.get_etf(
            symbol="sh510300",
            start_date="2024-01-02",
            end_date="2024-01-19",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_etf_data_row_count_matches_source(
        self,
        system_cache_manager: CacheManager,
        etf_source_df: pd.DataFrame,
    ) -> None:
        """Returned row count matches source data length."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "etf_daily", etf_source_df)

        df = service.cn.fund.quote.daily(
            symbol="sh510300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert len(df) == len(etf_source_df)


@pytest.mark.system
class TestETFMinuteDataFlow:
    """End-to-end ETF minute-level data retrieval tests."""

    def test_etf_minute_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
        etf_minute_source_df: pd.DataFrame,
    ) -> None:
        """cn.stock.quote.minute() for ETF symbol returns minute-level DataFrame."""
        _seed_cache(system_cache_manager, "stock_minute", etf_minute_source_df)
        service = DataService(cache_manager=system_cache_manager)

        df = service.cn.stock.quote.minute(
            symbol="sh510300",
            freq="1min",
            start_date="2024-01-02 00:00:00",
            end_date="2024-01-02 23:59:59",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_etf_minute_columns(
        self,
        system_cache_manager: CacheManager,
        etf_minute_source_df: pd.DataFrame,
    ) -> None:
        """Minute ETF DataFrame has datetime, OHLC, volume columns."""
        _seed_cache(system_cache_manager, "stock_minute", etf_minute_source_df)
        service = DataService(cache_manager=system_cache_manager)

        df = service.cn.stock.quote.minute(
            symbol="sh510300",
            freq="1min",
            start_date="2024-01-02 00:00:00",
            end_date="2024-01-02 23:59:59",
            source="akshare",
        )
        assert "datetime" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert len(df) == 30

    def test_etf_minute_convenience_method(
        self,
        system_cache_manager: CacheManager,
        etf_minute_source_df: pd.DataFrame,
    ) -> None:
        """get_minute() convenience method works for ETF symbols."""
        _seed_cache(system_cache_manager, "stock_minute", etf_minute_source_df)
        service = DataService(cache_manager=system_cache_manager)

        df = service.get_minute(
            symbol="sh510300",
            freq="1min",
            start_date="2024-01-02 00:00:00",
            end_date="2024-01-02 23:59:59",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
