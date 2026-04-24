"""System tests for index data flows.

Verifies the complete end-to-end path:
  DataService -> cn.index.quote.daily() -> cache -> return

Tests cover:
- Daily index data retrieval with mock data
- Index components query
- Index-specific fields (pe, pb valuation metrics)
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager
from tests.system.conftest import _seed_cache


@pytest.mark.system
class TestIndexDailyDataFlow:
    """End-to-end index daily data retrieval tests."""

    def test_index_daily_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
        index_source_df: pd.DataFrame,
    ) -> None:
        """cn.index.quote.daily() returns a DataFrame for the requested index."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "index_daily", index_source_df)

        df = service.cn.index.quote.daily(
            symbol="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_index_daily_columns_match_schema(
        self,
        system_cache_manager: CacheManager,
        index_source_df: pd.DataFrame,
    ) -> None:
        """Index DataFrame contains OHLCV plus valuation fields."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "index_daily", index_source_df)

        df = service.cn.index.quote.daily(
            symbol="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        # Core OHLCV columns
        assert "date" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "amount" in df.columns
        # Index-specific valuation fields
        assert "pe" in df.columns
        assert "pb" in df.columns

    def test_index_daily_consistent_on_repeat_query(
        self,
        system_cache_manager: CacheManager,
        index_source_df: pd.DataFrame,
    ) -> None:
        """Repeat queries against pre-seeded Served return the same data."""
        _seed_cache(system_cache_manager, "index_daily", index_source_df)
        service = DataService(cache_manager=system_cache_manager)

        df1 = service.cn.index.quote.daily(
            symbol="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df1.empty
        first_len = len(df1)

        df2 = service.cn.index.quote.daily(
            symbol="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df2.empty
        assert len(df2) == first_len

    def test_index_daily_convenience_method(
        self,
        system_cache_manager: CacheManager,
        index_source_df: pd.DataFrame,
    ) -> None:
        """get_index() convenience method delegates to cn.index.quote.daily()."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "index_daily", index_source_df)

        df = service.get_index(
            index_code="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


@pytest.mark.system
class TestIndexComponentsFlow:
    """End-to-end index components retrieval tests."""

    def test_index_components_returns_dataframe(
        self,
        system_cache_manager: CacheManager,
        index_components_df: pd.DataFrame,
    ) -> None:
        """cn.index.meta.components() returns index constituent data."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "index_components", index_components_df)

        df = service.cn.index.meta.components(
            index_code="sh000300",
            source="akshare",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "code" in df.columns

    def test_index_components_cache_hit(
        self,
        system_cache_manager: CacheManager,
        index_components_df: pd.DataFrame,
    ) -> None:
        """Repeat component queries against pre-seeded Served are consistent."""
        _seed_cache(system_cache_manager, "index_components", index_components_df)
        service = DataService(cache_manager=system_cache_manager)

        df1 = service.cn.index.meta.components(index_code="sh000300", source="akshare")
        assert not df1.empty
        first_len = len(df1)

        df2 = service.cn.index.meta.components(index_code="sh000300", source="akshare")
        assert not df2.empty
        assert len(df2) == first_len

    def test_get_index_components_facade(
        self,
        system_cache_manager: CacheManager,
        index_components_df: pd.DataFrame,
    ) -> None:
        """get_index_components() facade works correctly."""
        service = DataService(cache_manager=system_cache_manager)
        _seed_cache(system_cache_manager, "index_components", index_components_df)

        df = service.get_index_components(index_code="sh000300")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_index_stocks_returns_list(
        self,
        system_cache_manager: CacheManager,
        index_components_df: pd.DataFrame,
    ) -> None:
        """get_index_stocks() reads constituents from the Served components table."""
        _seed_cache(system_cache_manager, "index_components", index_components_df)
        service = DataService(cache_manager=system_cache_manager)

        stocks = service.get_index_stocks(index_code="sh000300")
        assert isinstance(stocks, list)
        assert len(stocks) == 3
        assert "600000.XSHG" in stocks or "600000" in stocks
