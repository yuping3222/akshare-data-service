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
        service.akshare.get_index_daily = MagicMock(return_value=index_source_df.copy())
        service.lixinger.get_index_daily = MagicMock(
            return_value=index_source_df.copy()
        )

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
        service.akshare.get_index_daily = MagicMock(return_value=index_source_df.copy())
        service.lixinger.get_index_daily = MagicMock(
            return_value=index_source_df.copy()
        )

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

    def test_index_daily_source_called_once_on_miss(
        self,
        system_cache_manager: CacheManager,
        index_source_df: pd.DataFrame,
    ) -> None:
        """Source is fetched on first call, not on cache hit."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_index_daily = MagicMock(return_value=index_source_df.copy())
        service.lixinger.get_index_daily = MagicMock(
            return_value=index_source_df.copy()
        )

        df1 = service.cn.index.quote.daily(
            symbol="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df1.empty
        first_call_count = service.akshare.get_index_daily.call_count

        # Second call should hit cache
        df2 = service.cn.index.quote.daily(
            symbol="sh000300",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df2.empty
        assert service.akshare.get_index_daily.call_count == first_call_count

    def test_index_daily_convenience_method(
        self,
        system_cache_manager: CacheManager,
        index_source_df: pd.DataFrame,
    ) -> None:
        """get_index() convenience method delegates to cn.index.quote.daily()."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_index_daily = MagicMock(return_value=index_source_df.copy())
        service.lixinger.get_index_daily = MagicMock(
            return_value=index_source_df.copy()
        )

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
        service.akshare.get_index_components = MagicMock(
            return_value=index_components_df.copy()
        )
        service.lixinger.get_index_components = MagicMock(
            return_value=index_components_df.copy()
        )

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
        """Second components query uses cached data."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_index_components = MagicMock(
            return_value=index_components_df.copy()
        )
        service.lixinger.get_index_components = MagicMock(
            return_value=index_components_df.copy()
        )

        df1 = service.cn.index.meta.components(index_code="sh000300", source="akshare")
        assert not df1.empty
        first_call_count = service.akshare.get_index_components.call_count

        df2 = service.cn.index.meta.components(index_code="sh000300", source="akshare")
        assert not df2.empty
        assert service.akshare.get_index_components.call_count == first_call_count

    def test_get_index_components_facade(
        self,
        system_cache_manager: CacheManager,
        index_components_df: pd.DataFrame,
    ) -> None:
        """get_index_components() facade works correctly."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_index_components = MagicMock(
            return_value=index_components_df.copy()
        )
        service.lixinger.get_index_components = MagicMock(
            return_value=index_components_df.copy()
        )

        df = service.get_index_components(index_code="sh000300")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_index_stocks_returns_list(
        self,
        system_cache_manager: CacheManager,
        index_components_df: pd.DataFrame,
    ) -> None:
        """get_index_stocks() returns a list of stock codes."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_index_stocks = MagicMock(
            return_value=["600000", "600036", "000001"]
        )
        service.lixinger.get_index_stocks = MagicMock(
            return_value=["600000", "600036", "000001"]
        )

        stocks = service.get_index_stocks(index_code="sh000300")
        assert isinstance(stocks, list)
        assert len(stocks) == 3
        assert "600000" in stocks
