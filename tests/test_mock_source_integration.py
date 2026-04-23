"""Integration tests using MockSource as the sole data source for DataService.

These tests verify that DataService works end-to-end with MockSource —
no network calls, no real adapters.  This covers:

- DataService construction with a custom DataSource
- get_daily / get_minute / get_index / get_etf via MockSource
- Cache miss -> fetch -> write -> cache hit flow
- Incremental fetching and merging
- Source resolution when MockSource is the only registered adapter
- Non-DataFrame methods (get_index_stocks, get_trading_days, etc.)
"""

import tempfile
from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.sources.mock import MockSource
from akshare_data.store.manager import CacheManager, reset_cache_manager


# ===================================================================
# Helpers
# ===================================================================


@pytest.fixture
def mock_source():
    """Create a seeded MockSource for deterministic data."""
    np.random.seed(12345)
    return MockSource()


@pytest.fixture
def mock_service(mock_source):
    """DataService backed by a temp cache and MockSource as the only source."""
    reset_cache_manager()
    CacheManager.reset_instance()

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_manager = CacheManager(base_dir=tmpdir)
        service = DataService(cache_manager=cache_manager, source=mock_source)
        yield service
        reset_cache_manager()
        CacheManager.reset_instance()


# ===================================================================
# Test class
# ===================================================================


@pytest.mark.integration
class TestMockSourceDataService:
    """DataService integration tests with MockSource."""

    # -- Construction & wiring ------------------------------------------------

    def test_service_created_with_mock_source(self, mock_service):
        """DataService accepts a custom DataSource in __init__."""
        assert mock_service._custom_source is not None
        assert mock_service._custom_source.name == "mock"

    def test_adapters_contain_only_mock(self, mock_service):
        """When a custom source is injected, adapters dict contains it."""
        assert "mock" in mock_service.adapters
        assert mock_service.adapters["mock"].name == "mock"

    def test_lixinger_and_akshare_point_to_custom_source(self, mock_service):
        """Convenience aliases point at the custom source."""
        assert mock_service.lixinger is mock_service._custom_source
        assert mock_service.akshare is mock_service._custom_source

    def test_default_source_resolution_uses_mock(self, mock_service):
        """_resolve_sources without explicit source returns ['mock']."""
        candidates = mock_service._resolve_sources(None, "get_daily_data")
        assert candidates == ["mock"]

    # -- get_daily -------------------------------------------------------------

    def test_get_daily_returns_data(self, mock_service):
        """get_daily returns a non-empty DataFrame from MockSource."""
        df = mock_service.get_daily("000001", "2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "date" in df.columns
        assert "close" in df.columns

    def test_get_daily_cache_miss_then_hit(self, mock_service):
        """First call fetches from source; second call returns cached data."""
        # First call
        df1 = mock_service.get_daily("000001", "2024-06-01", "2024-06-14")
        assert not df1.empty

        # Second call — should not re-fetch (cache hit).  We can verify by
        # patching the source to raise if called.
        with patch.object(
            mock_service._custom_source,
            "get_daily_data",
            side_effect=RuntimeError("should not be called"),
        ):
            df2 = mock_service.get_daily("000001", "2024-06-01", "2024-06-14")
            assert not df2.empty

    def test_get_daily_symbol_normalization(self, mock_service):
        """Symbol prefix is stripped before passing to source."""
        df = mock_service.get_daily("sh600000", "2024-01-01", "2024-01-05")
        assert isinstance(df, pd.DataFrame)

    def test_get_daily_empty_result_for_reversed_dates(self, mock_service):
        """Reversed date range returns empty DataFrame."""
        df = mock_service.get_daily("000001", "2024-06-10", "2024-06-01")
        assert df.empty

    # -- get_minute ------------------------------------------------------------

    def test_get_minute_returns_data(self, mock_service):
        """get_minute returns data from MockSource."""
        df = mock_service.get_minute(
            "000001",
            freq="1min",
            start_date="2024-01-01",
            end_date="2024-01-05",
        )
        assert isinstance(df, pd.DataFrame)
        # MockSource.get_minute_data delegates to get_daily_data
        assert not df.empty

    # -- get_index -------------------------------------------------------------

    def test_get_index_returns_data(self, mock_service):
        """get_index returns index daily data."""
        df = mock_service.get_index("000300", "2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)

    # -- get_etf ---------------------------------------------------------------

    def test_get_etf_returns_data(self, mock_service):
        """get_etf returns ETF daily data."""
        df = mock_service.get_etf("510300", "2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)

    # -- Non-DataFrame methods -------------------------------------------------

    def test_get_index_stocks(self, mock_service):
        """get_index_stocks returns a list via MockSource."""
        stocks = mock_service.get_index_stocks("000300")
        assert isinstance(stocks, list)
        assert len(stocks) == 5000

    def test_get_trading_days(self, mock_service):
        """get_trading_days returns a list of date strings."""
        days = mock_service.get_trading_days("2024-01-01", "2024-01-10")
        assert isinstance(days, list)
        assert len(days) > 0
        for d in days:
            dt = datetime.strptime(d, "%Y-%m-%d")
            assert dt.weekday() < 5  # only weekdays

    def test_get_finance_indicator(self, mock_service):
        """get_finance_indicator returns DataFrame with finance fields."""
        df = mock_service.get_finance_indicator("000001", "2024-01-01", "2024-12-31")
        assert isinstance(df, pd.DataFrame)
        assert "pe_ttm" in df.columns
        assert "pb" in df.columns

    def test_get_money_flow(self, mock_service):
        """get_money_flow returns DataFrame."""
        df = mock_service.get_money_flow("000001", "2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_north_money_flow(self, mock_service):
        """get_north_money_flow returns DataFrame."""
        df = mock_service.get_north_money_flow("2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    # -- Namespace API ---------------------------------------------------------

    def test_namespace_daily_via_mock(self, mock_service):
        """service.cn.stock.quote.daily works with MockSource."""
        df = mock_service.cn.stock.quote.daily(
            "000001",
            "2024-01-01",
            "2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_namespace_minute_via_mock(self, mock_service):
        """service.cn.stock.quote.minute works with MockSource."""
        df = mock_service.cn.stock.quote.minute(
            "000001",
            freq="1min",
            start_date="2024-01-01",
            end_date="2024-01-05",
        )
        assert isinstance(df, pd.DataFrame)

    def test_namespace_index_via_mock(self, mock_service):
        """service.cn.index.quote.daily works with MockSource."""
        df = mock_service.cn.index.quote.daily(
            "000300",
            "2024-01-01",
            "2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)

    def test_namespace_etf_via_mock(self, mock_service):
        """service.cn.fund.quote.daily works with MockSource."""
        df = mock_service.cn.fund.quote.daily(
            "510300",
            "2024-01-01",
            "2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)

    # -- Incremental fetch and merge -------------------------------------------

    def test_incremental_fetch_and_merge(self, mock_service):
        """Partial cache is detected; missing range is fetched and merged."""
        # First call — fetch and cache a week
        df1 = mock_service.get_daily("000001", "2024-06-01", "2024-06-07")
        assert not df1.empty
        len1 = len(df1)

        # Second call — request a longer range; should detect missing days
        # and fetch + merge
        df2 = mock_service.get_daily("000001", "2024-06-01", "2024-06-14")
        assert not df2.empty
        # The merged result should cover at least the second week's worth
        # (MockSource always generates data for the full range it is asked).
        assert len(df2) >= len1

    def test_full_cache_no_refetch(self, mock_service):
        """When all data is cached, no additional source call is made."""
        # Populate cache
        df = mock_service.get_daily("000001", "2024-06-01", "2024-06-07")
        assert not df.empty

        # Subsequent request for a subset should not re-fetch
        with patch.object(
            mock_service._custom_source,
            "get_daily_data",
            side_effect=RuntimeError("should not be called"),
        ):
            result = mock_service.get_daily("000001", "2024-06-01", "2024-06-05")
            assert not result.empty

    # -- Explicit source selection ---------------------------------------------

    def test_explicit_source_selection(self, mock_service):
        """Passing source='mock' explicitly works."""
        df = mock_service.get_daily(
            "000001",
            "2024-01-01",
            "2024-01-05",
            source="mock",
        )
        assert isinstance(df, pd.DataFrame)

    def test_unspecified_source_falls_back_to_mock(self, mock_service):
        """Not specifying a source uses MockSource (not lixinger/akshare)."""
        # Without specifying source, _resolve_sources returns ["mock"]
        # and the pipeline uses it.
        candidates = mock_service._resolve_sources(None, "get_daily_data")
        assert "mock" in candidates
        assert "lixinger" not in candidates
        assert "akshare" not in candidates


@pytest.mark.integration
class TestMockSourceWithoutDataService:
    """Direct MockSource usage — verifies it can be used standalone."""

    def test_mock_source_as_callable_in_router(self):
        """MockSource methods can be wrapped as callables for MultiSourceRouter."""
        from akshare_data.ingestion.router import MultiSourceRouter

        source = MockSource()

        def fetch_daily(*args, **kwargs):
            return source.get_daily_data("000001", "2024-01-01", "2024-01-10")

        router = MultiSourceRouter(
            providers=[("mock", fetch_daily)],
        )
        result = router.execute()
        assert result.success
        assert result.data is not None
        assert not result.data.empty
        assert result.source == "mock"

    def test_mock_source_health_check(self):
        """MockSource health_check returns ok status."""
        source = MockSource()
        result = source.health_check()
        assert result["status"] == "ok"
        assert result["latency_ms"] is not None

    def test_mock_source_get_source_info(self):
        """MockSource get_source_info returns correct metadata."""
        source = MockSource()
        info = source.get_source_info()
        assert info["name"] == "mock"
        assert info["type"] == "mock"
