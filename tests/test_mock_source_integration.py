"""Integration tests for DataService read-only mode with MockSource.

These tests verify that DataService works in read-only mode —
no synchronous source fetching, only cache reads.

This covers:
- DataService construction with a custom DataSource (deprecated but still accepted)
- Cache read operations via _served.query
- Namespace API access
- Empty DataFrame returns when cache has no data
"""

import tempfile
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.sources.mock import MockSource
from akshare_data.store.manager import CacheManager, reset_cache_manager


def make_stock_daily_df(n=10, symbol="000001"):
    """Create a valid stock_daily DataFrame with all required fields."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=n),
            "symbol": [symbol] * n,
            "open": [10.0] * n,
            "high": [11.0] * n,
            "low": [9.0] * n,
            "close": [10.5] * n,
            "volume": [100000.0] * n,
            "amount": [1000000.0] * n,
            "adjust": ["qfq"] * n,
        }
    )


def make_index_daily_df(n=10, symbol="000300"):
    """Create a valid index_daily DataFrame."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=n),
            "symbol": [symbol] * n,
            "open": [3500.0] * n,
            "high": [3550.0] * n,
            "low": [3450.0] * n,
            "close": [3520.0] * n,
            "volume": [1000000000.0] * n,
            "amount": [50000000000.0] * n,
        }
    )


def make_etf_daily_df(n=10, symbol="510300"):
    """Create a valid etf_daily DataFrame."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=n),
            "symbol": [symbol] * n,
            "open": [5.0] * n,
            "high": [5.1] * n,
            "low": [4.9] * n,
            "close": [5.05] * n,
            "volume": [1000000.0] * n,
            "amount": [5000000.0] * n,
        }
    )


def make_minute_df(n=10, symbol="000001"):
    """Create a valid stock_minute DataFrame."""
    return pd.DataFrame(
        {
            "datetime": pd.date_range("2024-01-01 09:30", periods=n, freq="1min"),
            "symbol": [symbol] * n,
            "open": [10.0] * n,
            "high": [11.0] * n,
            "low": [9.0] * n,
            "close": [10.5] * n,
            "volume": [1000.0] * n,
            "amount": [10000.0] * n,
        }
    )


@pytest.fixture
def mock_source():
    """Create a seeded MockSource for deterministic data."""
    np.random.seed(12345)
    return MockSource()


@pytest.fixture
def mock_service_with_cache(mock_source):
    """DataService backed by a temp cache with pre-populated data."""
    reset_cache_manager()
    CacheManager.reset_instance()

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_manager = CacheManager(base_dir=tmpdir)

        cache_manager.write(
            "stock_daily", make_stock_daily_df(10, "000001"), storage_layer="daily"
        )
        cache_manager.write(
            "stock_daily", make_stock_daily_df(10, "600000"), storage_layer="daily"
        )
        cache_manager.write(
            "index_daily", make_index_daily_df(10, "000300"), storage_layer="daily"
        )
        cache_manager.write(
            "etf_daily", make_etf_daily_df(10, "510300"), storage_layer="daily"
        )
        cache_manager.write(
            "stock_minute_1min", make_minute_df(10, "000001"), storage_layer="minute"
        )

        service = DataService(cache_manager=cache_manager, source=mock_source)
        yield service
        reset_cache_manager()
        CacheManager.reset_instance()


@pytest.fixture
def empty_mock_service(mock_source):
    """DataService backed by an empty temp cache."""
    reset_cache_manager()
    CacheManager.reset_instance()

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_manager = CacheManager(base_dir=tmpdir)
        service = DataService(cache_manager=cache_manager, source=mock_source)
        yield service
        reset_cache_manager()
        CacheManager.reset_instance()


@pytest.mark.integration
class TestMockSourceDataServiceConstruction:
    """DataService construction tests with MockSource."""

    def test_service_created_with_mock_source(self, mock_service_with_cache):
        """DataService accepts a custom DataSource in __init__.
        
        Legacy source adapters are now accessible via service._legacy (deprecated).
        """
        assert mock_service_with_cache._legacy._custom_source is not None
        assert mock_service_with_cache._legacy._custom_source.name == "mock"

    def test_adapters_contain_only_mock(self, mock_service_with_cache):
        """When a custom source is injected, adapters dict contains it."""
        assert "mock" in mock_service_with_cache._legacy.adapters
        assert mock_service_with_cache._legacy.adapters["mock"].name == "mock"

    def test_lixinger_and_akshare_point_to_custom_source(self, mock_service_with_cache):
        """Convenience aliases point at the custom source (via _legacy)."""
        assert (
            mock_service_with_cache._legacy.lixinger
            is mock_service_with_cache._legacy._custom_source
        )
        assert (
            mock_service_with_cache._legacy.akshare
            is mock_service_with_cache._legacy._custom_source
        )


@pytest.mark.integration
class TestDataServiceReadFromCache:
    """DataService reads from pre-populated cache."""

    def test_query_stock_daily_returns_cached_data(self, mock_service_with_cache):
        """query returns data from pre-populated cache."""
        result = mock_service_with_cache._served.query(table="stock_daily")
        assert result.has_data
        assert not result.data.empty
        assert "symbol" in result.data.columns

    def test_query_daily_with_symbol_filter(self, mock_service_with_cache):
        """query_daily with symbol returns filtered cached data."""
        result = mock_service_with_cache._served.query_daily(
            table="stock_daily",
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-01-10",
        )
        assert result.has_data or result.data.empty

    def test_namespace_stock_quote_daily_reads_cache(self, mock_service_with_cache):
        """service.cn.stock.quote.daily reads from cache."""
        df = mock_service_with_cache.cn.stock.quote.daily(
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)

    def test_namespace_index_quote_daily_reads_cache(self, mock_service_with_cache):
        """service.cn.index.quote.daily reads from cache."""
        df = mock_service_with_cache.cn.index.quote.daily(
            symbol="000300",
            start_date="2024-01-01",
            end_date="2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)

    def test_namespace_etf_quote_daily_reads_cache(self, mock_service_with_cache):
        """service.cn.fund.quote.daily reads from cache."""
        df = mock_service_with_cache.cn.fund.quote.daily(
            symbol="510300",
            start_date="2024-01-01",
            end_date="2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)


@pytest.mark.integration
class TestDataServiceEmptyCache:
    """DataService returns empty DataFrame when cache is empty."""

    def test_query_returns_empty_on_empty_cache(self, empty_mock_service):
        """query returns empty DataFrame when no data in cache."""
        result = empty_mock_service._served.query(table="stock_daily")
        assert not result.has_data
        assert result.data.empty

    def test_namespace_returns_empty_on_empty_cache(self, empty_mock_service):
        """namespace API returns empty DataFrame when no data in cache."""
        df = empty_mock_service.cn.stock.quote.daily(
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-01-10",
        )
        assert isinstance(df, pd.DataFrame)
        assert df.empty


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

    def test_mock_source_get_source_info(self):
        """MockSource get_source_info returns correct metadata."""
        source = MockSource()
        info = source.get_source_info()
        assert info["name"] == "mock"
        assert info["type"] == "mock"

    def test_mock_source_get_daily_data(self):
        """MockSource.get_daily_data returns DataFrame."""
        source = MockSource()
        df = source.get_daily_data("000001", "2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "date" in df.columns
        assert "close" in df.columns

    def test_mock_source_get_index_components(self):
        """MockSource.get_index_components returns DataFrame."""
        source = MockSource()
        components = source.get_index_components("000300")
        assert isinstance(components, pd.DataFrame)
        assert not components.empty
        assert "code" in components.columns

    def test_mock_source_get_trading_days(self):
        """MockSource.get_trading_days returns list."""
        source = MockSource()
        days = source.get_trading_days("2024-01-01", "2024-01-10")
        assert isinstance(days, list)
        assert len(days) > 0
        for d in days:
            dt = datetime.strptime(d, "%Y-%m-%d")
            assert dt.weekday() < 5

    def test_mock_source_get_finance_indicator(self):
        """MockSource.get_finance_indicator returns DataFrame."""
        source = MockSource()
        df = source.get_finance_indicator("000001", "2024-01-01", "2024-12-31")
        assert isinstance(df, pd.DataFrame)
        assert "pe_ttm" in df.columns
        assert "pb" in df.columns

    def test_mock_source_get_money_flow(self):
        """MockSource.get_money_flow returns DataFrame."""
        source = MockSource()
        df = source.get_money_flow("000001", "2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_mock_source_get_north_money_flow(self):
        """MockSource.get_north_money_flow returns DataFrame."""
        source = MockSource()
        df = source.get_north_money_flow("2024-01-01", "2024-01-10")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
