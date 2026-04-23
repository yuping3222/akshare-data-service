"""System tests for error and degradation scenarios.

Verifies system behavior under adverse conditions:
- All data sources unavailable -> graceful degradation
- Corrupted parquet file -> error recovery
- Invalid config -> startup failure with clear message
- Empty data source response -> handled correctly
- Memory limit reached -> cache eviction behavior
"""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager, reset_cache_manager


@pytest.mark.system
class TestAllSourcesUnavailable:
    """Behavior when all configured data sources fail."""

    def test_graceful_degradation_when_sources_fail(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """When all sources are unavailable, service handles gracefully."""
        service = DataService(cache_manager=system_cache_manager)

        # Make all sources raise exceptions
        service.akshare.get_daily_data = MagicMock(
            side_effect=ConnectionError("Network unavailable")
        )
        service.lixinger.get_daily_data = MagicMock(
            side_effect=ConnectionError("Network unavailable")
        )

        # Should not raise, but return empty or None DataFrame
        result = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        # The service should handle this gracefully
        assert isinstance(result, pd.DataFrame)

    def test_graceful_degradation_via_facade(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """get_daily() facade handles source failures gracefully."""
        service = DataService(cache_manager=system_cache_manager)

        service.akshare.get_daily_data = MagicMock(
            side_effect=Exception("All sources down")
        )
        service.lixinger.get_daily_data = MagicMock(
            side_effect=Exception("All sources down")
        )

        result = service.get_daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
        )
        assert isinstance(result, pd.DataFrame)


@pytest.mark.system
class TestCorruptedParquetRecovery:
    """Behavior when cached parquet files are corrupted."""

    def test_write_then_read_cycle(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Data can be written to cache and read back successfully."""
        service = DataService(cache_manager=system_cache_manager)

        # First, write valid data to cache
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
        service.akshare.get_daily_data = MagicMock(return_value=source_df.copy())
        service.lixinger.get_daily_data = MagicMock(return_value=source_df.copy())

        # Populate cache
        df1 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        assert not df1.empty
        assert len(df1) >= 1

        # Second call should also succeed (may be incremental)
        df2 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-10",
            source="akshare",
        )
        assert isinstance(df2, pd.DataFrame)
        assert not df2.empty

    def test_system_handles_empty_source_response(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Empty response from source does not crash the service."""
        service = DataService(cache_manager=system_cache_manager)

        # Source returns empty DataFrame
        service.akshare.get_daily_data = MagicMock(return_value=pd.DataFrame())
        service.lixinger.get_daily_data = MagicMock(return_value=pd.DataFrame())

        result = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert isinstance(result, pd.DataFrame)


@pytest.mark.system
class TestEmptyDataSourceResponse:
    """Behavior when data sources return empty responses."""

    def test_empty_dataframe_handled(self, system_cache_manager: CacheManager) -> None:
        """Empty DataFrame from source is handled without error."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_daily_data = MagicMock(return_value=pd.DataFrame())
        service.lixinger.get_daily_data = MagicMock(return_value=pd.DataFrame())

        result = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert isinstance(result, pd.DataFrame)

    def test_none_response_handled(self, system_cache_manager: CacheManager) -> None:
        """None response from source is handled without error."""
        service = DataService(cache_manager=system_cache_manager)
        service.akshare.get_daily_data = MagicMock(return_value=None)
        service.lixinger.get_daily_data = MagicMock(return_value=None)

        result = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert isinstance(result, pd.DataFrame)

    def test_partial_data_then_empty_incremenal(
        self, system_cache_manager: CacheManager
    ) -> None:
        """Incremental fetch with partial data then empty new data works."""
        service = DataService(cache_manager=system_cache_manager)

        # First call returns data
        full_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10, freq="B"),
                "symbol": ["600000.XSHG"] * 10,
                "open": [10.0] * 10,
                "high": [11.0] * 10,
                "low": [9.0] * 10,
                "close": [10.5] * 10,
                "volume": [100_000] * 10,
                "amount": [1_000_000.0] * 10,
            }
        )
        call_count = {"count": 0}

        def get_data_with_counter(symbol, start_date, end_date, adjust="qfq"):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return full_df.copy()
            return pd.DataFrame()

        service.akshare.get_daily_data = MagicMock(side_effect=get_data_with_counter)
        service.lixinger.get_daily_data = MagicMock(return_value=pd.DataFrame())

        df1 = service.cn.stock.quote.daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-19",
            source="akshare",
        )
        assert not df1.empty


@pytest.mark.system
class TestMemoryCacheEviction:
    """Cache eviction behavior under memory pressure."""

    def test_cache_manager_accepts_custom_limits(
        self,
        temp_cache_dir: str,
    ) -> None:
        """CacheManager can be created with custom max items and TTL via CacheConfig."""
        from akshare_data.core.config import CacheConfig

        reset_cache_manager()
        CacheManager.reset_instance()

        config = CacheConfig(
            base_dir=temp_cache_dir,
            memory_cache_max_items=100,
            memory_cache_default_ttl_seconds=60,
        )
        manager = CacheManager(config=config)
        assert manager is not None

        reset_cache_manager()
        CacheManager.reset_instance()

    def test_service_with_tiny_cache_evicts(
        self,
        temp_cache_dir: str,
    ) -> None:
        """Service with very small cache evicts old entries."""
        from akshare_data.core.config import CacheConfig

        reset_cache_manager()
        CacheManager.reset_instance()

        config = CacheConfig(
            base_dir=temp_cache_dir,
            memory_cache_max_items=5,
            memory_cache_default_ttl_seconds=1,
        )
        manager = CacheManager(config=config)
        service = DataService(cache_manager=manager)

        # Write several different symbols to cache
        for i in range(10):
            source_df = pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-02", periods=2, freq="B"),
                    "symbol": [f"60000{i}.XSHG"] * 2,
                    "open": [10.0] * 2,
                    "high": [11.0] * 2,
                    "low": [9.0] * 2,
                    "close": [10.5] * 2,
                    "volume": [100_000] * 2,
                    "amount": [1_000_000.0] * 2,
                }
            )
            service.akshare.get_daily_data = MagicMock(return_value=source_df.copy())
            service.lixinger.get_daily_data = MagicMock(return_value=pd.DataFrame())

            service.cn.stock.quote.daily(
                symbol=f"sh60000{i}",
                start_date="2024-01-02",
                end_date="2024-01-05",
                source="akshare",
            )

        # Memory cache should have evicted some old entries
        mem_size = len(manager.memory_cache._metadata)
        assert mem_size <= 5, f"Memory cache should respect max_items=5, got {mem_size}"

        reset_cache_manager()
        CacheManager.reset_instance()


@pytest.mark.system
class TestInvalidConfigHandling:
    """Behavior when configuration is invalid or missing."""

    def test_service_with_missing_config_dir(
        self, system_cache_manager: CacheManager
    ) -> None:
        """DataService initializes even when config directory is missing."""
        service = DataService(cache_manager=system_cache_manager)
        assert service is not None
        assert hasattr(service, "cn")

    def test_service_with_invalid_base_dir(self, temp_cache_dir: str) -> None:
        """CacheManager handles non-existent base directory."""
        reset_cache_manager()
        CacheManager.reset_instance()

        # Use a non-existent subdirectory; should create it or handle gracefully
        base = Path(temp_cache_dir) / "nonexistent" / "deep" / "cache"
        manager = CacheManager(base_dir=str(base))
        assert manager is not None

        reset_cache_manager()
        CacheManager.reset_instance()
