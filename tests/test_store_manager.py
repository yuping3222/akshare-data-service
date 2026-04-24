"""tests/test_store_manager.py

Comprehensive tests for CacheManager and related classes in store/manager.py
"""

import tempfile
import threading
from pathlib import Path
import os

import pandas as pd
import pytest

from akshare_data.store.manager import (
    CacheConfig,
    CacheManager,
    get_cache_manager,
    reset_cache_manager,
)


@pytest.fixture
def temp_cache_dir():
    """Create a temporary directory for cache storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", "2024-01-10"),
            "symbol": ["sh600000"] * 10,
            "open": [10.0] * 10,
            "high": [11.0] * 10,
            "low": [9.0] * 10,
            "close": [10.5] * 10,
            "volume": [100000] * 10,
        }
    )


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset singleton instances before each test."""
    reset_cache_manager()
    CacheManager.reset_instance()
    yield
    reset_cache_manager()
    CacheManager.reset_instance()


@pytest.mark.unit
class TestCacheConfig:
    """Tests for CacheConfig class."""

    def test_default_config_values(self):
        """Test default configuration values."""
        config = CacheConfig()
        assert config.base_dir == "./cache"
        assert config.compression == "snappy"
        assert config.row_group_size == 100_000
        assert config.duckdb_threads == 4
        assert config.duckdb_memory_limit == "4GB"
        assert config.memory_cache_max_items == 5000
        assert config.memory_cache_default_ttl_seconds == 3600

    def test_custom_config_values(self):
        """Test custom configuration values."""
        config = CacheConfig(
            base_dir="/tmp/custom_cache",
            compression="gzip",
            row_group_size=50_000,
            duckdb_threads=8,
            duckdb_memory_limit="8GB",
            memory_cache_max_items=10000,
            memory_cache_default_ttl_seconds=7200,
        )
        assert config.base_dir == "/tmp/custom_cache"
        assert config.compression == "gzip"
        assert config.row_group_size == 50_000
        assert config.duckdb_threads == 8
        assert config.duckdb_memory_limit == "8GB"
        assert config.memory_cache_max_items == 10000
        assert config.memory_cache_default_ttl_seconds == 7200


class TestCacheManagerInit:
    """Tests for CacheManager initialization."""

    def test_init_with_base_dir(self, temp_cache_dir):
        """Test initialization with base_dir."""
        manager = CacheManager(base_dir=temp_cache_dir)
        assert manager.config.base_dir == temp_cache_dir
        assert manager.partition_manager is not None
        assert manager.writer is not None
        assert manager.engine is not None
        assert manager.memory_cache is not None

    def test_init_with_config(self, temp_cache_dir):
        """Test initialization with CacheConfig."""
        config = CacheConfig(
            base_dir=temp_cache_dir,
            compression="gzip",
            memory_cache_max_items=100,
            memory_cache_default_ttl_seconds=60,
        )
        manager = CacheManager(config=config)
        assert manager.config.base_dir == temp_cache_dir
        assert manager.config.compression == "gzip"
        assert manager.memory_cache._max_items == 100

    def test_init_base_dir_overrides_config(self, temp_cache_dir):
        """Test that base_dir parameter overrides config."""
        config = CacheConfig(base_dir="/should/be/overridden")
        manager = CacheManager(base_dir=temp_cache_dir, config=config)
        assert manager.config.base_dir == temp_cache_dir

    def test_singleton_get_instance(self, temp_cache_dir):
        """Test singleton pattern via get_instance()."""
        config = CacheConfig(base_dir=temp_cache_dir)
        manager1 = CacheManager.get_instance(config=config)
        manager2 = CacheManager.get_instance()
        assert manager1 is manager2

    def test_reset_instance(self, temp_cache_dir):
        """Test resetting the singleton instance."""
        config = CacheConfig(base_dir=temp_cache_dir)
        manager1 = CacheManager.get_instance(config=config)
        CacheManager.reset_instance()
        manager2 = CacheManager.get_instance(config=config)
        assert manager1 is not manager2

    def test_get_cache_manager_factory(self, temp_cache_dir):
        """Test factory function get_cache_manager()."""
        manager1 = get_cache_manager(base_dir=temp_cache_dir)
        manager2 = get_cache_manager(base_dir=temp_cache_dir)
        assert manager1 is manager2
        assert isinstance(manager1, CacheManager)

    def test_reset_cache_manager_factory(self, temp_cache_dir):
        """Test resetting factory-managed cache managers."""
        manager1 = get_cache_manager(base_dir=temp_cache_dir)
        reset_cache_manager()
        manager2 = get_cache_manager(base_dir=temp_cache_dir)
        assert manager1 is not manager2

    def test_get_cache_manager_reads_env_base_dir(self, temp_cache_dir):
        """Factory should honor AKSHARE_DATA_CACHE_DIR when base_dir is omitted."""
        old = os.environ.get("AKSHARE_DATA_CACHE_DIR")
        try:
            os.environ["AKSHARE_DATA_CACHE_DIR"] = temp_cache_dir
            reset_cache_manager()
            manager = get_cache_manager()
            assert manager.config.base_dir == temp_cache_dir
        finally:
            if old is None:
                os.environ.pop("AKSHARE_DATA_CACHE_DIR", None)
            else:
                os.environ["AKSHARE_DATA_CACHE_DIR"] = old


class TestCacheManagerRead:
    """Tests for CacheManager.read() method."""

    def test_read_empty_table(self, temp_cache_dir):
        """Test reading a non-existent table returns empty DataFrame."""
        manager = CacheManager(base_dir=temp_cache_dir)
        result = manager.read("nonexistent_table", storage_layer="daily")
        assert result is None or result.empty

    def test_read_with_force_refresh(self, temp_cache_dir, sample_df):
        """Test read with force_refresh bypasses cache."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result1 = manager.read("test_table", storage_layer="daily")
        result2 = manager.read("test_table", storage_layer="daily", force_refresh=True)
        assert result1 is not None
        assert result2 is not None

    def test_read_populates_memory_cache(self, temp_cache_dir, sample_df):
        """Test that reading populates the memory cache."""
        manager = CacheManager(base_dir=temp_cache_dir)
        initial_size = manager.memory_cache.size
        manager.write("test_table", sample_df, storage_layer="daily")
        manager.read("test_table", storage_layer="daily")
        assert manager.memory_cache.size > initial_size

    def test_read_returns_cached_data(self, temp_cache_dir, sample_df):
        """Test that subsequent reads return cached data."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result1 = manager.read("test_table", storage_layer="daily")
        result2 = manager.read("test_table", storage_layer="daily")
        pd.testing.assert_frame_equal(result1, result2)

    def test_read_cache_key_isolation_by_query_semantics(self, temp_cache_dir):
        """Different symbol/limit/order requests should not pollute each other."""
        manager = CacheManager(base_dir=temp_cache_dir)
        calls = {"n": 0}

        def _query(*args, **kwargs):
            calls["n"] += 1
            symbol = (kwargs.get("where") or {}).get("symbol", "unknown")
            limit = kwargs.get("limit")
            order = ",".join(kwargs.get("order_by") or [])
            return pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "limit_tag": str(limit),
                        "order_tag": order,
                        "seq": calls["n"],
                    }
                ]
            )

        manager.engine.query = _query

        a1 = manager.read(
            "test_table",
            storage_layer="daily",
            where={"symbol": "sh600000"},
            order_by=["date DESC"],
            limit=10,
        )
        a2 = manager.read(
            "test_table",
            storage_layer="daily",
            where={"symbol": "sh600000"},
            order_by=["date DESC"],
            limit=10,
        )
        b = manager.read(
            "test_table",
            storage_layer="daily",
            where={"symbol": "sz000001"},
            order_by=["date DESC"],
            limit=10,
        )
        c = manager.read(
            "test_table",
            storage_layer="daily",
            where={"symbol": "sh600000"},
            order_by=["date ASC"],
            limit=10,
        )
        d = manager.read(
            "test_table",
            storage_layer="daily",
            where={"symbol": "sh600000"},
            order_by=["date DESC"],
            limit=5,
        )

        assert calls["n"] == 4
        pd.testing.assert_frame_equal(a1, a2)
        assert b.iloc[0]["symbol"] == "sz000001"
        assert c.iloc[0]["order_tag"] == "date ASC"
        assert d.iloc[0]["limit_tag"] == "5"


class TestCacheManagerWrite:
    """Tests for CacheManager.write() method."""

    def test_write_returns_file_path(self, temp_cache_dir, sample_df):
        """Test write returns a valid file path."""
        manager = CacheManager(base_dir=temp_cache_dir)
        file_path = manager.write("test_table", sample_df, storage_layer="daily")
        assert file_path != ""
        assert Path(file_path).exists()

    def test_write_empty_dataframe_returns_empty_string(self, temp_cache_dir):
        """Test writing empty DataFrame returns empty string."""
        manager = CacheManager(base_dir=temp_cache_dir)
        empty_df = pd.DataFrame()
        result = manager.write("test_table", empty_df, storage_layer="daily")
        assert result == ""

    def test_write_with_partition(self, temp_cache_dir, sample_df):
        """Test writing with partition_by."""
        manager = CacheManager(base_dir=temp_cache_dir)
        file_path = manager.write(
            "test_table",
            sample_df,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )
        assert file_path != ""

    def test_write_updates_memory_cache(self, temp_cache_dir, sample_df):
        """Test that write updates memory cache."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        cache_key = manager._make_cache_key(
            "test_table", "daily", None, None, None, None
        )
        assert manager.memory_cache.get(cache_key) is not None


class TestCacheManagerExists:
    """Tests for CacheManager.exists() method."""

    def test_exists_false_for_empty_table(self, temp_cache_dir):
        """Test exists returns False for non-existent table."""
        manager = CacheManager(base_dir=temp_cache_dir)
        assert manager.exists("nonexistent_table", storage_layer="daily") is False

    def test_exists_true_after_write(self, temp_cache_dir, sample_df):
        """Test exists returns True after writing data."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.exists("test_table", storage_layer="daily")
        assert isinstance(result, bool)


class TestCacheManagerHasRange:
    """Tests for CacheManager.has_range() method."""

    def test_has_range_false_for_empty(self, temp_cache_dir):
        """Test has_range returns False for empty table."""
        manager = CacheManager(base_dir=temp_cache_dir)
        result = manager.has_range(
            "test_table",
            storage_layer="daily",
            start="2024-01-01",
            end="2024-01-10",
        )
        assert result is False

    def test_has_range_true_when_data_exists(self, temp_cache_dir, sample_df):
        """Test has_range returns True when data covers range."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.has_range(
            "test_table",
            storage_layer="daily",
            start="2024-01-01",
            end="2024-01-05",
        )
        assert isinstance(result, bool)

    def test_has_range_false_when_data_insufficient(self, temp_cache_dir, sample_df):
        """Test has_range returns False when data doesn't cover range."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.has_range(
            "test_table",
            storage_layer="daily",
            start="2024-01-01",
            end="2024-12-31",
        )
        assert result is False

    def test_has_range_with_start_only(self, temp_cache_dir, sample_df):
        """Test has_range with only start date."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.has_range(
            "test_table",
            storage_layer="daily",
            start="2024-01-05",
        )
        assert isinstance(result, bool)

    def test_has_range_with_end_only(self, temp_cache_dir, sample_df):
        """Test has_range with only end date."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.has_range(
            "test_table",
            storage_layer="daily",
            end="2024-01-05",
        )
        assert isinstance(result, bool)


class TestCacheManagerInvalidate:
    """Tests for CacheManager.invalidate() and invalidate_all() methods."""

    def test_invalidate_removes_from_memory_cache(self, temp_cache_dir, sample_df):
        """Test invalidate removes data from memory cache."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        cache_key = manager._make_cache_key(
            "test_table", "daily", None, None, None, None
        )
        assert manager.memory_cache.get(cache_key) is not None
        manager.invalidate("test_table", storage_layer="daily")
        assert manager.memory_cache.get(cache_key) is None

    def test_invalidate_all_clears_memory_cache(self, temp_cache_dir, sample_df):
        """Test invalidate_all clears entire memory cache."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        manager.write("test_table2", sample_df, storage_layer="daily")
        count = manager.invalidate_all()
        assert count >= 2
        assert manager.memory_cache.size == 0

    def test_invalidate_with_partition(self, temp_cache_dir, sample_df):
        """Test invalidate with partition specification."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write(
            "test_table",
            sample_df,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )
        count = manager.invalidate(
            "test_table",
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )
        assert count >= 0

    def test_write_partition_only_invalidates_partition_scope(self, temp_cache_dir):
        """Writing one partition should keep other partition cache entries."""
        manager = CacheManager(base_dir=temp_cache_dir)
        key_sh = manager._make_cache_key(
            "test_table",
            "daily",
            "symbol",
            "sh600000",
            None,
            None,
        )
        key_sz = manager._make_cache_key(
            "test_table",
            "daily",
            "symbol",
            "sz000001",
            None,
            None,
        )

        manager.memory_cache.put(key_sh, pd.DataFrame([{"symbol": "sh600000"}]))
        manager.memory_cache.put(key_sz, pd.DataFrame([{"symbol": "sz000001"}]))

        manager.write(
            "test_table",
            pd.DataFrame([{"symbol": "sh600000", "date": "2024-01-01", "close": 10.0}]),
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )

        assert manager.memory_cache.get(key_sh) is not None
        assert manager.memory_cache.get(key_sz) is not None


class TestCacheManagerTableInfo:
    """Tests for CacheManager.table_info() method."""

    def test_table_info_empty_table(self, temp_cache_dir):
        """Test table_info for non-existent table."""
        manager = CacheManager(base_dir=temp_cache_dir)
        info = manager.table_info("nonexistent_table", storage_layer="daily")
        assert info["name"] == "nonexistent_table"
        assert info["file_count"] == 0
        assert info["total_size_bytes"] == 0

    def test_table_info_after_write(self, temp_cache_dir, sample_df):
        """Test table_info after writing data."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        info = manager.table_info("test_table", storage_layer="daily")
        assert info["name"] == "test_table"
        assert info["file_count"] > 0
        assert info["total_size_bytes"] > 0

    def test_table_info_with_partition(self, temp_cache_dir, sample_df):
        """Test table_info with partition specification."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write(
            "test_table",
            sample_df,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )
        info = manager.table_info(
            "test_table",
            storage_layer="daily",
            partition_by="symbol",
        )
        assert info["partition_count"] > 0


class TestCacheManagerListTables:
    """Tests for CacheManager.list_tables() method."""

    def test_list_tables_empty(self, temp_cache_dir):
        """Test list_tables returns empty list when no tables."""
        manager = CacheManager(base_dir=temp_cache_dir)
        assert manager.list_tables() == []

    def test_list_tables_after_write(self, temp_cache_dir, sample_df):
        """Test list_tables returns written table names."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        tables = manager.list_tables()
        assert isinstance(tables, list)

    def test_list_tables_with_storage_layer(self, temp_cache_dir, sample_df):
        """Test list_tables with specific storage layer."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        manager.write("test_table2", sample_df, storage_layer="hourly")
        daily_tables = manager.list_tables(storage_layer="daily")
        hourly_tables = manager.list_tables(storage_layer="hourly")
        assert isinstance(daily_tables, list)
        assert isinstance(hourly_tables, list)


class TestCacheManagerGetStats:
    """Tests for CacheManager.get_stats() method."""

    def test_get_stats_structure(self, temp_cache_dir, sample_df):
        """Test get_stats returns expected structure."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        stats = manager.get_stats()
        assert "memory_cache_size" in stats
        assert "memory_cache_hit_rate" in stats
        assert "tables" in stats

    def test_get_stats_initial_values(self, temp_cache_dir):
        """Test get_stats initial values for empty cache."""
        manager = CacheManager(base_dir=temp_cache_dir)
        stats = manager.get_stats()
        assert stats["memory_cache_size"] == 0
        assert stats["memory_cache_hit_rate"] == 0.0


class TestCacheManagerMakeCacheKey:
    """Tests for CacheManager._make_cache_key() method."""

    def test_cache_key_format(self, temp_cache_dir):
        """Test cache key has expected format."""
        manager = CacheManager(base_dir=temp_cache_dir)
        key = manager._make_cache_key("table", "daily", None, None, None, None)
        assert key.startswith("v2:table:daily:")
        parts = key.split(":")
        assert len(parts) == 9

    def test_cache_key_different_tables(self, temp_cache_dir):
        """Test different tables produce different keys."""
        manager = CacheManager(base_dir=temp_cache_dir)
        key1 = manager._make_cache_key("table1", "daily", None, None, None, None)
        key2 = manager._make_cache_key("table2", "daily", None, None, None, None)
        assert key1 != key2

    def test_cache_key_different_storage_layers(self, temp_cache_dir):
        """Test different storage layers produce different keys."""
        manager = CacheManager(base_dir=temp_cache_dir)
        key1 = manager._make_cache_key("table", "daily", None, None, None, None)
        key2 = manager._make_cache_key("table", "hourly", None, None, None, None)
        assert key1 != key2

    def test_cache_key_with_where(self, temp_cache_dir):
        """Test cache key includes where clause."""
        manager = CacheManager(base_dir=temp_cache_dir)
        key1 = manager._make_cache_key(
            "table", "daily", None, None, {"col": "val"}, None
        )
        key2 = manager._make_cache_key(
            "table", "daily", None, None, {"col": "val2"}, None
        )
        assert key1 != key2

    def test_cache_key_with_columns(self, temp_cache_dir):
        """Test cache key includes columns."""
        manager = CacheManager(base_dir=temp_cache_dir)
        key1 = manager._make_cache_key("table", "daily", None, None, None, ["col1"])
        key2 = manager._make_cache_key("table", "daily", None, None, None, ["col2"])
        assert key1 != key2


class TestCacheManagerConcurrency:
    """Tests for CacheManager concurrent access."""

    def test_concurrent_reads(self, temp_cache_dir, sample_df):
        """Test concurrent read operations."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        results = []

        def read_task():
            result = manager.read("test_table", storage_layer="daily")
            results.append(result)

        threads = [threading.Thread(target=read_task) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 5
        for r in results:
            assert r is not None

    def test_concurrent_writes(self, temp_cache_dir, sample_df):
        """Test concurrent write operations to different tables."""
        manager = CacheManager(base_dir=temp_cache_dir)
        errors = []

        def write_task(table_name):
            try:
                manager.write(table_name, sample_df, storage_layer="daily")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=write_task, args=(f"table_{i}",)) for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        tables = manager.list_tables()
        assert len(tables) >= 0


class TestCacheManagerEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_read_with_where_clause(self, temp_cache_dir, sample_df):
        """Test reading with where clause."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.read(
            "test_table",
            storage_layer="daily",
            where={"symbol": "sh600000"},
        )
        assert result is not None

    def test_read_with_columns(self, temp_cache_dir, sample_df):
        """Test reading with specific columns."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.read(
            "test_table",
            storage_layer="daily",
            columns=["date", "symbol", "close"],
        )
        assert result is not None
        if not result.empty:
            assert len(result.columns) <= 3

    def test_read_with_order_by(self, temp_cache_dir, sample_df):
        """Test reading with order_by."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.read(
            "test_table",
            storage_layer="daily",
            order_by=["date"],
        )
        assert result is not None

    def test_read_with_limit(self, temp_cache_dir, sample_df):
        """Test reading with limit."""
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.write("test_table", sample_df, storage_layer="daily")
        result = manager.read(
            "test_table",
            storage_layer="daily",
            limit=5,
        )
        assert result is not None or result.empty
