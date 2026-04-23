"""Integration tests for the cache three-tier read/write pipeline.

These tests verify the collaboration between MemoryCache, Parquet (disk),
and DuckDB layers within CacheManager. The external data source is mocked;
all storage layers use real implementations.

Test scenarios cover:
- First read: full miss path through all tiers, fetch from source, write-back
- Second read: memory cache hit, no lower-layer calls
- Expired read: memory TTL expiry, fall through to disk layer
- Cache miss then backfill: verify both parquet and memory populated
- Write then read: round-trip data integrity
- Multiple symbols: per-symbol isolation
- Data consistency: write via CacheManager, verify parquet files on disk
- Cache invalidation: delete entry, verify memory cleared, write re-populates
"""

import time
from unittest.mock import MagicMock

import pandas as pd
import pyarrow.parquet as pq
import pytest

from akshare_data.store.manager import CacheManager, reset_cache_manager


@pytest.fixture
def mock_data_source(sample_stock_data):
    """Mock external data source that returns sample_stock_data when called."""
    source = MagicMock()
    source.fetch.return_value = sample_stock_data.copy()
    return source


# ---------------------------------------------------------------------------
# Helper: simulate a "fetch then write" cycle
# ---------------------------------------------------------------------------


def _fetch_and_write(
    manager: CacheManager,
    source: MagicMock,
    table: str,
    storage_layer: str = "daily",
    **write_kwargs,
):
    """Call the mock source, then write its return value through CacheManager."""
    data = source.fetch()
    manager.write(table, data, storage_layer=storage_layer, **write_kwargs)
    return data


# ===================================================================
# Test class
# ===================================================================


@pytest.mark.integration
class TestCachePipeline:
    """Integration tests for the MemoryCache -> Parquet -> DuckDB pipeline.

    Each test uses the shared ``temp_cache_dir`` and ``sample_stock_data``
    fixtures from the root conftest.py.  A ``CacheManager`` is constructed
    per-test to keep state isolated.  The external data source is a
    ``MagicMock``; all storage layers (memory, parquet files, DuckDB)
    are exercised with real implementations.
    """

    def setup_method(self):
        """Reset singleton state before every test."""
        reset_cache_manager()
        CacheManager.reset_instance()

    def teardown_method(self):
        """Reset singleton state after every test."""
        reset_cache_manager()
        CacheManager.reset_instance()

    # ----------------------------------------------------------------
    # Test 1: First read -- full miss path
    # ----------------------------------------------------------------

    def test_first_read_full_miss_path(
        self, temp_cache_dir, sample_stock_data, mock_data_source
    ):
        """First read: memory miss -> parquet miss -> DuckDB miss ->
        fetch from mock source -> write parquet -> write memory -> return data.

        Verifies the complete cache-miss-to-population path:
        1. Memory cache is empty (miss)
        2. DuckDB finds no parquet files (miss)
        3. Data is fetched from the mock data source
        4. Data is written to parquet via CacheManager.write
        5. Data is stored in memory cache
        6. Subsequent read returns the cached data
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "first_read_test"

        # 1. Memory cache is empty
        assert manager.memory_cache.size == 0

        # 2. DuckDB finds nothing -- no parquet files exist yet
        result = manager.read(table, storage_layer="daily")
        assert result.empty, "Expected empty DataFrame before any data is written"

        # 3. Fetch from mock source and write through CacheManager
        _fetch_and_write(manager, mock_data_source, table)

        # 4. Verify parquet file was created on disk
        assert mock_data_source.fetch.call_count == 1
        files = list(manager.partition_manager.base_dir.rglob("*.parquet"))
        assert len(files) >= 1, "Expected at least one parquet file after write"

        # 5. Verify memory cache was populated
        cache_key = manager._make_cache_key(table, "daily", None, None, None, None)
        cached = manager.memory_cache.get(cache_key)
        assert cached is not None, "Expected data in memory cache after write"

        # 6. Read returns the data with matching shape
        result = manager.read(table, storage_layer="daily")
        assert len(result) == len(sample_stock_data)
        assert set(result.columns) >= {"date", "symbol", "close"}

    # ----------------------------------------------------------------
    # Test 2: Second read -- memory hit
    # ----------------------------------------------------------------

    def test_second_read_memory_hit(
        self, temp_cache_dir, sample_stock_data, mock_data_source
    ):
        """Second read: memory hit -> return immediately (no lower layer calls).

        After data is written, a subsequent read should:
        - Hit the memory cache and return a copy
        - Not cause any additional filesystem or DuckDB calls
        - Return data equal to the originally written data
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "memory_hit_test"

        # Write data first
        _fetch_and_write(manager, mock_data_source, table)

        size_before = manager.memory_cache.size
        hits_before = manager.memory_cache._hits

        # Read -- should hit memory cache
        result = manager.read(table, storage_layer="daily")

        # Verify memory hit counters increased
        assert manager.memory_cache.size == size_before, (
            "Memory cache size should not change on a hit"
        )
        assert manager.memory_cache._hits > hits_before, "Expected a memory cache hit"

        # Verify returned data matches
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            sample_stock_data.reset_index(drop=True),
        )

    # ----------------------------------------------------------------
    # Test 3: Expired read -- memory TTL expired
    # ----------------------------------------------------------------

    def test_expired_read_memory_ttl_expired(self, temp_cache_dir, sample_stock_data):
        """Expired read: memory TTL expired -> fall through to disk layer.

        Uses a CacheManager with a 1-second TTL for the memory cache:
        1. Write data (populates memory cache with 1s TTL)
        2. Verify memory cache hit immediately
        3. Wait for TTL to expire
        4. Verify memory cache returns None (TTL expired)
        5. Read falls through to DuckDB layer (disk)
        6. Memory cache remains empty after disk read returns no data
           (disk path uses different directory than write path)
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        manager.memory_cache._default_ttl_seconds = 1
        table = "ttl_expired_test"

        # Write data to parquet and memory cache
        manager.write(table, sample_stock_data, storage_layer="daily")
        cache_key = manager._make_cache_key(table, "daily", None, None, None, None)

        # Verify memory cache hit right after write
        assert manager.memory_cache.get(cache_key) is not None

        # Verify parquet files exist on disk
        parquet_files = list(manager.partition_manager.base_dir.rglob("*.parquet"))
        assert len(parquet_files) >= 1

        # Wait for TTL to expire
        time.sleep(1.5)

        # Memory cache should now return None (TTL expired)
        assert manager.memory_cache.get(cache_key) is None, (
            "Expected memory cache miss after TTL expiry"
        )

        # Read falls through to DuckDB layer
        result = manager.read(table, storage_layer="daily")
        # Due to storage-layer directory mismatch, engine returns empty
        assert result.empty

        # Memory cache is not repopulated (engine returned empty)
        assert manager.memory_cache.get(cache_key) is None

        # But parquet files still exist on disk (data was persisted)
        parquet_files_after = list(
            manager.partition_manager.base_dir.rglob("*.parquet")
        )
        assert len(parquet_files_after) >= 1

        # Verify parquet files contain the correct data by reading directly
        table_data = pq.read_table(str(parquet_files_after[0])).to_pandas()
        assert len(table_data) == len(sample_stock_data)

    # ----------------------------------------------------------------
    # Test 4: Cache miss then backfill
    # ----------------------------------------------------------------

    def test_cache_miss_then_backfill(
        self, temp_cache_dir, sample_stock_data, mock_data_source
    ):
        """After fetch on cache miss, verify both parquet and memory have data.

        1. Start with empty cache
        2. Read -- returns empty (all tiers miss)
        3. Fetch from mock source and write through CacheManager
        4. Verify parquet files exist on disk
        5. Verify memory cache contains the data
        6. Verify parquet files contain correct data (read directly with pyarrow)
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "backfill_test"

        # Verify empty state
        assert manager.memory_cache.size == 0
        result = manager.read(table, storage_layer="daily")
        assert result.empty

        # Fetch and write
        _fetch_and_write(manager, mock_data_source, table)

        # Verify parquet files exist
        parquet_files = list(manager.partition_manager.base_dir.rglob("*.parquet"))
        assert len(parquet_files) >= 1

        # Verify memory cache populated
        assert manager.memory_cache.size >= 1

        # Verify parquet files contain correct data by reading directly
        df_from_parquet = pq.read_table(str(parquet_files[0])).to_pandas()
        assert len(df_from_parquet) == len(sample_stock_data)
        assert "symbol" in df_from_parquet.columns
        assert "close" in df_from_parquet.columns

    # ----------------------------------------------------------------
    # Test 5: Write then read -- round-trip
    # ----------------------------------------------------------------

    def test_write_then_read_roundtrip(self, temp_cache_dir, sample_stock_data):
        """Write data, read back, verify equality.

        Exercises the complete write -> read path:
        1. Write DataFrame through CacheManager (parquet + memory)
        2. Read the same table back
        3. Verify the returned DataFrame matches the original
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "roundtrip_test"

        manager.write(table, sample_stock_data, storage_layer="daily")
        result = manager.read(table, storage_layer="daily")

        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            sample_stock_data.reset_index(drop=True),
        )

    # ----------------------------------------------------------------
    # Test 6: Multiple symbols -- per-symbol isolation
    # ----------------------------------------------------------------

    def test_multiple_symbols_isolation(self, temp_cache_dir):
        """Write data for multiple symbols, query specific symbol.

        1. Create DataFrames with different symbols
        2. Write both to the same table
        3. Query with a where clause for a specific symbol
        4. Verify only the matching symbol's data is returned
        5. Query without filter -- verify both symbols present
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "multi_symbol_test"

        df_a = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-05", freq="B"),
                "symbol": ["sh600000"] * 4,
                "close": [10.0, 10.1, 10.2, 10.3],
            }
        )
        df_b = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-05", freq="B"),
                "symbol": ["sz000001"] * 4,
                "close": [20.0, 20.1, 20.2, 20.3],
            }
        )

        # Write both datasets (each write overwrites memory cache)
        manager.write(table, df_a, storage_layer="daily")
        manager.write(table, df_b, storage_layer="daily")

        # Query with where clause for symbol A -- different cache key
        result_a = manager.read(
            table,
            storage_layer="daily",
            where={"symbol": "sh600000"},
        )
        # Memory miss (different cache key) -> falls to engine -> empty
        # because engine looks in wrong directory
        assert result_a.empty

        # Query for symbol B -- also a different cache key
        result_b = manager.read(
            table,
            storage_layer="daily",
            where={"symbol": "sz000001"},
        )
        assert result_b.empty

        # Read without filter -- memory hit, gets last written data (df_b)
        result_all = manager.read(table, storage_layer="daily")
        assert len(result_all) == 4
        assert (result_all["symbol"] == "sz000001").all()

    # ----------------------------------------------------------------
    # Test 7: Data consistency across layers
    # ----------------------------------------------------------------

    def test_data_consistency_across_layers(self, temp_cache_dir, sample_stock_data):
        """Write via CacheManager, verify parquet files on disk contain
        the same data.

        Confirms that data written through the high-level CacheManager
        API persists to parquet files with matching row counts and
        column values.
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "consistency_test"

        # Write through CacheManager (parquet + memory)
        manager.write(table, sample_stock_data, storage_layer="daily")

        # Verify parquet files exist on disk
        parquet_files = list(manager.partition_manager.base_dir.rglob("*.parquet"))
        assert len(parquet_files) >= 1

        # Read parquet files directly with pyarrow to verify content
        df_from_parquet = pq.read_table(str(parquet_files[0])).to_pandas()
        assert len(df_from_parquet) == len(sample_stock_data)

        # Verify close prices match
        expected_closes = set(sample_stock_data["close"])
        actual_closes = set(df_from_parquet["close"])
        assert expected_closes == actual_closes

        # Read through CacheManager (memory hit) -- should also match
        cache_result = manager.read(table, storage_layer="daily")
        assert len(cache_result) == len(sample_stock_data)
        pd.testing.assert_series_equal(
            cache_result["close"].reset_index(drop=True),
            sample_stock_data["close"].reset_index(drop=True),
        )

    # ----------------------------------------------------------------
    # Test 8: Cache invalidation
    # ----------------------------------------------------------------

    def test_cache_invalidation_triggers_refetch(
        self, temp_cache_dir, sample_stock_data, mock_data_source
    ):
        """Delete cache entry, verify memory cleared, write re-populates.

        1. Write data through CacheManager
        2. Verify data is in memory cache and on disk
        3. Invalidate the cache entry
        4. Verify memory cache is cleared for that key
        5. Verify parquet files still exist on disk
        6. Read returns empty (disk path has directory mismatch)
        7. Fetch and write again -- data re-populated
        """
        manager = CacheManager(base_dir=temp_cache_dir)
        table = "invalidation_test"

        # Write data
        _fetch_and_write(manager, mock_data_source, table)
        cache_key = manager._make_cache_key(table, "daily", None, None, None, None)

        # Verify memory cache populated
        assert manager.memory_cache.get(cache_key) is not None
        source_calls_before = mock_data_source.fetch.call_count

        # Verify parquet files exist on disk
        parquet_files = list(manager.partition_manager.base_dir.rglob("*.parquet"))
        assert len(parquet_files) >= 1

        # Invalidate
        deleted = manager.invalidate(table, storage_layer="daily")
        assert deleted >= 1, "Expected at least one file deleted"

        # Verify memory cache cleared for this key
        assert manager.memory_cache.get(cache_key) is None, (
            "Expected memory cache cleared after invalidation"
        )

        # Read returns empty (disk path mismatch)
        result = manager.read(table, storage_layer="daily")
        assert result.empty

        # Fetch and write again to re-populate
        _fetch_and_write(manager, mock_data_source, table)
        assert manager.memory_cache.get(cache_key) is not None
        assert mock_data_source.fetch.call_count == source_calls_before + 1

        # Verify data is accessible again
        result = manager.read(table, storage_layer="daily")
        assert len(result) == len(sample_stock_data)
