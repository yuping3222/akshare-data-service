"""System-level tests for performance, memory pressure, and concurrency."""

from __future__ import annotations

import gc
import os
import threading
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from akshare_data.store.manager import CacheManager, reset_cache_manager


@pytest.fixture
def large_cache_dir():
    """Create temporary cache directory for large data tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def cache_manager_for_system_tests(large_cache_dir):
    """Create cache manager for system tests."""
    reset_cache_manager()
    CacheManager.reset_instance()
    manager = CacheManager(base_dir=large_cache_dir)
    yield manager
    reset_cache_manager()
    CacheManager.reset_instance()


def generate_large_dataframe(rows: int, columns: int = 10) -> pd.DataFrame:
    """Generate large DataFrame for testing."""
    data = {}
    for i in range(columns):
        if i == 0:
            data["date"] = pd.date_range("2024-01-01", periods=rows, freq="B")
        elif i == 1:
            data["symbol"] = ["sh600000"] * rows
        elif i < 4:
            data[f"price_{i}"] = np.random.uniform(10, 100, rows)
        elif i < 7:
            data[f"volume_{i}"] = np.random.randint(1000, 1000000, rows)
        else:
            data[f"metric_{i}"] = np.random.randn(rows)
    return pd.DataFrame(data)


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB."""
    import psutil
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


@pytest.mark.system
@pytest.mark.slow
class TestLargeDataPerformance:
    """Test performance with large datasets."""

    @pytest.mark.slow
    def test_write_100k_rows_performance(self, cache_manager_for_system_tests):
        """Test writing 100k+ rows meets performance target."""
        df = generate_large_dataframe(100000)
        start_time = time.time()
        file_path = cache_manager_for_system_tests.write(
            table="performance_test_100k",
            data=df,
            storage_layer="daily",
        )
        elapsed = time.time() - start_time
        assert elapsed < 30.0, f"Write took {elapsed}s, expected < 30s"
        assert file_path != ""

    @pytest.mark.slow
    def test_read_100k_rows_performance(self, cache_manager_for_system_tests):
        """Test reading 100k+ rows meets performance target."""
        df = generate_large_dataframe(100000)
        cache_manager_for_system_tests.write(
            table="read_test_100k",
            data=df,
            storage_layer="daily",
        )
        start_time = time.time()
        result = cache_manager_for_system_tests.read(
            table="read_test_100k",
            storage_layer="daily",
        )
        elapsed = time.time() - start_time
        assert elapsed < 10.0, f"Read took {elapsed}s, expected < 10s"
        assert len(result) == 100000

    @pytest.mark.slow
    def test_write_500k_rows_performance(self, cache_manager_for_system_tests):
        """Test writing 500k rows."""
        df = generate_large_dataframe(500000)
        start_time = time.time()
        file_path = cache_manager_for_system_tests.write(
            table="performance_test_500k",
            data=df,
            storage_layer="daily",
        )
        elapsed = time.time() - start_time
        assert elapsed < 120.0, f"Write took {elapsed}s, expected < 120s"
        assert file_path != ""

    @pytest.mark.slow
    def test_multiple_large_writes(self, cache_manager_for_system_tests):
        """Test multiple consecutive large writes."""
        elapsed_times = []
        for i in range(5):
            df = generate_large_dataframe(50000)
            start = time.time()
            cache_manager_for_system_tests.write(
                table=f"multi_write_{i}",
                data=df,
                storage_layer="daily",
            )
            elapsed_times.append(time.time() - start)
        avg_time = sum(elapsed_times) / len(elapsed_times)
        assert avg_time < 15.0, f"Average write time {avg_time}s, expected < 15s"


@pytest.mark.system
@pytest.mark.slow
class TestMemoryPressure:
    """Test memory handling under pressure."""

    @pytest.mark.slow
    def test_multiple_large_dataframes_memory(self, cache_manager_for_system_tests):
        """Test memory with multiple large DataFrames."""
        try:
            baseline_mem = get_memory_usage_mb()
        except ImportError:
            pytest.skip("psutil not available")

        dfs = []
        for i in range(10):
            dfs.append(generate_large_dataframe(20000))

        peak_mem = get_memory_usage_mb()
        mem_increase = peak_mem - baseline_mem

        for df in dfs:
            cache_manager_for_system_tests.write(
                table=f"mem_test_{i}",
                data=df,
                storage_layer="daily",
            )

        dfs.clear()
        gc.collect()

        after_mem = get_memory_usage_mb()
        assert mem_increase < 500, f"Memory increased {mem_increase}MB, expected < 500MB"

    @pytest.mark.slow
    def test_memory_cleanup_after_read(self, cache_manager_for_system_tests):
        """Test memory cleanup after reading large data."""
        try:
            baseline_mem = get_memory_usage_mb()
        except ImportError:
            pytest.skip("psutil not available")

        df = generate_large_dataframe(100000)
        cache_manager_for_system_tests.write(
            table="mem_cleanup_test",
            data=df,
            storage_layer="daily",
        )

        for _ in range(10):
            result = cache_manager_for_system_tests.read(
                table="mem_cleanup_test",
                storage_layer="daily",
            )
            del result
            gc.collect()

        after_mem = get_memory_usage_mb()
        mem_increase = after_mem - baseline_mem
        assert mem_increase < 200, f"Memory leak detected: {mem_increase}MB increase"


@pytest.mark.system
@pytest.mark.slow
class TestConcurrentWrites:
    """Test concurrent write operations."""

    @pytest.mark.slow
    def test_concurrent_writes_different_tables(self, cache_manager_for_system_tests):
        """Test concurrent writes to different tables."""
        results = []
        errors = []

        def write_table(idx):
            try:
                df = generate_large_dataframe(10000)
                path = cache_manager_for_system_tests.write(
                    table=f"concurrent_table_{idx}",
                    data=df,
                    storage_layer="daily",
                )
                results.append((idx, path))
            except Exception as e:
                errors.append((idx, str(e)))

        threads = []
        for i in range(10):
            t = threading.Thread(target=write_table, args=(i,))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert len(errors) == 0, f"Errors during concurrent writes: {errors}"
        assert len(results) == 10

    @pytest.mark.slow
    def test_concurrent_writes_same_table_different_partitions(self, cache_manager_for_system_tests):
        """Test concurrent writes to same table different partitions."""
        results = []
        errors = []

        def write_partition(idx):
            try:
                df = generate_large_dataframe(5000)
                df["date"] = pd.date_range(f"2024-01-{idx*10+1:02d}", periods=5000, freq="B")
                path = cache_manager_for_system_tests.write(
                    table="concurrent_partition_test",
                    data=df,
                    storage_layer="daily",
                    partition_by="symbol",
                    partition_value=f"symbol_{idx}",
                )
                results.append((idx, path))
            except Exception as e:
                errors.append((idx, str(e)))

        threads = []
        for i in range(5):
            t = threading.Thread(target=write_partition, args=(i,))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 5

    @pytest.mark.slow
    def test_concurrent_writes_with_threadpool(self, cache_manager_for_system_tests):
        """Test concurrent writes using ThreadPoolExecutor."""
        def write_task(idx):
            df = generate_large_dataframe(8000)
            return cache_manager_for_system_tests.write(
                table=f"pool_table_{idx}",
                data=df,
                storage_layer="daily",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(write_task, i) for i in range(20)]
            results = []
            for future in as_completed(futures, timeout=120):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    pytest.fail(f"ThreadPoolExecutor task failed: {e}")

        assert len(results) == 20


@pytest.mark.system
@pytest.mark.slow
class TestConcurrentReads:
    """Test concurrent read operations."""

    @pytest.mark.slow
    def test_concurrent_reads_same_table(self, cache_manager_for_system_tests):
        """Test concurrent reads of same table."""
        df = generate_large_dataframe(50000)
        cache_manager_for_system_tests.write(
            table="concurrent_read_test",
            data=df,
            storage_layer="daily",
        )

        results = []
        errors = []

        def read_table():
            try:
                result = cache_manager_for_system_tests.read(
                    table="concurrent_read_test",
                    storage_layer="daily",
                )
                results.append(len(result))
            except Exception as e:
                errors.append(str(e))

        threads = []
        for i in range(20):
            t = threading.Thread(target=read_table)
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert len(errors) == 0, f"Read errors: {errors}"
        assert all(r == 50000 for r in results)

    @pytest.mark.slow
    def test_concurrent_reads_different_tables(self, cache_manager_for_system_tests):
        """Test concurrent reads of different tables."""
        for i in range(10):
            df = generate_large_dataframe(10000)
            cache_manager_for_system_tests.write(
                table=f"read_table_{i}",
                data=df,
                storage_layer="daily",
            )

        results = {}
        errors = []

        def read_table(idx):
            try:
                result = cache_manager_for_system_tests.read(
                    table=f"read_table_{idx}",
                    storage_layer="daily",
                )
                results[idx] = len(result)
            except Exception as e:
                errors.append((idx, str(e)))

        threads = []
        for i in range(10):
            t = threading.Thread(target=read_table, args=(i,))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert len(errors) == 0
        assert all(results[i] == 10000 for i in range(10))

    @pytest.mark.slow
    def test_mixed_concurrent_reads_writes(self, cache_manager_for_system_tests):
        """Test concurrent reads and writes simultaneously."""
        df_initial = generate_large_dataframe(20000)
        cache_manager_for_system_tests.write(
            table="mixed_test",
            data=df_initial,
            storage_layer="daily",
        )

        read_counts = []
        write_paths = []
        errors = []

        def read_task():
            for _ in range(5):
                try:
                    result = cache_manager_for_system_tests.read(
                        table="mixed_test",
                        storage_layer="daily",
                    )
                    read_counts.append(len(result))
                    time.sleep(0.1)
                except Exception as e:
                    errors.append(("read", str(e)))

        def write_task(idx):
            try:
                df = generate_large_dataframe(5000)
                path = cache_manager_for_system_tests.write(
                    table="mixed_test",
                    data=df,
                    storage_layer="daily",
                )
                write_paths.append(path)
            except Exception as e:
                errors.append(("write", str(e)))

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=read_task))
        for i in range(3):
            threads.append(threading.Thread(target=write_task, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert len(errors) <= 2, f"Too many errors: {errors}"


@pytest.mark.system
@pytest.mark.slow
class TestDataConsistency:
    """Test data consistency under concurrent access."""

    @pytest.mark.slow
    def test_write_read_consistency(self, cache_manager_for_system_tests):
        """Test written data matches read data."""
        df_original = generate_large_dataframe(50000)
        df_original["test_checksum"] = df_original.sum(axis=1, numeric_only=True)

        cache_manager_for_system_tests.write(
            table="consistency_test",
            data=df_original,
            storage_layer="daily",
        )

        df_read = cache_manager_for_system_tests.read(
            table="consistency_test",
            storage_layer="daily",
        )

        assert len(df_read) == len(df_original)
        assert set(df_read.columns) == set(df_original.columns)
        checksum_match = np.allclose(
            df_original["test_checksum"].values,
            df_read["test_checksum"].values,
            rtol=1e-5,
        )
        assert checksum_match, "Checksum mismatch between written and read data"

    @pytest.mark.slow
    def test_schema_preservation(self, cache_manager_for_system_tests):
        """Test schema is preserved across writes."""
        df = pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=100),
            "symbol": ["sh600000"] * 100,
            "open": np.random.uniform(10, 20, 100),
            "high": np.random.uniform(20, 30, 100),
            "low": np.random.uniform(5, 10, 100),
            "close": np.random.uniform(10, 20, 100),
            "volume": np.random.randint(1000, 100000, 100),
        })

        original_dtypes = df.dtypes.to_dict()

        cache_manager_for_system_tests.write(
            table="schema_test",
            data=df,
            storage_layer="daily",
        )

        df_read = cache_manager_for_system_tests.read(
            table="schema_test",
            storage_layer="daily",
        )

        for col, original_dtype in original_dtypes.items():
            read_dtype = df_read[col].dtype
            if original_dtype.kind == "f":
                assert read_dtype.kind == "f", f"Column {col} lost float type"
            elif original_dtype.kind == "i":
                assert read_dtype.kind in ("i", "f"), f"Column {col} type changed unexpectedly"

    @pytest.mark.slow
    def test_partition_consistency(self, cache_manager_for_system_tests):
        """Test partition-based writes preserve data."""
        df1 = generate_large_dataframe(10000)
        df1["symbol"] = "sh600000"

        df2 = generate_large_dataframe(10000)
        df2["symbol"] = "sh600001"

        cache_manager_for_system_tests.write(
            table="partition_consistency",
            data=df1,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )

        cache_manager_for_system_tests.write(
            table="partition_consistency",
            data=df2,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600001",
        )

        result1 = cache_manager_for_system_tests.read(
            table="partition_consistency",
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )

        result2 = cache_manager_for_system_tests.read(
            table="partition_consistency",
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600001",
        )

        assert len(result1) == 10000
        assert len(result2) == 10000
        assert all(result1["symbol"] == "sh600000")
        assert all(result2["symbol"] == "sh600001")

    @pytest.mark.slow
    def test_concurrent_write_no_data_loss(self, cache_manager_for_system_tests):
        """Test concurrent writes don't lose data."""
        expected_rows = {}

        def write_task(idx):
            df = generate_large_dataframe(5000)
            expected_rows[idx] = len(df)
            cache_manager_for_system_tests.write(
                table=f"no_loss_{idx}",
                data=df,
                storage_layer="daily",
            )

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(write_task, i) for i in range(10)]
            for future in as_completed(futures):
                future.result()

        for idx, expected in expected_rows.items():
            result = cache_manager_for_system_tests.read(
                table=f"no_loss_{idx}",
                storage_layer="daily",
            )
            assert len(result) == expected, f"Data loss in table {idx}"


@pytest.mark.system
@pytest.mark.slow
class TestCacheManagerStats:
    """Test cache manager statistics under load."""

    @pytest.mark.slow
    def test_stats_after_large_operations(self, cache_manager_for_system_tests):
        """Test stats reflect large operations."""
        for i in range(5):
            df = generate_large_dataframe(20000)
            cache_manager_for_system_tests.write(
                table=f"stats_test_{i}",
                data=df,
                storage_layer="daily",
            )

        stats = cache_manager_for_system_tests.get_stats()
        assert stats["memory_cache_size"] >= 0
        assert "tables" in stats

    @pytest.mark.slow
    def test_table_info_accuracy(self, cache_manager_for_system_tests):
        """Test table_info returns accurate info."""
        df = generate_large_dataframe(30000)
        cache_manager_for_system_tests.write(
            table="table_info_test",
            data=df,
            storage_layer="daily",
        )

        info = cache_manager_for_system_tests.table_info(
            table="table_info_test",
            storage_layer="daily",
        )
        assert info["name"] == "table_info_test"
        assert info["file_count"] >= 1
        assert info["total_size_bytes"] > 0

    @pytest.mark.slow
    def test_list_tables_accuracy(self, cache_manager_for_system_tests):
        """Test list_tables returns all written tables."""
        written_tables = set()
        for i in range(10):
            df = generate_large_dataframe(5000)
            table_name = f"list_tables_{i}"
            cache_manager_for_system_tests.write(
                table=table_name,
                data=df,
                storage_layer="daily",
            )
            written_tables.add(table_name)

        listed_tables = cache_manager_for_system_tests.list_tables(storage_layer="daily")
        for table in written_tables:
            assert table in listed_tables, f"Table {table} missing from list"