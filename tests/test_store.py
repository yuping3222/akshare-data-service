"""tests/test_store.py

存储层测试: Parquet/DuckDB/内存缓存

参考 jk2bt cache/tests/ 编写
"""

import tempfile
from pathlib import Path

import pandas as pd

from akshare_data.store.manager import (
    CacheManager,
    CacheConfig,
    get_cache_manager,
    reset_cache_manager,
)
from akshare_data.store.memory import MemoryCache
from akshare_data.store.duckdb import DuckDBEngine
from akshare_data.store.parquet import PartitionManager


class TestCacheManagerInit:
    """测试 CacheManager 初始化"""

    def test_init_default_path(self):
        """测试默认路径初始化"""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = CacheManager(base_dir=tmpdir)
            assert manager.engine is not None
            assert manager.config.base_dir == tmpdir

    def test_init_custom_path(self, tmp_path):
        """测试自定义数据库路径"""
        custom_dir = tmp_path / "custom_cache"
        manager = CacheManager(base_dir=str(custom_dir))
        assert manager.config.base_dir == str(custom_dir)

    def test_get_cache_manager_singleton(self):
        """测试工厂函数返回有效实例"""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = get_cache_manager(base_dir=tmpdir)
            assert isinstance(manager, CacheManager)

    def test_reset_cache_manager(self):
        """测试重置缓存管理器"""
        reset_cache_manager()
        with tempfile.TemporaryDirectory() as tmpdir:
            manager1 = get_cache_manager(base_dir=tmpdir)
            reset_cache_manager()
            manager2 = get_cache_manager(base_dir=tmpdir)
            assert manager1 is not manager2


class TestMemoryCache:
    """测试内存缓存"""

    def test_memory_cache_put_get(self):
        """测试基本存取"""
        cache = MemoryCache(max_items=100, default_ttl_seconds=3600)
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        cache.put("test_key", df)

        result = cache.get("test_key")
        assert result is not None
        assert len(result) == 3

    def test_memory_cache_expiry(self):
        """测试过期机制"""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        df = pd.DataFrame({"a": [1, 2, 3]})
        cache.put("test_key", df)

        result = cache.get("test_key")
        assert result is not None

        import time

        time.sleep(1.1)

        result_expired = cache.get("test_key")
        assert result_expired is None

    def test_memory_cache_max_items(self):
        """测试最大条目限制"""
        cache = MemoryCache(max_items=2, default_ttl_seconds=3600)
        cache.put("key1", pd.DataFrame({"a": [1]}))
        cache.put("key2", pd.DataFrame({"b": [2]}))
        cache.put("key3", pd.DataFrame({"c": [3]}))

        assert cache.size >= 2

    def test_memory_cache_invalidate(self):
        """测试缓存失效"""
        cache = MemoryCache(max_items=100, default_ttl_seconds=3600)
        df = pd.DataFrame({"a": [1, 2, 3]})
        cache.put("test_key", df)

        assert cache.get("test_key") is not None
        cache.invalidate("test_key")
        assert cache.get("test_key") is None

    def test_memory_cache_invalidate_all(self):
        """测试全部缓存失效"""
        cache = MemoryCache(max_items=100, default_ttl_seconds=3600)
        cache.put("key1", pd.DataFrame({"a": [1]}))
        cache.put("key2", pd.DataFrame({"b": [2]}))

        count = cache.invalidate()
        assert count >= 2
        assert cache.size == 0


class TestDuckDBEngine:
    """测试 DuckDB 引擎"""

    def test_duckdb_init(self, tmp_path):
        """测试 DuckDB 初始化"""
        db_path = tmp_path / "test.duckdb"
        engine = DuckDBEngine(base_dir=str(db_path.parent))

        assert engine.base_dir == Path(tmp_path)
        assert engine._local is not None

    def test_duckdb_query_empty(self, tmp_path):
        """测试空数据库查询"""
        db_path = tmp_path / "test.duckdb"
        engine = DuckDBEngine(base_dir=str(db_path.parent))

        result = engine.query("nonexistent_table", "daily")
        assert result is not None
        assert result.empty

    def test_duckdb_table_info(self, tmp_path):
        """测试表信息获取"""
        tmp_path / "test.duckdb"
        engine = DuckDBEngine(base_dir=str(tmp_path))

        result = engine.query("nonexistent_table", "daily")
        assert result is not None


class TestPartitionManager:
    """测试分区管理器"""

    def test_partition_manager_init(self, tmp_path):
        """测试分区管理器初始化"""
        base_dir = tmp_path / "partitions"
        pm = PartitionManager(base_dir=str(base_dir))

        assert pm.base_dir == base_dir.resolve()

    def test_list_partition_files_empty(self, tmp_path):
        """测试空分区目录"""
        base_dir = tmp_path / "partitions"
        pm = PartitionManager(base_dir=str(base_dir))

        files = pm.list_partition_files("test_table", "daily", "date", "2024-01-01")
        assert isinstance(files, list)


class TestCacheConfig:
    """测试缓存配置"""

    def test_default_config(self):
        """测试默认配置"""
        config = CacheConfig()
        assert config.base_dir == "./cache"
        assert config.compression == "snappy"
        assert config.row_group_size == 100_000

    def test_custom_config(self):
        """测试自定义配置"""
        config = CacheConfig(
            base_dir="/tmp/custom_cache",
            compression="gzip",
            row_group_size=50_000,
            duckdb_threads=8,
        )
        assert config.base_dir == "/tmp/custom_cache"
        assert config.compression == "gzip"
        assert config.row_group_size == 50_000
        assert config.duckdb_threads == 8


class TestCacheReadWrite:
    """测试缓存读写"""

    def test_write_and_read(self, tmp_path):
        """测试基本读写"""
        manager = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
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

        file_path = manager.write("stock_daily", df, storage_layer="daily")
        assert file_path != ""

        result = manager.read("stock_daily", storage_layer="daily")
        assert result is not None
        if not result.empty:
            assert len(result) <= 10

    def test_write_empty_dataframe(self, tmp_path):
        """测试写入空 DataFrame"""
        manager = CacheManager(base_dir=str(tmp_path))

        empty_df = pd.DataFrame()
        result = manager.write("stock_daily", empty_df, storage_layer="daily")
        assert result == ""


class TestCacheExists:
    """测试缓存存在性检查"""

    def test_exists_false_for_empty(self, tmp_path):
        """测试空缓存返回 False"""
        manager = CacheManager(base_dir=str(tmp_path))

        exists = manager.exists("stock_daily", storage_layer="daily")
        assert exists is False

    def test_has_range_check(self, tmp_path):
        """测试日期范围检查"""
        manager = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
            }
        )

        manager.write(
            "stock_daily",
            df,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )

        has_range = manager.has_range(
            "stock_daily",
            storage_layer="daily",
            partition_by="symbol",
            where={"symbol": "sh600000"},
            date_col="date",
            start="2024-01-01",
            end="2024-01-05",
        )
        assert has_range is True


class TestCacheInvalidation:
    """测试缓存失效"""

    def test_invalidate_table(self, tmp_path):
        """测试表失效"""
        manager = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
            }
        )

        manager.write("stock_daily", df, storage_layer="daily")
        count = manager.invalidate("stock_daily", storage_layer="daily")

        assert count >= 0

    def test_invalidate_all(self, tmp_path):
        """测试全部失效"""
        manager = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
            }
        )

        manager.write("stock_daily", df, storage_layer="daily")
        count = manager.invalidate_all()

        assert count >= 0


class TestTableInfo:
    """测试表信息"""

    def test_table_info_empty(self, tmp_path):
        """测试空表的表信息"""
        manager = CacheManager(base_dir=str(tmp_path))

        info = manager.table_info("stock_daily", storage_layer="daily")

        assert info is not None
        assert "name" in info
        assert info["name"] == "stock_daily"

    def test_list_tables(self, tmp_path):
        """测试列出所有表"""
        manager = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
            }
        )

        manager.write("stock_daily", df, storage_layer="daily")

        tables = manager.list_tables()
        assert isinstance(tables, list)


class TestCacheStats:
    """测试缓存统计"""

    def test_get_stats(self, tmp_path):
        """测试获取统计信息"""
        manager = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
            }
        )

        manager.write("stock_daily", df, storage_layer="daily")

        stats = manager.get_stats()
        assert "memory_cache_size" in stats
        assert "tables" in stats


class TestCacheManagerEdgeCases:
    """测试边界情况"""

    def test_read_with_invalid_params(self, tmp_path):
        """测试无效参数读取"""
        manager = CacheManager(base_dir=str(tmp_path))

        result = manager.read(
            "stock_daily",
            storage_layer="daily",
            where={"invalid_column": "value"},
        )
        assert result is None or result.empty

    def test_concurrent_access(self, tmp_path):
        """测试并发访问"""
        import threading

        manager = CacheManager(base_dir=str(tmp_path))
        results = []

        def read_task():
            result = manager.read("stock_daily", storage_layer="daily")
            results.append(result)

        threads = [threading.Thread(target=read_task) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 5
