"""测试 DataService API 的核心逻辑"""

import pytest
import pandas as pd
import os
import shutil
from pathlib import Path

from akshare_data import DataService, get_service
from akshare_data.store.manager import CacheManager, CacheConfig, reset_cache_manager


TEST_CACHE_DIR = "./test_data_cache"


@pytest.fixture(scope="function", autouse=True)
def setup_test_env():
    reset_cache_manager()
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)
    Path(TEST_CACHE_DIR).mkdir(parents=True)

    cache_cfg = CacheConfig(
        base_dir=TEST_CACHE_DIR,
        memory_cache_max_items=100,
        memory_cache_default_ttl_seconds=60,
    )
    cache_manager = CacheManager(config=cache_cfg)
    service = DataService(cache_manager=cache_manager)

    yield service, cache_manager

    reset_cache_manager()
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)


def test_dataservice_creation(setup_test_env):
    service, _ = setup_test_env
    assert service is not None
    assert isinstance(service, DataService)
    assert service.cache is not None
    assert isinstance(service.cache, CacheManager)


def test_get_service_singleton():
    reset_cache_manager()
    s1 = get_service()
    s2 = get_service()
    assert s1 is s2


def test_cache_manager_creation():
    cfg = CacheConfig(base_dir=TEST_CACHE_DIR)
    manager = CacheManager(config=cfg)
    assert manager is not None
    assert manager.config.base_dir == TEST_CACHE_DIR


def test_cache_config_properties():
    cfg = CacheConfig(
        base_dir="/tmp/test",
        compression="zstd",
        row_group_size=50000,
        duckdb_threads=2,
        memory_cache_max_items=1000,
        memory_cache_default_ttl_seconds=1800,
    )
    assert cfg.base_dir == "/tmp/test"
    assert cfg.compression == "zstd"
    assert cfg.row_group_size == 50000
    assert cfg.duckdb_threads == 2
    assert cfg.memory_cache_max_items == 1000
    assert cfg.memory_cache_default_ttl_seconds == 1800


def test_service_has_required_methods(setup_test_env):
    service, _ = setup_test_env
    assert hasattr(service, "get_daily")
    assert hasattr(service, "get_minute")
    assert hasattr(service, "get_index")
    assert hasattr(service, "get_etf")
    assert hasattr(service, "get_index_stocks")
    assert hasattr(service, "get_trading_days")
    assert hasattr(service, "get_money_flow")
    assert hasattr(service, "get_north_money_flow")
    assert hasattr(service, "get_finance_indicator")


def test_cache_manager_read_write(setup_test_env):
    _, cache_manager = setup_test_env

    test_df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "symbol": ["000001", "000001", "000001"],
            "close": [10.0, 10.5, 11.0],
            "volume": [1000, 2000, 3000],
        }
    )

    cache_manager.write(
        "stock_daily",
        test_df,
        storage_layer="daily",
        partition_by="symbol",
        partition_value="000001",
    )

    result = cache_manager.read(
        "stock_daily",
        storage_layer="daily",
        partition_by="symbol",
        where={"symbol": "000001"},
        order_by=["date"],
    )

    assert not result.empty
    assert len(result) == 3
    assert "close" in result.columns


def test_cache_manager_invalidate(setup_test_env):
    _, cache_manager = setup_test_env

    test_df = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["000001"],
            "close": [10.0],
        }
    )

    cache_manager.write(
        "stock_daily",
        test_df,
        storage_layer="daily",
        partition_by="symbol",
        partition_value="000001",
    )

    assert cache_manager.exists(
        "stock_daily",
        storage_layer="daily",
        partition_by="symbol",
        where={"symbol": "000001"},
    )

    count = cache_manager.invalidate(
        "stock_daily",
        storage_layer="daily",
    )
    assert count >= 0


def test_cache_manager_list_tables(setup_test_env):
    _, cache_manager = setup_test_env

    test_df = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["000001"],
            "close": [10.0],
        }
    )

    cache_manager.write(
        "stock_daily",
        test_df,
        storage_layer="daily",
        partition_by="symbol",
        partition_value="000001",
    )

    tables = cache_manager.list_tables(storage_layer="daily")
    assert "stock_daily" in tables


def test_cache_manager_table_info(setup_test_env):
    _, cache_manager = setup_test_env

    test_df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "symbol": ["000001", "000001"],
            "close": [10.0, 10.5],
        }
    )

    cache_manager.write(
        "stock_daily",
        test_df,
        storage_layer="daily",
        partition_by="symbol",
        partition_value="000001",
    )

    info = cache_manager.table_info(
        "stock_daily", storage_layer="daily", partition_by="symbol"
    )
    assert info["name"] == "stock_daily"
    assert info["file_count"] >= 0
    assert info["total_size_bytes"] >= 0


def test_cache_manager_get_stats(setup_test_env):
    _, cache_manager = setup_test_env
    stats = cache_manager.get_stats()
    assert "memory_cache_size" in stats
    assert "memory_cache_hit_rate" in stats
    assert "tables" in stats


def test_cache_manager_memory_cache(setup_test_env):
    _, cache_manager = setup_test_env

    test_df = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["000001"],
            "close": [10.0],
        }
    )

    initial_size = cache_manager.memory_cache.size

    cache_manager.write(
        "stock_daily",
        test_df,
        storage_layer="daily",
        partition_by="symbol",
        partition_value="000001",
    )

    assert cache_manager.memory_cache.size >= initial_size
