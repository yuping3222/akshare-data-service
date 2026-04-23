"""测试 akshare_data.core.config 模块"""

import json
import os
import tempfile
from pathlib import Path

from akshare_data.core.config import CacheConfig, TableConfig, DEFAULT_CONFIG


class TestCacheConfig:
    def test_cache_config_creation_with_defaults(self):
        cfg = CacheConfig()
        assert cfg.base_dir == "./cache"
        assert cfg.daily_dir == "daily"
        assert cfg.minute_dir == "minute"
        assert cfg.snapshot_dir == "snapshot"
        assert cfg.meta_dir == "meta"
        assert cfg.compression == "snappy"
        assert cfg.row_group_size == 100_000
        assert cfg.aggregation_enabled is True
        assert cfg.duckdb_read_only is True
        assert cfg.duckdb_threads == 4
        assert cfg.duckdb_memory_limit == "4GB"
        assert cfg.cleanup_retention_hours == 24
        assert cfg.strict_schema is True
        assert cfg.log_level == "INFO"
        assert cfg.log_format == "json"
        assert cfg.tables == {}

    def test_cache_config_creation_with_custom_values(self):
        cfg = CacheConfig(
            base_dir="/tmp/custom_cache",
            compression="zstd",
            row_group_size=50000,
            duckdb_threads=8,
            duckdb_memory_limit="8GB",
            memory_cache_max_items=10000,
            memory_cache_default_ttl_seconds=7200,
            log_level="DEBUG",
        )
        assert cfg.base_dir == "/tmp/custom_cache"
        assert cfg.compression == "zstd"
        assert cfg.row_group_size == 50000
        assert cfg.duckdb_threads == 8
        assert cfg.duckdb_memory_limit == "8GB"
        assert cfg.memory_cache_max_items == 10000
        assert cfg.memory_cache_default_ttl_seconds == 7200
        assert cfg.log_level == "DEBUG"

    def test_cache_config_source_priority(self):
        cfg = CacheConfig()
        assert cfg.source_priority == [
            "lixinger",
            "akshare",
        ]

    def test_cache_config_tushare_token_default_empty(self):
        cfg = CacheConfig()
        assert cfg.tushare_token == ""


class TestTableConfig:
    def test_table_config_defaults(self):
        cfg = TableConfig()
        assert cfg.partition_by is None
        assert cfg.ttl_hours == 0
        assert cfg.compaction_threshold == 20
        assert cfg.aggregation_enabled is True

    def test_table_config_custom_values(self):
        cfg = TableConfig(
            partition_by="report_date",
            ttl_hours=2160,
            compaction_threshold=5,
            aggregation_enabled=False,
        )
        assert cfg.partition_by == "report_date"
        assert cfg.ttl_hours == 2160
        assert cfg.compaction_threshold == 5
        assert cfg.aggregation_enabled is False


class TestCacheConfigFromJson:
    def test_from_json_basic(self):
        config_data = {
            "base_dir": "/tmp/json_cache",
            "compression": "zstd",
            "row_group_size": 75000,
            "duckdb_threads": 6,
            "memory_cache_max_items": 2000,
            "memory_cache_default_ttl_seconds": 1800,
            "log_level": "WARNING",
            "tables": {
                "stock_daily": {
                    "partition_by": "date",
                    "ttl_hours": 0,
                    "compaction_threshold": 30,
                    "aggregation_enabled": True,
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        try:
            cfg = CacheConfig.from_json(temp_path)
            assert cfg.base_dir == "/tmp/json_cache"
            assert cfg.compression == "zstd"
            assert cfg.row_group_size == 75000
            assert cfg.duckdb_threads == 6
            assert cfg.memory_cache_max_items == 2000
            assert cfg.memory_cache_default_ttl_seconds == 1800
            assert cfg.log_level == "WARNING"
            assert "stock_daily" in cfg.tables
            assert cfg.tables["stock_daily"].partition_by == "date"
            assert cfg.tables["stock_daily"].ttl_hours == 0
            assert cfg.tables["stock_daily"].compaction_threshold == 30
        finally:
            os.unlink(temp_path)

    def test_from_json_without_tables(self):
        config_data = {
            "base_dir": "/tmp/simple_cache",
            "compression": "lz4",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        try:
            cfg = CacheConfig.from_json(temp_path)
            assert cfg.base_dir == "/tmp/simple_cache"
            assert cfg.compression == "lz4"
            assert cfg.tables == {}
        finally:
            os.unlink(temp_path)


class TestCacheConfigFromEnv:
    def test_from_env_no_overrides(self):
        env_backup = {
            "AKSHARE_DATA_CACHE_DIR": os.environ.get("AKSHARE_DATA_CACHE_DIR"),
            "AKSHARE_DATA_CACHE_MAX_ITEMS": os.environ.get(
                "AKSHARE_DATA_CACHE_MAX_ITEMS"
            ),
            "AKSHARE_DATA_CACHE_TTL_SECONDS": os.environ.get(
                "AKSHARE_DATA_CACHE_TTL_SECONDS"
            ),
            "AKSHARE_DATA_CACHE_COMPRESSION": os.environ.get(
                "AKSHARE_DATA_CACHE_COMPRESSION"
            ),
            "AKSHARE_DATA_CACHE_ROW_GROUP_SIZE": os.environ.get(
                "AKSHARE_DATA_CACHE_ROW_GROUP_SIZE"
            ),
            "AKSHARE_DATA_CACHE_DUCKDB_THREADS": os.environ.get(
                "AKSHARE_DATA_CACHE_DUCKDB_THREADS"
            ),
            "AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT": os.environ.get(
                "AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT"
            ),
            "AKSHARE_DATA_CACHE_LOG_LEVEL": os.environ.get(
                "AKSHARE_DATA_CACHE_LOG_LEVEL"
            ),
            "AKSHARE_DATA_CACHE_RETENTION_HOURS": os.environ.get(
                "AKSHARE_DATA_CACHE_RETENTION_HOURS"
            ),
        }

        try:
            os.environ["AKSHARE_DATA_CACHE_DIR"] = "/tmp/env_cache"
            os.environ["AKSHARE_DATA_CACHE_MAX_ITEMS"] = "9999"
            os.environ["AKSHARE_DATA_CACHE_TTL_SECONDS"] = "3600"
            os.environ["AKSHARE_DATA_CACHE_COMPRESSION"] = "zstd"
            os.environ["AKSHARE_DATA_CACHE_ROW_GROUP_SIZE"] = "50000"
            os.environ["AKSHARE_DATA_CACHE_DUCKDB_THREADS"] = "16"
            os.environ["AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT"] = "16GB"
            os.environ["AKSHARE_DATA_CACHE_LOG_LEVEL"] = "DEBUG"
            os.environ["AKSHARE_DATA_CACHE_RETENTION_HOURS"] = "48"

            cfg = CacheConfig.from_env()

            assert cfg.base_dir == "/tmp/env_cache"
            assert cfg.memory_cache_max_items == 9999
            assert cfg.memory_cache_default_ttl_seconds == 3600
            assert cfg.compression == "zstd"
            assert cfg.row_group_size == 50000
            assert cfg.duckdb_threads == 16
            assert cfg.duckdb_memory_limit == "16GB"
            assert cfg.log_level == "DEBUG"
            assert cfg.cleanup_retention_hours == 48
        finally:
            for key, val in env_backup.items():
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val

    def test_from_env_partial_overrides(self):
        env_backup = os.environ.get("AKSHARE_DATA_CACHE_DIR")

        try:
            if "AKSHARE_DATA_CACHE_DIR" in os.environ:
                del os.environ["AKSHARE_DATA_CACHE_DIR"]

            cfg = CacheConfig.from_env(base_dir="/custom/override")

            assert cfg.base_dir == "/custom/override"
            assert cfg.memory_cache_max_items == 5000
        finally:
            if env_backup is not None:
                os.environ["AKSHARE_DATA_CACHE_DIR"] = env_backup


class TestCacheConfigToDict:
    def test_to_dict_basic(self):
        cfg = CacheConfig(
            base_dir="/tmp/test_cache",
            compression="zstd",
            row_group_size=50000,
        )
        d = cfg.to_dict()
        assert d["base_dir"] == "/tmp/test_cache"
        assert d["compression"] == "zstd"
        assert d["row_group_size"] == 50000
        assert "source_priority" in d
        assert "tables" in d

    def test_to_dict_with_tables(self):
        cfg = CacheConfig(
            tables={
                "stock_daily": TableConfig(
                    partition_by="date",
                    ttl_hours=0,
                    compaction_threshold=20,
                ),
                "finance_indicator": TableConfig(
                    partition_by="report_date",
                    ttl_hours=2160,
                    compaction_threshold=5,
                ),
            }
        )
        d = cfg.to_dict()
        assert "stock_daily" in d["tables"]
        assert d["tables"]["stock_daily"]["partition_by"] == "date"
        assert d["tables"]["stock_daily"]["ttl_hours"] == 0
        assert "finance_indicator" in d["tables"]
        assert d["tables"]["finance_indicator"]["partition_by"] == "report_date"


class TestCacheConfigGetTableConfig:
    def test_get_table_config_existing(self):
        cfg = CacheConfig(
            tables={
                "stock_daily": TableConfig(
                    partition_by="date",
                    ttl_hours=100,
                ),
            }
        )
        table_cfg = cfg.get_table_config("stock_daily")
        assert table_cfg.partition_by == "date"
        # Schema registry is the source of truth, so ttl_hours comes from schema (0)
        assert table_cfg.ttl_hours == 0

    def test_get_table_config_nonexistent(self):
        cfg = CacheConfig()
        table_cfg = cfg.get_table_config("nonexistent_table")
        assert table_cfg.partition_by is None
        assert table_cfg.ttl_hours == 0
        assert table_cfg.compaction_threshold == 20
        assert table_cfg.aggregation_enabled is True


class TestCacheConfigProperties:
    def test_lock_dir_path_when_set(self):
        cfg = CacheConfig(lock_dir="/custom/locks")
        assert cfg.lock_dir_path == Path("/custom/locks")

    def test_lock_dir_path_default(self):
        cfg = CacheConfig(base_dir="/tmp/cache")
        assert cfg.lock_dir_path == Path("/tmp/cache") / "_locks"

    def test_aggregated_dir(self):
        cfg = CacheConfig(base_dir="/tmp/cache")
        assert cfg.aggregated_dir == Path("/tmp/cache") / "aggregated"


class TestDefaultConfig:
    def test_default_config_exists(self):
        assert DEFAULT_CONFIG is not None
        assert isinstance(DEFAULT_CONFIG, CacheConfig)

    def test_default_config_has_tables(self):
        # DEFAULT_CONFIG is built from from_env() which starts with empty tables
        # Table configs are resolved dynamically via SCHEMA_REGISTRY
        assert isinstance(DEFAULT_CONFIG.tables, dict)
        assert DEFAULT_CONFIG.base_dir == "./cache"

    def test_default_config_stock_daily(self):
        stock_daily = DEFAULT_CONFIG.get_table_config("stock_daily")
        assert stock_daily.partition_by == "date"
        assert stock_daily.ttl_hours == 0
        assert stock_daily.compaction_threshold == 20

    def test_default_config_finance_indicator(self):
        fi = DEFAULT_CONFIG.get_table_config("finance_indicator")
        assert fi.partition_by == "report_date"
        assert fi.ttl_hours == 2160
        assert fi.compaction_threshold == 5

    def test_default_config_securities(self):
        securities = DEFAULT_CONFIG.get_table_config("securities")
        assert securities.partition_by is None
        assert securities.ttl_hours == 0
        assert securities.compaction_threshold == 0

    def test_default_config_trade_calendar(self):
        tc = DEFAULT_CONFIG.get_table_config("trade_calendar")
        assert tc.partition_by is None
        assert tc.ttl_hours == 0
        assert tc.compaction_threshold == 0
