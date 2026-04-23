"""Cache configuration for akshare_data_service.

Provides dataclass-based configuration with environment variable overrides
and JSON serialization support.

Sources:
- jk2bt/cache/config.py
- akshare_one/cache/config.py
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from akshare_data.core.schema import CacheTable, SCHEMA_REGISTRY


@dataclass
class TableConfig:
    """Per-table cache configuration.

    Attributes:
        partition_by: Column name to partition parquet files by, or None.
        ttl_hours: Time-to-live in hours. 0 means no expiration.
        compaction_threshold: Number of files before triggering compaction.
        aggregation_enabled: Whether to enable aggregated storage.
    """

    partition_by: str | None = None
    ttl_hours: int = 0
    compaction_threshold: int = 20
    aggregation_enabled: bool = True

    @classmethod
    def from_schema(cls, schema: CacheTable) -> "TableConfig":
        """Build TableConfig from a CacheTable schema."""
        return cls(
            partition_by=schema.partition_by,
            ttl_hours=schema.ttl_hours,
            compaction_threshold=schema.compaction_threshold,
            aggregation_enabled=schema.aggregation_enabled,
        )

    def resolve(self, table_name: str | None = None) -> "TableConfig":
        """Resolve against SCHEMA_REGISTRY — schema wins over config defaults."""
        if table_name:
            schema = SCHEMA_REGISTRY.get_or_none(table_name)
            if schema:
                return TableConfig.from_schema(schema)
        return self


@dataclass
class CacheConfig:
    """Top-level cache configuration.

    Attributes:
        base_dir: Root directory for cache storage.
        daily_dir: Subdirectory for daily data.
        minute_dir: Subdirectory for minute data.
        snapshot_dir: Subdirectory for snapshot data.
        meta_dir: Subdirectory for meta data.
        source_priority: Priority order for data sources (lower index = higher priority).
        tushare_token: Tushare API token for authenticated requests.
        memory_cache_max_items: Maximum items in the in-memory LRU cache.
        memory_cache_default_ttl_seconds: Default TTL for in-memory cache entries.
        compression: Parquet compression codec.
        row_group_size: Parquet row group size.
        aggregation_enabled: Global toggle for aggregated storage.
        aggregation_schedule: Schedule for aggregation jobs.
        lock_dir: Directory for file-based locks.
        duckdb_read_only: Open DuckDB in read-only mode.
        duckdb_threads: DuckDB worker thread count.
        duckdb_memory_limit: DuckDB memory limit string (e.g. "4GB").
        cleanup_retention_hours: Hours to retain expired files before deletion.
        log_level: Logging level for cache operations.
        log_format: Log format ("json" or "text").
        tables: Per-table configuration overrides.
    """

    base_dir: str = "./cache"
    daily_dir: str = "daily"
    minute_dir: str = "minute"
    snapshot_dir: str = "snapshot"
    meta_dir: str = "meta"
    source_priority: list[str] = field(default_factory=lambda: ["lixinger", "akshare"])
    tushare_token: str = ""
    memory_cache_max_items: int = 5000
    memory_cache_default_ttl_seconds: int = 3600
    compression: str = "snappy"
    row_group_size: int = 100_000
    aggregation_enabled: bool = True
    aggregation_schedule: str = "daily"
    lock_dir: str = ""
    duckdb_read_only: bool = True
    duckdb_threads: int = 4
    duckdb_memory_limit: str = "4GB"
    cleanup_retention_hours: int = 24
    strict_schema: bool = True
    log_level: str = "INFO"
    log_format: str = "json"
    tables: dict[str, TableConfig] = field(default_factory=dict)

    @classmethod
    def from_json(cls, path: str) -> CacheConfig:
        """Load configuration from a JSON file.

        Args:
            path: Path to the JSON configuration file.

        Returns:
            CacheConfig instance populated from the file.
        """
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        tables_data = data.pop("tables", {})
        tables = {name: TableConfig(**cfg) for name, cfg in tables_data.items()}

        return cls(**data, tables=tables)

    @classmethod
    def from_env(cls, **overrides) -> CacheConfig:
        """Create configuration with environment variable overrides.

        Recognized environment variables:
            AKSHARE_DATA_CACHE_DIR: Override base_dir.
            AKSHARE_DATA_CACHE_MAX_ITEMS: Override memory_cache_max_items.
            AKSHARE_DATA_CACHE_TTL_SECONDS: Override memory_cache_default_ttl_seconds.
            AKSHARE_DATA_CACHE_COMPRESSION: Override compression.
            AKSHARE_DATA_CACHE_ROW_GROUP_SIZE: Override row_group_size.
            AKSHARE_DATA_CACHE_DUCKDB_THREADS: Override duckdb_threads.
            AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT: Override duckdb_memory_limit.
            AKSHARE_DATA_CACHE_LOG_LEVEL: Override log_level.
            AKSHARE_DATA_CACHE_RETENTION_HOURS: Override cleanup_retention_hours.
            AKSHARE_DATA_CACHE_STRICT_SCHEMA: Override strict_schema.

        Args:
            **overrides: Additional keyword arguments to override defaults.

        Returns:
            CacheConfig instance with environment variable overrides applied.
        """
        config = cls(**overrides)

        base_dir = os.environ.get("AKSHARE_DATA_CACHE_DIR")
        if base_dir:
            config.base_dir = base_dir

        max_items = os.environ.get("AKSHARE_DATA_CACHE_MAX_ITEMS")
        if max_items:
            config.memory_cache_max_items = int(max_items)

        ttl = os.environ.get("AKSHARE_DATA_CACHE_TTL_SECONDS")
        if ttl:
            config.memory_cache_default_ttl_seconds = int(ttl)

        compression = os.environ.get("AKSHARE_DATA_CACHE_COMPRESSION")
        if compression:
            config.compression = compression

        row_group = os.environ.get("AKSHARE_DATA_CACHE_ROW_GROUP_SIZE")
        if row_group:
            config.row_group_size = int(row_group)

        threads = os.environ.get("AKSHARE_DATA_CACHE_DUCKDB_THREADS")
        if threads:
            config.duckdb_threads = int(threads)

        mem_limit = os.environ.get("AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT")
        if mem_limit:
            config.duckdb_memory_limit = mem_limit

        log_level = os.environ.get("AKSHARE_DATA_CACHE_LOG_LEVEL")
        if log_level:
            config.log_level = log_level

        retention = os.environ.get("AKSHARE_DATA_CACHE_RETENTION_HOURS")
        if retention:
            config.cleanup_retention_hours = int(retention)

        strict = os.environ.get("AKSHARE_DATA_CACHE_STRICT_SCHEMA")
        if strict:
            config.strict_schema = strict.lower() in ("1", "true", "yes")

        return config

    def to_dict(self) -> dict:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation suitable for JSON serialization.
        """
        return {
            "base_dir": self.base_dir,
            "daily_dir": self.daily_dir,
            "minute_dir": self.minute_dir,
            "snapshot_dir": self.snapshot_dir,
            "meta_dir": self.meta_dir,
            "source_priority": self.source_priority,
            "tushare_token": self.tushare_token,
            "memory_cache_max_items": self.memory_cache_max_items,
            "memory_cache_default_ttl_seconds": self.memory_cache_default_ttl_seconds,
            "compression": self.compression,
            "row_group_size": self.row_group_size,
            "aggregation_enabled": self.aggregation_enabled,
            "aggregation_schedule": self.aggregation_schedule,
            "lock_dir": self.lock_dir,
            "duckdb_read_only": self.duckdb_read_only,
            "duckdb_threads": self.duckdb_threads,
            "duckdb_memory_limit": self.duckdb_memory_limit,
            "cleanup_retention_hours": self.cleanup_retention_hours,
            "strict_schema": self.strict_schema,
            "log_level": self.log_level,
            "log_format": self.log_format,
            "tables": {
                name: {
                    "partition_by": cfg.partition_by,
                    "ttl_hours": cfg.ttl_hours,
                    "compaction_threshold": cfg.compaction_threshold,
                    "aggregation_enabled": cfg.aggregation_enabled,
                }
                for name, cfg in self.tables.items()
            },
        }

    def get_table_config(self, table_name: str) -> TableConfig:
        """Get configuration for a specific table.

        Schema registry is the source of truth — TableConfig wraps CacheTable.

        Args:
            table_name: Name of the table.

        Returns:
            TableConfig for the table, or a default TableConfig if not found.
        """
        schema = SCHEMA_REGISTRY.get_or_none(table_name)
        if schema:
            return TableConfig.from_schema(schema)
        return self.tables.get(table_name, TableConfig())

    @property
    def lock_dir_path(self) -> Path:
        """Get the lock directory path."""
        if self.lock_dir:
            return Path(self.lock_dir)
        return Path(self.base_dir) / "_locks"

    @property
    def aggregated_dir(self) -> Path:
        """Get the aggregated storage directory path."""
        return Path(self.base_dir) / "aggregated"


DEFAULT_CONFIG = CacheConfig.from_env()
