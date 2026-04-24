import hashlib
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd

from .duckdb import DuckDBEngine
from .memory import MemoryCache
from .parquet import AtomicWriter, PartitionManager
from ..core.config import CacheConfig
from ..core.schema import get_table_schema

logger = logging.getLogger(__name__)


CACHE_KEY_VERSION = "v2"


def _create_cache_config(**kwargs) -> CacheConfig:
    """创建缓存配置（统一使用 core.config.CacheConfig）"""
    return CacheConfig.from_env(**kwargs)


class CacheManager:
    _instance: "CacheManager | None" = None
    _lock = threading.Lock()

    def __init__(
        self,
        base_dir: str | None = None,
        config: CacheConfig | None = None,
    ):
        cfg = config or _create_cache_config()
        if base_dir:
            cfg.base_dir = base_dir

        self.config = cfg
        self.partition_manager = PartitionManager(cfg.base_dir)
        default_strict_level: str = "error" if cfg.strict_schema else "warn"
        self.writer = AtomicWriter(
            cfg.base_dir,
            compression=cfg.compression,
            row_group_size=cfg.row_group_size,
            strict_level=default_strict_level,
        )
        self.engine = DuckDBEngine(
            cfg.base_dir,
            threads=cfg.duckdb_threads,
            memory_limit=cfg.duckdb_memory_limit,
        )
        self.memory_cache = MemoryCache(
            max_items=cfg.memory_cache_max_items,
            default_ttl_seconds=cfg.memory_cache_default_ttl_seconds,
        )

    @classmethod
    def get_instance(cls, config: CacheConfig | None = None) -> "CacheManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(config=config)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._lock:
            cls._instance = None

    def read(
        self,
        table: str,
        storage_layer: str | None = None,
        partition_by: str | None = None,
        partition_value: str | None = None,
        where: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        table_schema = get_table_schema(table)
        if table_schema is not None:
            if storage_layer is None:
                storage_layer = table_schema.storage_layer
            if partition_by is None:
                partition_by = table_schema.partition_by

        if storage_layer is None:
            storage_layer = "daily"

        self._cleanup_expired_files(table, table_schema, storage_layer, partition_by)

        if not force_refresh:
            cache_key = self._make_cache_key(
                table=table,
                storage_layer=storage_layer,
                partition_by=partition_by,
                partition_value=partition_value,
                where=where,
                columns=columns,
                order_by=order_by,
                limit=limit,
                force_refresh=force_refresh,
            )
            cached = self.memory_cache.get(cache_key)
            if cached is not None:
                logger.debug("Memory cache hit for table=%s key=%s", table, cache_key)
                return cached.copy()

        result = self.engine.query(
            table,
            storage_layer,
            partition_by=partition_by,
            where=where,
            columns=columns,
            order_by=order_by,
            limit=limit,
        )

        if result is not None and not result.empty:
            cache_key = self._make_cache_key(
                table=table,
                storage_layer=storage_layer,
                partition_by=partition_by,
                partition_value=partition_value,
                where=where,
                columns=columns,
                order_by=order_by,
                limit=limit,
                force_refresh=force_refresh,
            )
            self.memory_cache.put(cache_key, result)
            logger.debug("Query hit for table=%s, wrote to memory cache", table)

        return result if result is not None else pd.DataFrame()

    def write(
        self,
        table: str,
        data: pd.DataFrame,
        storage_layer: str | None = None,
        partition_by: str | None = None,
        partition_value: str | None = None,
        schema: dict[str, str] | None = None,
        primary_key: list[str] | None = None,
    ) -> str:
        if data.empty:
            logger.debug("Empty DataFrame for table=%s, skipping write", table)
            return ""

        table_schema = get_table_schema(table)
        skip_schema_validation = schema is None and primary_key is None
        if table_schema is not None:
            if storage_layer is None:
                storage_layer = table_schema.storage_layer
            if not skip_schema_validation:
                if partition_by is None:
                    partition_by = table_schema.partition_by
                if schema is None:
                    schema = table_schema.schema
                if primary_key is None:
                    primary_key = table_schema.primary_key

        if storage_layer is None:
            storage_layer = "daily"

        layer_strict_level = self._get_strict_level_for_layer(storage_layer)
        file_path = self.writer.write(
            table,
            storage_layer,
            data,
            partition_by=partition_by,
            partition_value=partition_value,
            schema=schema,
            primary_key=primary_key,
            skip_validation=schema is None and primary_key is None,
            strict_level=layer_strict_level,
        )

        # Writing modifies persisted data; clear potentially stale query-shape keys
        # at table (or partition) granularity, then only cache the precise "full read"
        # key for this write request.
        self._invalidate_memory_keys(
            table=table,
            storage_layer=storage_layer,
            partition_by=partition_by,
            partition_value=partition_value,
        )
        cache_key = self._make_cache_key(
            table=table,
            storage_layer=storage_layer,
            partition_by=partition_by,
            partition_value=partition_value,
            where=None,
            columns=None,
            order_by=None,
            limit=None,
            force_refresh=False,
        )
        self.memory_cache.put(cache_key, data)

        logger.info("Wrote table=%s to %s", table, file_path)
        return str(file_path)

    def _get_strict_level_for_layer(self, storage_layer: str | None) -> str:
        """Determine strict_level based on storage layer.

        Raw layer uses warn+quarantine; standardized and served layers raise on errors.
        """
        layer_map: dict[str, str] = {
            "raw": "warn",
            "standardized": "error",
            "served": "error",
        }
        return layer_map.get(storage_layer or "", "error")

    def has_range(
        self,
        table: str,
        storage_layer: str | None = None,
        partition_by: str | None = None,
        where: dict[str, Any] | None = None,
        date_col: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> bool:
        table_schema = get_table_schema(table)
        if table_schema is not None:
            if storage_layer is None:
                storage_layer = table_schema.storage_layer
            if partition_by is None:
                partition_by = table_schema.partition_by

        if storage_layer is None:
            storage_layer = "daily"

        if date_col is None:
            date_col = (
                table_schema.partition_by
                if table_schema and table_schema.partition_by
                else "date"
            )

        result = self.read(
            table,
            storage_layer,
            partition_by=partition_by,
            where=where,
            columns=[date_col],
        )
        if result is None or result.empty:
            return False

        min_date = pd.to_datetime(result[date_col].min())
        max_date = pd.to_datetime(result[date_col].max())

        if start:
            start_dt = pd.to_datetime(start)
            if min_date > start_dt:
                return False

        if end:
            end_dt = pd.to_datetime(end)
            if max_date < end_dt:
                return False

        return True

    def exists(
        self,
        table: str,
        storage_layer: str = "daily",
        partition_by: str | None = None,
        where: dict[str, Any] | None = None,
    ) -> bool:
        return self.engine.exists(
            table,
            storage_layer,
            partition_by=partition_by,
            where=where,
        )

    def invalidate(
        self,
        table: str,
        storage_layer: str = "daily",
        partition_by: str | None = None,
        partition_value: str | None = None,
    ) -> int:
        self._invalidate_memory_keys(
            table=table,
            storage_layer=storage_layer,
            partition_by=partition_by,
            partition_value=partition_value,
        )

        if partition_by and partition_value:
            files = self.partition_manager.list_partition_files(
                table, storage_layer, partition_by, partition_value
            )
        elif partition_by:
            partitions = self.partition_manager.list_all_partitions(
                table, storage_layer, partition_by
            )
            files = []
            for pv in partitions:
                files.extend(
                    self.partition_manager.list_partition_files(
                        table, storage_layer, partition_by, pv
                    )
                )
        else:
            files = self.partition_manager.list_partition_files(
                table, storage_layer, partition_by
            )

        deleted = 0
        for f in files:
            if self.partition_manager.remove_file(f):
                deleted += 1

        logger.info("Invalidated %d files for table=%s", deleted, table)
        return deleted

    def invalidate_all(self, table: str | None = None) -> int:
        count = self.memory_cache.invalidate()
        logger.info("Invalidated all cache, cleared %d memory entries", count)
        return count

    def table_info(
        self,
        table: str,
        storage_layer: str = "daily",
        partition_by: str | None = None,
    ) -> dict:
        files = self.partition_manager.list_partition_files(
            table, storage_layer, partition_by
        )
        if not files and partition_by:
            partitions = self.partition_manager.list_all_partitions(
                table, storage_layer, partition_by
            )
            for pv in partitions:
                files.extend(
                    self.partition_manager.list_partition_files(
                        table, storage_layer, partition_by, pv
                    )
                )

        file_count = len(files)
        total_size_bytes = sum(f.stat().st_size for f in files) if files else 0

        last_updated = None
        if files:
            mtimes = [f.stat().st_mtime for f in files]
            from datetime import datetime

            last_updated = datetime.fromtimestamp(max(mtimes))

        partitions = self.partition_manager.list_all_partitions(
            table, storage_layer, partition_by
        )
        partition_count = len(partitions) if partition_by else 0

        return {
            "name": table,
            "file_count": file_count,
            "total_size_bytes": total_size_bytes,
            "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
            "last_updated": last_updated,
            "partition_count": partition_count,
        }

    def list_tables(self, storage_layer: str | None = None) -> list[str]:
        base = Path(self.config.base_dir)
        if not base.exists():
            return []

        if storage_layer:
            layer_path = base / storage_layer
            if not layer_path.exists():
                return []
            tables = [d.name for d in layer_path.iterdir() if d.is_dir()]
            if storage_layer == "meta":
                tables.extend(
                    p.stem for p in layer_path.glob("*.parquet") if p.is_file()
                )
            return sorted(set(tables))

        layers = [d for d in base.iterdir() if d.is_dir() and d.name != "_locks"]
        tables: set[str] = set()
        for layer in layers:
            if layer.name == "aggregated":
                continue
            tables.update(self.list_tables(layer.name))
        return sorted(tables)

    def get_stats(self) -> dict:
        tables = {}
        for table_name in self.list_tables(storage_layer="daily"):
            tables[table_name] = self.table_info(table_name, storage_layer="daily")

        return {
            "memory_cache_size": self.memory_cache.size,
            "memory_cache_hit_rate": self.memory_cache.hit_rate,
            "tables": tables,
        }

    def _cleanup_expired_files(
        self,
        table: str,
        table_schema: Any,
        storage_layer: str,
        partition_by: str | None,
    ) -> None:
        """Delete parquet files older than the table's ttl_hours."""
        if table_schema is None or table_schema.ttl_hours <= 0:
            return

        ttl_seconds = table_schema.ttl_hours * 3600
        cutoff = time.time() - ttl_seconds
        deleted = 0

        if partition_by is None:
            # Non-partitioned table: single directory
            dir_path = self.partition_manager.raw_partition_path(
                table, storage_layer, partition_by
            )
            dirs_to_scan = [dir_path] if dir_path.exists() else []
        else:
            # Partitioned table: scan all partition directories
            base = Path(self.config.base_dir) / storage_layer / table
            if not base.exists():
                return
            dirs_to_scan = [d for d in base.iterdir() if d.is_dir()]

        for dir_path in dirs_to_scan:
            for f in dir_path.iterdir():
                if f.suffix != ".parquet" or f.name.endswith(".tmp"):
                    continue
                if f.stat().st_mtime < cutoff:
                    try:
                        f.unlink()
                        deleted += 1
                    except OSError:
                        pass

        if deleted:
            logger.info(
                "TTL cleanup for table=%s: deleted %d expired files (ttl=%dh)",
                table,
                deleted,
                table_schema.ttl_hours,
            )

    def _make_cache_key(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        partition_value: str | None,
        where: dict[str, Any] | None,
        columns: list[str] | None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        force_refresh: bool = False,
    ) -> str:
        partition_hash = hashlib.md5(
            json.dumps(
                {"partition_by": partition_by, "partition_value": partition_value},
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()[:8]
        where_hash = hashlib.md5(
            json.dumps(where or {}, sort_keys=True, default=str).encode()
        ).hexdigest()[:8]
        columns_hash = hashlib.md5(
            json.dumps(columns or [], sort_keys=True, default=str).encode()
        ).hexdigest()[:8]
        order_hash = hashlib.md5(
            json.dumps(order_by or [], sort_keys=True, default=str).encode()
        ).hexdigest()[:8]
        limit_hash = hashlib.md5(json.dumps(limit, default=str).encode()).hexdigest()[
            :8
        ]
        refresh_flag = "fr1" if force_refresh else "fr0"
        return (
            f"{CACHE_KEY_VERSION}:{table}:{storage_layer}:{partition_hash}:"
            f"{where_hash}:{columns_hash}:{order_hash}:{limit_hash}:{refresh_flag}"
        )

    def _invalidate_memory_keys(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None = None,
        partition_value: str | None = None,
    ) -> int:
        """Invalidate memory keys for a table/storage scope (optionally partition-scoped)."""
        prefix = f"{CACHE_KEY_VERSION}:{table}:{storage_layer}:"
        deleted = 0
        partition_scope = None
        if partition_by is not None or partition_value is not None:
            partition_scope = hashlib.md5(
                json.dumps(
                    {"partition_by": partition_by, "partition_value": partition_value},
                    sort_keys=True,
                    default=str,
                ).encode()
            ).hexdigest()[:8]

        keys = list(self.memory_cache._metadata.keys())
        for key in keys:
            if not key.startswith(prefix):
                continue
            if partition_scope is not None:
                parts = key.split(":")
                if len(parts) < 4 or parts[3] != partition_scope:
                    continue
            deleted += self.memory_cache.invalidate(key)
        return deleted

    def aggregate(self, table: str, threshold: int | None = None) -> int:
        """Run compaction for a table."""
        from .aggregator import Aggregator

        aggregator = Aggregator(self.config.base_dir)
        return aggregator.aggregate_table(table, threshold=threshold)

    def aggregate_all(self, priority: str | None = None) -> dict[str, int]:
        """Run compaction for all tables."""
        from .aggregator import Aggregator

        aggregator = Aggregator(self.config.base_dir)
        return aggregator.aggregate_all(priority=priority)

    def cleanup(self, retention_hours: int = 24) -> int:
        """Clean up old raw files after compaction."""
        from .aggregator import Aggregator

        aggregator = Aggregator(self.config.base_dir)
        return aggregator.cleanup(retention_hours=retention_hours)

    def aggregation_status(self, table: str) -> dict:
        """Get aggregation status for a table."""
        from .aggregator import Aggregator

        aggregator = Aggregator(self.config.base_dir)
        return aggregator.get_aggregation_status(table)


def get_cache_manager(
    base_dir: str | None = None,
    config: CacheConfig | None = None,
) -> "CacheManager":
    """Get or create the global CacheManager singleton."""
    if config is None:
        if base_dir:
            config = CacheConfig.from_env(base_dir=base_dir)
        else:
            config = CacheConfig.from_env()
    elif base_dir:
        config.base_dir = base_dir
    return CacheManager.get_instance(config=config)


def reset_cache_manager():
    """Reset the global CacheManager singleton (for testing)."""
    with CacheManager._lock:
        CacheManager._instance = None
