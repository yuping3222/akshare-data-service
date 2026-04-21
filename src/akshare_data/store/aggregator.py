from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from filelock import FileLock

from .parquet import PartitionManager
from .duckdb import DuckDBEngine
from .validator import deduplicate_by_key
from ..core.schema import get_table_schema

logger = logging.getLogger(__name__)


class AggregationError(Exception):
    pass


class Aggregator:
    def __init__(
        self,
        base_dir: str | Path,
        lock_timeout: int = 0,
    ):
        self.base_dir = Path(base_dir)
        self.lock_timeout = lock_timeout
        self.partition_manager = PartitionManager(self.base_dir)
        self.query_engine = DuckDBEngine(self.base_dir)

    def aggregate_table(
        self,
        table: str,
        threshold: int | None = None,
    ) -> int:
        table_schema = get_table_schema(table)
        if table_schema is None:
            return 0

        if not table_schema.aggregation_enabled:
            return 0

        lock = self._acquire_lock(table)
        if lock is None:
            return 0

        try:
            pending = self.needs_aggregation(table)
            aggregated_count = 0

            for partition_value in pending:
                files = self.partition_manager.list_partition_files(
                    table,
                    table_schema.storage_layer,
                    table_schema.partition_by,
                    partition_value,
                )
                file_count = len(files)
                effective_threshold = (
                    threshold
                    if threshold is not None
                    else table_schema.compaction_threshold
                )

                if file_count > effective_threshold:
                    success = self._aggregate_partition(table_schema, partition_value)
                    if success:
                        aggregated_count += 1

            return aggregated_count
        finally:
            lock.release()

    def aggregate_all(
        self,
        priority: str | None = None,
    ) -> dict[str, int]:
        result: dict[str, int] = {}

        from ..core.schema import SCHEMA_REGISTRY

        for name, table_schema in SCHEMA_REGISTRY.tables.items():
            if not table_schema.aggregation_enabled:
                continue
            if priority is not None and table_schema.priority != priority:
                continue
            count = self.aggregate_table(name)
            if count > 0:
                result[name] = count

        return result

    def cleanup(
        self,
        retention_hours: int = 24,
    ) -> int:
        deleted = 0
        now = time.time()
        retention_seconds = retention_hours * 3600

        from ..core.schema import SCHEMA_REGISTRY

        for name, table_schema in SCHEMA_REGISTRY.tables.items():
            if not table_schema.aggregation_enabled:
                continue
            if table_schema.partition_by is None:
                continue

            partitions = self.partition_manager.list_all_partitions(
                name,
                table_schema.storage_layer,
                table_schema.partition_by,
            )

            for partition_value in partitions:
                agg_path = self.partition_manager.aggregated_path(
                    name,
                    table_schema.storage_layer,
                    table_schema.partition_by,
                    partition_value,
                )
                if not agg_path.exists():
                    continue

                raw_path = self.partition_manager.raw_partition_path(
                    name,
                    table_schema.storage_layer,
                    table_schema.partition_by,
                    partition_value,
                )
                if not raw_path.exists():
                    continue

                for f in raw_path.iterdir():
                    if f.suffix != ".parquet":
                        continue
                    if f.name.endswith(".tmp"):
                        continue
                    if (now - f.stat().st_mtime) > retention_seconds:
                        try:
                            f.unlink()
                            deleted += 1
                        except OSError:
                            pass

        return deleted

    def needs_aggregation(
        self,
        table: str,
    ) -> list[str]:
        table_schema = get_table_schema(table)
        if table_schema is None:
            return []

        if not table_schema.aggregation_enabled:
            return []

        if table_schema.partition_by is None:
            return []

        partitions = self.partition_manager.list_all_partitions(
            table,
            table_schema.storage_layer,
            table_schema.partition_by,
        )

        pending = []
        for partition_value in partitions:
            files = self.partition_manager.list_partition_files(
                table,
                table_schema.storage_layer,
                table_schema.partition_by,
                partition_value,
            )
            if len(files) > table_schema.compaction_threshold:
                pending.append(partition_value)

        return pending

    def get_aggregation_status(
        self,
        table: str,
    ) -> dict:
        table_schema = get_table_schema(table)
        if table_schema is None:
            return {}

        if table_schema.partition_by is None:
            partitions = [""]
        else:
            partitions = self.partition_manager.list_all_partitions(
                table,
                table_schema.storage_layer,
                table_schema.partition_by,
            )

        total_partitions = len(partitions)
        aggregated_partitions = 0
        total_raw_files = 0
        total_aggregated_files = 0

        for partition_value in partitions:
            raw_files = self.partition_manager.list_partition_files(
                table,
                table_schema.storage_layer,
                table_schema.partition_by,
                partition_value,
            )
            total_raw_files += len(raw_files)

            agg_path = self.partition_manager.aggregated_path(
                table,
                table_schema.storage_layer,
                table_schema.partition_by,
                partition_value,
            )
            if agg_path.exists():
                aggregated_partitions += 1
                total_aggregated_files += 1

        pending_partitions = total_partitions - aggregated_partitions

        return {
            "total_partitions": total_partitions,
            "aggregated_partitions": aggregated_partitions,
            "pending_partitions": pending_partitions,
            "total_raw_files": total_raw_files,
            "total_aggregated_files": total_aggregated_files,
        }

    def _acquire_lock(self, table: str) -> FileLock | None:
        lock_file = self.partition_manager.lock_path(f"agg_{table}")
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(lock_file), timeout=self.lock_timeout)
        try:
            lock.acquire(blocking=self.lock_timeout > 0)
            return lock
        except Exception:
            return None

    def _aggregate_partition(
        self,
        table_schema,
        partition_value: str,
    ) -> bool:
        try:
            data = self._read_partition_raw(table_schema, partition_value)
            if data is None or data.empty:
                return False

            if table_schema.primary_key:
                data = deduplicate_by_key(data, table_schema.primary_key)

            self._write_aggregated(table_schema, partition_value, data)

            return True
        except Exception as e:
            logger.error(
                "Failed to aggregate partition %s=%s: %s",
                table_schema.partition_by,
                partition_value,
                e,
            )
            return False

    def _read_partition_raw(
        self,
        table_schema,
        partition_value: str,
    ) -> pd.DataFrame | None:
        raw_path = self.partition_manager.raw_partition_path(
            table_schema.name,
            table_schema.storage_layer,
            table_schema.partition_by,
            partition_value,
        )
        if not raw_path.exists():
            return None

        parquet_files = list(raw_path.glob("part_*.parquet"))
        if not parquet_files:
            return None

        paths_str = ", ".join(f"'{f}'" for f in parquet_files)
        conn = duckdb.connect(database=":memory:")
        try:
            df = conn.execute(f"SELECT * FROM read_parquet([{paths_str}])").fetchdf()
            return df if not df.empty else None
        finally:
            conn.close()

    def _write_aggregated(
        self,
        table_schema,
        partition_value: str,
        data: pd.DataFrame,
    ) -> Path:
        agg_path = self.partition_manager.aggregated_path(
            table_schema.name,
            table_schema.storage_layer,
            table_schema.partition_by,
            partition_value,
        )
        agg_path.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = agg_path.with_suffix(".parquet.tmp")
        try:
            table = pa.Table.from_pandas(data)
            pq.write_table(
                table,
                str(tmp_path),
                compression="snappy",
                row_group_size=100_000,
            )
            os.replace(str(tmp_path), str(agg_path))
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise

        return agg_path


def run_aggregation(
    base_dir: str,
    tables: list[str] | None = None,
    priority: str | None = None,
    cleanup: bool = True,
    retention_hours: int = 24,
) -> dict:
    aggregator = Aggregator(base_dir)

    if tables is not None:
        aggregated: dict[str, int] = {}
        for table in tables:
            count = aggregator.aggregate_table(table)
            if count > 0:
                aggregated[table] = count
    else:
        aggregated = aggregator.aggregate_all(priority=priority)

    cleaned_files = 0
    if cleanup:
        cleaned_files = aggregator.cleanup(retention_hours=retention_hours)

    return {
        "aggregated": aggregated,
        "cleaned_files": cleaned_files,
    }
