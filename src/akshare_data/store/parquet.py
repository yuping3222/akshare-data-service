import logging
import os
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..core.schema import get_table_schema
from .validator import SchemaValidator, SchemaValidationError

logger = logging.getLogger(__name__)

_TYPE_MAP = {
    "string": str,
    "float64": "float64",
    "int64": "int64",
    "bool": "bool",
    "date": "datetime64[ns]",
    "timestamp": "datetime64[ns]",
}


class PartitionManager:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)

    def raw_partition_path(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        partition_value: str | None = None,
    ) -> Path:
        if partition_by and partition_value:
            return (
                self.base_dir
                / storage_layer
                / table
                / f"{partition_by}={partition_value}"
            )
        return self.base_dir / storage_layer / table

    def aggregated_path(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        partition_value: str | None = None,
    ) -> Path:
        if partition_by and partition_value:
            return (
                self.base_dir
                / "aggregated"
                / storage_layer
                / table
                / f"{partition_by}={partition_value}.parquet"
            )
        return self.base_dir / "aggregated" / "meta" / f"{table}.parquet"

    def generate_filename(self, partition_value: str | None = None) -> str:
        pid = os.getpid()
        uid = uuid.uuid4().hex[:8]
        return f"part_{pid}_{uid}.parquet"

    def list_partition_files(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        partition_value: str | None = None,
    ) -> list[Path]:
        path = self.raw_partition_path(
            table, storage_layer, partition_by, partition_value
        )
        if not path.exists():
            return []
        return [f for f in path.glob("*.parquet") if not f.name.endswith(".tmp")]

    def list_all_partitions(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
    ) -> list[str]:
        if partition_by is None:
            return [""]
        path = self.base_dir / storage_layer / table
        if not path.exists():
            return []
        partitions = []
        for d in path.iterdir():
            if d.is_dir() and d.name.startswith(f"{partition_by}="):
                partitions.append(d.name.split("=", 1)[1])
        return sorted(partitions)

    def list_all_glob_paths(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        layer: str = "raw",
    ) -> str:
        if layer == "raw":
            base = self.base_dir / storage_layer / table
        else:
            base = self.base_dir / "aggregated" / storage_layer / table

        if partition_by is None:
            return str(base / "*.parquet")

        if layer == "raw":
            return str(base / "**/*.parquet")
        return str(base / "*.parquet")

    def ensure_dir(self, path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    def file_count(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        partition_value: str | None = None,
    ) -> int:
        return len(
            self.list_partition_files(
                table, storage_layer, partition_by, partition_value
            )
        )

    def total_size_bytes(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        partition_value: str | None = None,
    ) -> int:
        files = self.list_partition_files(
            table, storage_layer, partition_by, partition_value
        )
        return sum(f.stat().st_size for f in files)

    def lock_path(self, name: str) -> Path:
        return self.base_dir / "_locks" / f"{name}.lock"

    def remove_file(self, path: Path) -> bool:
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return False

    def remove_dir(self, path: Path) -> bool:
        try:
            import shutil

            shutil.rmtree(path)
            return True
        except FileNotFoundError:
            return False


class AtomicWriter:
    def __init__(
        self,
        base_dir: str | Path,
        compression: str = "snappy",
        row_group_size: int = 100_000,
        strict_schema: bool = False,
    ):
        self.base_dir = Path(base_dir)
        self.compression = compression
        self.row_group_size = row_group_size
        self.strict_schema = strict_schema
        self.partition_manager = PartitionManager(self.base_dir)

    def write(
        self,
        table: str,
        storage_layer: str,
        data: pd.DataFrame,
        partition_by: str | None = None,
        partition_value: str | None = None,
        schema: dict[str, str] | None = None,
        primary_key: list[str] | None = None,
    ) -> Path:
        partition_path = self.partition_manager.raw_partition_path(
            table, storage_layer, partition_by, partition_value
        )
        self.partition_manager.ensure_dir(partition_path)

        prepared_data = self._validate_and_prepare(table, data, schema, primary_key)

        filename = self.partition_manager.generate_filename(partition_value)
        target_path = partition_path / filename

        return self._write_atomic(prepared_data, target_path)

    def write_meta(
        self,
        table: str,
        data: pd.DataFrame,
        schema: dict[str, str] | None = None,
        primary_key: list[str] | None = None,
    ) -> Path:
        meta_path = self.base_dir / "meta"
        self.partition_manager.ensure_dir(meta_path)

        prepared_data = self._validate_and_prepare(table, data, schema, primary_key)

        target_path = meta_path / f"{table}.parquet"
        return self._write_atomic(prepared_data, target_path)

    def _validate_and_prepare(
        self,
        table: str,
        data: pd.DataFrame,
        schema: dict[str, str] | None,
        primary_key: list[str] | None,
    ) -> pd.DataFrame:
        table_schema = get_table_schema(table)
        effective_schema = schema
        if effective_schema is None and table_schema is not None:
            effective_schema = table_schema.schema

        effective_pk = primary_key
        if effective_pk is None and table_schema is not None:
            effective_pk = table_schema.primary_key

        if effective_schema is not None:
            validator = SchemaValidator(table, effective_schema)
            try:
                result = validator.validate_and_cast(data, primary_key=effective_pk)
            except SchemaValidationError as e:
                logger.error(
                    "Schema validation failed for table=%s: %s. "
                    "Falling back to legacy type coercion — data may be inconsistent.",
                    table,
                    e,
                )
                if self.strict_schema:
                    raise
                result = self._coerce_columns(data, effective_schema)
        else:
            result = data

        if effective_pk is not None:
            pk_cols = [c for c in effective_pk if c in result.columns]
            if pk_cols:
                result = self._deduplicate_by_key(result, pk_cols)

        return result

    def _coerce_columns(self, df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
        result = df.copy()
        for col, dtype_str in schema.items():
            if col not in result.columns:
                continue
            target_type = _TYPE_MAP.get(dtype_str)
            if target_type is None:
                continue
            try:
                if dtype_str in ("date", "timestamp"):
                    result[col] = pd.to_datetime(result[col], errors="coerce")
                else:
                    result[col] = result[col].astype(target_type)
            except (ValueError, TypeError):
                pass
        return result

    def _deduplicate_by_key(self, df: pd.DataFrame, key: list[str]) -> pd.DataFrame:
        return df.drop_duplicates(subset=key, keep="last")

    def _write_atomic(
        self,
        data: pd.DataFrame,
        target_path: Path,
    ) -> Path:
        tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
        try:
            table = pa.Table.from_pandas(data)
            pq.write_table(
                table,
                str(tmp_path),
                compression=self.compression,
                row_group_size=self.row_group_size,
            )
            os.replace(str(tmp_path), str(target_path))
        except OSError as e:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise IOError(f"Failed to write parquet file '{target_path}': {e}") from e
        return target_path
