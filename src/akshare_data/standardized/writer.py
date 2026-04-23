"""Standardized (L1) writer.

Writes normalized DataFrames to the Standardized layer with:
- Schema validation against entity config
- Primary key deduplication
- System field injection (quality_status, publish_time, release_version)
- Partitioning by business time
- Atomic Parquet writes (tmp → rename)
- Manifest tracking

Spec: docs/design/40-standardized-storage-spec.md §4
"""

from __future__ import annotations

import hashlib
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.standardized.manifest import (
    BatchEntry,
    MANIFEST_FILENAME,
    load_or_create_manifest,
)

logger = logging.getLogger(__name__)

STANDARDIZED_SYSTEM_FIELDS = (
    "batch_id",
    "source_name",
    "interface_name",
    "ingest_time",
    "normalize_version",
    "schema_version",
    "quality_status",
    "publish_time",
    "release_version",
)

_DEFAULT_TYPE_MAP = {
    "string": str,
    "double": "float64",
    "int64": "int64",
    "bool": "bool",
    "date": "object",
    "timestamp": "datetime64[ns, UTC]",
}


class StandardizedWriter:
    """Writes DataFrames to Standardized (L1) storage.

    Usage::

        writer = StandardizedWriter(base_dir="data/standardized")
        writer.write(
            df=normalized_df,
            dataset="market_quote_daily",
            domain="market",
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
            schema=entity_schema,
            batch_id="20260421_abc123",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            normalize_version="v1",
            schema_version="v1",
        )
    """

    def __init__(
        self,
        base_dir: str = "data/standardized",
        compression: str = "snappy",
    ) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._compression = compression

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def _partition_dir(
        self,
        domain: str,
        dataset: str,
        partition_key: str,
        partition_value: str,
    ) -> Path:
        return self._base_dir / domain / dataset / f"{partition_key}={partition_value}"

    def _validate_schema(
        self,
        df: pd.DataFrame,
        schema: Optional[Dict[str, Any]],
        primary_key: List[str],
        strict: bool = True,
    ) -> pd.DataFrame:
        """Validate DataFrame against entity schema.

        Args:
            df: Input DataFrame.
            schema: Entity field definitions from YAML config.
            primary_key: Primary key columns.
            strict: If True, reject extra columns not in schema.

        Returns:
            Validated (and type-cast) DataFrame.

        Raises:
            ValueError: If required fields are missing or types are incompatible.
        """
        if schema is None:
            return df

        fields_def = schema.get("fields", {})
        required_fields = set(schema.get("required_fields", []))
        pk_set = set(primary_key)

        missing_required = required_fields - set(df.columns)
        if missing_required:
            raise ValueError(f"Missing required fields: {sorted(missing_required)}")

        missing_pk = pk_set - set(df.columns)
        if missing_pk:
            raise ValueError(f"Missing primary key columns: {sorted(missing_pk)}")

        result = df.copy()

        for col_name, col_def in fields_def.items():
            if col_name not in result.columns:
                continue
            expected_type = col_def.get("type")
            if expected_type is None:
                continue
            target = _DEFAULT_TYPE_MAP.get(expected_type)
            if target is None:
                continue
            try:
                if expected_type == "date":
                    result[col_name] = pd.to_datetime(
                        result[col_name], errors="coerce"
                    ).dt.date
                elif expected_type == "timestamp":
                    result[col_name] = pd.to_datetime(
                        result[col_name], errors="coerce", utc=True
                    )
                elif expected_type in ("double", "int64"):
                    result[col_name] = pd.to_numeric(result[col_name], errors="coerce")
            except (ValueError, TypeError) as e:
                logger.warning("Type coercion failed for column '%s': %s", col_name, e)

        if strict:
            allowed = set(fields_def.keys()) | set(STANDARDIZED_SYSTEM_FIELDS)
            extra = set(result.columns) - allowed
            if extra:
                logger.warning(
                    "Extra columns not in schema (dropping): %s", sorted(extra)
                )
                result = result.drop(columns=list(extra))

        return result

    def _deduplicate_by_key(
        self, df: pd.DataFrame, primary_key: List[str]
    ) -> pd.DataFrame:
        """Deduplicate by primary key, keeping the last row."""
        before = len(df)
        result = df.drop_duplicates(subset=primary_key, keep="last")
        dropped = before - len(result)
        if dropped:
            logger.debug(
                "Deduplication dropped %d duplicate rows (pk=%s)",
                dropped,
                primary_key,
            )
        return result

    def _inject_system_fields(
        self,
        df: pd.DataFrame,
        *,
        batch_id: str,
        source_name: str,
        interface_name: str,
        normalize_version: str,
        schema_version: str,
        ingest_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Inject mandatory system fields."""
        df = df.copy()
        now = ingest_time or datetime.now(timezone.utc)

        df["batch_id"] = batch_id
        df["source_name"] = source_name
        df["interface_name"] = interface_name
        df["ingest_time"] = now
        df["normalize_version"] = normalize_version
        df["schema_version"] = schema_version

        if "quality_status" not in df.columns:
            df["quality_status"] = "pending"
        if "publish_time" not in df.columns:
            df["publish_time"] = pd.NaT
        if "release_version" not in df.columns:
            df["release_version"] = None

        return df

    def _partition_by_key(
        self,
        df: pd.DataFrame,
        partition_key: str,
    ) -> Dict[str, pd.DataFrame]:
        """Split DataFrame into groups by partition_key values."""
        if partition_key not in df.columns:
            raise ValueError(
                f"Partition key '{partition_key}' not found in DataFrame columns"
            )

        groups: Dict[str, pd.DataFrame] = {}
        for value, group_df in df.groupby(partition_key, sort=True):
            pv = str(value)
            if isinstance(value, datetime):
                pv = value.strftime("%Y-%m-%d")
            groups[pv] = group_df.copy()
        return groups

    def write(
        self,
        df: pd.DataFrame,
        *,
        dataset: str,
        domain: str,
        partition_key: str,
        primary_key: List[str],
        schema: Optional[Dict[str, Any]] = None,
        batch_id: str,
        source_name: str,
        interface_name: str,
        normalize_version: str = "v1",
        schema_version: str = "v1",
        ingest_time: Optional[datetime] = None,
    ) -> Dict[str, Path]:
        """Write a normalized DataFrame to Standardized storage.

        Args:
            df: Normalized DataFrame (standard field names).
            dataset: Entity name (e.g. "market_quote_daily").
            domain: Logical domain (e.g. "market", "macro").
            partition_key: Business time column for partitioning.
            primary_key: Columns forming the primary key.
            schema: Entity schema definition from YAML config.
            batch_id: Batch identifier for traceability.
            source_name: Source adapter name.
            interface_name: Source interface name.
            normalize_version: Normalization rule version.
            schema_version: Entity schema version.
            ingest_time: Override ingest timestamp.

        Returns:
            Dict mapping partition_value -> partition directory path.
        """
        if df.empty:
            logger.info("Empty DataFrame, skipping write for dataset=%s", dataset)
            return {}

        validated = self._validate_schema(df, schema, primary_key)
        deduped = self._deduplicate_by_key(validated, primary_key)
        enriched = self._inject_system_fields(
            deduped,
            batch_id=batch_id,
            source_name=source_name,
            interface_name=interface_name,
            normalize_version=normalize_version,
            schema_version=schema_version,
            ingest_time=ingest_time,
        )

        partitions = self._partition_by_key(enriched, partition_key)
        result_paths: Dict[str, Path] = {}

        for partition_value, part_df in partitions.items():
            part_dir = self._write_partition(
                df=part_df,
                dataset=dataset,
                domain=domain,
                partition_key=partition_key,
                partition_value=partition_value,
                primary_key=primary_key,
                batch_id=batch_id,
                source_name=source_name,
                interface_name=interface_name,
                normalize_version=normalize_version,
                schema_version=schema_version,
            )
            result_paths[partition_value] = part_dir

        return result_paths

    def _write_partition(
        self,
        df: pd.DataFrame,
        *,
        dataset: str,
        domain: str,
        partition_key: str,
        partition_value: str,
        primary_key: List[str],
        batch_id: str,
        source_name: str,
        interface_name: str,
        normalize_version: str,
        schema_version: str,
    ) -> Path:
        """Write a single partition and update its manifest."""
        part_dir = self._partition_dir(domain, dataset, partition_key, partition_value)
        part_dir.mkdir(parents=True, exist_ok=True)

        file_hash = hashlib.md5(
            f"{batch_id}-{datetime.now(timezone.utc).isoformat()}".encode()
        ).hexdigest()[:8]
        parquet_filename = f"part-{file_hash}.parquet"
        parquet_path = part_dir / parquet_filename

        tmp_fd, tmp_path_str = tempfile.mkstemp(
            suffix=".parquet.tmp", dir=str(part_dir)
        )
        tmp_path = Path(tmp_path_str)

        try:
            df.to_parquet(
                tmp_path,
                engine="pyarrow",
                compression=self._compression,
                index=False,
            )
            tmp_path.rename(parquet_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        manifest = load_or_create_manifest(
            partition_dir=part_dir,
            dataset=dataset,
            domain=domain,
            partition_key=partition_key,
            partition_value=partition_value,
        )

        entry = BatchEntry.create(
            batch_id=batch_id,
            normalize_version=normalize_version,
            schema_version=schema_version,
            source_name=source_name,
            interface_name=interface_name,
            record_count=len(df),
            files=[parquet_filename],
        )
        manifest.add_batch(entry)

        manifest_path = part_dir / MANIFEST_FILENAME
        manifest.save(manifest_path)

        logger.info(
            "Written %d records to %s (batch=%s)",
            len(df),
            part_dir,
            batch_id,
        )

        return part_dir
