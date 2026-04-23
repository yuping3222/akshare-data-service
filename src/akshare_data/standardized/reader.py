"""Standardized (L1) reader.

Reads standardized data by entity, partition, time range, and batch_id.
Supports version filtering and compaction-aware reading.

Spec: docs/design/40-standardized-storage-spec.md §8
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.standardized.manifest import (
    PartitionManifest,
    MANIFEST_FILENAME,
)

logger = logging.getLogger(__name__)


class StandardizedReader:
    """Reads DataFrames from Standardized (L1) storage.

    Usage::

        reader = StandardizedReader(base_dir="data/standardized")

        df = reader.read(
            dataset="market_quote_daily",
            domain="market",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 21),
        )

        df = reader.read_by_batch(
            dataset="market_quote_daily",
            domain="market",
            batch_id="20260421_abc123",
        )
    """

    def __init__(self, base_dir: str = "data/standardized") -> None:
        self._base_dir = Path(base_dir).resolve()

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def _dataset_dir(self, domain: str, dataset: str) -> Path:
        return self._base_dir / domain / dataset

    def _list_partition_dirs(
        self,
        domain: str,
        dataset: str,
        partition_key: str,
        start_value: Optional[str] = None,
        end_value: Optional[str] = None,
    ) -> List[Path]:
        """List partition directories within an optional range."""
        dataset_dir = self._dataset_dir(domain, dataset)
        if not dataset_dir.exists():
            return []

        prefix = f"{partition_key}="
        dirs = []
        for d in sorted(dataset_dir.iterdir()):
            if d.is_dir() and d.name.startswith(prefix):
                value = d.name[len(prefix) :]
                if start_value and value < start_value:
                    continue
                if end_value and value > end_value:
                    continue
                dirs.append(d)
        return dirs

    def _read_partition_dir(self, part_dir: Path) -> Optional[pd.DataFrame]:
        """Read all parquet files from a partition directory.

        Checks for compacted files first (in _compacted/), then falls back
        to original files in the partition directory.
        """
        compacted_dir = (
            self._base_dir
            / "_compacted"
            / part_dir.parent.parent.name
            / part_dir.parent.name
            / part_dir.name
        )
        compacted_files = (
            list(compacted_dir.glob("compacted-*.parquet"))
            if compacted_dir.exists()
            else []
        )
        if compacted_files:
            frames = []
            for f in sorted(compacted_files):
                try:
                    frames.append(pd.read_parquet(f))
                except Exception as e:
                    logger.warning("Failed to read compacted file %s: %s", f, e)
            if frames:
                return pd.concat(frames, ignore_index=True)

        parquet_files = list(part_dir.glob("part-*.parquet"))
        if not parquet_files:
            return None

        frames = []
        for f in sorted(parquet_files):
            try:
                frames.append(pd.read_parquet(f))
            except Exception as e:
                logger.warning("Failed to read parquet file %s: %s", f, e)

        if not frames:
            return None

        return pd.concat(frames, ignore_index=True)

    def read(
        self,
        *,
        dataset: str,
        domain: str,
        partition_key: str = "trade_date",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        normalize_version: Optional[str] = None,
        batch_id: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read standardized data by entity and time range.

        Args:
            dataset: Entity name (e.g. "market_quote_daily").
            domain: Logical domain (e.g. "market").
            partition_key: Business time column for partitioning.
            start_date: Inclusive start date.
            end_date: Inclusive end date.
            normalize_version: Filter by normalization version.
            batch_id: Filter by batch identifier.
            columns: Optional column selection.

        Returns:
            Combined DataFrame from all matching partitions.
        """
        start_str = start_date.isoformat() if start_date else None
        end_str = end_date.isoformat() if end_date else None

        partition_dirs = self._list_partition_dirs(
            domain, dataset, partition_key, start_str, end_str
        )

        frames: List[pd.DataFrame] = []
        for part_dir in partition_dirs:
            df = self._read_partition_dir(part_dir)
            if df is None or df.empty:
                continue

            if normalize_version:
                df = df[df["normalize_version"] == normalize_version]
            if batch_id:
                df = df[df["batch_id"] == batch_id]
            if df.empty:
                continue

            if columns:
                available = [c for c in columns if c in df.columns]
                df = df[available]

            frames.append(df)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)

        if partition_key in result.columns:
            result = result.sort_values(
                by=[partition_key]
                + (["security_id"] if "security_id" in result.columns else [])
            )

        return result

    def read_by_batch(
        self,
        *,
        dataset: str,
        domain: str,
        batch_id: str,
        partition_key: str = "trade_date",
    ) -> pd.DataFrame:
        """Read all data written by a specific batch.

        Useful for replay and debugging.

        Args:
            dataset: Entity name.
            domain: Logical domain.
            batch_id: Batch identifier.
            partition_key: Partition key for scanning.

        Returns:
            DataFrame containing only rows from the specified batch.
        """
        dataset_dir = self._dataset_dir(domain, dataset)
        if not dataset_dir.exists():
            return pd.DataFrame()

        prefix = f"{partition_key}="
        frames: List[pd.DataFrame] = []

        for part_dir in sorted(dataset_dir.iterdir()):
            if not part_dir.is_dir() or not part_dir.name.startswith(prefix):
                continue

            manifest_path = part_dir / MANIFEST_FILENAME
            manifest = PartitionManifest.load(manifest_path)
            if manifest is None:
                continue

            batch_entry = manifest.get_batch(batch_id)
            if batch_entry is None:
                continue

            df = self._read_partition_dir(part_dir)
            if df is None or df.empty:
                continue

            df = df[df["batch_id"] == batch_id]
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def read_by_primary_key(
        self,
        *,
        dataset: str,
        domain: str,
        primary_key: Dict[str, Any],
        partition_key: str = "trade_date",
    ) -> pd.DataFrame:
        """Read data matching specific primary key values.

        Args:
            dataset: Entity name.
            domain: Logical domain.
            primary_key: Dict of {column: value} for primary key lookup.
            partition_key: Partition key (must be in primary_key dict).

        Returns:
            DataFrame matching the primary key.
        """
        if partition_key not in primary_key:
            raise ValueError(
                f"Primary key lookup requires '{partition_key}' in the key dict"
            )

        partition_value = str(primary_key[partition_key])
        if isinstance(primary_key[partition_key], date):
            partition_value = primary_key[partition_key].isoformat()

        part_dir = (
            self._dataset_dir(domain, dataset) / f"{partition_key}={partition_value}"
        )
        if not part_dir.exists():
            return pd.DataFrame()

        df = self._read_partition_dir(part_dir)
        if df is None or df.empty:
            return pd.DataFrame()

        for col, val in primary_key.items():
            if col in df.columns:
                df = df[df[col] == val]

        return df

    def list_partitions(
        self,
        *,
        dataset: str,
        domain: str,
        partition_key: str = "trade_date",
    ) -> List[str]:
        """List all available partition values for a dataset."""
        dirs = self._list_partition_dirs(domain, dataset, partition_key)
        return [d.name[len(partition_key) + 1 :] for d in dirs]

    def get_manifest(
        self,
        *,
        dataset: str,
        domain: str,
        partition_key: str,
        partition_value: str,
    ) -> Optional[PartitionManifest]:
        """Load the manifest for a specific partition."""
        part_dir = (
            self._dataset_dir(domain, dataset) / f"{partition_key}={partition_value}"
        )
        manifest_path = part_dir / MANIFEST_FILENAME
        return PartitionManifest.load(manifest_path)
