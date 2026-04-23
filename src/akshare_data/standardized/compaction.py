"""Compaction job for Standardized (L1) layer.

Merges small parquet files within a partition into larger files
without breaking version traceability.

Spec: docs/design/40-standardized-storage-spec.md §7
"""

from __future__ import annotations

import json
import logging
import tempfile
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.standardized.manifest import (
    PartitionManifest,
    MANIFEST_FILENAME,
)

logger = logging.getLogger(__name__)

COMPACTION_MANIFEST_FILENAME = "_compaction_manifest.json"


@dataclass
class CompactionManifest:
    """Records a single compaction operation."""

    compaction_id: str
    dataset: str
    domain: str
    partition_key: str
    partition_value: str
    source_files: List[str]
    compacted_file: str
    record_count: int
    compacted_at: str
    source_batches: List[str]

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        domain: str,
        partition_key: str,
        partition_value: str,
        source_files: List[str],
        compacted_file: str,
        record_count: int,
        source_batches: List[str],
    ) -> "CompactionManifest":
        return cls(
            compaction_id=f"comp-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}",
            dataset=dataset,
            domain=domain,
            partition_key=partition_key,
            partition_value=partition_value,
            source_files=source_files,
            compacted_file=compacted_file,
            record_count=record_count,
            compacted_at=datetime.now(timezone.utc).isoformat(),
            source_batches=source_batches,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompactionManifest":
        return cls(**data)

    @classmethod
    def from_json(cls, text: str) -> "CompactionManifest":
        return cls.from_dict(json.loads(text))

    @classmethod
    def load(cls, path: Path) -> Optional["CompactionManifest"]:
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                return cls.from_json(f.read())
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Corrupted compaction manifest at %s: %s", path, e)
            return None

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".json.tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(self.to_json())
            tmp_path.rename(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise


class CompactionJob:
    """Compacts small parquet files within a partition.

    Usage::

        job = CompactionJob(
            base_dir="data/standardized",
            compaction_threshold=10,
            compaction_min_size_bytes=50 * 1024 * 1024,
        )

        result = job.run(
            dataset="market_quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2026-04-21",
        )
    """

    def __init__(
        self,
        base_dir: str = "data/standardized",
        compaction_threshold: int = 10,
        compaction_min_size_bytes: int = 50 * 1024 * 1024,
        compression: str = "snappy",
    ) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._compaction_threshold = compaction_threshold
        self._compaction_min_size = compaction_min_size_bytes
        self._compression = compression

    def _dataset_dir(self, domain: str, dataset: str) -> Path:
        return self._base_dir / domain / dataset

    def _compacted_dir(
        self, domain: str, dataset: str, partition_key: str, partition_value: str
    ) -> Path:
        return (
            self._base_dir
            / "_compacted"
            / domain
            / dataset
            / f"{partition_key}={partition_value}"
        )

    def _partition_dir(
        self, domain: str, dataset: str, partition_key: str, partition_value: str
    ) -> Path:
        return self._base_dir / domain / dataset / f"{partition_key}={partition_value}"

    def needs_compaction(
        self,
        *,
        domain: str,
        dataset: str,
        partition_key: str,
        partition_value: str,
    ) -> bool:
        """Check if a partition needs compaction."""
        part_dir = self._partition_dir(domain, dataset, partition_key, partition_value)
        if not part_dir.exists():
            return False

        parquet_files = list(part_dir.glob("part-*.parquet"))
        if len(parquet_files) < self._compaction_threshold:
            return False

        total_size = sum(f.stat().st_size for f in parquet_files)
        if total_size >= self._compaction_min_size:
            return True

        return False

    def run(
        self,
        *,
        dataset: str,
        domain: str,
        partition_key: str,
        partition_value: str,
        primary_key: Optional[List[str]] = None,
    ) -> Optional[CompactionManifest]:
        """Run compaction on a single partition.

        Args:
            dataset: Entity name.
            domain: Logical domain.
            partition_key: Partition key.
            partition_value: Partition value.
            primary_key: Primary key columns for deduplication.

        Returns:
            CompactionManifest if compaction was performed, None otherwise.
        """
        part_dir = self._partition_dir(domain, dataset, partition_key, partition_value)
        if not part_dir.exists():
            logger.info("Partition dir not found: %s", part_dir)
            return None

        parquet_files = sorted(part_dir.glob("part-*.parquet"))
        if len(parquet_files) < 2:
            logger.info("Only %d files, skipping compaction", len(parquet_files))
            return None

        manifest_path = part_dir / MANIFEST_FILENAME
        manifest = PartitionManifest.load(manifest_path)
        source_batches = []
        if manifest:
            for batch_entry in manifest.batches:
                source_batches.append(batch_entry.get("batch_id", ""))

        frames: List[pd.DataFrame] = []
        source_filenames: List[str] = []
        for f in parquet_files:
            try:
                frames.append(pd.read_parquet(f))
                source_filenames.append(f.name)
            except Exception as e:
                logger.warning("Failed to read %s for compaction: %s", f, e)

        if not frames:
            logger.warning("No readable parquet files for compaction")
            return None

        combined = pd.concat(frames, ignore_index=True)

        if primary_key:
            pk_cols = [c for c in primary_key if c in combined.columns]
            if pk_cols:
                before = len(combined)
                combined = combined.drop_duplicates(subset=pk_cols, keep="last")
                dropped = before - len(combined)
                if dropped:
                    logger.info("Compaction deduplication dropped %d rows", dropped)

        compacted_dir = self._compacted_dir(
            domain, dataset, partition_key, partition_value
        )
        compacted_dir.mkdir(parents=True, exist_ok=True)

        compaction_id = f"comp-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
        compacted_filename = f"compacted-{compaction_id}.parquet"
        compacted_path = compacted_dir / compacted_filename

        tmp_fd, tmp_path_str = tempfile.mkstemp(
            suffix=".parquet.tmp", dir=str(compacted_dir)
        )
        tmp_path = Path(tmp_path_str)

        try:
            combined.to_parquet(
                tmp_path,
                engine="pyarrow",
                compression=self._compression,
                index=False,
            )
            tmp_path.rename(compacted_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        compaction_manifest = CompactionManifest.create(
            dataset=dataset,
            domain=domain,
            partition_key=partition_key,
            partition_value=partition_value,
            source_files=source_filenames,
            compacted_file=compacted_filename,
            record_count=len(combined),
            source_batches=source_batches,
        )

        compaction_manifest_path = compacted_dir / COMPACTION_MANIFEST_FILENAME
        compaction_manifest.save(compaction_manifest_path)

        logger.info(
            "Compacted %d files (%d rows) -> %s",
            len(source_filenames),
            len(combined),
            compacted_path,
        )

        return compaction_manifest

    def run_all(
        self,
        *,
        domain: str,
        dataset: str,
        partition_key: str,
        primary_key: Optional[List[str]] = None,
    ) -> List[CompactionManifest]:
        """Run compaction on all partitions of a dataset that need it.

        Args:
            domain: Logical domain.
            dataset: Entity name.
            partition_key: Partition key.
            primary_key: Primary key columns for deduplication.

        Returns:
            List of CompactionManifest for each compacted partition.
        """
        dataset_dir = self._dataset_dir(domain, dataset)
        if not dataset_dir.exists():
            return []

        results: List[CompactionManifest] = []
        prefix = f"{partition_key}="

        for part_dir in sorted(dataset_dir.iterdir()):
            if not part_dir.is_dir() or not part_dir.name.startswith(prefix):
                continue

            partition_value = part_dir.name[len(prefix) :]

            if not self.needs_compaction(
                domain=domain,
                dataset=dataset,
                partition_key=partition_key,
                partition_value=partition_value,
            ):
                continue

            result = self.run(
                dataset=dataset,
                domain=domain,
                partition_key=partition_key,
                partition_value=partition_value,
                primary_key=primary_key,
            )
            if result:
                results.append(result)

        return results
