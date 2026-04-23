"""Manifest management for Standardized (L1) layer.

Tracks dataset, schema_version, normalize_version, batch_id per partition.
Supports idempotent batch updates and version traceability.

Spec: docs/design/40-standardized-storage-spec.md §5
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MANIFEST_VERSION = "1.0"
MANIFEST_FILENAME = "_manifest.json"


@dataclass
class BatchEntry:
    """Records a single batch write into a partition."""

    batch_id: str
    normalize_version: str
    schema_version: str
    source_name: str
    interface_name: str
    record_count: int
    files: List[str]
    written_at: str
    status: str = "success"

    @classmethod
    def create(
        cls,
        *,
        batch_id: str,
        normalize_version: str,
        schema_version: str,
        source_name: str,
        interface_name: str,
        record_count: int,
        files: List[str],
        written_at: Optional[datetime] = None,
        status: str = "success",
    ) -> "BatchEntry":
        return cls(
            batch_id=batch_id,
            normalize_version=normalize_version,
            schema_version=schema_version,
            source_name=source_name,
            interface_name=interface_name,
            record_count=record_count,
            files=files,
            written_at=(written_at or datetime.now(timezone.utc)).isoformat(),
            status=status,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PartitionManifest:
    """Manifest for a single partition in the Standardized layer.

    Tracks all batches that have written to this partition.
    """

    manifest_version: str
    dataset: str
    domain: str
    partition_key: str
    partition_value: str
    batches: List[Dict[str, Any]]
    total_record_count: int
    last_updated: str

    @classmethod
    def new(
        cls,
        *,
        dataset: str,
        domain: str,
        partition_key: str,
        partition_value: str,
    ) -> "PartitionManifest":
        return cls(
            manifest_version=MANIFEST_VERSION,
            dataset=dataset,
            domain=domain,
            partition_key=partition_key,
            partition_value=partition_value,
            batches=[],
            total_record_count=0,
            last_updated=datetime.now(timezone.utc).isoformat(),
        )

    def add_batch(self, entry: BatchEntry) -> None:
        """Add or replace a batch entry (idempotent by batch_id)."""
        existing_idx = None
        for idx, b in enumerate(self.batches):
            if b["batch_id"] == entry.batch_id:
                existing_idx = idx
                old_count = b.get("record_count", 0)
                self.total_record_count -= old_count
                break

        entry_dict = entry.to_dict()
        if existing_idx is not None:
            self.batches[existing_idx] = entry_dict
        else:
            self.batches.append(entry_dict)

        self.total_record_count += entry.record_count
        self.last_updated = datetime.now(timezone.utc).isoformat()

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        for b in self.batches:
            if b["batch_id"] == batch_id:
                return b
        return None

    def list_batches(self) -> List[Dict[str, Any]]:
        return list(self.batches)

    def get_versions(self) -> Dict[str, List[str]]:
        """Return unique normalize_versions and schema_versions seen."""
        norm_versions: List[str] = []
        schema_versions: List[str] = []
        seen_norm: set = set()
        seen_schema: set = set()
        for b in self.batches:
            nv = b.get("normalize_version")
            sv = b.get("schema_version")
            if nv and nv not in seen_norm:
                norm_versions.append(nv)
                seen_norm.add(nv)
            if sv and sv not in seen_schema:
                schema_versions.append(sv)
                seen_schema.add(sv)
        return {
            "normalize_versions": norm_versions,
            "schema_versions": schema_versions,
        }

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PartitionManifest":
        return cls(**data)

    @classmethod
    def from_json(cls, text: str) -> "PartitionManifest":
        return cls.from_dict(json.loads(text))

    @classmethod
    def load(cls, path: Path) -> Optional["PartitionManifest"]:
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                return cls.from_json(f.read())
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Corrupted manifest at %s: %s", path, e)
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


def load_or_create_manifest(
    *,
    partition_dir: Path,
    dataset: str,
    domain: str,
    partition_key: str,
    partition_value: str,
) -> PartitionManifest:
    """Load existing manifest or create a new one."""
    manifest_path = partition_dir / MANIFEST_FILENAME
    manifest = PartitionManifest.load(manifest_path)
    if manifest is None:
        manifest = PartitionManifest.new(
            dataset=dataset,
            domain=domain,
            partition_key=partition_key,
            partition_value=partition_value,
        )
    return manifest
