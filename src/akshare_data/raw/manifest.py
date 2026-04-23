"""Manifest generation and loading for Raw (L0) batches.

Each batch directory contains a `_manifest.json` that records the
extraction event — not a business-date snapshot.

Spec: docs/design/20-raw-spec.md §6
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


MANIFEST_VERSION = "1.0"
MANIFEST_FILENAME = "_manifest.json"
SCHEMA_FILENAME = "_schema.json"


@dataclass
class Manifest:
    """Records a single Raw extraction batch.

    This manifest captures *how* the data was extracted, not *what
    business period* it represents.
    """

    manifest_version: str
    dataset: str
    domain: str
    batch_id: str
    extract_date: str
    source_name: str
    interface_name: str
    request_params: Dict[str, Any]
    record_count: int
    file_count: int
    schema_fingerprint: str
    extract_version: str
    status: str
    created_at: Optional[str] = None
    files: Optional[List[str]] = None
    schema_snapshot: Optional[List[Dict[str, str]]] = None

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        domain: str,
        batch_id: str,
        extract_date: date,
        source_name: str,
        interface_name: str,
        request_params: Dict[str, Any],
        record_count: int,
        file_count: int,
        schema_fingerprint: str,
        extract_version: str = "v1.0",
        status: str = "success",
        created_at: Optional[datetime] = None,
        files: Optional[List[str]] = None,
        schema_snapshot: Optional[List[Dict[str, str]]] = None,
    ) -> "Manifest":
        return cls(
            manifest_version=MANIFEST_VERSION,
            dataset=dataset,
            domain=domain,
            batch_id=batch_id,
            extract_date=extract_date.isoformat(),
            source_name=source_name,
            interface_name=interface_name,
            request_params=request_params,
            record_count=record_count,
            file_count=file_count,
            schema_fingerprint=schema_fingerprint,
            extract_version=extract_version,
            status=status,
            created_at=(created_at or datetime.now(timezone.utc)).isoformat(),
            files=files,
            schema_snapshot=schema_snapshot,
        )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Manifest":
        return cls(**data)

    @classmethod
    def from_json(cls, text: str) -> "Manifest":
        return cls.from_dict(json.loads(text))

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        with open(path, encoding="utf-8") as f:
            return cls.from_json(f.read())

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json())

    @property
    def extract_date_parsed(self) -> Optional[date]:
        """Parse extract_date string into a date object."""
        if not self.extract_date:
            return None
        try:
            return date.fromisoformat(self.extract_date)
        except (ValueError, TypeError):
            return None


def save_schema_snapshot(
    path: Path,
    schema: List[Dict[str, str]],
) -> None:
    """Write a `_schema.json` file for the batch.

    Args:
        path: Directory where _schema.json will be written.
        schema: List of {"name": ..., "dtype": ...} entries.
    """
    target = path / SCHEMA_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)


def load_schema_snapshot(path: Path) -> List[Dict[str, str]]:
    """Load a `_schema.json` file."""
    target = path / SCHEMA_FILENAME
    if not target.exists():
        return []
    with open(target, encoding="utf-8") as f:
        return json.load(f)
