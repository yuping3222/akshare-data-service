"""Schema change log for tracking field additions, modifications, and removals.

Provides versioned change records that integrate with SchemaRegistry,
DatasetCatalog, and OwnershipRegistry to create an auditable trail of
all schema modifications.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "system" / "metadata"
)


class ChangeType(str, Enum):
    """Types of schema changes."""

    ADD_FIELD = "ADD_FIELD"
    MODIFY_FIELD = "MODIFY_FIELD"
    REMOVE_FIELD = "REMOVE_FIELD"
    RENAME_FIELD = "RENAME_FIELD"
    DEPRECATE_FIELD = "DEPRECATE_FIELD"
    OWNER_CHANGE = "OWNER_CHANGE"
    SCHEMA_VERSION_BUMP = "SCHEMA_VERSION_BUMP"


@dataclass(frozen=True)
class SchemaChange:
    """Immutable record of a single schema change.

    Attributes:
        change_id: Unique change identifier.
        change_type: Type of change (ADD/MODIFY/REMOVE/etc.).
        entity_name: Target entity name.
        field_name: Affected field name (empty for entity-level changes).
        owner_id: Owner who initiated or approved the change.
        version_before: Schema version before the change.
        version_after: Schema version after the change.
        description: Human-readable description of the change.
        details: Structured change details (field definition, diff, etc.).
        emergency: Whether this is an emergency change.
        created_at: Timestamp when the change was recorded.
    """

    change_id: str
    change_type: ChangeType
    entity_name: str
    field_name: str = ""
    owner_id: str = ""
    version_before: str = ""
    version_after: str = ""
    description: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    emergency: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ChangeLog:
    """Append-only log of schema changes.

    Changes are recorded in memory and optionally persisted to a JSONL file.
    Integrates with SchemaRegistry for version tracking and OwnershipRegistry
    for permission verification.

    Persistence format: JSONL (one JSON object per line) at
    data/system/metadata/schema_changes.jsonl
    """

    def __init__(
        self,
        persist_dir: Path | str | None = None,
        auto_persist: bool = True,
    ) -> None:
        self._persist_dir = (
            Path(persist_dir) if persist_dir else _DEFAULT_CHANGE_LOG_PATH
        )
        self._persist_path = self._persist_dir / "schema_changes.jsonl"
        self._auto_persist = auto_persist
        self._changes: list[SchemaChange] = []
        self._change_counter = 0

    def record(
        self,
        change_type: ChangeType,
        entity_name: str,
        field_name: str = "",
        owner_id: str = "",
        version_before: str = "",
        version_after: str = "",
        description: str = "",
        details: dict[str, Any] | None = None,
        emergency: bool = False,
    ) -> SchemaChange:
        """Record a schema change.

        Args:
            change_type: Type of change.
            entity_name: Target entity name.
            field_name: Affected field name.
            owner_id: Owner who initiated the change.
            version_before: Schema version before change.
            version_after: Schema version after change.
            description: Human-readable description.
            details: Structured change details.
            emergency: Whether this is an emergency change.

        Returns:
            The created SchemaChange record.
        """
        self._change_counter += 1
        change_id = f"CHG-{self._change_counter:06d}"

        change = SchemaChange(
            change_id=change_id,
            change_type=change_type,
            entity_name=entity_name,
            field_name=field_name,
            owner_id=owner_id,
            version_before=version_before,
            version_after=version_after,
            description=description,
            details=details or {},
            emergency=emergency,
        )

        self._changes.append(change)
        logger.info(
            "Recorded schema change %s: %s on %s.%s (v%s -> v%s)",
            change_id,
            change_type.value,
            entity_name,
            field_name or "*",
            version_before,
            version_after,
        )

        if self._auto_persist:
            self._persist_single(change)

        return change

    def get_all(self) -> list[SchemaChange]:
        """Return all recorded changes."""
        return list(self._changes)

    def get_by_entity(self, entity_name: str) -> list[SchemaChange]:
        """Get all changes for a specific entity."""
        return [c for c in self._changes if c.entity_name == entity_name]

    def get_by_field(self, entity_name: str, field_name: str) -> list[SchemaChange]:
        """Get all changes for a specific field in an entity."""
        return [
            c
            for c in self._changes
            if c.entity_name == entity_name and c.field_name == field_name
        ]

    def get_by_owner(self, owner_id: str) -> list[SchemaChange]:
        """Get all changes initiated by a specific owner."""
        return [c for c in self._changes if c.owner_id == owner_id]

    def get_by_type(self, change_type: ChangeType) -> list[SchemaChange]:
        """Get all changes of a specific type."""
        return [c for c in self._changes if c.change_type == change_type]

    def get_emergency_changes(self) -> list[SchemaChange]:
        """Get all emergency changes."""
        return [c for c in self._changes if c.emergency]

    def get_changes_since(self, timestamp: datetime) -> list[SchemaChange]:
        """Get all changes recorded after a specific timestamp."""
        return [c for c in self._changes if c.created_at > timestamp]

    def get_latest_version(self, entity_name: str) -> str:
        """Get the latest schema version for an entity from change log.

        Returns the version_after of the most recent change, or empty string
        if no changes are recorded.

        Args:
            entity_name: Target entity name.

        Returns:
            Latest schema version string.
        """
        changes = self.get_by_entity(entity_name)
        if not changes:
            return ""
        latest = max(changes, key=lambda c: c.created_at)
        return latest.version_after

    def load_from_file(self) -> int:
        """Load existing changes from the JSONL file.

        Returns:
            Number of changes loaded.
        """
        persist_path = Path(self._persist_path)
        if not persist_path.exists():
            return 0

        count = 0
        with open(persist_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw: dict[str, Any] = json.loads(line)
                    change = SchemaChange(
                        change_id=raw["change_id"],
                        change_type=ChangeType(raw["change_type"]),
                        entity_name=raw["entity_name"],
                        field_name=raw.get("field_name", ""),
                        owner_id=raw.get("owner_id", ""),
                        version_before=raw.get("version_before", ""),
                        version_after=raw.get("version_after", ""),
                        description=raw.get("description", ""),
                        details=raw.get("details", {}),
                        emergency=raw.get("emergency", False),
                        created_at=datetime.fromisoformat(raw["created_at"]),
                    )
                    self._changes.append(change)
                    count += 1
                except (json.JSONDecodeError, KeyError, ValueError) as exc:
                    logger.warning("Failed to parse change log entry: %s", exc)

        if count > 0:
            self._change_counter = max(
                int(c.change_id.split("-")[1]) for c in self._changes
            )

        logger.info("Loaded %d changes from %s", count, persist_path)
        return count

    def _persist_single(self, change: SchemaChange) -> None:
        """Append a single change to the JSONL file."""
        persist_dir = Path(self._persist_dir)
        persist_dir.mkdir(parents=True, exist_ok=True)

        raw = {
            "change_id": change.change_id,
            "change_type": change.change_type.value,
            "entity_name": change.entity_name,
            "field_name": change.field_name,
            "owner_id": change.owner_id,
            "version_before": change.version_before,
            "version_after": change.version_after,
            "description": change.description,
            "details": change.details,
            "emergency": change.emergency,
            "created_at": change.created_at.isoformat(),
        }

        with open(self._persist_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(raw, ensure_ascii=False) + "\n")
