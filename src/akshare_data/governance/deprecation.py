"""Field deprecation tracking for managing field lifecycle from active to removed.

Provides deprecation registration, window management, and impact analysis
that integrates with SchemaRegistry, ChangeLog, and LineageTracker.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class DeprecationStatus(str, Enum):
    """Lifecycle states for a deprecated field."""

    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"
    REMOVED = "REMOVED"


@dataclass(frozen=True)
class DeprecationRecord:
    """Immutable record of a field deprecation.

    Attributes:
        entity_name: Target entity name.
        field_name: Field being deprecated.
        replacement_field: Replacement field name (empty if no replacement).
        reason: Reason for deprecation.
        status: Current deprecation status.
        deprecation_date: Date when deprecation was announced.
        window_days: Number of days in the deprecation window.
        removable_date: Date after which the field can be removed.
        removed_date: Date when the field was actually removed (empty if not yet).
        owner_id: Owner who approved the deprecation.
        emergency: Whether this is an emergency deprecation.
        impact_analysis: Structured impact analysis results.
        created_at: Timestamp when this record was created.
        updated_at: Timestamp when this record was last updated.
    """

    entity_name: str
    field_name: str
    replacement_field: str = ""
    reason: str = ""
    status: DeprecationStatus = DeprecationStatus.ACTIVE
    deprecation_date: date = field(default_factory=date.today)
    window_days: int = 0
    removable_date: date | None = None
    removed_date: date | None = None
    owner_id: str = ""
    emergency: bool = False
    impact_analysis: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DeprecationRegistry:
    """Registry for field deprecation records.

    Manages the full lifecycle of field deprecation from announcement to removal.
    Integrates with ChangeLog to record deprecation events and with
    SchemaRegistry to track schema version changes.

    Default deprecation windows by entity priority:
    - P0 core fields: 90 days
    - P0 non-core fields: 60 days
    - P1/P2 fields: 30 days
    """

    DEFAULT_WINDOWS = {
        "P0_core": 90,
        "P0_non_core": 60,
        "P1": 30,
        "P2": 30,
    }

    MINIMUM_WINDOW_DAYS = 7

    def __init__(self) -> None:
        self._records: dict[str, DeprecationRecord] = {}

    @staticmethod
    def _make_key(entity_name: str, field_name: str) -> str:
        return f"{entity_name}.{field_name}"

    def deprecate(
        self,
        entity_name: str,
        field_name: str,
        replacement_field: str = "",
        reason: str = "",
        window_days: int | None = None,
        owner_id: str = "",
        emergency: bool = False,
        impact_analysis: dict[str, Any] | None = None,
    ) -> DeprecationRecord:
        """Mark a field as deprecated.

        Args:
            entity_name: Target entity name.
            field_name: Field to deprecate.
            replacement_field: Replacement field name.
            reason: Reason for deprecation.
            window_days: Deprecation window in days. If None, uses default.
            owner_id: Owner who approved the deprecation.
            emergency: Whether this is an emergency deprecation.
            impact_analysis: Structured impact analysis.

        Returns:
            The created DeprecationRecord.

        Raises:
            ValueError: If window_days is below minimum.
        """
        if window_days is not None and window_days < self.MINIMUM_WINDOW_DAYS:
            raise ValueError(
                f"Deprecation window must be at least {self.MINIMUM_WINDOW_DAYS} days, "
                f"got {window_days}"
            )

        today = date.today()
        effective_window = window_days or self.DEFAULT_WINDOWS["P1"]
        removable = today + timedelta(days=effective_window)

        record = DeprecationRecord(
            entity_name=entity_name,
            field_name=field_name,
            replacement_field=replacement_field,
            reason=reason,
            status=DeprecationStatus.DEPRECATED,
            deprecation_date=today,
            window_days=effective_window,
            removable_date=removable,
            owner_id=owner_id,
            emergency=emergency,
            impact_analysis=impact_analysis or {},
        )

        key = self._make_key(entity_name, field_name)
        self._records[key] = record

        logger.info(
            "Deprecated field %s.%s (replacement=%s, removable=%s, owner=%s)",
            entity_name,
            field_name,
            replacement_field or "none",
            removable,
            owner_id or "unassigned",
        )

        return record

    def remove(
        self,
        entity_name: str,
        field_name: str,
        owner_id: str = "",
    ) -> DeprecationRecord | None:
        """Mark a deprecated field as removed.

        Args:
            entity_name: Target entity name.
            field_name: Field to remove.
            owner_id: Owner who approved the removal.

        Returns:
            Updated DeprecationRecord, or None if not found.

        Raises:
            ValueError: If the field is not yet removable.
        """
        key = self._make_key(entity_name, field_name)
        record = self._records.get(key)
        if record is None:
            return None

        if record.status != DeprecationStatus.DEPRECATED:
            raise ValueError(
                f"Field {entity_name}.{field_name} is not in DEPRECATED status "
                f"(current: {record.status.value})"
            )

        today = date.today()
        if record.removable_date and today < record.removable_date:
            raise ValueError(
                f"Field {entity_name}.{field_name} is not yet removable. "
                f"Removable date: {record.removable_date}"
            )

        updated = DeprecationRecord(
            entity_name=record.entity_name,
            field_name=record.field_name,
            replacement_field=record.replacement_field,
            reason=record.reason,
            status=DeprecationStatus.REMOVED,
            deprecation_date=record.deprecation_date,
            window_days=record.window_days,
            removable_date=record.removable_date,
            removed_date=today,
            owner_id=owner_id or record.owner_id,
            emergency=record.emergency,
            impact_analysis=record.impact_analysis,
            created_at=record.created_at,
            updated_at=datetime.now(timezone.utc),
        )

        self._records[key] = updated

        logger.info(
            "Removed field %s.%s (owner=%s)",
            entity_name,
            field_name,
            owner_id or updated.owner_id,
        )

        return updated

    def get(self, entity_name: str, field_name: str) -> DeprecationRecord | None:
        """Get deprecation record for a field.

        Args:
            entity_name: Target entity name.
            field_name: Field name.

        Returns:
            DeprecationRecord, or None if not found.
        """
        return self._records.get(self._make_key(entity_name, field_name))

    def get_by_status(self, status: DeprecationStatus) -> list[DeprecationRecord]:
        """Get all deprecation records with a specific status.

        Args:
            status: Target status.

        Returns:
            List of matching DeprecationRecord objects.
        """
        return [r for r in self._records.values() if r.status == status]

    def get_by_entity(self, entity_name: str) -> list[DeprecationRecord]:
        """Get all deprecation records for an entity."""
        return [r for r in self._records.values() if r.entity_name == entity_name]

    def get_by_owner(self, owner_id: str) -> list[DeprecationRecord]:
        """Get all deprecation records approved by an owner."""
        return [r for r in self._records.values() if r.owner_id == owner_id]

    def is_deprecated(self, entity_name: str, field_name: str) -> bool:
        """Check if a field is currently deprecated (not yet removed)."""
        record = self.get(entity_name, field_name)
        return record is not None and record.status == DeprecationStatus.DEPRECATED

    def is_removed(self, entity_name: str, field_name: str) -> bool:
        """Check if a field has been removed."""
        record = self.get(entity_name, field_name)
        return record is not None and record.status == DeprecationStatus.REMOVED

    def is_removable(self, entity_name: str, field_name: str) -> bool:
        """Check if a deprecated field is past its removable date."""
        record = self.get(entity_name, field_name)
        if record is None or record.removable_date is None:
            return False
        return date.today() >= record.removable_date

    def get_replacement(self, entity_name: str, field_name: str) -> str | None:
        """Get the replacement field for a deprecated field.

        Args:
            entity_name: Target entity name.
            field_name: Deprecated field name.

        Returns:
            Replacement field name, or None if not found or no replacement.
        """
        record = self.get(entity_name, field_name)
        if record is None:
            return None
        return record.replacement_field or None

    def list_all(self) -> dict[str, DeprecationRecord]:
        """Return a copy of all deprecation records."""
        return dict(self._records)

    def get_impact_summary(self, entity_name: str, field_name: str) -> dict[str, Any]:
        """Get a summary of the impact analysis for a deprecated field.

        Returns the impact_analysis dict from the deprecation record,
        augmented with status and timeline information.

        Args:
            entity_name: Target entity name.
            field_name: Field name.

        Returns:
            Impact summary dict, or empty dict if not found.
        """
        record = self.get(entity_name, field_name)
        if record is None:
            return {}

        return {
            "entity_name": entity_name,
            "field_name": field_name,
            "status": record.status.value,
            "replacement_field": record.replacement_field,
            "deprecation_date": record.deprecation_date.isoformat(),
            "removable_date": (
                record.removable_date.isoformat() if record.removable_date else None
            ),
            "removed_date": (
                record.removed_date.isoformat() if record.removed_date else None
            ),
            "window_days": record.window_days,
            "impact_analysis": record.impact_analysis,
        }
