"""Dataset catalog for managing registered datasets with standard names.

Datasets use canonical business names (e.g. market_quote_daily) rather than
legacy cache table names (e.g. stock_daily). Each dataset tracks version
information for batch_id, schema_version, normalize_version, and release_version.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .schema_registry import EntitySchema, SchemaRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetVersion:
    """Version tracking snapshot for a dataset.

    Attributes:
        batch_id: Batch identifier for this data load.
        schema_version: Entity schema version at registration time.
        normalize_version: Normalization rule version.
        release_version: Release version (None if not yet published).
        registered_at: Timestamp when this version was registered.
    """

    batch_id: str
    schema_version: str
    normalize_version: str
    release_version: str | None
    registered_at: datetime


@dataclass(frozen=True)
class Dataset:
    """A registered dataset with standard name and metadata.

    Attributes:
        name: Canonical dataset name (must match a standard entity).
        entity: Reference to the underlying EntitySchema.
        description: Human-readable description.
        priority: Priority tier (P0-P3).
        storage_layer: Logical storage layer.
        primary_key: Business primary key fields.
        partition_by: Partition columns.
        versions: Ordered list of version snapshots (newest last).
        metadata: Additional key-value metadata for future extension
                  (owner, deprecation, change log hooks for task 19).
    """

    name: str
    entity: EntitySchema
    description: str
    priority: str
    storage_layer: str
    primary_key: list[str]
    partition_by: list[str]
    versions: list[DatasetVersion] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def current_version(self) -> DatasetVersion | None:
        """Return the most recent version, or None if no versions recorded."""
        return self.versions[-1] if self.versions else None

    @property
    def field_names(self) -> list[str]:
        """All field names (business + system) for this dataset."""
        return list(self.entity.fields.keys()) + list(self.entity.system_fields.keys())


class DatasetCatalog:
    """Catalog of registered datasets.

    Datasets are identified by their canonical standard names.
    The catalog delegates schema authority to SchemaRegistry.
    """

    def __init__(self, schema_registry: SchemaRegistry | None = None) -> None:
        self._schema_registry = schema_registry or SchemaRegistry()
        self._datasets: dict[str, Dataset] = {}

    @property
    def schema_registry(self) -> SchemaRegistry:
        """Access the underlying schema registry."""
        return self._schema_registry

    def register(
        self,
        entity_name: str,
        batch_id: str,
        normalize_version: str,
        release_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Dataset:
        """Register or update a dataset.

        If the dataset already exists, a new version snapshot is appended.

        Args:
            entity_name: Standard entity name (must exist in SchemaRegistry).
            batch_id: Batch identifier for this registration.
            normalize_version: Normalization rule version.
            release_version: Optional release version for published datasets.
            metadata: Optional additional metadata.

        Returns:
            The registered (or updated) Dataset.

        Raises:
            KeyError: If the entity is not found in SchemaRegistry.
        """
        schema = self._schema_registry.get(entity_name)
        schema_version = schema.version

        version = DatasetVersion(
            batch_id=batch_id,
            schema_version=schema_version,
            normalize_version=normalize_version,
            release_version=release_version,
            registered_at=datetime.now(timezone.utc),
        )

        if entity_name in self._datasets:
            existing = self._datasets[entity_name]
            updated = Dataset(
                name=existing.name,
                entity=existing.entity,
                description=existing.description,
                priority=existing.priority,
                storage_layer=existing.storage_layer,
                primary_key=existing.primary_key,
                partition_by=existing.partition_by,
                versions=[*existing.versions, version],
                metadata={**existing.metadata, **(metadata or {})},
            )
        else:
            updated = Dataset(
                name=entity_name,
                entity=schema,
                description=schema.description,
                priority=schema.priority,
                storage_layer=schema.storage_layer,
                primary_key=list(schema.primary_key),
                partition_by=list(schema.partition_by),
                versions=[version],
                metadata=dict(metadata or {}),
            )

        self._datasets[entity_name] = updated
        logger.info(
            "Registered dataset %s (batch=%s, schema=%s, normalize=%s, release=%s)",
            entity_name,
            batch_id,
            schema_version,
            normalize_version,
            release_version,
        )
        return updated

    def get(self, name: str) -> Dataset:
        """Get a dataset by standard name.

        Args:
            name: Canonical dataset name.

        Returns:
            Dataset for the given name.

        Raises:
            KeyError: If the dataset is not registered.
        """
        return self._datasets[name]

    def get_or_none(self, name: str) -> Dataset | None:
        """Get a dataset by name, or None if not found."""
        return self._datasets.get(name)

    def has(self, name: str) -> bool:
        """Check if a dataset is registered."""
        return name in self._datasets

    def list_all(self) -> dict[str, Dataset]:
        """Return a copy of all registered datasets."""
        return dict(self._datasets)

    def list_by_priority(self, priority: str) -> list[Dataset]:
        """List datasets filtered by priority tier."""
        return [d for d in self._datasets.values() if d.priority == priority]

    def list_by_layer(self, layer: str) -> list[Dataset]:
        """List datasets filtered by storage layer."""
        return [d for d in self._datasets.values() if d.storage_layer == layer]

    def get_field_info(
        self, dataset_name: str, field_name: str
    ) -> dict[str, Any] | None:
        """Get detailed field information for a dataset.

        Returns field type, description, required flag, and unit if available.
        """
        dataset = self._datasets.get(dataset_name)
        if dataset is None:
            return None

        field_def = dataset.entity.fields.get(field_name)
        if field_def is not None:
            return {
                "name": field_def.name,
                "type": field_def.field_type,
                "description": field_def.description,
                "required": field_def.required,
                "unit": field_def.unit,
                "category": "business",
            }

        sys_def = dataset.entity.system_fields.get(field_name)
        if sys_def is not None:
            return {
                "name": sys_def.name,
                "type": sys_def.field_type,
                "required": sys_def.required,
                "category": "system",
            }

        return None

    def get_version_history(self, dataset_name: str) -> list[DatasetVersion]:
        """Get the full version history for a dataset."""
        dataset = self._datasets.get(dataset_name)
        if dataset is None:
            return []
        return list(dataset.versions)

    def load_from_registry(self, batch_id: str, normalize_version: str) -> int:
        """Register all entities from the schema registry as datasets.

        Convenience method to bulk-register datasets for a given batch.

        Args:
            batch_id: Batch identifier for all registrations.
            normalize_version: Normalization rule version for all registrations.

        Returns:
            Number of datasets registered.
        """
        count = 0
        for entity_name in self._schema_registry.list_all():
            if not self.has(entity_name):
                self.register(
                    entity_name=entity_name,
                    batch_id=batch_id,
                    normalize_version=normalize_version,
                )
                count += 1
        return count
