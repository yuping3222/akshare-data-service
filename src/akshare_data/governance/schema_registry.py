"""Schema registry that loads entity schemas from config/standards/entities/*.yaml.

This is the authoritative source for standard entity schemas, replacing the
legacy core/schema.py CacheTable approach for governance purposes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_DIR = (
    Path(__file__).resolve().parents[3] / "config" / "standards" / "entities"
)


@dataclass(frozen=True)
class FieldDef:
    """Definition of a single field within an entity schema."""

    name: str
    field_type: str
    description: str = ""
    required: bool = False
    unit: str | None = None


@dataclass(frozen=True)
class SystemFieldDef:
    """Definition of a system-level field (batch_id, schema_version, etc.)."""

    name: str
    field_type: str
    required: bool = True
    description: str = ""


@dataclass(frozen=True)
class EntitySchema:
    """Immutable schema for a standard entity, loaded from YAML config.

    Attributes:
        entity: Standard entity name (e.g. market_quote_daily).
        description: Human-readable description.
        priority: Priority tier (P0-P3).
        storage_layer: Logical storage layer (daily, meta, snapshot, minute).
        version: Schema config version string.
        primary_key: Ordered list of primary key field names.
        partition_by: Ordered list of partition column names.
        fields: Business field definitions keyed by field name.
        system_fields: System field definitions keyed by field name.
        required_fields: List of required field names.
        time_fields: Time-related field metadata keyed by field name.
    """

    entity: str
    description: str
    priority: str
    storage_layer: str
    version: str
    primary_key: list[str]
    partition_by: list[str]
    fields: dict[str, FieldDef]
    system_fields: dict[str, SystemFieldDef]
    required_fields: list[str]
    time_fields: dict[str, dict[str, Any]]
    source_path: str = ""


class SchemaRegistry:
    """Registry for standard entity schemas.

    Loads schemas from config/standards/entities/*.yaml by default.
    Provides lookup, listing, and field-level access operations.
    """

    def __init__(self, config_dir: Path | None = None) -> None:
        self._config_dir = config_dir or _DEFAULT_CONFIG_DIR
        self._schemas: dict[str, EntitySchema] = {}
        self._field_index: dict[str, dict[str, EntitySchema]] = {}

    def load_all(self) -> int:
        """Load all entity schemas from YAML config files.

        Returns:
            Number of schemas successfully loaded.
        """
        count = 0
        config_dir = Path(self._config_dir)
        if not config_dir.is_dir():
            logger.warning("Entity config directory not found: %s", config_dir)
            return 0

        for yaml_path in sorted(config_dir.glob("*.yaml")):
            try:
                schema = self._load_single(yaml_path)
                self.register(schema)
                count += 1
            except Exception as exc:
                logger.error("Failed to load schema from %s: %s", yaml_path, exc)

        logger.info("Loaded %d entity schemas from %s", count, config_dir)
        return count

    def _load_single(self, path: Path) -> EntitySchema:
        """Parse a single entity YAML file into an EntitySchema."""
        with open(path, encoding="utf-8") as fh:
            raw: dict[str, Any] = yaml.safe_load(fh)

        entity_name = raw["entity"]

        fields: dict[str, FieldDef] = {}
        for fname, fdef in raw.get("fields", {}).items():
            fields[fname] = FieldDef(
                name=fname,
                field_type=fdef.get("type", "string"),
                description=fdef.get("description", ""),
                required=fdef.get("required", False),
                unit=fdef.get("unit"),
            )

        system_fields: dict[str, SystemFieldDef] = {}
        for sname, sdef in raw.get("system_fields", {}).items():
            system_fields[sname] = SystemFieldDef(
                name=sname,
                field_type=sdef.get("type", "string"),
                required=sdef.get("required", True),
            )

        time_fields: dict[str, dict[str, Any]] = raw.get("time_fields", {})

        return EntitySchema(
            entity=entity_name,
            description=raw.get("description", ""),
            priority=raw.get("priority", "P2"),
            storage_layer=raw.get("storage_layer", "daily"),
            version=raw.get("version", "1.0"),
            primary_key=list(raw.get("primary_key", [])),
            partition_by=list(raw.get("partition_by", [])),
            fields=fields,
            system_fields=system_fields,
            required_fields=list(raw.get("required_fields", [])),
            time_fields=time_fields,
            source_path=str(path),
        )

    def register(self, schema: EntitySchema) -> None:
        """Register an entity schema.

        Args:
            schema: EntitySchema instance to register.
        """
        self._schemas[schema.entity] = schema
        self._rebuild_field_index()

    def _rebuild_field_index(self) -> None:
        """Rebuild the field-to-entity reverse index."""
        self._field_index.clear()
        for entity_name, schema in self._schemas.items():
            for fname in schema.fields:
                self._field_index.setdefault(fname, {})[entity_name] = schema
            for sname in schema.system_fields:
                self._field_index.setdefault(sname, {})[entity_name] = schema

    def get(self, entity_name: str) -> EntitySchema:
        """Get an entity schema by name.

        Args:
            entity_name: Standard entity name.

        Returns:
            EntitySchema for the given name.

        Raises:
            KeyError: If the entity is not registered.
        """
        return self._schemas[entity_name]

    def get_or_none(self, entity_name: str) -> EntitySchema | None:
        """Get an entity schema by name, or None if not found."""
        return self._schemas.get(entity_name)

    def has(self, entity_name: str) -> bool:
        """Check if an entity is registered."""
        return entity_name in self._schemas

    def list_all(self) -> dict[str, EntitySchema]:
        """Return a copy of all registered schemas."""
        return dict(self._schemas)

    def list_by_priority(self, priority: str) -> list[EntitySchema]:
        """List entities filtered by priority tier."""
        return [s for s in self._schemas.values() if s.priority == priority]

    def list_by_layer(self, layer: str) -> list[EntitySchema]:
        """List entities filtered by storage layer."""
        return [s for s in self._schemas.values() if s.storage_layer == layer]

    def get_field(
        self, entity_name: str, field_name: str
    ) -> FieldDef | SystemFieldDef | None:
        """Get a field definition within an entity.

        Checks business fields first, then system fields.
        """
        schema = self._schemas.get(entity_name)
        if schema is None:
            return None
        if field_name in schema.fields:
            return schema.fields[field_name]
        if field_name in schema.system_fields:
            return schema.system_fields[field_name]
        return None

    def find_entities_with_field(self, field_name: str) -> list[EntitySchema]:
        """Find all entities that contain a given field."""
        return list(self._field_index.get(field_name, {}).values())

    def get_all_field_names(self, entity_name: str) -> list[str]:
        """Get all field names (business + system) for an entity."""
        schema = self._schemas.get(entity_name)
        if schema is None:
            return []
        return list(schema.fields.keys()) + list(schema.system_fields.keys())

    def get_schema_version(self, entity_name: str) -> str:
        """Get the schema version for an entity."""
        schema = self._schemas.get(entity_name)
        if schema is None:
            raise KeyError(f"Entity not registered: {entity_name}")
        return schema.version
