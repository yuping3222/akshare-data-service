"""Governance module for metadata catalog, schema registry, lineage tracking,
ownership management, schema change logging, and field deprecation.
"""

from __future__ import annotations

from .catalog import Dataset, DatasetCatalog
from .change_log import ChangeLog, ChangeType, SchemaChange
from .deprecation import DeprecationRecord, DeprecationRegistry, DeprecationStatus
from .field_naming import FieldNameValidation, FieldNamingStandard
from .lineage import FieldLineage, LineageGraph, LineageTracker
from .metadata_catalog import ErrorSemantic, MetadataCatalog
from .ownership import Owner, OwnershipRecord, OwnershipRegistry
from .schema_registry import EntitySchema, SchemaRegistry

__all__ = [
    "ChangeLog",
    "ChangeType",
    "Dataset",
    "DatasetCatalog",
    "DeprecationRecord",
    "DeprecationRegistry",
    "DeprecationStatus",
    "EntitySchema",
    "ErrorSemantic",
    "FieldLineage",
    "FieldNameValidation",
    "FieldNamingStandard",
    "LineageGraph",
    "LineageTracker",
    "MetadataCatalog",
    "Owner",
    "OwnershipRecord",
    "OwnershipRegistry",
    "SchemaChange",
    "SchemaRegistry",
]
