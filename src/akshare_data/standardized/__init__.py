"""Standardized layer — Raw to Standard entity conversion.

Submodules:
- normalizer: Field mapping and normalization (base + dataset-specific)
- writer: Schema-validated, partitioned, atomic writes
- reader: Entity and time-range based reads
- merge: Upsert / late-arriving / incremental merge logic
- manifest: Partition-level batch tracking
- compaction: Small file merging with version traceability
"""

from akshare_data.standardized.mapping_loader import (
    MappingLoader,
    DatasetMapping,
    FieldMappingEntry,
    load_field_mapping,
    get_normalize_version,
)
from akshare_data.standardized.writer import StandardizedWriter
from akshare_data.standardized.reader import StandardizedReader
from akshare_data.standardized.merge import MergeEngine
from akshare_data.standardized.manifest import PartitionManifest, BatchEntry
from akshare_data.standardized.compaction import CompactionJob, CompactionManifest

__all__ = [
    "MappingLoader",
    "DatasetMapping",
    "FieldMappingEntry",
    "load_field_mapping",
    "get_normalize_version",
    "StandardizedWriter",
    "StandardizedReader",
    "MergeEngine",
    "PartitionManifest",
    "BatchEntry",
    "CompactionJob",
    "CompactionManifest",
]
