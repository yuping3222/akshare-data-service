"""Raw (L0) layer for akshare-data-service.

Provides:
- System field definitions
- Schema fingerprinting
- Manifest generation / loading
- Atomic parquet writer
- Raw batch reader
- Replay engine

Submodules:
- system_fields: Audit column names and types
- schema_fingerprint: Deterministic schema hashing
- manifest: Batch-level metadata (JSON)
- writer: RawWriter for parquet + manifest output
- reader: RawReader for batch discovery and reading
- replay: ReplayEngine for replaying Raw evidence
"""

from akshare_data.raw.system_fields import (
    SYSTEM_FIELD_NAMES,
    SYSTEM_FIELD_TYPES,
    SYSTEM_FIELDS,
    SystemField,
    is_system_field,
    get_system_field_names,
    get_system_field_types,
)
from akshare_data.raw.schema_fingerprint import (
    compute_schema_fingerprint,
    compute_column_fingerprint,
    schemas_match,
    describe_schema,
)
from akshare_data.raw.manifest import (
    Manifest,
    MANIFEST_VERSION,
    MANIFEST_FILENAME,
    SCHEMA_FILENAME,
    save_schema_snapshot,
    load_schema_snapshot,
)
from akshare_data.raw.writer import RawWriter
from akshare_data.raw.reader import RawReader
from akshare_data.raw.replay import ReplayEngine, ReplayResult, ReplayReport

__all__ = [
    "SYSTEM_FIELD_NAMES",
    "SYSTEM_FIELD_TYPES",
    "SYSTEM_FIELDS",
    "SystemField",
    "is_system_field",
    "get_system_field_names",
    "get_system_field_types",
    "compute_schema_fingerprint",
    "compute_column_fingerprint",
    "schemas_match",
    "describe_schema",
    "Manifest",
    "MANIFEST_VERSION",
    "MANIFEST_FILENAME",
    "SCHEMA_FILENAME",
    "save_schema_snapshot",
    "load_schema_snapshot",
    "RawWriter",
    "RawReader",
    "ReplayEngine",
    "ReplayResult",
    "ReplayReport",
]
