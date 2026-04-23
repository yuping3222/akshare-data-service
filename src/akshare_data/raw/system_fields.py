"""System fields for Raw (L0) layer.

Defines the set of audit/tracing columns that are appended to every
Raw DataFrame. These fields capture the extraction event and enable
replay, lineage, and schema-drift detection.

Spec: docs/design/20-raw-spec.md §4
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

SYSTEM_FIELD_NAMES: Tuple[str, ...] = (
    "batch_id",
    "source_name",
    "interface_name",
    "request_params_json",
    "request_time",
    "ingest_time",
    "extract_date",
    "extract_version",
    "source_schema_fingerprint",
    "raw_record_hash",
)

SYSTEM_FIELD_TYPES: Dict[str, str] = {
    "batch_id": "string",
    "source_name": "string",
    "interface_name": "string",
    "request_params_json": "string",
    "request_time": "timestamp",
    "ingest_time": "timestamp",
    "extract_date": "date",
    "extract_version": "string",
    "source_schema_fingerprint": "string",
    "raw_record_hash": "string",
}


@dataclass(frozen=True)
class SystemField:
    name: str
    dtype: str
    required: bool
    description: str


SYSTEM_FIELDS: List[SystemField] = [
    SystemField("batch_id", "string", True, "Batch identifier for traceability"),
    SystemField("source_name", "string", True, "Source adapter name (e.g. akshare)"),
    SystemField("interface_name", "string", True, "Source interface/function name"),
    SystemField(
        "request_params_json", "string", True, "Request parameters as JSON string"
    ),
    SystemField(
        "request_time", "timestamp", True, "Time when the request was initiated"
    ),
    SystemField("ingest_time", "timestamp", True, "Time when the record landed in Raw"),
    SystemField(
        "extract_date", "date", True, "Planned extraction date (partition column)"
    ),
    SystemField("extract_version", "string", True, "Extraction version tag"),
    SystemField(
        "source_schema_fingerprint",
        "string",
        True,
        "Fingerprint of source column schema",
    ),
    SystemField(
        "raw_record_hash",
        "string",
        True,
        "Hash of business content (excludes system fields)",
    ),
]


def is_system_field(name: str) -> bool:
    return name in SYSTEM_FIELD_NAMES


def get_system_field_names() -> List[str]:
    return list(SYSTEM_FIELD_NAMES)


def get_system_field_types() -> Dict[str, str]:
    return dict(SYSTEM_FIELD_TYPES)
