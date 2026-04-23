"""Metadata catalog service (T9-001 + T8-007).

Aggregates dataset/field/version/error semantics into queryable structures.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from akshare_data.common.errors import ErrorCode
from akshare_data.governance.catalog import DatasetCatalog


@dataclass(frozen=True)
class ErrorSemantic:
    code: str
    domain: str
    layer: str
    http_status: int
    retryable: bool
    message_template: str
    operator_action: str


class MetadataCatalog:
    """Unified metadata query surface for datasets, fields, versions, errors."""

    def __init__(
        self,
        *,
        dataset_catalog: DatasetCatalog,
        field_dictionary_path: Path,
        quality_config_dir: Path,
    ) -> None:
        self._dataset_catalog = dataset_catalog
        self._field_dictionary_path = field_dictionary_path
        self._quality_config_dir = quality_config_dir

    def list_datasets(self) -> list[str]:
        return sorted(self._dataset_catalog.list_all().keys())

    def get_dataset_versions(self, dataset: str) -> list[dict[str, Any]]:
        history = self._dataset_catalog.get_version_history(dataset)
        return [
            {
                "batch_id": v.batch_id,
                "schema_version": v.schema_version,
                "normalize_version": v.normalize_version,
                "release_version": v.release_version,
                "registered_at": v.registered_at.isoformat(),
            }
            for v in history
        ]

    def get_field_definition(self, field_name: str) -> dict[str, Any] | None:
        with open(self._field_dictionary_path, encoding="utf-8") as fh:
            dictionary = yaml.safe_load(fh) or {}

        if field_name in (dictionary.get("fields") or {}):
            return (dictionary.get("fields") or {})[field_name]
        if field_name in (dictionary.get("system_fields") or {}):
            return (dictionary.get("system_fields") or {})[field_name]
        return None

    def list_quality_rules(self, dataset: str) -> list[dict[str, Any]]:
        path = self._quality_config_dir / f"{dataset}.yaml"
        if not path.exists():
            return []
        with open(path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        return cfg.get("rules") or []

    def get_error_semantic(self, error: ErrorCode) -> ErrorSemantic:
        number = int(error.value.split("_")[0])

        if 1000 <= number < 2000:
            layer, domain, http_status, retryable = "ingestion", "source", 503, True
        elif 2000 <= number < 3000:
            layer, domain, http_status, retryable = "storage", "cache", 500, True
        elif 3000 <= number < 4000:
            layer, domain, http_status, retryable = "service", "request", 400, False
        elif 4000 <= number < 5000:
            layer, domain, http_status, retryable = "network", "dependency", 502, True
        elif 5000 <= number < 6000:
            layer, domain, http_status, retryable = "quality", "gate", 422, False
        else:
            layer, domain, http_status, retryable = "governance", "metadata", 500, False

        return ErrorSemantic(
            code=error.value,
            domain=domain,
            layer=layer,
            http_status=http_status,
            retryable=retryable,
            message_template=error.name,
            operator_action="check logs and corresponding runbook",
        )
