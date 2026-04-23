"""MappingLoader — loads source field mappings and normalize versions.

This module is the single entry point for:
  - Loading source-to-standard field mappings from config/mappings/sources/
  - Resolving the current normalize_version for a (dataset, source) pair
  - Providing a flat {raw_col: standard_col} dict for normalizers
  - Supplying lineage information (which source field maps to which standard field)

Design rules:
  - Mapping configs live under config/mappings/sources/<source>/<dataset>.yaml
  - Version registry lives at config/standards/normalize_versions.yaml
  - All field names on the right side MUST match standard entity schemas
  - Results are cached in memory after first load
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "config"
_MAPPINGS_ROOT = _CONFIG_ROOT / "mappings" / "sources"
_VERSIONS_PATH = _CONFIG_ROOT / "standards" / "normalize_versions.yaml"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldMappingEntry:
    """A single source-field-to-standard-field mapping entry."""

    source_field: str
    standard_field: Optional[str]
    status: str  # "active" | "deprecated" | "pending"
    description: str = ""

    @property
    def is_active(self) -> bool:
        return self.status == "active" and self.standard_field is not None


@dataclass
class DatasetMapping:
    """All mappings for a single (dataset, source) pair."""

    dataset: str
    source: str
    mapping_version: str
    normalize_version: str
    entries: Dict[str, FieldMappingEntry] = field(default_factory=dict)

    def to_rename_dict(self, active_only: bool = True) -> Dict[str, str]:
        """Return a flat {raw_col: standard_col} dict suitable for df.rename().

        Args:
            active_only: If True, only include entries with status="active"
                         and a non-null standard_field.
        """
        return {
            entry.source_field: entry.standard_field
            for entry in self.entries.values()
            if entry.is_active or not active_only
        }

    def active_fields(self) -> List[str]:
        """Return list of standard fields that are actively mapped."""
        return sorted(
            {entry.standard_field for entry in self.entries.values() if entry.is_active}
        )

    def pending_fields(self) -> List[FieldMappingEntry]:
        """Return entries with status="pending"."""
        return [e for e in self.entries.values() if e.status == "pending"]


# ---------------------------------------------------------------------------
# MappingLoader
# ---------------------------------------------------------------------------


class MappingLoader:
    """Loads and caches source field mappings and normalize versions.

    Usage:
        loader = MappingLoader()
        mapping = loader.get_mapping("market_quote_daily", "akshare")
        rename_dict = mapping.to_rename_dict()
        version = loader.get_normalize_version("market_quote_daily", "akshare")
    """

    def __init__(self, config_root: Optional[Path] = None) -> None:
        self._config_root = config_root or _CONFIG_ROOT
        self._mappings_root = self._config_root / "mappings" / "sources"
        self._versions_path = (
            self._config_root / "standards" / "normalize_versions.yaml"
        )

        self._mapping_cache: Dict[str, DatasetMapping] = {}
        self._versions_cache: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_mapping(self, dataset: str, source: str) -> DatasetMapping:
        """Load the field mapping for a (dataset, source) pair.

        Returns a DatasetMapping with all field entries.
        Results are cached after first load.
        """
        cache_key = f"{dataset}:{source}"
        if cache_key in self._mapping_cache:
            return self._mapping_cache[cache_key]

        mapping = self._load_mapping(dataset, source)
        self._mapping_cache[cache_key] = mapping
        return mapping

    def get_normalize_version(self, dataset: str, source: str) -> str:
        """Resolve the current normalize_version for a (dataset, source) pair.

        Looks up config/standards/normalize_versions.yaml.
        Falls back to "v1" if not found.
        """
        versions = self._load_versions()
        dataset_cfg = versions.get("datasets", {}).get(dataset, {})
        source_cfg = dataset_cfg.get("sources", {}).get(source, {})
        return source_cfg.get("current_version", "v1")

    def get_mapping_version(self, dataset: str, source: str) -> str:
        """Return the mapping_version from the source mapping config."""
        mapping = self.get_mapping(dataset, source)
        return mapping.mapping_version

    def list_datasets(self) -> List[str]:
        """List all datasets that have at least one source mapping."""
        if not self._mappings_root.exists():
            return []
        datasets = set()
        for source_dir in self._mappings_root.iterdir():
            if source_dir.is_dir():
                for f in source_dir.glob("*.yaml"):
                    datasets.add(f.stem)
        return sorted(datasets)

    def list_sources(self, dataset: str) -> List[str]:
        """List all sources configured for a given dataset."""
        sources = []
        if not self._mappings_root.exists():
            return []
        for source_dir in self._mappings_root.iterdir():
            if source_dir.is_dir() and (source_dir / f"{dataset}.yaml").exists():
                sources.append(source_dir.name)
        return sorted(sources)

    def get_lineage(self, dataset: str, source: str) -> List[Dict[str, str]]:
        """Return lineage records for all active field mappings.

        Each record: {source_field, standard_field, dataset, source, normalize_version}
        """
        mapping = self.get_mapping(dataset, source)
        return [
            {
                "source_field": entry.source_field,
                "standard_field": entry.standard_field or "",
                "dataset": dataset,
                "source": source,
                "normalize_version": mapping.normalize_version,
                "status": entry.status,
            }
            for entry in mapping.entries.values()
            if entry.is_active
        ]

    # ------------------------------------------------------------------
    # Internal loaders
    # ------------------------------------------------------------------

    def _load_mapping(self, dataset: str, source: str) -> DatasetMapping:
        """Load a single mapping config file."""
        mapping_path = self._mappings_root / source / f"{dataset}.yaml"
        if not mapping_path.exists():
            logger.warning(
                "Mapping config not found: %s — returning empty mapping",
                mapping_path,
            )
            return DatasetMapping(
                dataset=dataset,
                source=source,
                mapping_version="v0",
                normalize_version="v1",
            )

        with open(mapping_path, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}

        mapping_version = cfg.get("mapping_version", "v1")
        normalize_version = cfg.get("normalize_version", "v1")

        entries: Dict[str, FieldMappingEntry] = {}
        fields_cfg = cfg.get("fields", {})
        for raw_field, spec in fields_cfg.items():
            if isinstance(spec, dict):
                entries[raw_field] = FieldMappingEntry(
                    source_field=raw_field,
                    standard_field=spec.get("standard_field"),
                    status=spec.get("status", "active"),
                    description=spec.get("description", ""),
                )
            elif isinstance(spec, str):
                entries[raw_field] = FieldMappingEntry(
                    source_field=raw_field,
                    standard_field=spec,
                    status="active",
                )

        return DatasetMapping(
            dataset=cfg.get("dataset", dataset),
            source=cfg.get("source", source),
            mapping_version=mapping_version,
            normalize_version=normalize_version,
            entries=entries,
        )

    def _load_versions(self) -> Dict[str, Any]:
        """Load the normalize version registry."""
        if self._versions_cache is not None:
            return self._versions_cache

        if not self._versions_path.exists():
            logger.warning(
                "Normalize versions config not found: %s", self._versions_path
            )
            self._versions_cache = {}
            return self._versions_cache

        with open(self._versions_path, "r", encoding="utf-8") as fh:
            self._versions_cache = yaml.safe_load(fh) or {}
        return self._versions_cache


# ---------------------------------------------------------------------------
# Module-level convenience functions (backwards-compatible with base.py)
# ---------------------------------------------------------------------------

_default_loader: Optional[MappingLoader] = None


def _get_loader() -> MappingLoader:
    global _default_loader
    if _default_loader is None:
        _default_loader = MappingLoader()
    return _default_loader


def load_field_mapping(dataset: str, source: str) -> Dict[str, str]:
    """Return a flat {raw_col: standard_col} dict for df.rename().

    Backwards-compatible with the previous function in base.py.
    """
    mapping = _get_loader().get_mapping(dataset, source)
    return mapping.to_rename_dict()


def get_normalize_version(dataset: str, source: str) -> str:
    """Return the current normalize_version for a (dataset, source) pair."""
    return _get_loader().get_normalize_version(dataset, source)
