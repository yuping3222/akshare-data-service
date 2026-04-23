"""Normalizer base class — generic pipeline with overridable hooks.

Design principles:
- Base class owns the normalize() flow, not dataset-specific logic.
- Subclasses override hooks: build_field_map() / _field_mapping(),
  _coerce_types, _derive_fields, _validate_record, _extra_system_fields.
- Config loading is预留 (reserved) for a unified loader; subclasses may
  use inline constants as a temporary fallback.

Backward compatibility:
- Exports both BaseNormalizer (legacy API) and NormalizerBase (new API).
- Legacy subclasses use: ENTITY_NAME, PRIMARY_KEYS, DATE_FIELDS,
  FLOAT_FIELDS, INT_FIELDS, STR_FIELDS, build_field_map().
- New subclasses use: dataset_name, _required_standard_fields,
  _field_mapping().
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unified config loader placeholder
# ---------------------------------------------------------------------------

_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "config"

_MAPPING_CACHE: Dict[str, Dict[str, str]] = {}


def load_field_mapping(dataset: str, source: str) -> Dict[str, str]:
    """Load field mapping from config/mappings/sources/<dataset>/<source>.yaml.

    Falls back to an empty dict if the file does not exist yet.
    Results are cached in memory.
    """
    cache_key = f"{dataset}:{source}"
    if cache_key in _MAPPING_CACHE:
        return _MAPPING_CACHE[cache_key]

    mapping_path = _CONFIG_ROOT / "mappings" / "sources" / dataset / f"{source}.yaml"
    if mapping_path.exists():
        with open(mapping_path, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        result = cfg.get("field_mapping", {})
    else:
        result = {}

    _MAPPING_CACHE[cache_key] = result
    return result


def load_entity_schema(dataset: str) -> Dict[str, Any]:
    """Load entity schema from config/standards/entities/<dataset>.yaml.

    Falls back to an empty dict if the file does not exist yet.
    """
    schema_path = _CONFIG_ROOT / "standards" / "entities" / f"{dataset}.yaml"
    if schema_path.exists():
        with open(schema_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    return {}


# ---------------------------------------------------------------------------
# BaseNormalizer — legacy-compatible API
# ---------------------------------------------------------------------------


class BaseNormalizer(ABC):
    """Abstract base class for entity normalizers (legacy-compatible API).

    Subclass must set:
        ENTITY_NAME: str
        PRIMARY_KEYS: list[str]
        DATE_FIELDS: list[str]

    Subclass must implement:
        build_field_map(source) -> Dict[str, str]

    Optional class attributes:
        FLOAT_FIELDS: set[str]
        INT_FIELDS: set[str]
        STR_FIELDS: set[str]
    """

    ENTITY_NAME: str = ""
    PRIMARY_KEYS: list[str] = []
    DATE_FIELDS: list[str] = []
    FLOAT_FIELDS: set[str] = set()
    INT_FIELDS: set[str] = set()
    STR_FIELDS: set[str] = set()

    SYSTEM_FIELDS = [
        "batch_id",
        "source_name",
        "interface_name",
        "ingest_time",
        "normalize_version",
        "schema_version",
    ]

    def __init__(
        self,
        normalize_version: str = "v1",
        schema_version: str = "v1",
        mapping_loader: Optional[Callable[[str, str], Dict[str, str]]] = None,
    ):
        self.normalize_version = normalize_version
        self.schema_version = schema_version
        self._mapping_loader = mapping_loader

    def normalize(
        self,
        df: pd.DataFrame,
        source: str = "",
        interface_name: str = "",
        batch_id: str = "",
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Normalize a raw DataFrame to standard entity schema."""
        if df is None or df.empty:
            return pd.DataFrame()

        result = df.copy()

        # Step 1: Rename columns via field map
        field_map = self._resolve_field_map(source)
        if field_map:
            result = result.rename(columns=field_map)

        # Step 2: Convert date fields
        for field in self.DATE_FIELDS:
            if field in result.columns:
                result[field] = pd.to_datetime(result[field], errors="coerce")

        # Step 3: Coerce numeric types
        self._coerce_numeric(result)

        # Step 4: Inject system fields
        self._inject_system_fields(result, source, interface_name, batch_id)

        # Step 5: Apply extra fields
        if extra_fields:
            for key, value in extra_fields.items():
                if key not in result.columns:
                    result[key] = value

        # Step 6: Select output columns
        result = self._select_columns(result)

        return result

    def _resolve_field_map(self, source: str) -> Dict[str, str]:
        """Resolve field mapping for the given source."""
        if self._mapping_loader is not None:
            mapping = self._mapping_loader(self.ENTITY_NAME, source)
            if mapping:
                return mapping
        return self.build_field_map(source)

    @abstractmethod
    def build_field_map(self, source: str) -> Dict[str, str]:
        """Build column mapping for the given source."""
        ...

    def _coerce_numeric(self, df: pd.DataFrame) -> None:
        """Coerce numeric fields in place."""
        for col in self.FLOAT_FIELDS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in self.INT_FIELDS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    def _inject_system_fields(
        self,
        df: pd.DataFrame,
        source: str,
        interface_name: str,
        batch_id: str,
    ) -> None:
        """Inject system tracking fields in place."""
        now = datetime.now(timezone.utc)
        if "batch_id" not in df.columns:
            df["batch_id"] = batch_id or ""
        if "source_name" not in df.columns:
            df["source_name"] = source or ""
        if "interface_name" not in df.columns:
            df["interface_name"] = interface_name or ""
        if "ingest_time" not in df.columns:
            df["ingest_time"] = now
        if "normalize_version" not in df.columns:
            df["normalize_version"] = self.normalize_version
        if "schema_version" not in df.columns:
            df["schema_version"] = self.schema_version

    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and order output columns."""
        all_expected = (
            list(self.PRIMARY_KEYS)
            + [f for f in self._business_fields() if f not in self.PRIMARY_KEYS]
            + self.SYSTEM_FIELDS
        )
        available = [c for c in all_expected if c in df.columns]
        extra = [c for c in df.columns if c not in all_expected]
        return df[available + extra].copy()

    def _business_fields(self) -> list[str]:
        """Return all business field names."""
        return []


# ---------------------------------------------------------------------------
# NormalizerBase — new simplified API
# ---------------------------------------------------------------------------


class NormalizerBase(ABC):
    """Abstract base for all dataset normalizers (new simplified API).

    Subclass must implement:
        dataset_name: str
        _field_mapping(source_name) -> Dict[str, str]
        _required_standard_fields: set[str]

    Optional hooks:
        _coerce_types(df, source_name)
        _derive_fields(df, source_name)
        _validate_record(df)
        _extra_system_fields()
    """

    dataset_name: str = ""
    _required_standard_fields: set[str] = set()

    normalize_version: str = "v1"
    schema_version: str = "v1"

    _DEFAULT_SYSTEM_FIELDS = (
        "batch_id",
        "source_name",
        "interface_name",
        "ingest_time",
        "normalize_version",
        "schema_version",
    )

    def normalize(
        self,
        df: pd.DataFrame,
        *,
        batch_id: str,
        source_name: str,
        interface_name: str,
        ingest_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Run the full normalization pipeline."""
        if df is None or df.empty:
            return pd.DataFrame()

        result = df.copy()
        col_map = self._field_mapping(source_name)
        result = self._apply_mapping(result, col_map)
        result = self._coerce_types(result, source_name)
        result = self._derive_fields(result, source_name)
        result = self._validate_record(result)

        now = ingest_time or datetime.now(timezone.utc)
        result = self._inject_system_fields(
            result,
            batch_id=batch_id,
            source_name=source_name,
            interface_name=interface_name,
            ingest_time=now,
        )
        result = self._select_output_columns(result)
        return result

    @abstractmethod
    def _field_mapping(self, source_name: str) -> Dict[str, str]:
        """Return {raw_column: standard_column} for the given source."""
        ...

    def _coerce_types(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        return df

    def _derive_fields(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        return df

    def _validate_record(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _extra_system_fields(self) -> tuple[str, ...]:
        return ()

    @staticmethod
    def _apply_mapping(df: pd.DataFrame, col_map: Dict[str, str]) -> pd.DataFrame:
        effective = {old: new for old, new in col_map.items() if old in df.columns}
        if effective:
            df = df.rename(columns=effective)
        return df

    def _inject_system_fields(
        self,
        df: pd.DataFrame,
        *,
        batch_id: str,
        source_name: str,
        interface_name: str,
        ingest_time: datetime,
    ) -> pd.DataFrame:
        df = df.copy()
        df["batch_id"] = batch_id
        df["source_name"] = source_name
        df["interface_name"] = interface_name
        df["ingest_time"] = ingest_time
        df["normalize_version"] = self.normalize_version
        df["schema_version"] = self.schema_version
        for extra in self._extra_system_fields():
            if extra not in df.columns:
                df[extra] = None
        return df

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        all_needed = set(self._required_standard_fields) | set(self._DEFAULT_SYSTEM_FIELDS)
        all_needed |= set(self._extra_system_fields())
        available = [c for c in all_needed if c in df.columns]
        return df[available].copy()
