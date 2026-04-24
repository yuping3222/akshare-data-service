"""Contract tests for the field dictionary.

Verifies that:
- field_dictionary.yaml is valid and versioned
- All standard entity fields are present in the dictionary
- Alias mappings are consistent (no circular refs, no missing targets)
- No legacy names appear as canonical field names
- Field types match between entity configs and the dictionary
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from akshare_data.governance.schema_registry import SchemaRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIELD_DICT_PATH = PROJECT_ROOT / "config" / "standards" / "field_dictionary.yaml"
ENTITY_CONFIG_DIR = PROJECT_ROOT / "config" / "standards" / "entities"

P0_ENTITIES = ["market_quote_daily", "financial_indicator", "macro_indicator"]

LEGACY_NAMES = {
    "symbol",
    "code",
    "ts_code",
    "date",
    "datetime",
    "close",
    "open",
    "high",
    "low",
    "amount",
    "turnover",
    "pe",
    "roe",
    "roa",
    "vol",
    "adjust",
    "adjust_flag",
    "pct_chg",
    "pct_change",
    "announce_date",
    "total_revenue",
    "net_assets",
    "debt_ratio",
    "yoy",
    "mom",
    "agency",
    "publisher",
    "market",
    "country",
    "area",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def field_dict() -> dict[str, Any]:
    with open(FIELD_DICT_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture
def schema_registry() -> SchemaRegistry:
    reg = SchemaRegistry(config_dir=ENTITY_CONFIG_DIR)
    reg.load_all()
    return reg


# ---------------------------------------------------------------------------
# Structure and version
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestFieldDictionaryStructure:
    """Basic structure validation."""

    def test_file_exists(self):
        assert FIELD_DICT_PATH.exists(), f"Missing field dictionary: {FIELD_DICT_PATH}"

    def test_has_version(self, field_dict: dict):
        assert "version" in field_dict
        assert field_dict["version"]

    def test_has_system_fields(self, field_dict: dict):
        assert "system_fields" in field_dict
        assert len(field_dict["system_fields"]) > 0

    def test_has_fields_section(self, field_dict: dict):
        assert "fields" in field_dict
        assert len(field_dict["fields"]) > 0

    def test_has_alias_map(self, field_dict: dict):
        assert "alias_map" in field_dict
        assert len(field_dict["alias_map"]) > 0


# ---------------------------------------------------------------------------
# System fields consistency
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestSystemFieldsConsistency:
    """System fields in dictionary must match entity configs."""

    EXPECTED_SYSTEM_FIELDS = {
        "batch_id",
        "source_name",
        "interface_name",
        "ingest_time",
        "normalize_version",
        "schema_version",
        "quality_status",
        "publish_time",
        "release_version",
    }

    def test_all_system_fields_present(self, field_dict: dict):
        actual = set(field_dict["system_fields"].keys())
        assert actual == self.EXPECTED_SYSTEM_FIELDS

    def test_system_field_types(self, field_dict: dict):
        expected_types = {
            "batch_id": "string",
            "source_name": "string",
            "interface_name": "string",
            "ingest_time": "timestamp",
            "normalize_version": "string",
            "schema_version": "string",
            "quality_status": "string",
            "publish_time": "timestamp",
            "release_version": "string",
        }
        for fname, fdef in field_dict["system_fields"].items():
            assert fdef["type"] == expected_types[fname]


# ---------------------------------------------------------------------------
# Business field coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestBusinessFieldCoverage:
    """All entity business fields must be in the field dictionary."""

    def test_all_entity_fields_in_dictionary(
        self, field_dict: dict, schema_registry: SchemaRegistry
    ):
        dict_fields = set(field_dict["fields"].keys())
        for entity in P0_ENTITIES:
            schema = schema_registry.get(entity)
            for fname in schema.fields:
                assert fname in dict_fields, (
                    f"Entity {entity} field '{fname}' not in field dictionary"
                )


# ---------------------------------------------------------------------------
# Alias map integrity
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestAliasMapIntegrity:
    """Alias map must be well-formed."""

    def test_no_legacy_as_canonical(self, field_dict: dict):
        canonical = set(field_dict["fields"].keys())
        found = canonical & LEGACY_NAMES
        assert not found, f"Legacy names used as canonical fields: {found}"

    def test_alias_targets_exist(self, field_dict: dict):
        canonical = set(field_dict["fields"].keys())
        for alias, target in field_dict["alias_map"].items():
            assert target in canonical, (
                f"Alias '{alias}' -> '{target}' but '{target}' is not a canonical field"
            )

    def test_no_circular_aliases(self, field_dict: dict):
        alias_map = field_dict["alias_map"]
        for alias in alias_map:
            visited = set()
            current = alias
            while current in alias_map:
                assert current not in visited, (
                    f"Circular alias detected involving '{current}'"
                )
                visited.add(current)
                current = alias_map[current]

    def test_alias_not_same_as_target(self, field_dict: dict):
        for alias, target in field_dict["alias_map"].items():
            assert alias != target, f"Alias maps to itself: {alias}"

    def test_no_duplicate_alias_values(self, field_dict: dict):
        """Each canonical field should not appear as both alias and target inconsistently."""
        alias_keys = set(field_dict["alias_map"].keys())
        for alias, target in field_dict["alias_map"].items():
            if target in alias_keys:
                assert field_dict["alias_map"][target] != alias, (
                    f"Mutual aliasing: {alias} <-> {target}"
                )


# ---------------------------------------------------------------------------
# Field type consistency between dictionary and entity configs
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestFieldTypeConsistency:
    """Field types must be consistent across dictionary and entity configs."""

    def test_types_match_for_shared_fields(
        self, field_dict: dict, schema_registry: SchemaRegistry
    ):
        dict_fields = field_dict["fields"]
        for entity in P0_ENTITIES:
            schema = schema_registry.get(entity)
            for fname, fdef in schema.fields.items():
                if fname in dict_fields:
                    dict_type = dict_fields[fname]["type"]
                    assert fdef.field_type == dict_type, (
                        f"{entity}.{fname} type mismatch: entity={fdef.field_type}, dict={dict_type}"
                    )


# ---------------------------------------------------------------------------
# No forbidden names in new configs
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestNoForbiddenNames:
    """New configs must not use forbidden legacy names."""

    FORBIDDEN_DATASET_NAMES = {
        # "stock_daily" removed: now a registered canonical entity (P1-1 YAML migration)
        "quote_daily",
        "indicator",
        "finance_indicator",
    }

    def test_no_forbidden_dataset_names_in_config_files(self):
        for path in sorted(ENTITY_CONFIG_DIR.glob("*.yaml")):
            stem = path.stem
            assert stem not in self.FORBIDDEN_DATASET_NAMES, (
                f"Forbidden dataset name in config: {stem}"
            )

    def test_no_legacy_names_in_field_keys(self, field_dict: dict):
        canonical = set(field_dict["fields"].keys())
        found = canonical & LEGACY_NAMES
        assert not found, f"Legacy names found as canonical field keys: {found}"
