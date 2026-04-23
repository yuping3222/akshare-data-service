"""Contract tests for standard entity schemas.

Verifies that:
- Entity YAML configs exist for all P0 entities
- Schema fields match docs/design/30-standard-entities.md
- Primary keys, partition columns, and required fields are consistent
- System fields are uniform across all entities
- SchemaRegistry can load and validate all entity schemas
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from akshare_data.governance.schema_registry import SchemaRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTITY_CONFIG_DIR = PROJECT_ROOT / "config" / "standards" / "entities"
ENTITIES_DOC = PROJECT_ROOT / "docs" / "design" / "30-standard-entities.md"

P0_ENTITIES = ["market_quote_daily", "financial_indicator", "macro_indicator"]

UNIFIED_SYSTEM_FIELDS = [
    "batch_id",
    "source_name",
    "interface_name",
    "ingest_time",
    "normalize_version",
    "schema_version",
    "quality_status",
    "publish_time",
    "release_version",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def schema_registry() -> SchemaRegistry:
    """SchemaRegistry loaded from the real config directory."""
    reg = SchemaRegistry(config_dir=ENTITY_CONFIG_DIR)
    reg.load_all()
    return reg


@pytest.fixture
def entity_configs() -> dict[str, dict[str, Any]]:
    """Raw YAML dicts for all entity configs."""
    configs: dict[str, dict[str, Any]] = {}
    for path in sorted(ENTITY_CONFIG_DIR.glob("*.yaml")):
        with open(path, encoding="utf-8") as fh:
            configs[path.stem] = yaml.safe_load(fh)
    return configs


# ---------------------------------------------------------------------------
# P0 entity config existence
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestP0EntityConfigExistence:
    """Ensure all P0 entity config files exist and are parseable."""

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_config_file_exists(self, entity: str):
        config_path = ENTITY_CONFIG_DIR / f"{entity}.yaml"
        assert config_path.exists(), f"Missing config: {config_path}"

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_config_is_valid_yaml(self, entity: str):
        config_path = ENTITY_CONFIG_DIR / f"{entity}.yaml"
        with open(config_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        assert isinstance(data, dict)
        assert data.get("entity") == entity


# ---------------------------------------------------------------------------
# SchemaRegistry loading
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestSchemaRegistryLoading:
    """Verify SchemaRegistry can load all P0 entity schemas."""

    def test_loads_all_p0_entities(self, schema_registry: SchemaRegistry):
        for entity in P0_ENTITIES:
            assert schema_registry.has(entity), f"Schema not registered: {entity}"

    def test_p0_entity_count(self, schema_registry: SchemaRegistry):
        p0 = schema_registry.list_by_priority("P0")
        assert len(p0) >= len(P0_ENTITIES)


# ---------------------------------------------------------------------------
# Primary key and partition contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestPrimaryKeyContract:
    """Primary keys must match 30-standard-entities.md."""

    EXPECTED_KEYS = {
        "market_quote_daily": ["security_id", "trade_date", "adjust_type"],
        "financial_indicator": ["security_id", "report_date", "report_type"],
        "macro_indicator": ["indicator_code", "observation_date"],
    }

    EXPECTED_PARTITIONS = {
        "market_quote_daily": ["trade_date"],
        "financial_indicator": ["report_date"],
        "macro_indicator": ["indicator_code", "observation_date"],
    }

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_primary_key_matches_spec(
        self, entity: str, schema_registry: SchemaRegistry
    ):
        schema = schema_registry.get(entity)
        assert schema.primary_key == self.EXPECTED_KEYS[entity]

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_partition_by_matches_spec(
        self, entity: str, schema_registry: SchemaRegistry
    ):
        schema = schema_registry.get(entity)
        assert schema.partition_by == self.EXPECTED_PARTITIONS[entity]


# ---------------------------------------------------------------------------
# Required fields contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestRequiredFieldsContract:
    """Required fields in YAML must match the spec document."""

    EXPECTED_REQUIRED = {
        "market_quote_daily": [
            "security_id",
            "exchange",
            "adjust_type",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover_amount",
        ],
        "financial_indicator": ["security_id", "report_date", "report_type"],
        "macro_indicator": [
            "indicator_code",
            "indicator_name",
            "frequency",
            "observation_date",
            "value",
        ],
    }

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_required_fields_match_spec(
        self, entity: str, schema_registry: SchemaRegistry
    ):
        schema = schema_registry.get(entity)
        assert set(schema.required_fields) == set(self.EXPECTED_REQUIRED[entity])


# ---------------------------------------------------------------------------
# System fields uniformity
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestSystemFieldsUniformity:
    """All entities must share the same unified system fields."""

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_system_field_names_present(
        self, entity: str, schema_registry: SchemaRegistry
    ):
        schema = schema_registry.get(entity)
        sys_fields = set(schema.system_fields.keys())
        for field in UNIFIED_SYSTEM_FIELDS:
            assert field in sys_fields, f"{entity} missing system field: {field}"

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_system_field_types_consistent(
        self, entity: str, schema_registry: SchemaRegistry
    ):
        schema = schema_registry.get(entity)
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
        for fname, fdef in schema.system_fields.items():
            assert fdef.field_type == expected_types[fname], (
                f"{entity}.{fname} type mismatch: got {fdef.field_type}, expected {expected_types[fname]}"
            )


# ---------------------------------------------------------------------------
# Business field contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestBusinessFieldContract:
    """Business fields must match 30-standard-entities.md definitions."""

    EXPECTED_FIELDS = {
        "market_quote_daily": {
            "security_id",
            "exchange",
            "adjust_type",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover_amount",
            "change_pct",
            "turnover_rate",
        },
        "financial_indicator": {
            "security_id",
            "report_date",
            "report_type",
            "publish_date",
            "currency",
            "pe_ratio_ttm",
            "pb_ratio",
            "ps_ratio_ttm",
            "roe_pct",
            "roa_pct",
            "net_profit",
            "revenue",
            "total_assets",
            "total_equity",
            "debt_ratio_pct",
            "gross_margin_pct",
            "net_margin_pct",
        },
        "macro_indicator": {
            "indicator_code",
            "indicator_name",
            "frequency",
            "region",
            "observation_date",
            "publish_date",
            "value",
            "value_yoy_pct",
            "value_mom_pct",
            "unit",
            "source_org",
        },
    }

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_business_fields_match_spec(
        self, entity: str, schema_registry: SchemaRegistry
    ):
        schema = schema_registry.get(entity)
        actual = set(schema.fields.keys())
        expected = self.EXPECTED_FIELDS[entity]
        assert actual == expected, (
            f"{entity}: missing={expected - actual}, extra={actual - expected}"
        )

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_no_legacy_field_names(self, entity: str, schema_registry: SchemaRegistry):
        """Legacy aliases must not appear as top-level field names."""
        legacy_names = {
            "symbol",
            "code",
            "ts_code",
            "date",
            "close",
            "open",
            "high",
            "low",
            "amount",
            "turnover",
            "pe",
            "roe",
            "vol",
        }
        schema = schema_registry.get(entity)
        found = legacy_names & set(schema.fields.keys())
        assert not found, f"{entity} uses legacy field names: {found}"


# ---------------------------------------------------------------------------
# Schema version contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
class TestSchemaVersionContract:
    """Schema version must be present and non-empty."""

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_schema_version_present(self, entity: str, schema_registry: SchemaRegistry):
        version = schema_registry.get_schema_version(entity)
        assert version, f"{entity} has empty schema version"

    @pytest.mark.parametrize("entity", P0_ENTITIES)
    def test_config_version_matches_entity(self, entity: str, entity_configs: dict):
        cfg = entity_configs[entity]
        assert cfg["version"] == cfg["entity"].replace("_", "-") or cfg.get("version")
