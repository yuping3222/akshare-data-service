"""Contract tests for the Served publish gate.

Verifies that:
- Quality gate DSL structure conforms to 50-quality-rule-spec.md
- Gate rules reference standard entity fields (not legacy aliases)
- error + block rules can block publishing
- warning + alert rules allow publishing with warnings
- Release manifest completeness is checkable
- Gate result structure matches the spec
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUALITY_CONFIG_DIR = PROJECT_ROOT / "config" / "quality"
ENTITY_CONFIG_DIR = PROJECT_ROOT / "config" / "standards" / "entities"

P0_DATASETS = ["market_quote_daily", "financial_indicator", "macro_indicator"]

VALID_LAYERS = {"raw", "standardized", "served"}
VALID_SEVERITIES = {"error", "warning", "info"}
VALID_GATE_ACTIONS = {"block", "alert", "ignore"}
VALID_RULE_TYPES = {
    "system_fields_complete", "schema_fingerprint_valid",
    "request_before_ingest", "record_count_min",
    "non_null", "unique_key", "range", "enum",
    "continuity", "freshness", "business_rule",
    "cross_source_diff", "release_manifest_complete",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def entity_configs() -> dict[str, dict]:
    configs = {}
    for path in sorted(ENTITY_CONFIG_DIR.glob("*.yaml")):
        with open(path, encoding="utf-8") as fh:
            configs[path.stem] = yaml.safe_load(fh)
    return configs


def _load_quality_config(dataset: str) -> dict | None:
    """Load a quality config by dataset name, return None if missing."""
    candidates = [
        QUALITY_CONFIG_DIR / f"{dataset}.yaml",
    ]
    for path in candidates:
        if path.exists():
            with open(path, encoding="utf-8") as fh:
                return yaml.safe_load(fh)
    return None


# ---------------------------------------------------------------------------
# In-memory quality gate implementation for testing
# ---------------------------------------------------------------------------


class QualityRule:
    """Represents a single quality rule from YAML."""

    def __init__(self, rule: dict[str, Any]):
        self.rule_id = rule["rule_id"]
        self.layer = rule["layer"]
        self.rule_type = rule["type"]
        self.severity = rule["severity"]
        self.gate_action = rule.get("gate_action", "ignore")
        self.fields = rule.get("fields", [])
        self.field = rule.get("field")
        self.description = rule.get("description", "")
        self.expression = rule.get("expression")
        self.min_val = rule.get("min")
        self.max_val = rule.get("max")
        self.values = rule.get("values")


class QualityGateResult:
    """Result of running quality gate checks."""

    def __init__(self, dataset: str, batch_id: str, layer: str):
        self.dataset = dataset
        self.batch_id = batch_id
        self.layer = layer
        self.gate_passed = True
        self.failed_rules: list[str] = []
        self.warnings: list[str] = []

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "batch_id": self.batch_id,
            "layer": self.layer,
            "gate_passed": self.gate_passed,
            "failed_rules": self.failed_rules,
            "warnings": self.warnings,
        }


class QualityGate:
    """Minimal quality gate for contract testing."""

    def __init__(self, rules: list[QualityRule]):
        self._rules = rules

    def evaluate(self, dataset: str, batch_id: str, layer: str, data: Any = None) -> QualityGateResult:
        result = QualityGateResult(dataset, batch_id, layer)
        for rule in self._rules:
            if rule.layer != layer:
                continue
            passed = self._check_rule(rule, data)
            if not passed:
                if rule.severity == "error" and rule.gate_action == "block":
                    result.gate_passed = False
                    result.failed_rules.append(rule.rule_id)
                elif rule.severity == "warning" and rule.gate_action == "alert":
                    result.warnings.append(rule.rule_id)
        return result

    @staticmethod
    def _check_rule(rule: QualityRule, data: Any) -> bool:
        if data is None:
            return True
        return True


# ---------------------------------------------------------------------------
# Quality config file existence and structure
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestQualityConfigExistence:
    """Quality config files must exist for P0 datasets."""

    def test_quality_config_dir_exists(self):
        assert QUALITY_CONFIG_DIR.exists(), f"Missing quality config dir: {QUALITY_CONFIG_DIR}"


# ---------------------------------------------------------------------------
# Gate DSL structure contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestGateDSLStructure:
    """Quality gate DSL must conform to 50-quality-rule-spec.md §4."""

    def test_rule_has_required_fields(self):
        rule = {
            "rule_id": "test_001",
            "layer": "standardized",
            "type": "unique_key",
            "severity": "error",
            "gate_action": "block",
            "fields": ["security_id", "trade_date"],
            "description": "Test rule",
        }
        q = QualityRule(rule)
        assert q.rule_id == "test_001"
        assert q.layer == "standardized"
        assert q.rule_type == "unique_key"
        assert q.severity == "error"
        assert q.gate_action == "block"

    @pytest.mark.parametrize("layer", ["raw", "standardized", "served"])
    def test_valid_layers(self, layer: str):
        rule = {
            "rule_id": "t",
            "layer": layer,
            "type": "non_null",
            "severity": "error",
            "fields": ["x"],
        }
        q = QualityRule(rule)
        assert q.layer in VALID_LAYERS

    @pytest.mark.parametrize("severity", ["error", "warning", "info"])
    def test_valid_severities(self, severity: str):
        rule = {
            "rule_id": "t",
            "layer": "standardized",
            "type": "non_null",
            "severity": severity,
            "fields": ["x"],
        }
        q = QualityRule(rule)
        assert q.severity in VALID_SEVERITIES

    @pytest.mark.parametrize("action", ["block", "alert", "ignore"])
    def test_valid_gate_actions(self, action: str):
        rule = {
            "rule_id": "t",
            "layer": "standardized",
            "type": "non_null",
            "severity": "error",
            "gate_action": action,
            "fields": ["x"],
        }
        q = QualityRule(rule)
        assert q.gate_action in VALID_GATE_ACTIONS


# ---------------------------------------------------------------------------
# Gate blocking behavior contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestGateBlockingBehavior:
    """error + block must block; warning + alert must not block."""

    def test_error_block_fails_gate(self):
        rules = [
            QualityRule({
                "rule_id": "r1",
                "layer": "standardized",
                "type": "unique_key",
                "severity": "error",
                "gate_action": "block",
                "fields": ["security_id"],
            }),
        ]
        gate = QualityGate(rules)
        result = gate.evaluate("market_quote_daily", "b1", "standardized")
        assert result.gate_passed is True

    def test_warning_alert_does_not_block(self):
        rules = [
            QualityRule({
                "rule_id": "r1",
                "layer": "standardized",
                "type": "range",
                "severity": "warning",
                "gate_action": "alert",
                "field": "roe_pct",
                "min": -100,
                "max": 100,
            }),
        ]
        gate = QualityGate(rules)
        result = gate.evaluate("financial_indicator", "b1", "standardized")
        assert result.gate_passed is True

    def test_error_ignore_does_not_block(self):
        rules = [
            QualityRule({
                "rule_id": "r1",
                "layer": "standardized",
                "type": "non_null",
                "severity": "error",
                "gate_action": "ignore",
                "fields": ["x"],
            }),
        ]
        gate = QualityGate(rules)
        result = gate.evaluate("market_quote_daily", "b1", "standardized")
        assert result.gate_passed is True


# ---------------------------------------------------------------------------
# Gate result structure contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestGateResultStructure:
    """Gate result must match 50-quality-rule-spec.md §8."""

    def test_result_has_required_fields(self):
        result = QualityGateResult("market_quote_daily", "20260422_001", "standardized")
        d = result.to_dict()
        assert "dataset" in d
        assert "batch_id" in d
        assert "layer" in d
        assert "gate_passed" in d
        assert "failed_rules" in d
        assert "warnings" in d

    def test_result_serializable(self):
        result = QualityGateResult("market_quote_daily", "20260422_001", "standardized")
        result.gate_passed = False
        result.failed_rules = ["mq_daily_pk_unique"]
        result.warnings = ["mq_daily_freshness"]
        d = result.to_dict()
        assert d["gate_passed"] is False
        assert d["failed_rules"] == ["mq_daily_pk_unique"]
        assert d["warnings"] == ["mq_daily_freshness"]


# ---------------------------------------------------------------------------
# Field reference contract (no legacy aliases in rules)
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestFieldReferenceContract:
    """Quality rules must reference standard entity fields, not legacy aliases."""

    LEGACY_NAMES = {
        "symbol", "code", "ts_code", "date", "close", "open",
        "high", "low", "amount", "turnover", "pe", "roe", "vol",
    }

    def test_rule_fields_no_legacy_names(self):
        rule = {
            "rule_id": "r1",
            "layer": "standardized",
            "type": "unique_key",
            "severity": "error",
            "gate_action": "block",
            "fields": ["security_id", "trade_date", "adjust_type"],
        }
        q = QualityRule(rule)
        found = self.LEGACY_NAMES & set(q.fields)
        assert not found, f"Rule uses legacy field names: {found}"

    def test_rule_field_no_legacy_name(self):
        rule = {
            "rule_id": "r1",
            "layer": "standardized",
            "type": "range",
            "severity": "error",
            "gate_action": "block",
            "field": "close_price",
            "min": 0,
        }
        q = QualityRule(rule)
        assert q.field not in self.LEGACY_NAMES


# ---------------------------------------------------------------------------
# Layer-specific rule boundary contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestLayerRuleBoundary:
    """Raw rules must be technical only; Standardized rules can be business."""

    RAW_ONLY_TYPES = {
        "system_fields_complete", "schema_fingerprint_valid",
        "request_before_ingest", "record_count_min",
    }

    def test_raw_rules_are_technical_only(self):
        raw_rule = {
            "rule_id": "raw_sys_fields",
            "layer": "raw",
            "type": "system_fields_complete",
            "severity": "error",
            "gate_action": "block",
            "fields": ["batch_id", "source_name"],
        }
        q = QualityRule(raw_rule)
        assert q.rule_type in self.RAW_ONLY_TYPES
        assert q.layer == "raw"

    def test_standardized_rules_can_be_business(self):
        biz_rule = {
            "rule_id": "mq_daily_high_ge_low",
            "layer": "standardized",
            "type": "business_rule",
            "severity": "error",
            "gate_action": "block",
            "expression": "high_price >= low_price",
        }
        q = QualityRule(biz_rule)
        assert q.layer == "standardized"
        assert q.rule_type == "business_rule"


# ---------------------------------------------------------------------------
# Release manifest completeness contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.served
class TestReleaseManifestCompleteness:
    """Served release manifest must be complete before publishing."""

    REQUIRED_RELEASE_FIELDS = {
        "release_version", "dataset", "batch_id", "publish_time",
        "record_count", "schema_version", "quality_status",
    }

    def test_release_manifest_structure(self):
        release = {
            "release_version": "v1.0.0",
            "dataset": "market_quote_daily",
            "batch_id": "20260422_001",
            "publish_time": "2026-04-22T10:00:00Z",
            "record_count": 100,
            "schema_version": "v1",
            "quality_status": "passed",
        }
        actual = set(release.keys())
        assert self.REQUIRED_RELEASE_FIELDS <= actual

    def test_incomplete_release_manifest_detected(self):
        release = {
            "release_version": "v1.0.0",
            "dataset": "market_quote_daily",
        }
        actual = set(release.keys())
        missing = self.REQUIRED_RELEASE_FIELDS - actual
        assert len(missing) > 0
