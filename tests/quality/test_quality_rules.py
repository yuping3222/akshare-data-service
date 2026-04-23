"""Quality-rule focused tests (T10-005)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from akshare_data.quality.engine import GateAction, Layer, QualityEngine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUALITY_CONFIG_DIR = PROJECT_ROOT / "config" / "quality"


def _load_quality_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@pytest.mark.unit
@pytest.mark.contract
class TestQualityDslGovernance:
    def test_quality_config_files_exist(self):
        files = sorted(QUALITY_CONFIG_DIR.glob("*.yaml"))
        assert files, f"No quality config found under {QUALITY_CONFIG_DIR}"

    def test_each_quality_file_has_required_sections(self):
        for path in sorted(QUALITY_CONFIG_DIR.glob("*.yaml")):
            cfg = _load_quality_config(path)
            assert cfg.get("version"), f"Missing version in {path}"
            assert cfg.get("dataset"), f"Missing dataset in {path}"
            assert cfg.get("entity"), f"Missing entity in {path}"
            assert cfg.get("schema_version"), f"Missing schema_version in {path}"
            assert isinstance(cfg.get("rules"), list) and cfg["rules"], (
                f"Missing rules in {path}"
            )

    def test_rule_layers_actions_are_valid_enums(self):
        valid_layers = {layer.value for layer in Layer}
        valid_actions = {action.value for action in GateAction}

        for path in sorted(QUALITY_CONFIG_DIR.glob("*.yaml")):
            cfg = _load_quality_config(path)
            for rule in cfg.get("rules", []):
                assert rule["layer"] in valid_layers, (
                    f"{path}: invalid layer {rule['layer']}"
                )
                assert rule.get("gate_action", "ignore") in valid_actions, (
                    f"{path}: invalid gate_action {rule.get('gate_action')}"
                )

    def test_no_legacy_dataset_names(self):
        forbidden = {"stock_daily", "finance_indicator", "quote_daily"}
        for path in sorted(QUALITY_CONFIG_DIR.glob("*.yaml")):
            cfg = _load_quality_config(path)
            assert cfg.get("dataset") not in forbidden, (
                f"Forbidden legacy dataset name in {path}: {cfg.get('dataset')}"
            )


@pytest.mark.unit
class TestQualityEngineLoading:
    def test_engine_can_load_all_quality_configs(self):
        engine = QualityEngine()
        for path in sorted(QUALITY_CONFIG_DIR.glob("*.yaml")):
            engine.load_config(path)
            assert engine._rules, f"No rules loaded from {path}"  # noqa: SLF001

    def test_block_rule_exists_for_each_dataset(self):
        engine = QualityEngine()
        for path in sorted(QUALITY_CONFIG_DIR.glob("*.yaml")):
            engine.load_config(path)
            has_block = any(r.gate_action == GateAction.BLOCK for r in engine._rules)  # noqa: SLF001
            assert has_block, f"{path} has no block rule; release gate would be non-blocking"
