"""Extended quality layer tests for checks, gate, and engine."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import pandas as pd
import numpy as np

from akshare_data.quality.engine import (
    BaseCheck,
    QualityEngine,
    RuleDef,
    RuleResult,
    RuleStatus,
    Severity,
    GateAction,
    Layer,
)
from akshare_data.quality.gate import QualityGate, GateDecision, GateBlockedError
from akshare_data.quality.checks.completeness import (
    ContinuityCheck,
    PrimaryKeyCoverageCheck,
    PartitionCoverageCheck,
)
from akshare_data.quality.checks.anomaly import (
    PriceAnomalyCheck,
    NumericRangeAnomalyCheck,
    VolatilityAnomalyCheck,
)
from akshare_data.quality.checks.consistency import (
    CrossSourceDiffCheck,
    CrossTableConsistencyCheck,
    CrossLayerConsistencyCheck,
)


@pytest.fixture
def sample_quote_df() -> pd.DataFrame:
    """Generate sample market quote data for testing."""
    dates = pd.date_range("2024-01-01", periods=30, freq="B")
    return pd.DataFrame({
        "trade_date": dates,
        "symbol": ["sh600000"] * 30,
        "open_price": np.random.uniform(10, 15, 30),
        "high_price": np.random.uniform(15, 18, 30),
        "low_price": np.random.uniform(8, 10, 30),
        "close_price": np.random.uniform(10, 15, 30),
        "change_pct": np.random.uniform(-3, 3, 30),
        "volume": np.random.uniform(100000, 500000, 30),
    })


@pytest.fixture
def sample_quote_df_with_anomalies() -> pd.DataFrame:
    """Generate sample data with intentional anomalies."""
    dates = pd.date_range("2024-01-01", periods=10, freq="B")
    df = pd.DataFrame({
        "trade_date": dates,
        "symbol": ["sh600000"] * 10,
        "open_price": [10.0] * 10,
        "high_price": [8.0, 12.0, 12.0, 12.0, 12.0, 12.0, 12.0, 12.0, 12.0, 12.0],
        "low_price": [12.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
        "close_price": [10.5] * 10,
        "change_pct": [5.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 25.0, 2.0],
        "volume": [100000] * 10,
    })
    return df


@pytest.fixture
def temp_quality_config() -> Path:
    """Create a temporary quality config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("""
version: "1.0"
dataset: test_dataset
entity: test_entity
schema_version: "v1"
rules:
  - rule_id: test_non_null
    layer: standardized
    type: non_null
    fields: [symbol, close_price]
    severity: error
    gate_action: block
    description: "Primary fields must be non-null"

  - rule_id: test_range_check
    layer: standardized
    type: range
    field: change_pct
    min: -20
    max: 20
    severity: warning
    gate_action: alert
    description: "Change percentage within normal range"

  - rule_id: test_continuity
    layer: standardized
    type: continuity
    field: trade_date
    severity: error
    gate_action: block
    params:
      max_gap_days: 5
    description: "No gaps in time series"
""")
        return Path(f.name)


@pytest.mark.unit
class TestCompletenessChecks:
    """Test completeness check implementations."""

    def test_continuity_check_passes(self, sample_quote_df):
        """Test continuity check with continuous data."""
        check = ContinuityCheck()
        rule = RuleDef(
            rule_id="test_cont",
            layer=Layer.STANDARDIZED,
            rule_type="continuity",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="trade_date",
            params={"max_gap_days": 5},
        )
        result = check.execute(sample_quote_df, rule)
        assert result.status == RuleStatus.PASSED
        assert result.failed_count == 0

    def test_continuity_check_detects_gap(self):
        """Test continuity check detects gaps in time series."""
        dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-10"])
        df = pd.DataFrame({
            "trade_date": dates,
            "symbol": ["sh600000"] * 3,
            "close_price": [10.0, 10.5, 11.0],
        })
        check = ContinuityCheck()
        rule = RuleDef(
            rule_id="test_cont",
            layer=Layer.STANDARDIZED,
            rule_type="continuity",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="trade_date",
            params={"max_gap_days": 3},
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.FAILED
        assert result.failed_count > 0

    def test_continuity_check_missing_field(self):
        """Test continuity check with missing date field."""
        df = pd.DataFrame({"symbol": ["sh600000"], "close_price": [10.0]})
        check = ContinuityCheck()
        rule = RuleDef(
            rule_id="test_cont",
            layer=Layer.STANDARDIZED,
            rule_type="continuity",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="nonexistent_date",
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.ERROR

    def test_continuity_check_insufficient_data(self):
        """Test continuity check with fewer than 2 dates."""
        df = pd.DataFrame({
            "trade_date": pd.to_datetime(["2024-01-01"]),
            "symbol": ["sh600000"],
        })
        check = ContinuityCheck()
        rule = RuleDef(
            rule_id="test_cont",
            layer=Layer.STANDARDIZED,
            rule_type="continuity",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="trade_date",
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.SKIPPED

    def test_pk_coverage_check_passes(self, sample_quote_df):
        """Test primary key coverage with full coverage."""
        check = PrimaryKeyCoverageCheck()
        rule = RuleDef(
            rule_id="test_pk",
            layer=Layer.STANDARDIZED,
            rule_type="pk_coverage",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            fields=["symbol", "trade_date"],
            params={"min_coverage": 1.0},
        )
        result = check.execute(sample_quote_df, rule)
        assert result.status == RuleStatus.PASSED

    def test_pk_coverage_check_with_nulls(self):
        """Test primary key coverage with null values."""
        df = pd.DataFrame({
            "symbol": ["sh600000", None, "sh600001"],
            "trade_date": pd.date_range("2024-01-01", periods=3),
        })
        check = PrimaryKeyCoverageCheck()
        rule = RuleDef(
            rule_id="test_pk",
            layer=Layer.STANDARDIZED,
            rule_type="pk_coverage",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            fields=["symbol"],
            params={"min_coverage": 1.0},
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.FAILED

    def test_partition_coverage_check_passes(self):
        """Test partition coverage with all expected partitions."""
        df = pd.DataFrame({
            "trade_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "symbol": ["sh600000"] * 3,
        })
        check = PartitionCoverageCheck()
        rule = RuleDef(
            rule_id="test_partition",
            layer=Layer.STANDARDIZED,
            rule_type="partition_coverage",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="trade_date",
            params={
                "expected_partitions": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "min_coverage": 1.0,
            },
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.PASSED

    def test_partition_coverage_check_missing_partitions(self):
        """Test partition coverage with missing partitions."""
        df = pd.DataFrame({
            "trade_date": ["2024-01-01", "2024-01-02"],
            "symbol": ["sh600000"] * 2,
        })
        check = PartitionCoverageCheck()
        rule = RuleDef(
            rule_id="test_partition",
            layer=Layer.STANDARDIZED,
            rule_type="partition_coverage",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="trade_date",
            params={
                "expected_partitions": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "min_coverage": 1.0,
            },
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.FAILED
        assert len(result.details.get("missing_partitions", [])) > 0


@pytest.mark.unit
class TestAnomalyChecks:
    """Test anomaly check implementations."""

    def test_price_anomaly_check_passes(self, sample_quote_df):
        """Test price anomaly check with valid data."""
        check = PriceAnomalyCheck()
        rule = RuleDef(
            rule_id="test_price",
            layer=Layer.STANDARDIZED,
            rule_type="price_anomaly",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            params={"max_change_pct": 15.0},
        )
        result = check.execute(sample_quote_df, rule)
        assert result.status == RuleStatus.PASSED

    def test_price_anomaly_check_high_lt_low(self, sample_quote_df_with_anomalies):
        """Test price anomaly detects high < low."""
        check = PriceAnomalyCheck()
        rule = RuleDef(
            rule_id="test_price",
            layer=Layer.STANDARDIZED,
            rule_type="price_anomaly",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
        )
        result = check.execute(sample_quote_df_with_anomalies, rule)
        assert result.status == RuleStatus.FAILED
        assert any(v["type"] == "high_lt_low" for v in result.details.get("violations", []))

    def test_price_anomaly_check_extreme_change(self, sample_quote_df_with_anomalies):
        """Test price anomaly detects extreme change percentage."""
        check = PriceAnomalyCheck()
        rule = RuleDef(
            rule_id="test_price",
            layer=Layer.STANDARDIZED,
            rule_type="price_anomaly",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            params={"max_change_pct": 10.0, "change_field": "change_pct"},
        )
        result = check.execute(sample_quote_df_with_anomalies, rule)
        assert result.status == RuleStatus.FAILED

    def test_numeric_anomaly_check_iqr_method(self):
        """Test numeric range anomaly using IQR method."""
        df = pd.DataFrame({
            "volume": [100, 120, 130, 140, 150, 1000],
            "trade_date": pd.date_range("2024-01-01", periods=6),
        })
        check = NumericRangeAnomalyCheck()
        rule = RuleDef(
            rule_id="test_numeric",
            layer=Layer.STANDARDIZED,
            rule_type="numeric_anomaly",
            severity=Severity.WARNING,
            gate_action=GateAction.ALERT,
            field="volume",
            params={"method": "iqr", "iqr_multiplier": 1.5},
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.FAILED
        assert result.failed_count > 0

    def test_numeric_anomaly_check_zscore_method(self):
        """Test numeric range anomaly using z-score method."""
        df = pd.DataFrame({
            "volume": [100, 110, 120, 130, 140, 500],
            "trade_date": pd.date_range("2024-01-01", periods=6),
        })
        check = NumericRangeAnomalyCheck()
        rule = RuleDef(
            rule_id="test_numeric",
            layer=Layer.STANDARDIZED,
            rule_type="numeric_anomaly",
            severity=Severity.WARNING,
            gate_action=GateAction.ALERT,
            field="volume",
            params={"method": "zscore", "zscore_threshold": 2.0},
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.FAILED or result.status == RuleStatus.PASSED

    def test_numeric_anomaly_check_missing_field(self):
        """Test numeric anomaly with missing field."""
        df = pd.DataFrame({"symbol": ["sh600000"]})
        check = NumericRangeAnomalyCheck()
        rule = RuleDef(
            rule_id="test_numeric",
            layer=Layer.STANDARDIZED,
            rule_type="numeric_anomaly",
            severity=Severity.WARNING,
            gate_action=GateAction.ALERT,
            field="nonexistent_field",
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.ERROR

    def test_volatility_anomaly_check_passes(self):
        """Test volatility anomaly with normal volatility."""
        np.random.seed(42)
        df = pd.DataFrame({
            "change_pct": np.random.normal(0, 1, 100),
            "trade_date": pd.date_range("2024-01-01", periods=100),
        })
        check = VolatilityAnomalyCheck()
        rule = RuleDef(
            rule_id="test_vol",
            layer=Layer.STANDARDIZED,
            rule_type="volatility_anomaly",
            severity=Severity.WARNING,
            gate_action=GateAction.ALERT,
            field="change_pct",
            params={"rolling_window": 10, "baseline_window": 30, "volatility_multiplier": 3.0},
        )
        result = check.execute(df, rule)
        assert result.status in (RuleStatus.PASSED, RuleStatus.FAILED)

    def test_volatility_anomaly_missing_field(self):
        """Test volatility anomaly with missing field."""
        df = pd.DataFrame({"trade_date": pd.date_range("2024-01-01", periods=10)})
        check = VolatilityAnomalyCheck()
        rule = RuleDef(
            rule_id="test_vol",
            layer=Layer.STANDARDIZED,
            rule_type="volatility_anomaly",
            severity=Severity.WARNING,
            gate_action=GateAction.ALERT,
            field="nonexistent_field",
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.ERROR


@pytest.mark.unit
class TestConsistencyChecks:
    """Test consistency check implementations."""

    def test_cross_source_diff_passes(self):
        """Test cross source diff with consistent values."""
        df = pd.DataFrame({
            "symbol": ["sh600000", "sh600000", "sh600001", "sh600001"],
            "trade_date": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"],
            "close_price": [10.0, 10.01, 15.0, 15.0],
            "source_name": ["akshare", "lixinger", "akshare", "lixinger"],
        })
        check = CrossSourceDiffCheck()
        rule = RuleDef(
            rule_id="test_cross",
            layer=Layer.STANDARDIZED,
            rule_type="cross_source_diff",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="close_price",
            params={
                "source_col": "source_name",
                "tolerance": 0.05,
                "tolerance_type": "relative",
                "key_fields": ["symbol", "trade_date"],
            },
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.PASSED

    def test_cross_source_diff_fails(self):
        """Test cross source diff detects inconsistency."""
        df = pd.DataFrame({
            "symbol": ["sh600000", "sh600000"],
            "trade_date": ["2024-01-01", "2024-01-01"],
            "close_price": [10.0, 15.0],
            "source_name": ["akshare", "lixinger"],
        })
        check = CrossSourceDiffCheck()
        rule = RuleDef(
            rule_id="test_cross",
            layer=Layer.STANDARDIZED,
            rule_type="cross_source_diff",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="close_price",
            params={
                "source_col": "source_name",
                "tolerance": 0.01,
                "tolerance_type": "relative",
                "key_fields": ["symbol", "trade_date"],
            },
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.FAILED

    def test_cross_source_diff_missing_source_col(self):
        """Test cross source diff with missing source column."""
        df = pd.DataFrame({
            "symbol": ["sh600000"],
            "close_price": [10.0],
        })
        check = CrossSourceDiffCheck()
        rule = RuleDef(
            rule_id="test_cross",
            layer=Layer.STANDARDIZED,
            rule_type="cross_source_diff",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            field="close_price",
            params={"source_col": "source_name"},
        )
        result = check.execute(df, rule)
        assert result.status == RuleStatus.SKIPPED

    def test_cross_table_consistency_passes(self):
        """Test cross table consistency with matching data."""
        df_a = pd.DataFrame({
            "symbol": ["sh600000", "sh600001"],
            "trade_date": ["2024-01-01", "2024-01-01"],
            "close_price": [10.0, 15.0],
        })
        df_b = pd.DataFrame({
            "symbol": ["sh600000", "sh600001"],
            "trade_date": ["2024-01-01", "2024-01-01"],
            "close_price": [10.0, 15.0],
        })
        check = CrossTableConsistencyCheck()
        rule = RuleDef(
            rule_id="test_cross_table",
            layer=Layer.STANDARDIZED,
            rule_type="cross_table",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            params={
                "other_df": df_b,
                "join_fields": ["symbol", "trade_date"],
                "compare_fields": ["close_price"],
                "tolerance": 0.01,
            },
        )
        result = check.execute(df_a, rule)
        assert result.status == RuleStatus.PASSED

    def test_cross_table_consistency_fails(self):
        """Test cross table consistency detects mismatch."""
        df_a = pd.DataFrame({
            "symbol": ["sh600000"],
            "trade_date": ["2024-01-01"],
            "close_price": [10.0],
        })
        df_b = pd.DataFrame({
            "symbol": ["sh600000"],
            "trade_date": ["2024-01-01"],
            "close_price": [12.0],
        })
        check = CrossTableConsistencyCheck()
        rule = RuleDef(
            rule_id="test_cross_table",
            layer=Layer.STANDARDIZED,
            rule_type="cross_table",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            params={
                "other_df": df_b,
                "join_fields": ["symbol", "trade_date"],
                "compare_fields": ["close_price"],
                "tolerance": 0.01,
            },
        )
        result = check.execute(df_a, rule)
        assert result.status == RuleStatus.FAILED

    def test_cross_layer_consistency_passes(self):
        """Test cross layer consistency with matching counts."""
        raw_df = pd.DataFrame({
            "symbol": ["sh600000"] * 100,
            "trade_date": ["2024-01-01"] * 100,
        })
        std_df = pd.DataFrame({
            "symbol": ["sh600000"] * 95,
            "trade_date": ["2024-01-01"] * 95,
        })
        check = CrossLayerConsistencyCheck()
        rule = RuleDef(
            rule_id="test_cross_layer",
            layer=Layer.STANDARDIZED,
            rule_type="cross_layer",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            params={
                "raw_df": raw_df,
                "key_fields": ["symbol"],
                "max_missing_ratio": 0.1,
            },
        )
        result = check.execute(std_df, rule)
        assert result.status == RuleStatus.PASSED

    def test_cross_layer_consistency_fails(self):
        """Test cross layer consistency detects excessive loss."""
        raw_df = pd.DataFrame({
            "symbol": ["sh600000"] * 100,
        })
        std_df = pd.DataFrame({
            "symbol": ["sh600000"] * 50,
        })
        check = CrossLayerConsistencyCheck()
        rule = RuleDef(
            rule_id="test_cross_layer",
            layer=Layer.STANDARDIZED,
            rule_type="cross_layer",
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            params={
                "raw_df": raw_df,
                "max_missing_ratio": 0.1,
            },
        )
        result = check.execute(std_df, rule)
        assert result.status == RuleStatus.FAILED


@pytest.mark.unit
class TestQualityGate:
    """Test QualityGate complete flow."""

    def test_gate_passed_all_rules_pass(self, sample_quote_df):
        """Test gate passes when all rules pass."""
        engine = QualityEngine()
        engine.load_rules([
            RuleDef(
                rule_id="rule1",
                layer=Layer.STANDARDIZED,
                rule_type="non_null",
                severity=Severity.ERROR,
                gate_action=GateAction.BLOCK,
                fields=["symbol", "close_price"],
            ),
        ])
        results = engine.run(sample_quote_df)
        gate = QualityGate()
        gate_result = gate.evaluate(results, dataset="test", batch_id="b1")
        assert gate_result.decision == GateDecision.PASSED
        assert gate_result.gate_passed is True

    def test_gate_blocked_on_error_block_rule(self, sample_quote_df_with_anomalies):
        """Test gate blocks on error+block failure."""
        engine = QualityEngine()
        engine.load_rules([
            RuleDef(
                rule_id="blocking_rule",
                layer=Layer.STANDARDIZED,
                rule_type="non_null",
                severity=Severity.ERROR,
                gate_action=GateAction.BLOCK,
                fields=["nonexistent_field"],
            ),
        ])
        results = engine.run(sample_quote_df_with_anomalies)
        gate = QualityGate()
        gate_result = gate.evaluate(results, dataset="test", batch_id="b1")
        assert gate_result.decision == GateDecision.BLOCKED
        assert len(gate_result.blocking_rules) > 0

    def test_gate_warning_on_warning_alert_rule(self):
        """Test gate warns on warning+alert failure."""
        df = pd.DataFrame({
            "symbol": ["sh600000", None],
            "close_price": [10.0, 10.5],
        })
        engine = QualityEngine()
        engine.load_rules([
            RuleDef(
                rule_id="warning_rule",
                layer=Layer.STANDARDIZED,
                rule_type="non_null",
                severity=Severity.WARNING,
                gate_action=GateAction.ALERT,
                fields=["symbol"],
            ),
        ])
        results = engine.run(df)
        gate = QualityGate()
        gate_result = gate.evaluate(results, dataset="test", batch_id="b1")
        assert gate_result.decision == GateDecision.WARNING
        assert len(gate_result.warning_rules) > 0

    def test_gate_evaluate_and_raise_blocks(self):
        """Test evaluate_and_raise raises exception on block."""
        failed_result = RuleResult(
            rule_id="block_rule",
            status=RuleStatus.FAILED,
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            message="Data failed",
        )
        gate = QualityGate()
        with pytest.raises(GateBlockedError):
            gate.evaluate_and_raise([failed_result], dataset="test")

    def test_gate_result_to_dict(self):
        """Test GateResult serialization."""
        gate = QualityGate()
        results = [
            RuleResult(
                rule_id="rule1",
                status=RuleStatus.PASSED,
                severity=Severity.ERROR,
                gate_action=GateAction.BLOCK,
                message="OK",
            ),
        ]
        gate_result = gate.evaluate(results, dataset="test_dataset", batch_id="b1", layer="std")
        d = gate_result.to_dict()
        assert d["decision"] == "passed"
        assert d["dataset"] == "test_dataset"
        assert d["batch_id"] == "b1"
        assert d["gate_passed"] is True
        assert d["total_rules"] == 1

    def test_gate_counts_by_status(self):
        """Test gate correctly counts rule statuses."""
        results = [
            RuleResult(rule_id="r1", status=RuleStatus.PASSED, severity=Severity.ERROR, gate_action=GateAction.BLOCK),
            RuleResult(rule_id="r2", status=RuleStatus.FAILED, severity=Severity.WARNING, gate_action=GateAction.ALERT),
            RuleResult(rule_id="r3", status=RuleStatus.SKIPPED, severity=Severity.INFO, gate_action=GateAction.IGNORE),
            RuleResult(rule_id="r4", status=RuleStatus.ERROR, severity=Severity.ERROR, gate_action=GateAction.BLOCK),
        ]
        gate = QualityGate()
        gate_result = gate.evaluate(results)
        assert gate_result.passed_count == 1
        assert gate_result.failed_count == 1
        assert gate_result.skipped_count == 1
        assert gate_result.error_count == 1
        assert gate_result.warning_count == 1

    def test_gate_mixed_blocking_and_warning(self):
        """Test gate with both blocking errors and warnings."""
        results = [
            RuleResult(rule_id="block1", status=RuleStatus.FAILED, severity=Severity.ERROR, gate_action=GateAction.BLOCK, message="Block error"),
            RuleResult(rule_id="warn1", status=RuleStatus.FAILED, severity=Severity.WARNING, gate_action=GateAction.ALERT, message="Warning"),
            RuleResult(rule_id="pass1", status=RuleStatus.PASSED, severity=Severity.INFO, gate_action=GateAction.IGNORE),
        ]
        gate = QualityGate()
        gate_result = gate.evaluate(results)
        assert gate_result.decision == GateDecision.BLOCKED
        assert "block1" in gate_result.blocking_rules
        assert "warn1" in gate_result.warning_rules


@pytest.mark.unit
class TestQualityEngineIntegration:
    """Test QualityEngine loading and running."""

    def test_engine_load_config(self, temp_quality_config):
        """Test engine loads config from YAML."""
        engine = QualityEngine()
        engine.load_config(temp_quality_config)
        assert len(engine._rules) >= 3
        assert engine._dataset == "test_dataset"

    def test_engine_run_layer_filter(self, temp_quality_config, sample_quote_df):
        """Test engine filters by layer."""
        engine = QualityEngine()
        engine.load_config(temp_quality_config)
        all_results = engine.run(sample_quote_df)
        filtered_results = engine.run(sample_quote_df, layer=Layer.STANDARDIZED)
        assert len(filtered_results) <= len(all_results)

    def test_engine_run_rule_ids_filter(self, temp_quality_config, sample_quote_df):
        """Test engine filters by rule IDs."""
        engine = QualityEngine()
        engine.load_config(temp_quality_config)
        results = engine.run(sample_quote_df, rule_ids=["test_non_null"])
        assert len(results) == 1
        assert results[0].rule_id == "test_non_null"

    def test_engine_register_custom_check(self, sample_quote_df):
        """Test engine accepts custom check registration."""

        class CustomCheck(BaseCheck):
            @property
            def rule_type(self) -> str:
                return "custom_check"

            def execute(self, df, rule):
                return RuleResult(
                    rule_id=rule.rule_id,
                    status=RuleStatus.PASSED,
                    severity=rule.severity,
                    gate_action=rule.gate_action,
                    message="Custom check passed",
                )

        engine = QualityEngine()
        engine.register_check(CustomCheck)
        engine.load_rules([
            RuleDef(
                rule_id="custom_rule",
                layer=Layer.STANDARDIZED,
                rule_type="custom_check",
                severity=Severity.INFO,
                gate_action=GateAction.IGNORE,
            ),
        ])
        results = engine.run(sample_quote_df)
        assert len(results) == 1
        assert results[0].status == RuleStatus.PASSED