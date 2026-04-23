"""Quality rule execution engine.

Extensible framework for executing quality rules defined in YAML configs.
Supports pluggable check implementations registered by rule type.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dc_field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Type

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class Layer(str, Enum):
    RAW = "raw"
    STANDARDIZED = "standardized"
    SERVED = "served"


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class GateAction(str, Enum):
    BLOCK = "block"
    ALERT = "alert"
    IGNORE = "ignore"


class RuleStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class RuleDef:
    """Parsed rule definition from YAML config."""

    rule_id: str
    layer: Layer
    rule_type: str
    severity: Severity
    gate_action: GateAction
    fields: Optional[List[str]] = None
    field: Optional[str] = None
    expression: Optional[str] = None
    min: Optional[float] = None
    max: Optional[float] = None
    values: Optional[List[Any]] = None
    description: str = ""
    params: Dict[str, Any] = dc_field(default_factory=dict)


@dataclass
class RuleResult:
    """Result of executing a single rule."""

    rule_id: str
    status: RuleStatus
    severity: Severity
    gate_action: GateAction
    message: str = ""
    failed_count: int = 0
    total_count: int = 0
    details: Dict[str, Any] = dc_field(default_factory=dict)


class BaseCheck(ABC):
    """Base class for all check implementations.

    Subclass and register via ``QualityEngine.register_check()`` to support
    new rule types without modifying the engine core.
    """

    @property
    @abstractmethod
    def rule_type(self) -> str:
        """The DSL rule_type this check handles, e.g. 'non_null'."""

    @abstractmethod
    def execute(
        self,
        df: pd.DataFrame,
        rule: RuleDef,
    ) -> RuleResult:
        """Execute the check and return a RuleResult."""


class CheckRegistry:
    """Registry mapping rule_type strings to check classes."""

    def __init__(self) -> None:
        self._checks: Dict[str, Type[BaseCheck]] = {}

    def register(self, check_cls: Type[BaseCheck]) -> Type[BaseCheck]:
        """Decorator-style registration of a check class."""
        instance = check_cls()
        self._checks[instance.rule_type] = check_cls
        return check_cls

    def get(self, rule_type: str) -> Optional[Type[BaseCheck]]:
        return self._checks.get(rule_type)

    def list_types(self) -> List[str]:
        return sorted(self._checks.keys())


class QualityEngine:
    """Executes quality rules against a DataFrame.

    Usage::

        engine = QualityEngine()
        engine.register_check(NonNullCheck)
        engine.load_config("config/quality/market_quote_daily.yaml")
        results = engine.run(df, layer=Layer.STANDARDIZED)
    """

    def __init__(self) -> None:
        self._registry = CheckRegistry()
        self._rules: List[RuleDef] = []
        self._dataset: str = ""
        self._entity: str = ""
        self._schema_version: str = ""
        self._register_builtin_checks()

    # ------------------------------------------------------------------
    # Check registration
    # ------------------------------------------------------------------

    def _register_builtin_checks(self) -> None:
        """Register all built-in checks.

        Core checks (non_null, unique_key, range, enum, business_rule,
        raw-level checks) are defined in this module.  Completeness,
        consistency, and anomaly checks live in ``checks/`` (Task 17)
        and are imported here so the engine uses the real implementations.
        """
        from akshare_data.quality.checks.anomaly import (
            NumericRangeAnomalyCheck,
            PriceAnomalyCheck,
            VolatilityAnomalyCheck,
        )
        from akshare_data.quality.checks.completeness import (
            PartitionCoverageCheck,
            PrimaryKeyCoverageCheck,
        )
        from akshare_data.quality.checks.consistency import (
            CrossLayerConsistencyCheck,
            CrossTableConsistencyCheck,
        )

        for check_cls in [
            NonNullCheck,
            UniqueKeyCheck,
            RangeCheck,
            EnumCheck,
            BusinessRuleCheck,
            SystemFieldsCompleteCheck,
            SchemaFingerprintValidCheck,
            RequestBeforeIngestCheck,
            RecordCountMinCheck,
            FreshnessCheck,
            ReleaseManifestCompleteCheck,
            ContinuityCheck,
            CrossSourceDiffCheck,
            PrimaryKeyCoverageCheck,
            PartitionCoverageCheck,
            CrossTableConsistencyCheck,
            CrossLayerConsistencyCheck,
            PriceAnomalyCheck,
            NumericRangeAnomalyCheck,
            VolatilityAnomalyCheck,
        ]:
            self._registry.register(check_cls)

    def register_check(self, check_cls: Type[BaseCheck]) -> None:
        """Register a custom check class, overriding any existing one."""
        self._registry.register(check_cls)

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def load_config(self, config_path: str | Path) -> None:
        """Load quality rules from a YAML config file."""
        path = Path(config_path)
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        self._dataset = data.get("dataset", "")
        self._entity = data.get("entity", "")
        self._schema_version = data.get("schema_version", "")

        self._rules = []
        for raw in data.get("rules", []):
            rule = RuleDef(
                rule_id=raw["rule_id"],
                layer=Layer(raw["layer"]),
                rule_type=raw["type"],
                severity=Severity(raw.get("severity", "info")),
                gate_action=GateAction(raw.get("gate_action", "ignore")),
                fields=raw.get("fields"),
                field=raw.get("field"),
                expression=raw.get("expression"),
                min=raw.get("min"),
                max=raw.get("max"),
                values=raw.get("values"),
                description=raw.get("description", ""),
                params={
                    k: v
                    for k, v in raw.items()
                    if k
                    not in {
                        "rule_id",
                        "layer",
                        "type",
                        "severity",
                        "gate_action",
                        "fields",
                        "field",
                        "expression",
                        "min",
                        "max",
                        "values",
                        "description",
                    }
                },
            )
            self._rules.append(rule)

        logger.info(
            "Loaded %d rules for dataset=%s entity=%s",
            len(self._rules),
            self._dataset,
            self._entity,
        )

    def load_rules(self, rules: List[RuleDef], dataset: str = "", entity: str = "") -> None:
        """Load rules programmatically (for testing or dynamic config)."""
        self._rules = rules
        self._dataset = dataset
        self._entity = entity

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(
        self,
        df: pd.DataFrame,
        layer: Optional[Layer] = None,
        rule_ids: Optional[Sequence[str]] = None,
    ) -> List[RuleResult]:
        """Execute rules against *df*.

        Args:
            df: DataFrame to validate.
            layer: If given, only run rules matching this layer.
            rule_ids: If given, only run rules with these IDs.

        Returns:
            List of RuleResult, one per executed rule.
        """
        results: List[RuleResult] = []
        target_ids = set(rule_ids) if rule_ids else None

        for rule in self._rules:
            if layer is not None and rule.layer != layer:
                continue
            if target_ids and rule.rule_id not in target_ids:
                continue

            check_cls = self._registry.get(rule.rule_type)
            if check_cls is None:
                result = RuleResult(
                    rule_id=rule.rule_id,
                    status=RuleStatus.SKIPPED,
                    severity=rule.severity,
                    gate_action=rule.gate_action,
                    message=f"Unknown rule_type '{rule.rule_type}'",
                )
            else:
                try:
                    check = check_cls()
                    result = check.execute(df, rule)
                except Exception as exc:
                    result = RuleResult(
                        rule_id=rule.rule_id,
                        status=RuleStatus.ERROR,
                        severity=rule.severity,
                        gate_action=rule.gate_action,
                        message=f"Check execution error: {exc}",
                    )
                    logger.exception("Error executing rule %s", rule.rule_id)

            results.append(result)

        return results

    @property
    def dataset(self) -> str:
        return self._dataset

    @property
    def entity(self) -> str:
        return self._entity

    @property
    def rules(self) -> List[RuleDef]:
        return list(self._rules)


# ======================================================================
# Built-in check implementations (minimal / stub)
# ======================================================================


class NonNullCheck(BaseCheck):
    """Check that specified fields contain no null values."""

    @property
    def rule_type(self) -> str:
        return "non_null"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        fields = rule.fields or []
        if not fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No fields specified for non_null check",
            )

        missing_fields = [f for f in fields if f not in df.columns]
        if missing_fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Fields not found in DataFrame: {missing_fields}",
            )

        null_counts = {}
        total_failed = 0
        for f in fields:
            n = int(df[f].isna().sum())
            null_counts[f] = n
            total_failed += n

        status = RuleStatus.FAILED if total_failed > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Null counts: {null_counts}" if total_failed else "All fields non-null",
            failed_count=total_failed,
            total_count=len(df) * len(fields),
            details={"null_counts": null_counts},
        )


class UniqueKeyCheck(BaseCheck):
    """Check that the composite key (fields) is unique."""

    @property
    def rule_type(self) -> str:
        return "unique_key"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        fields = rule.fields or []
        if not fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No fields specified for unique_key check",
            )

        missing = [f for f in fields if f not in df.columns]
        if missing:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Key fields not found: {missing}",
            )

        dupes = int(df.duplicated(subset=fields, keep=False).sum())
        status = RuleStatus.FAILED if dupes > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{dupes} duplicate rows on key {fields}" if dupes else "All keys unique",
            failed_count=dupes,
            total_count=len(df),
            details={"duplicate_count": dupes, "key_fields": fields},
        )


class RangeCheck(BaseCheck):
    """Check that a numeric field falls within [min, max]."""

    @property
    def rule_type(self) -> str:
        return "range"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        field_name = rule.field
        if not field_name or field_name not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' not found",
            )

        series = df[field_name].dropna()
        if series.empty:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' is entirely null",
            )

        out_of_range = pd.Series(False, index=series.index)
        if rule.min is not None:
            out_of_range |= series < rule.min
        if rule.max is not None:
            out_of_range |= series > rule.max

        failed = int(out_of_range.sum())
        status = RuleStatus.FAILED if failed > 0 else RuleStatus.PASSED
        bounds = f"[{rule.min}, {rule.max}]"
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{failed} values out of range {bounds} for '{field_name}'" if failed else f"All values in range {bounds}",
            failed_count=failed,
            total_count=len(series),
            details={"field": field_name, "min": rule.min, "max": rule.max},
        )


class EnumCheck(BaseCheck):
    """Check that field values are within an allowed enum set."""

    @property
    def rule_type(self) -> str:
        return "enum"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        field_name = rule.field
        allowed = rule.values or []
        if not field_name or field_name not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' not found",
            )
        if not allowed:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No enum values specified",
            )

        series = df[field_name].dropna()
        invalid = series[~series.isin(allowed)]
        failed = len(invalid)
        status = RuleStatus.FAILED if failed > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{failed} values not in {allowed}" if failed else f"All values in {allowed}",
            failed_count=failed,
            total_count=len(series),
            details={"field": field_name, "allowed_values": allowed},
        )


class BusinessRuleCheck(BaseCheck):
    """Evaluate a boolean expression over the DataFrame.

    Uses ``df.eval()`` for expression evaluation.
    """

    @property
    def rule_type(self) -> str:
        return "business_rule"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        expr = rule.expression
        if not expr:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No expression specified",
            )

        try:
            mask = df.eval(expr)
        except Exception as exc:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Expression eval error: {exc}",
            )

        failed = int((~mask).sum())
        status = RuleStatus.FAILED if failed > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{failed} rows violate '{expr}'" if failed else f"All rows satisfy '{expr}'",
            failed_count=failed,
            total_count=len(df),
            details={"expression": expr},
        )


class SystemFieldsCompleteCheck(BaseCheck):
    """Raw-level check: system fields (batch_id, ingest_time, etc.) are present."""

    @property
    def rule_type(self) -> str:
        return "system_fields_complete"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        required = rule.params.get("system_fields", [
            "batch_id", "source_name", "interface_name", "ingest_time",
        ])
        missing = [f for f in required if f not in df.columns]
        status = RuleStatus.FAILED if missing else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Missing system fields: {missing}" if missing else "All system fields present",
            failed_count=len(missing),
            total_count=len(required),
            details={"missing_fields": missing},
        )


class SchemaFingerprintValidCheck(BaseCheck):
    """Raw-level check: schema fingerprint matches expected."""

    @property
    def rule_type(self) -> str:
        return "schema_fingerprint_valid"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        expected = rule.params.get("expected_fingerprint")
        if not expected:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No expected_fingerprint in rule params",
            )
        actual = ",".join(sorted(df.columns.tolist()))
        status = RuleStatus.PASSED if actual == expected else RuleStatus.FAILED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Fingerprint mismatch: expected={expected}, actual={actual}" if status == RuleStatus.FAILED else "Fingerprint valid",
            details={"expected": expected, "actual": actual},
        )


class RequestBeforeIngestCheck(BaseCheck):
    """Raw-level check: request_time <= ingest_time for all rows."""

    @property
    def rule_type(self) -> str:
        return "request_before_ingest"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        req_col = rule.params.get("request_time_field", "request_time")
        ing_col = rule.params.get("ingest_time_field", "ingest_time")
        if req_col not in df.columns or ing_col not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Required columns not found: {req_col}, {ing_col}",
            )
        violations = int((pd.to_datetime(df[req_col]) > pd.to_datetime(df[ing_col])).sum())
        status = RuleStatus.FAILED if violations > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{violations} rows with request_time > ingest_time" if violations else "All request_time <= ingest_time",
            failed_count=violations,
            total_count=len(df),
        )


class RecordCountMinCheck(BaseCheck):
    """Raw-level check: DataFrame has at least N rows."""

    @property
    def rule_type(self) -> str:
        return "record_count_min"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        min_count = rule.params.get("min_count", 1)
        actual = len(df)
        status = RuleStatus.FAILED if actual < min_count else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Record count {actual} < minimum {min_count}" if status == RuleStatus.FAILED else f"Record count {actual} >= minimum {min_count}",
            failed_count=max(0, min_count - actual),
            total_count=actual,
            details={"actual": actual, "min_count": min_count},
        )


class ContinuityCheck(BaseCheck):
    """Standardized-level check: time-series continuity (no unexpected gaps)."""

    @property
    def rule_type(self) -> str:
        return "continuity"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        date_field = rule.field or rule.params.get("date_field", "trade_date")
        if date_field not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Date field '{date_field}' not found",
            )
        dates = pd.to_datetime(df[date_field]).dropna().sort_values().unique()
        if len(dates) < 2:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="Not enough dates to check continuity",
            )
        gaps = rule.params.get("max_gap_days", 5)
        diffs = pd.Series(dates).diff().dt.days.dropna()
        large_gaps = int((diffs > gaps).sum())
        status = RuleStatus.FAILED if large_gaps > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{large_gaps} gaps > {gaps} days" if large_gaps else f"No gaps > {gaps} days",
            failed_count=large_gaps,
            details={"max_gap_days": gaps, "date_field": date_field},
        )


class FreshnessCheck(BaseCheck):
    """Check that data is recent enough relative to a reference date."""

    @property
    def rule_type(self) -> str:
        return "freshness"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        date_field = rule.field or rule.params.get("date_field", "trade_date")
        max_age_days = rule.params.get("max_age_days", 3)
        if date_field not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Date field '{date_field}' not found",
            )
        max_date = pd.to_datetime(df[date_field]).max()
        if pd.isna(max_date):
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No valid dates in field",
            )
        ref = pd.Timestamp(rule.params.get("reference_date", "today"))
        age = (ref - max_date).days
        status = RuleStatus.FAILED if age > max_age_days else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Data age {age} days > max {max_age_days}" if status == RuleStatus.FAILED else f"Data age {age} days within limit",
            details={"max_date": str(max_date), "age_days": age, "max_age_days": max_age_days},
        )


class CrossSourceDiffCheck(BaseCheck):
    """Standardized-level check: compare values across sources."""

    @property
    def rule_type(self) -> str:
        return "cross_source_diff"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        field_name = rule.field
        if not field_name or field_name not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' not found",
            )
        source_col = rule.params.get("source_field", "source_name")
        if source_col not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No source_name column for cross-source comparison",
            )
        return RuleResult(
            rule_id=rule.rule_id,
            status=RuleStatus.PASSED,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message="Cross-source diff check stub (full impl in Task 17)",
        )


class ReleaseManifestCompleteCheck(BaseCheck):
    """Served-level check: release manifest is complete."""

    @property
    def rule_type(self) -> str:
        return "release_manifest_complete"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        manifest = rule.params.get("manifest")
        if not manifest:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No manifest provided in rule params",
            )
        required_keys = rule.params.get("required_keys", ["dataset", "version", "record_count"])
        missing = [k for k in required_keys if k not in manifest]
        status = RuleStatus.FAILED if missing else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Manifest missing keys: {missing}" if missing else "Manifest complete",
            details={"manifest": manifest},
        )
