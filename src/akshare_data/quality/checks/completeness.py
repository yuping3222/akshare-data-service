"""Completeness checks: date continuity, primary key coverage, partition coverage."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

from akshare_data.quality.engine import BaseCheck, RuleDef, RuleResult, RuleStatus

logger = logging.getLogger(__name__)


class ContinuityCheck(BaseCheck):
    """Check time-series continuity: no unexpected gaps in date sequence.

    Supports trade calendar awareness via ``use_trade_calendar`` param.
    When enabled, gaps are measured against expected trading days,
    not calendar days.
    """

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
                message=f"Date field '{date_field}' not found in DataFrame",
                details={"date_field": date_field, "available_columns": list(df.columns)},
            )

        dates = pd.to_datetime(df[date_field]).dropna().sort_values().unique()
        if len(dates) < 2:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="Not enough dates to check continuity (need >= 2)",
                details={"date_count": len(dates)},
            )

        max_gap_days = rule.params.get("max_gap_days", 5)
        use_trade_calendar = rule.params.get("use_trade_calendar", False)
        trade_calendar: Optional[List[pd.Timestamp]] = rule.params.get("trade_calendar")

        if use_trade_calendar and trade_calendar:
            expected = set(pd.to_datetime(trade_calendar))
            actual = set(pd.to_datetime(dates))
            missing = sorted(expected & set(pd.date_range(dates[0], dates[-1])) - actual)
            gaps = len(missing)
        else:
            diffs = pd.Series(dates).diff().dt.days.dropna()
            large_gaps_mask = diffs > max_gap_days
            gaps = int(large_gaps_mask.sum())
            missing = []
            if gaps > 0:
                gap_indices = large_gaps_mask[large_gaps_mask].index.tolist()
                missing = [str(dates[i]) for i in gap_indices]

        status = RuleStatus.FAILED if gaps > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{gaps} gaps detected (max_gap={max_gap_days}d)" if gaps else f"No gaps > {max_gap_days} days",
            failed_count=gaps,
            total_count=len(dates) - 1,
            details={
                "date_field": date_field,
                "max_gap_days": max_gap_days,
                "date_range": [str(dates[0]), str(dates[-1])],
                "missing_dates": missing[:20] if missing else [],
                "use_trade_calendar": use_trade_calendar,
            },
        )


class PrimaryKeyCoverageCheck(BaseCheck):
    """Check that primary key fields have acceptable non-null coverage.

    Ensures each PK field has a minimum ratio of non-null values.
    Default threshold: 100% (all PK values must be present).
    """

    @property
    def rule_type(self) -> str:
        return "pk_coverage"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        pk_fields = rule.fields or rule.params.get("primary_key", [])
        if not pk_fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No primary key fields specified",
            )

        min_coverage = rule.params.get("min_coverage", 1.0)
        missing_fields = [f for f in pk_fields if f not in df.columns]
        if missing_fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"PK fields not found: {missing_fields}",
                details={"missing_fields": missing_fields},
            )

        total_rows = len(df)
        coverage_by_field: Dict[str, float] = {}
        failed_fields: List[str] = []

        for field in pk_fields:
            non_null = int(df[field].notna().sum())
            coverage = non_null / total_rows if total_rows > 0 else 0.0
            coverage_by_field[field] = round(coverage, 4)
            if coverage < min_coverage:
                failed_fields.append(field)

        status = RuleStatus.FAILED if failed_fields else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"PK coverage below {min_coverage:.0%} for: {failed_fields}" if failed_fields else f"All PK fields >= {min_coverage:.0%} coverage",
            failed_count=len(failed_fields),
            total_count=len(pk_fields),
            details={
                "coverage_by_field": coverage_by_field,
                "min_coverage": min_coverage,
                "failed_fields": failed_fields,
                "primary_key": pk_fields,
            },
        )


class PartitionCoverageCheck(BaseCheck):
    """Check that expected partitions are present in the data.

    Compares actual partition values against an expected set.
    Useful for verifying date range coverage or category coverage.
    """

    @property
    def rule_type(self) -> str:
        return "partition_coverage"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        partition_field = rule.field or rule.params.get("partition_field", "trade_date")
        if partition_field not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Partition field '{partition_field}' not found",
                details={"partition_field": partition_field},
            )

        expected_partitions = rule.params.get("expected_partitions")
        if not expected_partitions:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No expected_partitions specified",
            )

        actual_partitions = set(df[partition_field].dropna().unique().tolist())
        expected_set = set(expected_partitions)
        missing = sorted(expected_set - actual_partitions)
        extra = sorted(actual_partitions - expected_set)

        coverage = (len(expected_set) - len(missing)) / len(expected_set) if expected_set else 1.0
        min_coverage = rule.params.get("min_coverage", 1.0)
        status = RuleStatus.FAILED if coverage < min_coverage else RuleStatus.PASSED

        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"Partition coverage {coverage:.1%} (missing {len(missing)})" if coverage < 1.0 else "All expected partitions present",
            failed_count=len(missing),
            total_count=len(expected_set),
            details={
                "partition_field": partition_field,
                "coverage": round(coverage, 4),
                "missing_partitions": missing[:20],
                "extra_partitions": extra[:20],
                "expected_count": len(expected_set),
                "actual_count": len(actual_partitions),
            },
        )
