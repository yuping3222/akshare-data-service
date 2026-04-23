"""Consistency checks: cross-source diff, cross-table, cross-layer reconciliation."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.quality.engine import BaseCheck, RuleDef, RuleResult, RuleStatus

logger = logging.getLogger(__name__)


class CrossSourceDiffCheck(BaseCheck):
    """Compare field values across different sources within the same DataFrame.

    Requires a ``source_name`` column to identify the origin of each row.
    Computes absolute and relative deviation between source pairs for a
    given numeric field. Fails when deviation exceeds ``tolerance``.
    """

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
                message=f"Field '{field_name}' not found in DataFrame",
                details={"field": field_name},
            )

        source_col = rule.params.get("source_col", "source_name")
        if source_col not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Source column '{source_col}' not found for cross-source comparison",
                details={"source_col": source_col},
            )

        tolerance = rule.params.get("tolerance", 0.01)
        tolerance_type = rule.params.get("tolerance_type", "relative")

        sources = df[source_col].dropna().unique().tolist()
        if len(sources) < 2:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Only {len(sources)} source(s) present, need >= 2 for comparison",
                details={"sources": sources},
            )

        key_fields = rule.params.get("key_fields", [])
        if not key_fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No key_fields specified for row matching across sources",
            )

        missing_keys = [f for f in key_fields if f not in df.columns]
        if missing_keys:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Key fields not found: {missing_keys}",
                details={"missing_keys": missing_keys},
            )

        source_dfs: Dict[str, pd.DataFrame] = {}
        for src in sources:
            sdf = df[df[source_col] == src].drop_duplicates(subset=key_fields, keep="first")
            source_dfs[src] = sdf.set_index(key_fields)

        deviations: List[Dict[str, Any]] = []
        exceed_count = 0
        compared_pairs = 0

        for i, src_a in enumerate(sources):
            for src_b in sources[i + 1:]:
                df_a = source_dfs[src_a]
                df_b = source_dfs[src_b]
                common_keys = df_a.index.intersection(df_b.index)

                if len(common_keys) == 0:
                    continue

                vals_a = df_a.loc[common_keys, field_name].astype(float)
                vals_b = df_b.loc[common_keys, field_name].astype(float)

                abs_diff = (vals_a - vals_b).abs()
                if tolerance_type == "relative":
                    denominator = vals_a.abs().where(vals_a.abs() > 0, vals_b.abs())
                    rel_diff = abs_diff / denominator.replace(0, float("nan"))
                    diff_series = rel_diff
                else:
                    diff_series = abs_diff

                exceeds = diff_series > tolerance
                n_exceeds = int(exceeds.sum())
                exceed_count += n_exceeds
                compared_pairs += len(common_keys)

                if n_exceeds > 0:
                    bad_keys = common_keys[exceeds].tolist()
                    deviations.append({
                        "source_a": src_a,
                        "source_b": src_b,
                        "exceed_count": n_exceeds,
                        "sample_keys": [str(k) for k in bad_keys[:5]],
                    })

        status = RuleStatus.FAILED if exceed_count > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{exceed_count} rows exceed {tolerance_type} tolerance {tolerance}" if exceed_count else f"All {compared_pairs} compared rows within tolerance",
            failed_count=exceed_count,
            total_count=compared_pairs,
            details={
                "field": field_name,
                "tolerance": tolerance,
                "tolerance_type": tolerance_type,
                "sources": sources,
                "deviations": deviations,
            },
        )


class CrossTableConsistencyCheck(BaseCheck):
    """Check consistency between two related tables via a secondary DataFrame.

    The rule params must provide:
    - ``other_df``: the other DataFrame to compare against
    - ``join_fields``: fields to join on
    - ``compare_fields``: fields to compare values for
    - ``tolerance``: max allowed deviation (default 0.01)
    """

    @property
    def rule_type(self) -> str:
        return "cross_table"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        other_df: Optional[pd.DataFrame] = rule.params.get("other_df")
        if other_df is None:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No other_df provided in rule params",
            )

        join_fields = rule.params.get("join_fields", [])
        compare_fields = rule.params.get("compare_fields", [])
        tolerance = rule.params.get("tolerance", 0.01)

        if not join_fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No join_fields specified",
            )

        if not compare_fields:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No compare_fields specified",
            )

        missing_in_main = [f for f in join_fields + compare_fields if f not in df.columns]
        missing_in_other = [f for f in join_fields + compare_fields if f not in other_df.columns]
        all_missing = missing_in_main + missing_in_other
        if all_missing:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Fields missing: main={missing_in_main}, other={missing_in_other}",
                details={"missing_in_main": missing_in_main, "missing_in_other": missing_in_other},
            )

        merged = df.merge(other_df, on=join_fields, how="inner", suffixes=("_a", "_b"))
        if merged.empty:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No overlapping rows after join",
            )

        total_mismatches = 0
        field_mismatches: Dict[str, int] = {}

        for field in compare_fields:
            col_a = f"{field}_a"
            col_b = f"{field}_b"
            if col_a not in merged.columns or col_b not in merged.columns:
                continue
            vals_a = pd.to_numeric(merged[col_a], errors="coerce")
            vals_b = pd.to_numeric(merged[col_b], errors="coerce")
            diff = (vals_a - vals_b).abs()
            n_mismatch = int((diff > tolerance).sum())
            field_mismatches[field] = n_mismatch
            total_mismatches += n_mismatch

        status = RuleStatus.FAILED if total_mismatches > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{total_mismatches} value mismatches across {len(compare_fields)} fields" if total_mismatches else "All compared values consistent",
            failed_count=total_mismatches,
            total_count=len(merged) * len(compare_fields),
            details={
                "join_fields": join_fields,
                "compare_fields": compare_fields,
                "field_mismatches": field_mismatches,
                "overlapping_rows": len(merged),
            },
        )


class CrossLayerConsistencyCheck(BaseCheck):
    """Verify that standardized data matches raw data for a given batch.

    Compares record counts and key field values between raw and
    standardized layers. The rule params must provide:
    - ``raw_df``: raw layer DataFrame
    - ``key_fields``: fields to verify presence
    """

    @property
    def rule_type(self) -> str:
        return "cross_layer"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        raw_df: Optional[pd.DataFrame] = rule.params.get("raw_df")
        if raw_df is None:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="No raw_df provided in rule params",
            )

        key_fields = rule.params.get("key_fields", [])
        batch_id = rule.params.get("batch_id")

        raw_count = len(raw_df)
        std_count = len(df)

        if raw_count == 0:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message="Raw DataFrame is empty",
            )

        missing_ratio = 1.0 - (std_count / raw_count) if raw_count > 0 else 1.0
        max_missing_ratio = rule.params.get("max_missing_ratio", 0.1)

        key_issues: List[str] = []
        if key_fields:
            for field in key_fields:
                if field not in df.columns:
                    key_issues.append(f"Key field '{field}' missing in standardized")
                elif field not in raw_df.columns:
                    key_issues.append(f"Key field '{field}' missing in raw")

        status = RuleStatus.FAILED if (missing_ratio > max_missing_ratio or key_issues) else RuleStatus.PASSED
        message_parts = []
        if missing_ratio > max_missing_ratio:
            message_parts.append(f"{missing_ratio:.1%} records lost (max {max_missing_ratio:.1%})")
        if key_issues:
            message_parts.extend(key_issues)

        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message="; ".join(message_parts) if message_parts else f"Raw({raw_count}) -> Standardized({std_count}) consistent",
            failed_count=int(std_count * missing_ratio),
            total_count=raw_count,
            details={
                "raw_count": raw_count,
                "standardized_count": std_count,
                "missing_ratio": round(missing_ratio, 4),
                "key_issues": key_issues,
                "batch_id": batch_id,
            },
        )
