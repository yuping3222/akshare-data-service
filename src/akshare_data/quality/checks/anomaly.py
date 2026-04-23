"""Anomaly checks: price anomalies, numeric range anomalies, volatility anomalies."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

from akshare_data.quality.engine import BaseCheck, RuleDef, RuleResult, RuleStatus

logger = logging.getLogger(__name__)


class PriceAnomalyCheck(BaseCheck):
    """Detect price-related anomalies in market data.

    Checks:
    - high_price >= low_price for every row
    - open_price, high_price, low_price, close_price all non-negative
    - price change percentage within reasonable bounds
    """

    @property
    def rule_type(self) -> str:
        return "price_anomaly"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        violations: List[Dict[str, Any]] = []

        high_col = rule.params.get("high_field", "high_price")
        low_col = rule.params.get("low_field", "low_price")
        open_col = rule.params.get("open_field", "open_price")
        close_col = rule.params.get("close_field", "close_price")
        change_col = rule.params.get("change_field", "change_pct")

        max_change_pct = rule.params.get("max_change_pct", 20.0)
        date_col = rule.params.get("date_field", "trade_date")

        price_fields = [high_col, low_col, open_col, close_col]
        available = {f: f in df.columns for f in price_fields}

        if high_col in df.columns and low_col in df.columns:
            high_vals = pd.to_numeric(df[high_col], errors="coerce")
            low_vals = pd.to_numeric(df[low_col], errors="coerce")
            invalid_mask = high_vals < low_vals
            n_invalid = int(invalid_mask.sum())
            if n_invalid > 0:
                bad_indices = invalid_mask[invalid_mask].index.tolist()[:10]
                for idx in bad_indices:
                    row_info = {
                        "type": "high_lt_low",
                        "high": float(df.loc[idx, high_col]),
                        "low": float(df.loc[idx, low_col]),
                    }
                    if date_col in df.columns:
                        row_info["date"] = str(df.loc[idx, date_col])
                    violations.append(row_info)

        for field_name in [open_col, high_col, low_col, close_col]:
            if field_name not in df.columns:
                continue
            if not available.get(field_name, False):
                continue
            vals = pd.to_numeric(df[field_name], errors="coerce")
            negative_mask = vals < 0
            n_negative = int(negative_mask.sum())
            if n_negative > 0:
                bad_indices = negative_mask[negative_mask].index.tolist()[:5]
                for idx in bad_indices:
                    row_info = {
                        "type": "negative_price",
                        "field": field_name,
                        "value": float(df.loc[idx, field_name]),
                    }
                    if date_col in df.columns:
                        row_info["date"] = str(df.loc[idx, date_col])
                    violations.append(row_info)

        if change_col in df.columns:
            change_vals = pd.to_numeric(df[change_col], errors="coerce")
            extreme_mask = change_vals.abs() > max_change_pct
            n_extreme = int(extreme_mask.sum())
            if n_extreme > 0:
                bad_indices = extreme_mask[extreme_mask].index.tolist()[:10]
                for idx in bad_indices:
                    row_info = {
                        "type": "extreme_change",
                        "change_pct": float(df.loc[idx, change_col]),
                        "threshold": max_change_pct,
                    }
                    if date_col in df.columns:
                        row_info["date"] = str(df.loc[idx, date_col])
                    violations.append(row_info)

        total_violations = len(violations)
        status = RuleStatus.FAILED if total_violations > 0 else RuleStatus.PASSED
        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{total_violations} price anomalies detected" if total_violations else "No price anomalies",
            failed_count=total_violations,
            total_count=len(df),
            details={
                "violations": violations[:20],
                "violation_types": list({v["type"] for v in violations}),
            },
        )


class NumericRangeAnomalyCheck(BaseCheck):
    """Detect numeric values outside statistically expected ranges.

    Uses IQR-based outlier detection or z-score method.
    """

    @property
    def rule_type(self) -> str:
        return "numeric_anomaly"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        field_name = rule.field
        if not field_name or field_name not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' not found",
                details={"field": field_name},
            )

        method = rule.params.get("method", "iqr")
        vals = pd.to_numeric(df[field_name], errors="coerce").dropna()

        if vals.empty:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' has no numeric values",
            )

        if method == "iqr":
            multiplier = rule.params.get("iqr_multiplier", 3.0)
            q1, q3 = vals.quantile(0.25), vals.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - multiplier * iqr
            upper = q3 + multiplier * iqr
            outlier_mask = (vals < lower) | (vals > upper)
        elif method == "zscore":
            threshold = rule.params.get("zscore_threshold", 3.0)
            mean, std = vals.mean(), vals.std()
            if std == 0:
                return RuleResult(
                    rule_id=rule.rule_id,
                    status=RuleStatus.PASSED,
                    severity=rule.severity,
                    gate_action=rule.gate_action,
                    message="Zero std dev, no outliers possible",
                    total_count=len(vals),
                )
            zscores = (vals - mean).abs() / std
            outlier_mask = zscores > threshold
            lower = mean - threshold * std
            upper = mean + threshold * std
        else:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Unknown method '{method}', use 'iqr' or 'zscore'",
            )

        n_outliers = int(outlier_mask.sum())
        status = RuleStatus.FAILED if n_outliers > 0 else RuleStatus.PASSED

        outlier_values = []
        if n_outliers > 0:
            outlier_indices = outlier_mask[outlier_mask].index.tolist()[:10]
            for idx in outlier_indices:
                outlier_values.append({
                    "index": idx,
                    "value": float(vals.loc[idx]),
                })

        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{n_outliers} outliers detected ({method}: [{lower:.2f}, {upper:.2f}])" if n_outliers else f"All values within {method} bounds",
            failed_count=n_outliers,
            total_count=len(vals),
            details={
                "field": field_name,
                "method": method,
                "lower_bound": round(float(lower), 4),
                "upper_bound": round(float(upper), 4),
                "outlier_samples": outlier_values,
            },
        )


class VolatilityAnomalyCheck(BaseCheck):
    """Detect abnormal volatility in time-series data.

    Compares rolling volatility against a historical baseline.
    Flags periods where volatility exceeds a multiple of the baseline.
    """

    @property
    def rule_type(self) -> str:
        return "volatility_anomaly"

    def execute(self, df: pd.DataFrame, rule: RuleDef) -> RuleResult:
        field_name = rule.field or rule.params.get("field", "change_pct")
        if field_name not in df.columns:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.ERROR,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"Field '{field_name}' not found",
                details={"field": field_name},
            )

        date_col = rule.params.get("date_field", "trade_date")
        window = rule.params.get("rolling_window", 20)
        baseline_window = rule.params.get("baseline_window", 60)
        multiplier = rule.params.get("volatility_multiplier", 2.5)

        vals = pd.to_numeric(df[field_name], errors="coerce")
        if vals.dropna().empty:
            return RuleResult(
                rule_id=rule.rule_id,
                status=RuleStatus.SKIPPED,
                severity=rule.severity,
                gate_action=rule.gate_action,
                message=f"No numeric values in '{field_name}'",
            )

        rolling_std = vals.rolling(window=window, min_periods=window).std()
        baseline_std = vals.rolling(window=baseline_window, min_periods=window).std()

        threshold = baseline_std * multiplier
        abnormal_mask = rolling_std > threshold
        n_abnormal = int(abnormal_mask.sum())

        status = RuleStatus.FAILED if n_abnormal > 0 else RuleStatus.PASSED

        abnormal_details: List[Dict[str, Any]] = []
        if n_abnormal > 0 and date_col in df.columns:
            abnormal_indices = abnormal_mask[abnormal_mask].index.tolist()[:10]
            for idx in abnormal_indices:
                detail = {
                    "volatility": float(rolling_std.loc[idx]) if pd.notna(rolling_std.loc[idx]) else None,
                    "threshold": float(threshold.loc[idx]) if pd.notna(threshold.loc[idx]) else None,
                }
                if date_col in df.columns:
                    detail["date"] = str(df.loc[idx, date_col])
                abnormal_details.append(detail)

        return RuleResult(
            rule_id=rule.rule_id,
            status=status,
            severity=rule.severity,
            gate_action=rule.gate_action,
            message=f"{n_abnormal} periods of abnormal volatility" if n_abnormal else "Volatility within normal range",
            failed_count=n_abnormal,
            total_count=len(vals),
            details={
                "field": field_name,
                "rolling_window": window,
                "baseline_window": baseline_window,
                "multiplier": multiplier,
                "abnormal_samples": abnormal_details,
            },
        )
