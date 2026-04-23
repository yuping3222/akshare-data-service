"""Rule-based scoring: computes quality scores from rule results and weights.

Replaces hardcoded scoring logic. Scores are derived from:
- Rule severity weights (error > warning > info)
- Rule pass/fail status
- Configurable per-rule weights
- Coverage ratio (passed / total applicable rules)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from akshare_data.quality.engine import RuleResult, RuleStatus, Severity

logger = logging.getLogger(__name__)

DEFAULT_SEVERITY_WEIGHTS = {
    Severity.ERROR: 3.0,
    Severity.WARNING: 1.5,
    Severity.INFO: 0.5,
}


class RuleBasedScorer:
    """Compute quality scores from rule results using weighted aggregation.

    Score formula:
        score = sum(weight_i * pass_i) / sum(weight_i) * 100

    Where:
    - weight_i = severity_weight * rule_weight (default rule_weight = 1.0)
    - pass_i = 1 if rule passed, 0 if failed
    - Skipped rules are excluded from both numerator and denominator
    - Error-status rules (execution errors) count as failed with 0 contribution
    """

    def __init__(
        self,
        severity_weights: Optional[Dict[Severity, float]] = None,
        rule_weights: Optional[Dict[str, float]] = None,
    ) -> None:
        self._severity_weights = severity_weights or DEFAULT_SEVERITY_WEIGHTS
        self._rule_weights = rule_weights or {}

    def compute_score(self, results: List[RuleResult]) -> float:
        """Compute a 0-100 quality score from rule results.

        Args:
            results: List of RuleResult from QualityEngine.run()

        Returns:
            Score in range [0, 100]. Returns 100 if no applicable rules.
        """
        if not results:
            return 100.0

        weighted_pass_sum = 0.0
        weighted_total_sum = 0.0
        breakdown: Dict[str, Dict[str, Any]] = {}

        for r in results:
            if r.status == RuleStatus.SKIPPED:
                continue

            severity_weight = self._severity_weights.get(r.severity, 1.0)
            rule_weight = self._rule_weights.get(r.rule_id, 1.0)
            total_weight = severity_weight * rule_weight

            passed = r.status == RuleStatus.PASSED
            contribution = total_weight if passed else 0.0

            weighted_pass_sum += contribution
            weighted_total_sum += total_weight

            breakdown[r.rule_id] = {
                "status": r.status.value,
                "severity": r.severity.value,
                "severity_weight": severity_weight,
                "rule_weight": rule_weight,
                "total_weight": total_weight,
                "contribution": contribution,
                "passed": passed,
            }

        if weighted_total_sum == 0:
            return 100.0

        score = (weighted_pass_sum / weighted_total_sum) * 100
        return round(score, 2)

    def compute_score_with_breakdown(
        self, results: List[RuleResult]
    ) -> Tuple[float, Dict[str, Any]]:
        """Compute score and return detailed breakdown.

        Returns:
            Tuple of (score, breakdown_dict) where breakdown contains
            per-rule weights, contributions, and aggregate stats.
        """
        if not results:
            return 100.0, {"total_rules": 0, "applicable_rules": 0}

        weighted_pass_sum = 0.0
        weighted_total_sum = 0.0
        breakdown: Dict[str, Dict[str, Any]] = {}
        total_rules = len(results)
        applicable_rules = 0
        passed_rules = 0
        failed_rules = 0
        skipped_rules = 0

        for r in results:
            if r.status == RuleStatus.SKIPPED:
                skipped_rules += 1
                continue

            applicable_rules += 1
            severity_weight = self._severity_weights.get(r.severity, 1.0)
            rule_weight = self._rule_weights.get(r.rule_id, 1.0)
            total_weight = severity_weight * rule_weight

            passed = r.status == RuleStatus.PASSED
            if passed:
                passed_rules += 1
            else:
                failed_rules += 1

            contribution = total_weight if passed else 0.0
            weighted_pass_sum += contribution
            weighted_total_sum += total_weight

            breakdown[r.rule_id] = {
                "status": r.status.value,
                "severity": r.severity.value,
                "severity_weight": severity_weight,
                "rule_weight": rule_weight,
                "total_weight": round(total_weight, 2),
                "contribution": round(contribution, 2),
                "passed": passed,
                "failed_count": r.failed_count,
                "total_count": r.total_count,
            }

        score = (weighted_pass_sum / weighted_total_sum * 100) if weighted_total_sum > 0 else 100.0

        summary = {
            "total_rules": total_rules,
            "applicable_rules": applicable_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "skipped_rules": skipped_rules,
            "weighted_pass_sum": round(weighted_pass_sum, 2),
            "weighted_total_sum": round(weighted_total_sum, 2),
            "severity_weights": {k.value: v for k, v in self._severity_weights.items()},
            "rule_weights": self._rule_weights,
            "per_rule": breakdown,
        }

        return round(score, 2), summary
