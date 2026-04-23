"""Quality gate: decides whether data can proceed to the next layer.

A gate evaluates RuleResults and produces a GateDecision.
Any ``error + block`` failure blocks the release.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field as dc_field
from enum import Enum
from typing import Dict, List, Optional

from akshare_data.quality.engine import (
    GateAction,
    QualityEngine,
    RuleResult,
    RuleStatus,
    Severity,
)

logger = logging.getLogger(__name__)


class GateDecision(str, Enum):
    PASSED = "passed"
    BLOCKED = "blocked"
    WARNING = "warning"


@dataclass
class GateResult:
    """Outcome of a gate evaluation."""

    decision: GateDecision
    dataset: str = ""
    batch_id: str = ""
    layer: str = ""
    total_rules: int = 0
    passed_count: int = 0
    failed_count: int = 0
    warning_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    blocking_rules: List[str] = dc_field(default_factory=list)
    warning_rules: List[str] = dc_field(default_factory=list)
    messages: List[str] = dc_field(default_factory=list)

    @property
    def gate_passed(self) -> bool:
        return self.decision == GateDecision.PASSED

    def to_dict(self) -> Dict:
        return {
            "decision": self.decision.value,
            "dataset": self.dataset,
            "batch_id": self.batch_id,
            "layer": self.layer,
            "gate_passed": self.gate_passed,
            "total_rules": self.total_rules,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "warning_count": self.warning_count,
            "skipped_count": self.skipped_count,
            "error_count": self.error_count,
            "blocking_rules": self.blocking_rules,
            "warning_rules": self.warning_rules,
            "messages": self.messages,
        }


class QualityGate:
    """Evaluates quality results and decides whether to block or allow.

    Rules:
    - Any ``severity=error`` + ``gate_action=block`` with ``status=failed`` => BLOCKED
    - Any ``severity=warning`` + ``gate_action=alert`` with ``status=failed`` => WARNING
    - Otherwise => PASSED
    """

    def __init__(self, engine: Optional[QualityEngine] = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> Optional[QualityEngine]:
        return self._engine

    @engine.setter
    def engine(self, value: QualityEngine) -> None:
        self._engine = value

    def evaluate(
        self,
        results: List[RuleResult],
        dataset: str = "",
        batch_id: str = "",
        layer: str = "",
    ) -> GateResult:
        """Evaluate a list of RuleResults and produce a GateResult."""
        blocking: List[str] = []
        warnings: List[str] = []
        messages: List[str] = []
        passed = 0
        failed = 0
        warning_cnt = 0
        skipped = 0
        error_cnt = 0

        for r in results:
            if r.status == RuleStatus.PASSED:
                passed += 1
            elif r.status == RuleStatus.FAILED:
                failed += 1
                if r.severity == Severity.ERROR and r.gate_action == GateAction.BLOCK:
                    blocking.append(r.rule_id)
                    messages.append(f"BLOCK: {r.rule_id} - {r.message}")
                elif r.severity == Severity.WARNING and r.gate_action == GateAction.ALERT:
                    warning_cnt += 1
                    warnings.append(r.rule_id)
                    messages.append(f"WARN: {r.rule_id} - {r.message}")
                else:
                    messages.append(f"FAIL (no gate action): {r.rule_id} - {r.message}")
            elif r.status == RuleStatus.SKIPPED:
                skipped += 1
            elif r.status == RuleStatus.ERROR:
                error_cnt += 1
                if r.gate_action == GateAction.BLOCK:
                    blocking.append(r.rule_id)
                    messages.append(f"BLOCK (error): {r.rule_id} - {r.message}")
                else:
                    messages.append(f"ERROR: {r.rule_id} - {r.message}")

        if blocking:
            decision = GateDecision.BLOCKED
        elif warnings:
            decision = GateDecision.WARNING
        else:
            decision = GateDecision.PASSED

        result = GateResult(
            decision=decision,
            dataset=dataset,
            batch_id=batch_id,
            layer=layer,
            total_rules=len(results),
            passed_count=passed,
            failed_count=failed,
            warning_count=warning_cnt,
            skipped_count=skipped,
            error_count=error_cnt,
            blocking_rules=blocking,
            warning_rules=warnings,
            messages=messages,
        )

        logger.info(
            "Gate decision=%s dataset=%s batch=%s blocking=%s warnings=%s",
            decision.value,
            dataset,
            batch_id,
            blocking,
            warnings,
        )
        return result

    def evaluate_and_raise(
        self,
        results: List[RuleResult],
        dataset: str = "",
        batch_id: str = "",
        layer: str = "",
    ) -> GateResult:
        """Evaluate and raise a GateBlockedError if blocked."""
        gate_result = self.evaluate(results, dataset, batch_id, layer)
        if gate_result.decision == GateDecision.BLOCKED:
            raise GateBlockedError(gate_result)
        return gate_result


class GateBlockedError(Exception):
    """Raised when the quality gate blocks the release."""

    def __init__(self, gate_result: GateResult) -> None:
        self.gate_result = gate_result
        rules = ", ".join(gate_result.blocking_rules)
        super().__init__(
            f"Quality gate BLOCKED for dataset={gate_result.dataset} "
            f"batch={gate_result.batch_id}: blocking rules: [{rules}]"
        )
