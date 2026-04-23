"""Structured quality report generation.

Produces machine-readable quality reports consumable by the Served publisher
and service layer.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from akshare_data.quality.engine import RuleResult, RuleStatus
from akshare_data.quality.gate import GateResult

logger = logging.getLogger(__name__)


@dataclass
class RuleReportEntry:
    """Single rule entry in the report."""

    rule_id: str
    status: str
    severity: str
    gate_action: str
    message: str
    failed_count: int = 0
    total_count: int = 0
    details: Dict[str, Any] = dc_field(default_factory=dict)


@dataclass
class QualityReport:
    """Complete quality report for a dataset batch."""

    dataset: str
    batch_id: str
    layer: str
    schema_version: str = ""
    report_time: str = ""
    gate_passed: bool = True
    decision: str = "passed"
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    warning_rules: int = 0
    skipped_rules: int = 0
    error_rules: int = 0
    rule_results: List[RuleReportEntry] = dc_field(default_factory=list)
    failed_rule_ids: List[str] = dc_field(default_factory=list)
    warning_rule_ids: List[str] = dc_field(default_factory=list)
    blocking_rule_ids: List[str] = dc_field(default_factory=list)
    messages: List[str] = dc_field(default_factory=list)
    metadata: Dict[str, Any] = dc_field(default_factory=dict)

    @classmethod
    def from_results(
        cls,
        dataset: str,
        batch_id: str,
        layer: str,
        rule_results: List[RuleResult],
        gate_result: Optional[GateResult] = None,
        schema_version: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "QualityReport":
        """Build a QualityReport from engine results and optional gate result."""
        now = datetime.now(timezone.utc).isoformat()

        entries: List[RuleReportEntry] = []
        failed_ids: List[str] = []
        warning_ids: List[str] = []
        passed_cnt = 0
        failed_cnt = 0
        warning_cnt = 0
        skipped_cnt = 0
        error_cnt = 0

        for r in rule_results:
            entry = RuleReportEntry(
                rule_id=r.rule_id,
                status=r.status.value,
                severity=r.severity.value,
                gate_action=r.gate_action.value,
                message=r.message,
                failed_count=r.failed_count,
                total_count=r.total_count,
                details=r.details,
            )
            entries.append(entry)

            if r.status == RuleStatus.PASSED:
                passed_cnt += 1
            elif r.status == RuleStatus.FAILED:
                failed_cnt += 1
                failed_ids.append(r.rule_id)
            elif r.status == RuleStatus.SKIPPED:
                skipped_cnt += 1
            elif r.status == RuleStatus.ERROR:
                error_cnt += 1

        gate_passed = True
        decision = "passed"
        blocking_ids: List[str] = []
        messages: List[str] = []

        if gate_result:
            gate_passed = gate_result.gate_passed
            decision = gate_result.decision.value
            blocking_ids = gate_result.blocking_rules
            warning_ids = gate_result.warning_rules
            messages = gate_result.messages

        return cls(
            dataset=dataset,
            batch_id=batch_id,
            layer=layer,
            schema_version=schema_version,
            report_time=now,
            gate_passed=gate_passed,
            decision=decision,
            total_rules=len(rule_results),
            passed_rules=passed_cnt,
            failed_rules=failed_cnt,
            warning_rules=warning_cnt,
            skipped_rules=skipped_cnt,
            error_rules=error_cnt,
            rule_results=entries,
            failed_rule_ids=failed_ids,
            warning_rule_ids=warning_ids,
            blocking_rule_ids=blocking_ids,
            messages=messages,
            metadata=metadata or {},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "batch_id": self.batch_id,
            "layer": self.layer,
            "schema_version": self.schema_version,
            "report_time": self.report_time,
            "gate_passed": self.gate_passed,
            "decision": self.decision,
            "total_rules": self.total_rules,
            "passed_rules": self.passed_rules,
            "failed_rules": self.failed_rules,
            "warning_rules": self.warning_rules,
            "skipped_rules": self.skipped_rules,
            "error_rules": self.error_rules,
            "rule_results": [asdict(e) for e in self.rule_results],
            "failed_rule_ids": self.failed_rule_ids,
            "warning_rule_ids": self.warning_rule_ids,
            "blocking_rule_ids": self.blocking_rule_ids,
            "messages": self.messages,
            "metadata": self.metadata,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def save(self, output_path: str | Path) -> None:
        """Save report as JSON file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json())
        logger.info("Quality report saved to %s", path)

    def is_blocking(self) -> bool:
        """Return True if any rule blocks the release."""
        return not self.gate_passed or len(self.blocking_rule_ids) > 0
