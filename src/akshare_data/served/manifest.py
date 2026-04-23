"""Release manifest model for Served (L2) layer.

A ReleaseManifest records which standardized batches were combined into a
single served release, which partitions are covered, and the quality-gate
decision that authorised the publish.

Storage layout:
    data/served/<dataset>/releases/<release_version>/data/*.parquet
    data/served/<dataset>/releases/<release_version>/_manifest.json
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

MANIFEST_VERSION = "1.0"
MANIFEST_FILENAME = "_manifest.json"


# ---------------------------------------------------------------------------
# GateDecision protocol
# ---------------------------------------------------------------------------


class GateDecision(Protocol):
    """Minimum interface the publisher requires from a quality-gate result.

    Implementations may come from the quality module or from an external
    system.  The publisher never runs quality checks itself — it only
    inspects the decision.
    """

    @property
    def dataset(self) -> str:
        """Standard dataset name, e.g. 'market_quote_daily'."""
        ...

    @property
    def batch_id(self) -> str:
        """The standardized batch that was evaluated."""
        ...

    @property
    def gate_passed(self) -> bool:
        """True when no error+block rules failed."""
        ...

    @property
    def evaluated_at(self) -> datetime:
        """When the gate decision was made."""
        ...

    @property
    def failed_rules(self) -> List[str]:
        """Rule IDs that failed with severity=error."""
        ...

    @property
    def warnings(self) -> List[str]:
        """Rule IDs that triggered warnings."""
        ...


# ---------------------------------------------------------------------------
# Release status
# ---------------------------------------------------------------------------


class ReleaseStatus(str, Enum):
    PUBLISHED = "published"
    ROLLED_BACK = "rolled_back"
    SUPERSEDED = "superseded"


# ---------------------------------------------------------------------------
# Source batch summary
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceBatch:
    """Summary of one standardized batch included in a release."""

    batch_id: str
    source_name: str
    interface_name: str
    record_count: int
    partition_values: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceBatch":
        return cls(**data)


# ---------------------------------------------------------------------------
# ReleaseManifest
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReleaseManifest:
    """Describes a single served release.

    Answers:
    - Which batch(es) generated this release_version?
    - Which partitions are covered?
    - What was the gate decision?
    - How many records?
    """

    manifest_version: str
    dataset: str
    release_version: str
    source_batches: List[SourceBatch]
    partitions_covered: List[str]
    total_record_count: int
    published_at: str
    schema_version: str
    normalize_version: str
    status: str = ReleaseStatus.PUBLISHED.value
    gate_passed: bool = True
    gate_failed_rules: List[str] = field(default_factory=list)
    gate_warnings: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    rolled_back_at: Optional[str] = None
    rollback_reason: Optional[str] = None

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        release_version: str,
        source_batches: List[SourceBatch],
        partitions_covered: List[str],
        total_record_count: int,
        schema_version: str = "v1",
        normalize_version: str = "v1",
        gate_decision: Optional[GateDecision] = None,
        published_at: Optional[datetime] = None,
        files: Optional[List[str]] = None,
    ) -> "ReleaseManifest":
        gate_passed = True
        failed_rules: List[str] = []
        warnings: List[str] = []

        if gate_decision is not None:
            gate_passed = gate_decision.gate_passed
            failed_rules = list(gate_decision.failed_rules)
            warnings = list(gate_decision.warnings)

        return cls(
            manifest_version=MANIFEST_VERSION,
            dataset=dataset,
            release_version=release_version,
            source_batches=source_batches,
            partitions_covered=sorted(partitions_covered),
            total_record_count=total_record_count,
            published_at=(published_at or datetime.now(timezone.utc)).isoformat(),
            schema_version=schema_version,
            normalize_version=normalize_version,
            gate_passed=gate_passed,
            gate_failed_rules=failed_rules,
            gate_warnings=warnings,
            files=files or [],
        )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["source_batches"] = [sb.to_dict() for sb in self.source_batches]
        return {k: v for k, v in d.items() if v is not None}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReleaseManifest":
        batches = [SourceBatch.from_dict(sb) for sb in data.get("source_batches", [])]
        return cls(
            manifest_version=data.get("manifest_version", MANIFEST_VERSION),
            dataset=data["dataset"],
            release_version=data["release_version"],
            source_batches=batches,
            partitions_covered=data.get("partitions_covered", []),
            total_record_count=data["total_record_count"],
            published_at=data["published_at"],
            schema_version=data.get("schema_version", "v1"),
            normalize_version=data.get("normalize_version", "v1"),
            status=data.get("status", ReleaseStatus.PUBLISHED.value),
            gate_passed=data.get("gate_passed", True),
            gate_failed_rules=data.get("gate_failed_rules", []),
            gate_warnings=data.get("gate_warnings", []),
            files=data.get("files", []),
            rolled_back_at=data.get("rolled_back_at"),
            rollback_reason=data.get("rollback_reason"),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReleaseManifest":
        return cls.from_dict(json.loads(text))

    @classmethod
    def load(cls, path: Path) -> "ReleaseManifest":
        with open(path, encoding="utf-8") as fh:
            return cls.from_json(fh.read())

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_json())

    def mark_rolled_back(self, reason: str = "") -> "ReleaseManifest":
        now = datetime.now(timezone.utc).isoformat()
        return type(self)(
            manifest_version=self.manifest_version,
            dataset=self.dataset,
            release_version=self.release_version,
            source_batches=self.source_batches,
            partitions_covered=self.partitions_covered,
            total_record_count=self.total_record_count,
            published_at=self.published_at,
            schema_version=self.schema_version,
            normalize_version=self.normalize_version,
            status=ReleaseStatus.ROLLED_BACK.value,
            gate_passed=self.gate_passed,
            gate_failed_rules=self.gate_failed_rules,
            gate_warnings=self.gate_warnings,
            files=self.files,
            rolled_back_at=now,
            rollback_reason=reason,
        )
