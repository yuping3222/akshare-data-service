"""Batch context model.

A ``BatchContext`` groups multiple ``ExtractTask`` instances that are planned
or executed together. It carries batch-level metadata, status aggregation, and
timing information.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from .task import ExtractTask


class BatchStatus(str, Enum):
    """Lifecycle states for a batch of extract tasks."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class BatchContext:
    """Context for a batch of extract tasks.

    Attributes
    ----------
    batch_id : str
        Unique batch identifier. Format suggestion: ``YYYYMMDD_<seq>``.
    tasks : list[ExtractTask]
        Tasks belonging to this batch.
    status : BatchStatus
        Aggregate batch status.
    domain : str
        Logical domain shared by tasks in the batch.
    created_at : datetime
        Batch creation timestamp.
    updated_at : datetime
        Last status-change timestamp.
    started_at : datetime | None
        When the first task started running.
    finished_at : datetime | None
        When all tasks reached a terminal state.
    metadata : dict[str, Any]
        Free-form batch-level metadata (e.g. trigger source, schedule name).
    """

    batch_id: str
    tasks: List[ExtractTask] = field(default_factory=list)
    status: BatchStatus = BatchStatus.PENDING
    domain: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def new(
        cls,
        *,
        tasks: List[ExtractTask],
        domain: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BatchContext:
        """Create a batch with an auto-generated ID."""
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%d") + f"_{uuid.uuid4().hex[:6]}"
        if not domain and tasks:
            domain = tasks[0].domain
        return cls(
            batch_id=batch_id,
            tasks=tasks,
            domain=domain,
            metadata=metadata or {},
        )

    # ------------------------------------------------------------------
    # Status aggregation
    # ------------------------------------------------------------------

    def aggregate_status(self) -> BatchStatus:
        """Recompute batch status from constituent task statuses."""
        if not self.tasks:
            return self.status

        statuses = {t.status for t in self.tasks}

        if all(s.value == "succeeded" for s in statuses):
            return BatchStatus.SUCCEEDED
        if any(s.value == "running" for s in statuses):
            return BatchStatus.RUNNING
        if any(s.value == "retrying" for s in statuses):
            return BatchStatus.RUNNING
        if any(s.value == "pending" for s in statuses):
            return BatchStatus.PENDING
        if any(s.value in ("failed", "partial") for s in statuses):
            if any(s.value == "succeeded" for s in statuses):
                return BatchStatus.PARTIAL
            return BatchStatus.FAILED
        return self.status

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def with_status(self, status: BatchStatus) -> BatchContext:
        now = datetime.now(timezone.utc)
        changes: Dict[str, Any] = {"status": status, "updated_at": now}
        if status == BatchStatus.RUNNING and not self.started_at:
            changes["started_at"] = now
        if status in (
            BatchStatus.SUCCEEDED,
            BatchStatus.FAILED,
            BatchStatus.PARTIAL,
            BatchStatus.CANCELLED,
        ):
            if not self.finished_at:
                changes["finished_at"] = now
        return _dc_replace(self, **changes)

    def mark_running(self) -> BatchContext:
        return self.with_status(BatchStatus.RUNNING)

    def mark_succeeded(self) -> BatchContext:
        return self.with_status(BatchStatus.SUCCEEDED)

    def mark_failed(self) -> BatchContext:
        return self.with_status(BatchStatus.FAILED)

    def mark_partial(self) -> BatchContext:
        return self.with_status(BatchStatus.PARTIAL)

    def mark_cancelled(self) -> BatchContext:
        return self.with_status(BatchStatus.CANCELLED)

    # ------------------------------------------------------------------
    # Task access
    # ------------------------------------------------------------------

    def get_task_by_id(self, task_id: str) -> Optional[ExtractTask]:
        for t in self.tasks:
            if t.task_id == task_id:
                return t
        return None

    def get_tasks_by_status(self, status: str) -> List[ExtractTask]:
        return [t for t in self.tasks if t.status.value == status]

    def pending_tasks(self) -> List[ExtractTask]:
        return self.get_tasks_by_status("pending")

    def retryable_tasks(self) -> List[ExtractTask]:
        return [t for t in self.tasks if t.can_retry()]

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status.value,
            "domain": self.domain,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "task_count": len(self.tasks),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> BatchContext:
        return cls(
            batch_id=data["batch_id"],
            status=BatchStatus(data.get("status", "pending")),
            domain=data.get("domain", ""),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            started_at=(
                datetime.fromisoformat(data["started_at"])
                if data.get("started_at")
                else None
            ),
            finished_at=(
                datetime.fromisoformat(data["finished_at"])
                if data.get("finished_at")
                else None
            ),
            metadata=data.get("metadata", {}),
        )


def _dc_replace(instance: Any, **changes: Any) -> Any:
    return type(instance)(
        **{
            **{
                f.name: getattr(instance, f.name)
                for f in instance.__dataclass_fields__.values()
            },
            **changes,
        }
    )
