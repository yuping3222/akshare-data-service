"""Unified extraction task model.

Expresses a single extractable unit of work: one dataset, one source interface,
one set of parameters, and one planned extract_date.

This model is the stable input contract for:
- Raw writer
- Scheduler
- Backfill / replay utilities
- Audit recorder
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, replace, InitVar
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

from ..idempotency import compute_idempotency_key
from ..task_state import TaskStatus


@dataclass(frozen=True)
class ExtractTask:
    """A single extract task that can be scheduled, executed, retried, and audited.

    Attributes
    ----------
    task_id : str
        Unique identifier for this task instance.
    batch_id : str
        The batch this task belongs to. Groups tasks that run together.
    dataset : str
        Standard dataset name (e.g. ``market_quote_daily``, ``financial_indicator``).
        Must NOT be a legacy cache table name like ``stock_daily``.
    domain : str
        Logical domain (e.g. ``cn``, ``us``, ``system``).
    source_name : str
        Source provider name (e.g. ``akshare``, ``lixinger``, ``tushare``).
    interface_name : str
        Source-specific interface / function name (e.g. ``stock_zh_a_hist``).
    params : dict[str, Any]
        Request parameters passed to the source adapter.
    extract_date : date
        Planned extraction date; used as the Raw physical partition column.
    task_window : tuple[date | None, date | None]
        (start, end) date range the task intends to cover.
    status : TaskStatus
        Current lifecycle status. Defaults to ``pending``.
    retry_count : int
        Number of retry attempts already performed.
    max_retries : int
        Maximum allowed retries before the task is considered terminal.
    idempotency_key : str
        Stable, re-computable key derived from (dataset, source_name,
        interface_name, params, extract_date).
    priority : int
        Scheduling priority (lower = higher priority).
    created_at : datetime
        Task creation timestamp.
    updated_at : datetime
        Last status-change timestamp.
    metadata : dict[str, Any]
        Free-form metadata for downstream consumers.
    """

    task_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    batch_id: str = ""
    dataset: str = ""
    domain: str = ""
    source_name: str = ""
    interface_name: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    extract_date: date = field(default_factory=date.today)
    task_window: tuple[Optional[date], Optional[date]] = (None, None)
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    idempotency_key: str = ""
    priority: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Backward-compatibility: accept `request_params` as an alias for `params`.
    request_params: InitVar[Optional[Dict[str, Any]]] = None

    def __post_init__(self, request_params: Optional[Dict[str, Any]] = None) -> None:
        if request_params is not None and not self.params:
            object.__setattr__(self, "params", request_params)
        if not self.idempotency_key:
            object.__setattr__(
                self,
                "idempotency_key",
                compute_idempotency_key(
                    dataset=self.dataset,
                    source_name=self.source_name,
                    interface_name=self.interface_name,
                    params=self.params,
                    extract_date=self.extract_date,
                ),
            )

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def new(
        cls,
        *,
        batch_id: str,
        dataset: str,
        domain: str,
        source_name: str,
        interface_name: str,
        params: Dict[str, Any],
        extract_date: date,
        task_window: tuple[Optional[date], Optional[date]] = (None, None),
        max_retries: int = 3,
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ExtractTask:
        """Create a new task with an auto-generated UUID and idempotency key."""
        return cls(
            task_id=uuid.uuid4().hex[:12],
            batch_id=batch_id,
            dataset=dataset,
            domain=domain,
            source_name=source_name,
            interface_name=interface_name,
            params=params,
            extract_date=extract_date,
            task_window=task_window,
            max_retries=max_retries,
            priority=priority,
            metadata=metadata or {},
        )

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def with_status(self, status: TaskStatus) -> ExtractTask:
        """Return a new task with the given status and an updated timestamp."""
        return replace(self, status=status, updated_at=datetime.now(timezone.utc))

    def mark_running(self) -> ExtractTask:
        return self.with_status(TaskStatus.RUNNING)

    def mark_succeeded(self) -> ExtractTask:
        return self.with_status(TaskStatus.SUCCEEDED)

    def mark_failed(self) -> ExtractTask:
        return self.with_status(TaskStatus.FAILED)

    def mark_partial(self) -> ExtractTask:
        return self.with_status(TaskStatus.PARTIAL)

    def mark_retrying(self) -> ExtractTask:
        return replace(
            self,
            status=TaskStatus.RETRYING,
            retry_count=self.retry_count + 1,
            updated_at=datetime.now(timezone.utc),
        )

    def can_retry(self) -> bool:
        """Return True if the task has remaining retry attempts."""
        return self.retry_count < self.max_retries and self.status in (
            TaskStatus.FAILED,
            TaskStatus.PARTIAL,
        )

    def is_terminal(self) -> bool:
        """Return True if the task is in a final state."""
        return self.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_request_params_json(self) -> str:
        import json

        return json.dumps(self.params, ensure_ascii=False, default=str)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict."""
        window_start, window_end = self.task_window
        return {
            "task_id": self.task_id,
            "batch_id": self.batch_id,
            "dataset": self.dataset,
            "domain": self.domain,
            "source_name": self.source_name,
            "interface_name": self.interface_name,
            "params": self.params,
            "extract_date": self.extract_date.isoformat(),
            "task_window": {
                "start": window_start.isoformat() if window_start else None,
                "end": window_end.isoformat() if window_end else None,
            },
            "status": self.status.value,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "idempotency_key": self.idempotency_key,
            "priority": self.priority,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ExtractTask:
        """Deserialize from a plain dict."""
        tw = data.get("task_window", {})
        window_start = date.fromisoformat(tw["start"]) if tw.get("start") else None
        window_end = date.fromisoformat(tw["end"]) if tw.get("end") else None
        # Handle legacy `request_params` key
        params = data.get("params") or data.get("request_params", {})
        return cls(
            task_id=data["task_id"],
            batch_id=data["batch_id"],
            dataset=data["dataset"],
            domain=data["domain"],
            source_name=data["source_name"],
            interface_name=data["interface_name"],
            params=params,
            extract_date=date.fromisoformat(data["extract_date"]),
            task_window=(window_start, window_end),
            status=TaskStatus(data.get("status", "pending")),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            idempotency_key=data.get("idempotency_key", ""),
            priority=data.get("priority", 0),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            metadata=data.get("metadata", {}),
        )
