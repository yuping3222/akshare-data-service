"""Audit record for extract tasks.

Captures request parameters, timing, status, and error information for each
task execution. Used for observability, debugging, and compliance.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class AuditRecord:
    """Audit trail for a single task execution attempt.

    Attributes
    ----------
    audit_id : str
        Unique identifier for this audit record.
    task_id : str
        The task being audited.
    batch_id : str
        The batch the task belongs to.
    idempotency_key : str
        Stable task key for cross-reference.
    dataset : str
        Standard dataset name.
    source_name : str
        Source provider name.
    interface_name : str
        Source interface name.
    request_params : dict[str, Any]
        Exact parameters sent to the source adapter.
    request_time : datetime
        When the request was initiated.
    response_time : datetime | None
        When the response was received (``None`` if still running).
    duration_ms : float | None
        Elapsed time in milliseconds.
    status : str
        Final status of this attempt (e.g. ``succeeded``, ``failed``, ``partial``).
    records_fetched : int
        Number of data records returned by the source.
    error_code : str | None
        System error code if the attempt failed.
    error_message : str | None
        Human-readable error description.
    error_traceback : str | None
        Full traceback for debugging.
    metadata : dict[str, Any]
        Additional context (e.g. HTTP status, rate-limit headers).
    """

    audit_id: str
    task_id: str
    batch_id: str
    idempotency_key: str
    dataset: str
    source_name: str
    interface_name: str
    request_params: Dict[str, Any]
    request_time: datetime
    response_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    status: str = "running"
    records_fetched: int = 0
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def start(
        cls,
        *,
        task_id: str,
        batch_id: str,
        idempotency_key: str,
        dataset: str,
        source_name: str,
        interface_name: str,
        request_params: Dict[str, Any],
    ) -> AuditRecord:
        """Create an audit record at the start of an attempt."""
        return cls(
            audit_id=str(uuid.uuid4()),
            task_id=task_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            dataset=dataset,
            source_name=source_name,
            interface_name=interface_name,
            request_params=request_params,
            request_time=datetime.now(),
        )

    def complete(
        self,
        *,
        status: str,
        records_fetched: int = 0,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        """Finalize the audit record with response information."""
        now = datetime.now()
        duration = (now - self.request_time).total_seconds() * 1000
        return type(self)(
            audit_id=self.audit_id,
            task_id=self.task_id,
            batch_id=self.batch_id,
            idempotency_key=self.idempotency_key,
            dataset=self.dataset,
            source_name=self.source_name,
            interface_name=self.interface_name,
            request_params=self.request_params,
            request_time=self.request_time,
            response_time=now,
            duration_ms=round(duration, 2),
            status=status,
            records_fetched=records_fetched,
            error_code=error_code,
            error_message=error_message,
            error_traceback=error_traceback,
            metadata=metadata or self.metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "task_id": self.task_id,
            "batch_id": self.batch_id,
            "idempotency_key": self.idempotency_key,
            "dataset": self.dataset,
            "source_name": self.source_name,
            "interface_name": self.interface_name,
            "request_params": self.request_params,
            "request_time": self.request_time.isoformat(),
            "response_time": self.response_time.isoformat()
            if self.response_time
            else None,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "records_fetched": self.records_fetched,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "error_traceback": self.error_traceback,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditRecord:
        return cls(
            audit_id=data["audit_id"],
            task_id=data["task_id"],
            batch_id=data["batch_id"],
            idempotency_key=data["idempotency_key"],
            dataset=data["dataset"],
            source_name=data["source_name"],
            interface_name=data["interface_name"],
            request_params=data["request_params"],
            request_time=datetime.fromisoformat(data["request_time"]),
            response_time=(
                datetime.fromisoformat(data["response_time"])
                if data.get("response_time")
                else None
            ),
            duration_ms=data.get("duration_ms"),
            status=data.get("status", "running"),
            records_fetched=data.get("records_fetched", 0),
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            error_traceback=data.get("error_traceback"),
            metadata=data.get("metadata", {}),
        )
