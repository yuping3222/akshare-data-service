"""Async backfill request model and registry.

Backfill requests are **explicit, asynchronous, and controlled** — they are
not synchronous service fallback. A caller (service layer, CLI, or external
scheduler) submits a ``BackfillRequest`` which is queued for later execution
by the ingestion pipeline.

Key design:
- Requests are immutable once submitted.
- A registry tracks request state and prevents duplicate submissions.
- Requests can be converted into ``ExtractTask`` / ``BatchContext`` by the
  scheduler when they are ready to execute.
"""

from __future__ import annotations

import json
import pathlib
import threading
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Request state
# ---------------------------------------------------------------------------


class BackfillStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL = "partial"


# ---------------------------------------------------------------------------
# BackfillRequest
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BackfillRequest:
    """An explicit, asynchronous backfill request.

    Attributes
    ----------
    request_id : str
        Unique request identifier.
    dataset : str
        Canonical dataset name (e.g. ``"market_quote_daily"``).
    domain : str
        Data domain (e.g. ``"cn"``).
    source_name : str
        Preferred source adapter.
    interface_name : str
        Source interface / function name.
    start_date : date
        Inclusive start of the backfill window.
    end_date : date
        Inclusive end of the backfill window.
    partitions : list[str]
        Optional partition keys (e.g. symbol list). If empty, backfill all.
    priority : str
        Priority level: ``p0``–``p3``.
    reason : str
        Why this backfill is needed (e.g. ``"data_quality_issue"``,
        ``"schema_fix"``, ``"manual"``).
    requested_by : str
        Caller identifier (service name, user, etc.).
    requested_at : datetime
        Submission timestamp.
    params : dict
        Extra parameters merged into each generated task.
    status : BackfillStatus
        Current lifecycle state.
    batch_id : str | None
        Assigned batch ID once the request is materialised into tasks.
    completed_at : datetime | None
        When the request reached a terminal state.
    error_message : str | None
        Failure description if the request failed.
    """

    request_id: str
    dataset: str
    domain: str
    source_name: str
    interface_name: str
    start_date: date
    end_date: date
    partitions: List[str] = field(default_factory=list)
    priority: str = "p2"
    reason: str = "manual"
    requested_by: str = "unknown"
    requested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    params: Dict[str, Any] = field(default_factory=dict)
    status: BackfillStatus = BackfillStatus.PENDING
    batch_id: Optional[str] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    # -- factory ---------------------------------------------------------

    @classmethod
    def new(
        cls,
        *,
        dataset: str,
        domain: str,
        source_name: str,
        interface_name: str,
        start_date: date,
        end_date: date,
        partitions: Optional[List[str]] = None,
        priority: str = "p2",
        reason: str = "manual",
        requested_by: str = "unknown",
        params: Optional[Dict[str, Any]] = None,
    ) -> "BackfillRequest":
        return cls(
            request_id=uuid.uuid4().hex[:16],
            dataset=dataset,
            domain=domain,
            source_name=source_name,
            interface_name=interface_name,
            start_date=start_date,
            end_date=end_date,
            partitions=partitions or [],
            priority=priority,
            reason=reason,
            requested_by=requested_by,
            params=params or {},
        )

    # -- transitions -----------------------------------------------------

    def with_status(self, status: BackfillStatus) -> "BackfillRequest":
        changes: Dict[str, Any] = {"status": status}
        if status in (
            BackfillStatus.SUCCEEDED,
            BackfillStatus.FAILED,
            BackfillStatus.CANCELLED,
            BackfillStatus.PARTIAL,
        ):
            changes["completed_at"] = datetime.now(timezone.utc)
        return _dc_replace(self, **changes)

    def with_batch_id(self, batch_id: str) -> "BackfillRequest":
        return _dc_replace(self, batch_id=batch_id, status=BackfillStatus.QUEUED)

    def with_error(self, message: str) -> "BackfillRequest":
        return _dc_replace(
            self,
            status=BackfillStatus.FAILED,
            error_message=message,
            completed_at=datetime.now(timezone.utc),
        )

    # -- serialization ---------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "dataset": self.dataset,
            "domain": self.domain,
            "source_name": self.source_name,
            "interface_name": self.interface_name,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "partitions": self.partitions,
            "priority": self.priority,
            "reason": self.reason,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat(),
            "params": self.params,
            "status": self.status.value,
            "batch_id": self.batch_id,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BackfillRequest":
        return cls(
            request_id=data["request_id"],
            dataset=data["dataset"],
            domain=data["domain"],
            source_name=data["source_name"],
            interface_name=data["interface_name"],
            start_date=date.fromisoformat(data["start_date"]),
            end_date=date.fromisoformat(data["end_date"]),
            partitions=data.get("partitions", []),
            priority=data.get("priority", "p2"),
            reason=data.get("reason", "manual"),
            requested_by=data.get("requested_by", "unknown"),
            requested_at=(
                datetime.fromisoformat(data["requested_at"])
                if data.get("requested_at")
                else datetime.now(timezone.utc)
            ),
            params=data.get("params", {}),
            status=BackfillStatus(data.get("status", "pending")),
            batch_id=data.get("batch_id"),
            completed_at=(
                datetime.fromisoformat(data["completed_at"])
                if data.get("completed_at")
                else None
            ),
            error_message=data.get("error_message"),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, raw: str) -> "BackfillRequest":
        return cls.from_dict(json.loads(raw))

    # -- idempotency key -------------------------------------------------

    def idempotency_key(self) -> str:
        """Stable key for deduplication."""
        import hashlib

        canon = json.dumps(
            {
                "dataset": self.dataset,
                "domain": self.domain,
                "source_name": self.source_name,
                "interface_name": self.interface_name,
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
                "partitions": sorted(self.partitions),
            },
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# BackfillRegistry
# ---------------------------------------------------------------------------


class BackfillRegistry:
    """Thread-safe registry for backfill requests.

    Prevents duplicate submissions via idempotency keys and provides
    query access by status, dataset, and date range.

    Usage::

        registry = BackfillRegistry()
        req = BackfillRequest.new(...)
        registry.submit(req)
        pending = registry.get_pending()
    """

    def __init__(self, persist_path: str | None = None) -> None:
        self._requests: Dict[str, BackfillRequest] = {}
        self._idempotency_keys: Dict[str, str] = {}  # key -> request_id
        self._lock = threading.Lock()
        self._persist_path = persist_path
        if persist_path:
            self.load()

    def save(self) -> None:
        """Persist pending/queued requests to YAML file.

        Only active (pending/queued) requests are saved; terminal states are
        not needed after a process restart.
        """
        if not self._persist_path:
            return
        with self._lock:
            active = [
                r
                for r in self._requests.values()
                if r.status in (BackfillStatus.PENDING, BackfillStatus.QUEUED)
            ]
        data: Dict[str, Any] = {
            "version": "1.0",
            "requests": [r.to_dict() for r in active],
        }
        path = pathlib.Path(self._persist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            yaml.dump(data, fh, allow_unicode=True, default_flow_style=False)

    def load(self) -> None:
        """Restore pending/queued requests from YAML file (process restart recovery)."""
        if not self._persist_path:
            return
        path = pathlib.Path(self._persist_path)
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not data or "requests" not in data:
            return
        with self._lock:
            for record in data["requests"]:
                try:
                    req = BackfillRequest.from_dict(record)
                    idem_key = req.idempotency_key()
                    if idem_key not in self._idempotency_keys:
                        self._requests[req.request_id] = req
                        self._idempotency_keys[idem_key] = req.request_id
                except (KeyError, ValueError):
                    pass

    def submit(self, request: BackfillRequest) -> BackfillRequest:
        """Submit a new backfill request.

        Returns the existing request if a duplicate idempotency key is found.
        """
        idem_key = request.idempotency_key()
        with self._lock:
            existing_id = self._idempotency_keys.get(idem_key)
            if existing_id and existing_id in self._requests:
                return self._requests[existing_id]

            self._requests[request.request_id] = request
            self._idempotency_keys[idem_key] = request.request_id
            result = request
        self.save()
        return result

    def get(self, request_id: str) -> Optional[BackfillRequest]:
        with self._lock:
            return self._requests.get(request_id)

    def update_status(
        self, request_id: str, status: BackfillStatus
    ) -> Optional[BackfillRequest]:
        with self._lock:
            req = self._requests.get(request_id)
            if req is None:
                return None
            updated = req.with_status(status)
            self._requests[request_id] = updated
            return updated

    def assign_batch(self, request_id: str, batch_id: str) -> Optional[BackfillRequest]:
        with self._lock:
            req = self._requests.get(request_id)
            if req is None:
                return None
            updated = req.with_batch_id(batch_id)
            self._requests[request_id] = updated
            return updated

    def get_pending(self) -> List[BackfillRequest]:
        with self._lock:
            return [
                r for r in self._requests.values() if r.status == BackfillStatus.PENDING
            ]

    def get_by_status(self, status: BackfillStatus) -> List[BackfillRequest]:
        with self._lock:
            return [r for r in self._requests.values() if r.status == status]

    def get_by_dataset(self, dataset: str) -> List[BackfillRequest]:
        with self._lock:
            return [r for r in self._requests.values() if r.dataset == dataset]

    def get_active(self) -> List[BackfillRequest]:
        with self._lock:
            return [
                r
                for r in self._requests.values()
                if r.status
                in (
                    BackfillStatus.PENDING,
                    BackfillStatus.QUEUED,
                    BackfillStatus.RUNNING,
                )
            ]

    def cancel(self, request_id: str) -> Optional[BackfillRequest]:
        with self._lock:
            req = self._requests.get(request_id)
            if req is None:
                return None
            if req.status in (BackfillStatus.PENDING, BackfillStatus.QUEUED):
                updated = req.with_status(BackfillStatus.CANCELLED)
                self._requests[request_id] = updated
                return updated
            return req

    def list_all(self) -> List[BackfillRequest]:
        with self._lock:
            return list(self._requests.values())

    def count(self) -> int:
        with self._lock:
            return len(self._requests)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_registry_instance: BackfillRegistry | None = None
_registry_lock = threading.Lock()


def get_backfill_registry(persist_path: str | None = None) -> BackfillRegistry:
    """Return the process-wide BackfillRegistry singleton.

    Args:
        persist_path: Optional path for persisting requests to YAML.
                     Only used on the first call (when creating the singleton).

    Returns:
        The global BackfillRegistry instance.
    """
    global _registry_instance
    if _registry_instance is None:
        with _registry_lock:
            if _registry_instance is None:
                _registry_instance = BackfillRegistry(persist_path=persist_path)
    return _registry_instance
