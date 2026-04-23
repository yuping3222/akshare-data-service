"""Checkpoint model for resumable extraction.

A checkpoint records how far a task has progressed so that a subsequent run
can resume from the last known position instead of starting over.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class Checkpoint:
    """Progress checkpoint for a single extract task.

    Attributes
    ----------
    task_id : str
        The task this checkpoint belongs to.
    idempotency_key : str
        Stable key for deduplication.
    dataset : str
        Standard dataset name.
    last_processed_date : date | None
        The latest business date successfully processed.
    last_processed_offset : str | None
        Opaque cursor / offset for APIs that support pagination or cursors.
    records_processed : int
        Total records processed so far.
    pages_completed : int
        Number of pages / batches completed.
    state : dict[str, Any]
        Arbitrary state carried across resume boundaries (e.g. partial
        responses, intermediate aggregates).
    created_at : datetime
        Checkpoint creation time.
    updated_at : datetime
        Last update time.
    """

    task_id: str
    idempotency_key: str
    dataset: str
    last_processed_date: Optional[date] = None
    last_processed_offset: Optional[str] = None
    records_processed: int = 0
    pages_completed: int = 0
    state: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def with_progress(
        self,
        *,
        last_processed_date: Optional[date] = None,
        last_processed_offset: Optional[str] = None,
        records_processed: int = 0,
        pages_completed: int = 0,
        state: Optional[Dict[str, Any]] = None,
    ) -> Checkpoint:
        """Return a new checkpoint with updated progress."""
        return type(self)(
            task_id=self.task_id,
            idempotency_key=self.idempotency_key,
            dataset=self.dataset,
            last_processed_date=last_processed_date or self.last_processed_date,
            last_processed_offset=last_processed_offset or self.last_processed_offset,
            records_processed=records_processed or self.records_processed,
            pages_completed=pages_completed or self.pages_completed,
            state=state if state is not None else self.state,
            created_at=self.created_at,
            updated_at=datetime.now(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "idempotency_key": self.idempotency_key,
            "dataset": self.dataset,
            "last_processed_date": self.last_processed_date.isoformat()
            if self.last_processed_date
            else None,
            "last_processed_offset": self.last_processed_offset,
            "records_processed": self.records_processed,
            "pages_completed": self.pages_completed,
            "state": self.state,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Checkpoint:
        return cls(
            task_id=data["task_id"],
            idempotency_key=data["idempotency_key"],
            dataset=data["dataset"],
            last_processed_date=(
                date.fromisoformat(data["last_processed_date"])
                if data.get("last_processed_date")
                else None
            ),
            last_processed_offset=data.get("last_processed_offset"),
            records_processed=data.get("records_processed", 0),
            pages_completed=data.get("pages_completed", 0),
            state=data.get("state", {}),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, raw: str) -> Checkpoint:
        return cls.from_dict(json.loads(raw))
