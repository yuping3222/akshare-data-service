"""Task state machine.

Defines the canonical lifecycle states for an ``ExtractTask`` and provides
a lightweight validator for legal state transitions.
"""

from __future__ import annotations

from enum import Enum
from typing import FrozenSet


class TaskStatus(str, Enum):
    """Lifecycle states for an extract task.

    State flow (simplified):

        pending -> running -> succeeded
                          -> failed   -> retrying -> running -> ...
                          -> partial  -> retrying -> running -> ...
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


# Legal transitions: from_status -> set of allowed next states.
_VALID_TRANSITIONS: dict[TaskStatus, FrozenSet[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.PARTIAL}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset({TaskStatus.RETRYING}),
    TaskStatus.PARTIAL: frozenset({TaskStatus.RETRYING}),
    TaskStatus.RETRYING: frozenset({TaskStatus.RUNNING, TaskStatus.FAILED}),
    TaskStatus.CANCELLED: frozenset(),
}


class InvalidTransitionError(Exception):
    """Raised when a state transition is not allowed."""

    def __init__(self, current: TaskStatus, target: TaskStatus) -> None:
        self.current = current
        self.target = target
        super().__init__(f"Invalid state transition: {current.value} -> {target.value}")


def validate_transition(current: TaskStatus, target: TaskStatus) -> None:
    """Validate a state transition, raising ``InvalidTransitionError`` on failure."""
    allowed = _VALID_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise InvalidTransitionError(current, target)


def is_terminal(status: TaskStatus) -> bool:
    """Return True if the status has no legal outgoing transitions."""
    return len(_VALID_TRANSITIONS.get(status, frozenset())) == 0


def is_retriable(status: TaskStatus) -> bool:
    """Return True if the status can transition to retrying."""
    return TaskStatus.RETRYING in _VALID_TRANSITIONS.get(status, frozenset())
