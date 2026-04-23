"""Unified executor contracts for ingestion workflows."""
"""统一执行器接口。"""
"""统一执行器接口（T2-004）。

本模块定义 ingestion 层统一执行抽象，供下载、探测、回放等任务复用。
该接口强调：

1. 任务上下文（batch/request/source）可追踪
2. 执行结果结构化（状态、指标、错误）
3. 资源生命周期可控（open/close、上下文管理器）
"""
"""Unified executor contracts for ingestion and offline workflows."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Generic, Mapping, MutableMapping, Optional, TypeVar

TaskT = TypeVar("TaskT")
ResultT = TypeVar("ResultT")
PayloadT = TypeVar("PayloadT")


class ExecutionMode(str, Enum):
    """Execution mode for unified executors."""
    """Execution mode."""

    SYNC = "sync"
    ASYNC = "async"
    BATCH = "batch"


@dataclass(frozen=True)
class ExecutionContext:
    """Context for structured unified execution."""
    """统一执行上下文（新接口）。"""
    """Context for structured executor interface."""

    request_id: str
    batch_id: str
    source: str
    dataset: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutorContext:
    """兼容旧接口的执行上下文。"""
    """Context for task-style executor interface."""

    batch_id: str = ""
    run_id: str = ""
    trigger: str = "manual"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutorStats:
    """Metrics for a unified execution run."""
    """Execution metrics."""

    attempt: int = 1
    latency_ms: float = 0.0
    input_count: int = 0
    output_count: int = 0


@dataclass(frozen=True)
class ExecutionResult(Generic[ResultT]):
    """Structured execution result used by modern executor APIs."""
class ExecutionResult(Generic[PayloadT]):
    """统一执行结果（兼容新旧两套调用语义）。"""
    """Unified result model, compatible with both new/legacy callers."""

    ok: bool
    payload: Optional[PayloadT] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    stats: ExecutorStats = field(default_factory=ExecutorStats)
    metadata: Dict[str, Any] = field(default_factory=dict)
    task_name: str = ""
    rows: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def success(self) -> bool:
        return self.ok

    @property
    def error(self) -> str:
        return self.error_message or self.error_code or ""

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.ok,
            "task": self.task_name,
            "rows": self.rows,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "metadata": dict(self.metadata),
        }

    @property
    def success(self) -> bool:
        return self.ok

    @property
    def error(self) -> str:
        return self.error_message or self.error_code or ""

    @property
    def rows(self) -> int:
        if self.stats.output_count:
            return int(self.stats.output_count)
        if self.payload is None:
            return 0
        if hasattr(self.payload, "__len__"):
            try:
                return int(len(self.payload))
            except TypeError:
                return 0
        return 1

    @property
    def task_name(self) -> str:
        return str(self.metadata.get("task", ""))

    @property
    def duration_ms(self) -> int:
        return int(self.stats.latency_ms)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "task": self.task_name,
            "rows": self.rows,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def create_success(
        cls,
        payload: Optional[PayloadT] = None,
        *,
        stats: Optional[ExecutorStats] = None,
        metadata: Optional[Dict[str, Any]] = None,
        task_name: str = "",
        rows: int = 0,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> "ExecutionResult[PayloadT]":
        return cls(
            ok=True,
            payload=payload,
            stats=stats or ExecutorStats(output_count=(len(payload) if hasattr(payload, "__len__") else 0) if payload is not None else 0),
            metadata=metadata or {},
            task_name=task_name,
            rows=rows,
            started_at=started_at or datetime.now(timezone.utc),
            finished_at=finished_at or datetime.now(timezone.utc),
        )

    @classmethod
    def create_failure(
        cls,
        *,
        error_code: str,
        error_message: str,
        stats: Optional[ExecutorStats] = None,
        metadata: Optional[Dict[str, Any]] = None,
        task_name: str = "",
        rows: int = 0,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> "ExecutionResult[PayloadT]":
        return cls(
            ok=False,
            error_code=error_code,
            error_message=error_message,
            stats=stats or ExecutorStats(),
            metadata=metadata or {},
            task_name=task_name,
            rows=rows,
            started_at=started_at or datetime.now(timezone.utc),
            finished_at=finished_at or datetime.now(timezone.utc),
        )

class Executor(ABC, Generic[TaskT, PayloadT]):
    """Structured executor abstraction."""

    mode: ExecutionMode = ExecutionMode.SYNC

    def __enter__(self) -> "Executor[TaskT, PayloadT]":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def open(self) -> None:
        """Optional resource setup hook."""

    def close(self) -> None:
        """Optional resource cleanup hook."""

    @abstractmethod
    def execute(
        self,
        task: TaskT,
        context: ExecutionContext | None = None,
    ) -> Any:
        """执行单任务。"""

    def healthcheck(self) -> bool:
        return True


class BaseTaskExecutor(ABC, Generic[TaskT, PayloadT]):
    """旧执行器抽象。"""
        """Execute one task."""

    def healthcheck(self) -> bool:
        return True
TaskT = TypeVar("TaskT")
PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True)
class ExecutorContext:
    """Context for legacy task executors."""

    batch_id: str = ""
    run_id: str = ""
    trigger: str = "manual"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskExecutionResult(Generic[PayloadT]):
    """Unified result model for extraction execution."""

    success: bool
    task_name: str
    rows: int = 0
    payload: Optional[PayloadT] = None
    error: str = ""
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "task": self.task_name,
            "rows": self.rows,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "metadata": dict(self.metadata),
        }


class BaseTaskExecutor(ABC, Generic[TaskT, PayloadT]):
    """Base contract for legacy task executors."""
    """Task-style executor abstraction for offline workflows."""

    @abstractmethod
    def run(
        self,
        task: TaskT,
        *,
        context: Optional[ExecutorContext] = None,
    ) -> TaskExecutionResult[PayloadT]:
        """Run a task and return a normalized legacy result."""
        """Run a task and return unified execution result."""

    def result(
        self,
        *,
        success: bool,
        task_name: str,
        rows: int = 0,
        payload: Optional[PayloadT] = None,
        error: str = "",
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        metadata: Optional[MutableMapping[str, Any]] = None,
    ) -> TaskExecutionResult[PayloadT]:
    ) -> ExecutionResult[PayloadT]:
        start = started_at or datetime.now(timezone.utc)
        end = finished_at or datetime.now(timezone.utc)
        if success:
            return ExecutionResult.success_result(
                payload=payload,
                task_name=task_name,
                rows=rows,
                metadata=dict(metadata or {}),
                started_at=start,
                finished_at=end,
            )
        return ExecutionResult.failure_result(
            error_code="task_failed",
            error_message=error,
    ) -> TaskExecutionResult[PayloadT]:
        """Helper to build normalized results with timestamps."""
        start = started_at or datetime.now(timezone.utc)
        end = finished_at or datetime.now(timezone.utc)
        return TaskExecutionResult(
            success=success,
            task_name=task_name,
            rows=rows,
            metadata=dict(metadata or {}),
            started_at=start,
            finished_at=end,
            metadata=dict(metadata or {}),
        stats = ExecutorStats(
            latency_ms=max(0.0, (end - start).total_seconds() * 1000),
            output_count=rows,
        )
        merged_metadata = {
            "task": task_name,
            "started_at": start.isoformat(),
            "finished_at": end.isoformat(),
            **dict(metadata or {}),
        }
        if success:
            return ExecutionResult.create_success(payload=payload, stats=stats, metadata=merged_metadata)
        return ExecutionResult.create_failure(
            error_code="task_failed",
            error_message=error,
            stats=stats,
            metadata=merged_metadata,
        )


__all__ = [
    "BaseTaskExecutor",
    "ExecutionContext",
    "ExecutionMode",
    "ExecutionResult",
    "Executor",
    "ExecutorContext",
    "ExecutorStats",
    "TaskExecutionResult",
    "ExecutorContext",
    "TaskExecutionResult",
    "ExecutionContext",
    "ExecutionMode",
    "ExecutionResult",
    "Executor",
    "ExecutorStats",
    "ExecutorContext",
    "BaseTaskExecutor",
    "ExecutorContext",
]
