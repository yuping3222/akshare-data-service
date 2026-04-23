"""统一执行器接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Generic, Mapping, MutableMapping, Optional, TypeVar

TaskT = TypeVar("TaskT")
PayloadT = TypeVar("PayloadT")


class ExecutionMode(str, Enum):
    """执行模式。"""

    SYNC = "sync"
    ASYNC = "async"
    BATCH = "batch"


@dataclass(frozen=True)
class ExecutionContext:
    """统一执行上下文（新接口）。"""

    request_id: str
    batch_id: str
    source: str
    dataset: str
    started_at: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutorContext:
    """兼容旧接口的执行上下文。"""

    batch_id: str = ""
    run_id: str = ""
    trigger: str = "manual"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutorStats:
    """执行统计信息。"""

    attempt: int = 1
    latency_ms: float = 0.0
    input_count: int = 0
    output_count: int = 0


@dataclass(frozen=True)
class ExecutionResult(Generic[PayloadT]):
    """统一执行结果（兼容新旧两套调用语义）。"""

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

    @classmethod
    def success_result(
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
            stats=stats or ExecutorStats(),
            metadata=metadata or {},
            task_name=task_name,
            rows=rows,
            started_at=started_at or datetime.now(timezone.utc),
            finished_at=finished_at or datetime.now(timezone.utc),
        )

    @classmethod
    def failure_result(
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
    """新执行器抽象。"""

    mode: ExecutionMode = ExecutionMode.SYNC

    def __enter__(self) -> "Executor[TaskT, PayloadT]":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def open(self) -> None:
        """可选资源初始化钩子。"""

    def close(self) -> None:
        """可选资源释放钩子。"""

    @abstractmethod
    def execute(
        self,
        task: TaskT,
        *,
        context: ExecutionContext | None = None,
    ) -> Any:
        """执行单任务。"""

    def healthcheck(self) -> bool:
        return True


class BaseTaskExecutor(ABC, Generic[TaskT, PayloadT]):
    """旧执行器抽象。"""

    @abstractmethod
    def run(
        self,
        task: TaskT,
        *,
        context: Optional[ExecutorContext] = None,
    ) -> ExecutionResult[PayloadT]:
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
            task_name=task_name,
            rows=rows,
            metadata=dict(metadata or {}),
            started_at=start,
            finished_at=end,
        )


__all__ = [
    "ExecutionContext",
    "ExecutionMode",
    "ExecutionResult",
    "Executor",
    "ExecutorStats",
    "BaseTaskExecutor",
    "ExecutorContext",
]
