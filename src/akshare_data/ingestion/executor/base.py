"""统一执行器接口（T2-004）。

本模块定义 ingestion 层统一执行抽象，供下载、探测、回放等任务复用。
该接口强调：

1. 任务上下文（batch/request/source）可追踪
2. 执行结果结构化（状态、指标、错误）
3. 资源生命周期可控（open/close、上下文管理器）
"""Unified extraction executor contracts.

Task executors in online ingestion, offline downloader, and backfill/replay
should all conform to this contract so scheduling, audit and metrics can be
shared.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, Optional, TypeVar

TaskT = TypeVar("TaskT")
ResultT = TypeVar("ResultT")


class ExecutionMode(str, Enum):
    """执行模式。"""

    SYNC = "sync"
    ASYNC = "async"
    BATCH = "batch"


@dataclass(frozen=True)
class ExecutionContext:
    """单次执行上下文。"""

    request_id: str
    batch_id: str
    source: str
    dataset: str
    started_at: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutorStats:
    """执行统计信息。"""

    attempt: int = 1
    latency_ms: float = 0.0
    input_count: int = 0
    output_count: int = 0


@dataclass(frozen=True)
class ExecutionResult(Generic[ResultT]):
    """统一执行结果。"""

    ok: bool
    payload: Optional[ResultT] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    stats: ExecutorStats = field(default_factory=ExecutorStats)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def success(
        cls,
        payload: Optional[ResultT] = None,
        *,
        stats: Optional[ExecutorStats] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ExecutionResult[ResultT]":
        return cls(
            ok=True,
            payload=payload,
            stats=stats or ExecutorStats(),
            metadata=metadata or {},
        )

    @classmethod
    def failure(
        cls,
        *,
        error_code: str,
        error_message: str,
        stats: Optional[ExecutorStats] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ExecutionResult[ResultT]":
        return cls(
            ok=False,
            error_code=error_code,
            error_message=error_message,
            stats=stats or ExecutorStats(),
            metadata=metadata or {},
        )


class Executor(ABC, Generic[TaskT, ResultT]):
    """统一执行器抽象。

    子类实现 `execute`，负责把一个任务转换为结构化结果。

    为兼容历史调用，`context` 允许为空；推荐显式传入。
    """

    mode: ExecutionMode = ExecutionMode.SYNC

    def __enter__(self) -> "Executor[TaskT, ResultT]":
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
    ) -> ExecutionResult[ResultT]:
        """执行单任务并返回统一结果。"""

    def healthcheck(self) -> bool:
        """执行器健康检查，默认可用。"""
        return True
from datetime import datetime, timezone
from typing import Any, Dict, Generic, Mapping, MutableMapping, Optional, TypeVar


TaskT = TypeVar("TaskT")
PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True)
class ExecutorContext:
    """Execution context shared by all task runs."""

    batch_id: str = ""
    run_id: str = ""
    trigger: str = "manual"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionResult(Generic[PayloadT]):
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
    """Base contract for all extract task executors."""

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
        """Helper to build normalized results with timestamps."""
        start = started_at or datetime.now(timezone.utc)
        end = finished_at or datetime.now(timezone.utc)
        return ExecutionResult(
            success=success,
            task_name=task_name,
            rows=rows,
            payload=payload,
            error=error,
            started_at=start,
            finished_at=end,
            metadata=dict(metadata or {}),
        )


__all__ = [
    "ExecutorContext",
    "ExecutionResult",
    "BaseTaskExecutor",
]
