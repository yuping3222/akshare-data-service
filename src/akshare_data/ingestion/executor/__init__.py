"""Executor abstractions for ingestion workloads."""

from akshare_data.ingestion.executor.base import (
    BaseTaskExecutor,
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    Executor,
    ExecutorContext,
    ExecutorStats,
    TaskExecutionResult,
)

__all__ = [
    "ExecutionContext",
    "ExecutionMode",
    "ExecutionResult",
    "Executor",
    "ExecutorStats",
    "BaseTaskExecutor",
    "ExecutorContext",
    "TaskExecutionResult",
]
