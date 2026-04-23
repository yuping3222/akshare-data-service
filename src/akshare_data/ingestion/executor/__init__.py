"""Executor abstractions for ingestion workloads."""

from akshare_data.ingestion.executor.base import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    Executor,
    ExecutorStats,
)

__all__ = [
    "ExecutionContext",
    "ExecutionMode",
    "ExecutionResult",
    "Executor",
    "ExecutorStats",
]
"""Ingestion task executors."""

from .base import BaseTaskExecutor, ExecutionResult, ExecutorContext

__all__ = ["BaseTaskExecutor", "ExecutionResult", "ExecutorContext"]
