"""Ingestion module.

Responsible for data acquisition: scheduling, fetching, rate limiting,
circuit breaking, batching, auditing, checkpointing, and backfill.

This module does NOT serve data directly to end users.
"""

from .models import ExtractTask, TaskStatus, BatchContext, BatchStatus
from .task_state import (
    validate_transition,
    is_terminal,
    is_retriable,
    InvalidTransitionError,
)
from .idempotency import compute_idempotency_key
from .checkpoint import Checkpoint
from .audit import AuditRecord
from .base import DataSource
from .router import (
    MultiSourceRouter,
    DomainRateLimiter,
    SourceHealthMonitor,
    ExecutionResult,
    EmptyDataPolicy,
    create_simple_router,
)
from .rate_limiter import RateLimiter, RateRule
from .source_health import (
    SourceHealthTracker,
    HealthRecord,
    CircuitState,
    CircuitBreakerConfig,
)
from .scheduler import Scheduler, ScheduleDef, Priority, Frequency
from .backfill_request import BackfillRequest, BackfillRegistry, BackfillStatus
from .executor.base import (
    ExecutionContext,
    ExecutionMode,
    Executor,
    ExecutorStats,
)
from .executor import BaseTaskExecutor, ExecutionResult as TaskExecutionResult, ExecutorContext

__all__ = [
    "ExtractTask",
    "TaskStatus",
    "BatchContext",
    "BatchStatus",
    "validate_transition",
    "is_terminal",
    "is_retriable",
    "InvalidTransitionError",
    "compute_idempotency_key",
    "Checkpoint",
    "AuditRecord",
    "DataSource",
    "MultiSourceRouter",
    "DomainRateLimiter",
    "SourceHealthMonitor",
    "ExecutionResult",
    "EmptyDataPolicy",
    "create_simple_router",
    "RateLimiter",
    "RateRule",
    "SourceHealthTracker",
    "HealthRecord",
    "CircuitState",
    "CircuitBreakerConfig",
    "Scheduler",
    "ScheduleDef",
    "Priority",
    "Frequency",
    "BackfillRequest",
    "BackfillRegistry",
    "BackfillStatus",
    "ExecutionContext",
    "ExecutionMode",
    "Executor",
    "ExecutorStats",
    "BaseTaskExecutor",
    "TaskExecutionResult",
    "ExecutorContext",
]
