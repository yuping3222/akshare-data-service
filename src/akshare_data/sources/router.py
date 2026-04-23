"""Compatibility router exports for ``akshare_data.sources``."""

from akshare_data.ingestion.router import (
    DomainRateLimiter,
    EmptyDataPolicy,
    ExecutionResult,
    MultiSourceRouter,
    SourceHealthMonitor,
    create_router,
    create_simple_router,
)


def create_router(*args, **kwargs):
    """Backward-compatible alias for ``create_simple_router``."""
    return create_simple_router(*args, **kwargs)

__all__ = [
    "DomainRateLimiter",
    "EmptyDataPolicy",
    "ExecutionResult",
    "MultiSourceRouter",
    "SourceHealthMonitor",
__all__ = [
    "EmptyDataPolicy",
    "ExecutionResult",
    "DomainRateLimiter",
    "SourceHealthMonitor",
    "MultiSourceRouter",
    "create_simple_router",
    "create_router",
    "create_simple_router",
]
