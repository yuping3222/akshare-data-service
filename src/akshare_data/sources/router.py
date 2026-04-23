"""Compatibility router exports for ``akshare_data.sources``.

Canonical router implementation lives in ``akshare_data.ingestion.router``.
This module re-exports the same symbols to avoid duplicated implementations.
"""

from akshare_data.ingestion.router import (
    DomainRateLimiter,
    EmptyDataPolicy,
    ExecutionResult,
    MultiSourceRouter,
    SourceHealthMonitor,
    create_router,
)

__all__ = [
    "DomainRateLimiter",
    "EmptyDataPolicy",
    "ExecutionResult",
    "MultiSourceRouter",
    "SourceHealthMonitor",
    "create_router",
]
