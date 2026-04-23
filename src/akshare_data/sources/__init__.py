"""Data source layer — backward compatibility shim.

This module re-exports from the new ``ingestion/`` layer for backward
compatibility.  New code should import directly from
``akshare_data.ingestion``.

The legacy adapter implementations (``akshare_source.py``,
``lixinger_source.py``, ``tushare_source.py``) are kept as-is to avoid
breaking existing callers.  They will gradually be replaced by the
ingestion-layer adapters.
"""

# -- New ingestion layer (canonical) ----------------------------------------
from akshare_data.ingestion.base import DataSource
from akshare_data.ingestion.router import (
    MultiSourceRouter,
    DomainRateLimiter,
    SourceHealthMonitor,
    ExecutionResult,
    EmptyDataPolicy,
    create_simple_router,
)

# -- Legacy adapters (kept for backward compatibility) ----------------------
from akshare_data.sources.akshare_source import AkShareAdapter
from akshare_data.sources.lixinger_source import LixingerAdapter
from akshare_data.sources.tushare_source import TushareAdapter, set_tushare_token
from akshare_data.sources.lixinger_client import get_lixinger_client, set_lixinger_token

__all__ = [
    # Canonical (from ingestion/)
    "DataSource",
    "MultiSourceRouter",
    "DomainRateLimiter",
    "SourceHealthMonitor",
    "ExecutionResult",
    "EmptyDataPolicy",
    "create_simple_router",
    # Legacy adapters (backward compat)
    "AkShareAdapter",
    "LixingerAdapter",
    "TushareAdapter",
    "set_tushare_token",
    "set_lixinger_token",
    "get_lixinger_client",
]
