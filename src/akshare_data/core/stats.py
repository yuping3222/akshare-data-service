# ruff: noqa: E402
"""Stats collection module - compatibility shell.

DEPRECATED: Use `akshare_data.common.types` instead.
This module re-exports from common/types.py for backward compatibility.
"""
# ruff: noqa: E402

import warnings

warnings.warn(
    "akshare_data.core.stats is deprecated. Use akshare_data.common.types instead.",
    DeprecationWarning,
    stacklevel=2,
)

from akshare_data.common.types import (
    RequestStats,
    CacheStats,
    StatsCollector,
    get_stats_collector,
    reset_stats_collector,
    log_api_request,
    log_data_quality,
    _stats_collector,
)

__all__ = [
    "RequestStats",
    "CacheStats",
    "StatsCollector",
    "get_stats_collector",
    "reset_stats_collector",
    "log_api_request",
    "log_data_quality",
    "_stats_collector",
]
