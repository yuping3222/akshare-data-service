# ruff: noqa: E402
"""Error codes and exception hierarchy - compatibility shell.

DEPRECATED: Use `akshare_data.common.errors` instead.
This module re-exports from common/errors.py for backward compatibility.
"""
# ruff: noqa: E402

import warnings

warnings.warn(
    "akshare_data.core.errors is deprecated. Use akshare_data.common.errors instead.",
    DeprecationWarning,
    stacklevel=2,
)

from akshare_data.common.errors import (
    ErrorCode,
    DataAccessException,
    DataSourceError,
    SourceUnavailableError,
    NoDataError,
    TimeoutError,
    RateLimitError,
    CacheError,
    ValidationError,
    DataQualityError,
    StorageError,
    AuthError,
    NetworkError,
    SystemError,
)

__all__ = [
    "ErrorCode",
    "DataAccessException",
    "DataSourceError",
    "SourceUnavailableError",
    "NoDataError",
    "TimeoutError",
    "RateLimitError",
    "CacheError",
    "ValidationError",
    "DataQualityError",
    "StorageError",
    "AuthError",
    "NetworkError",
    "SystemError",
]
