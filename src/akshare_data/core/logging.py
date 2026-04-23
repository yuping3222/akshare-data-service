"""Structured logging module - compatibility shell.

DEPRECATED: Use `akshare_data.common.logging` instead.
This module re-exports from common/logging.py for backward compatibility.
"""
# ruff: noqa: E402

import warnings

warnings.warn(
    "akshare_data.core.logging is deprecated. Use akshare_data.common.logging instead.",
    DeprecationWarning,
    stacklevel=2,
)

from akshare_data.common.logging import (
    STANDARD_FORMAT,
    SIMPLE_FORMAT,
    STRATEGY_FORMAT,
    StructuredFormatter,
    StandardFormatter,
    JSONFormatter,
    get_formatter,
    ContextFilter,
    create_rotating_file_handler,
    create_timed_rotating_file_handler,
    create_strategy_log_file,
    close_handler_safely,
    JQLogAdapter,
    create_jq_log_adapter,
    LogAdapter,
    LoggingConfig,
    apply_config,
    get_default_config,
    LogManager,
    setup_logging,
    setup_logging_simple,
    get_logger,
    get_jq_log,
    get_default_logger,
    LogContext,
    log_exception,
    # Re-exports from types (backward compat)
    RequestStats,
    CacheStats,
    StatsCollector,
    get_stats_collector,
    reset_stats_collector,
    log_api_request,
    log_data_quality,
)

# Re-export TimedRotatingFileHandler for test patching compatibility
from logging.handlers import TimedRotatingFileHandler

__all__ = [
    # Formatters
    "STANDARD_FORMAT",
    "SIMPLE_FORMAT",
    "STRATEGY_FORMAT",
    "StructuredFormatter",
    "StandardFormatter",
    "JSONFormatter",
    "get_formatter",
    # Context Filter
    "ContextFilter",
    # Handlers
    "create_rotating_file_handler",
    "create_timed_rotating_file_handler",
    "create_strategy_log_file",
    "close_handler_safely",
    "TimedRotatingFileHandler",
    # JQLogAdapter
    "JQLogAdapter",
    "create_jq_log_adapter",
    "LogAdapter",
    # Config
    "LoggingConfig",
    "apply_config",
    "get_default_config",
    # Manager
    "LogManager",
    # Setup
    "setup_logging",
    "setup_logging_simple",
    "get_logger",
    "get_jq_log",
    "get_default_logger",
    # Context
    "LogContext",
    # Helpers
    "log_exception",
    # Re-exports from types (backward compat)
    "RequestStats",
    "CacheStats",
    "StatsCollector",
    "get_stats_collector",
    "reset_stats_collector",
    "log_api_request",
    "log_data_quality",
]
