"""Common cross-layer shared capabilities for akshare-data-service.

This module provides foundational utilities shared across all layers:
- errors: Error codes and exception hierarchy
- types: Basic types (RequestStats, CacheStats, StatsCollector)
- config: Configuration directory resolution, config cache, token management
- logging: Structured logging setup and utilities

Note: This module does NOT contain schema, normalizer, or source adapter business logic.
Those belong in governance/, standardized/, or ingestion/ respectively.
"""

from akshare_data.common.events import (
    EventBus,
    PipelineEvent,
    PipelineEventType,
    get_event_bus,
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

from akshare_data.common.types import (
    RequestStats,
    CacheStats,
    StatsCollector,
    get_stats_collector,
    reset_stats_collector,
    log_api_request,
    log_data_quality,
)

from akshare_data.common.config import (
    get_config_dir,
    get_project_root,
    ConfigCache,
    TokenManager,
    get_token,
    set_token,
)

from akshare_data.common.logging import (
    StructuredFormatter,
    StandardFormatter,
    JSONFormatter,
    ContextFilter,
    JQLogAdapter,
    LogAdapter,
    LoggingConfig,
    LogManager,
    LogContext,
    setup_logging,
    setup_logging_simple,
    get_logger,
    get_jq_log,
    get_default_logger,
    log_exception,
    create_rotating_file_handler,
    create_timed_rotating_file_handler,
    get_formatter,
)

__all__ = [
    # Events / Pipeline bus
    "EventBus",
    "PipelineEvent",
    "PipelineEventType",
    "get_event_bus",
    # Errors
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
    # Types
    "RequestStats",
    "CacheStats",
    "StatsCollector",
    "get_stats_collector",
    "reset_stats_collector",
    "log_api_request",
    "log_data_quality",
    # Config
    "get_config_dir",
    "get_project_root",
    "ConfigCache",
    "TokenManager",
    "get_token",
    "set_token",
    # Logging
    "StructuredFormatter",
    "StandardFormatter",
    "JSONFormatter",
    "ContextFilter",
    "JQLogAdapter",
    "LogAdapter",
    "LoggingConfig",
    "LogManager",
    "LogContext",
    "setup_logging",
    "setup_logging_simple",
    "get_logger",
    "get_jq_log",
    "get_default_logger",
    "log_exception",
    "create_rotating_file_handler",
    "create_timed_rotating_file_handler",
    "get_formatter",
]
