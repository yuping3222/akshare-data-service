"""Structured logging module for akshare-data-service.

Provides:
- Formatters: StructuredFormatter, StandardFormatter, JSONFormatter
- ContextFilter: Adds default context to log records
- Handlers: Rotating file handlers, timed rotating, strategy log
- JQLogAdapter: JoinQuant-style log adapter
- LoggingConfig, LogManager: Configuration and management
- setup_logging, get_logger: Main entry points
- LogContext: Context manager for temporary log context
- log_exception: Helper for logging exceptions

This is the canonical location for logging.
The old `akshare_data.core.logging` is a compatibility shell.
"""

import json
import logging
import logging.handlers
import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

# Re-export types module items for backward compatibility
from akshare_data.common.types import (
    RequestStats,
    CacheStats,
    StatsCollector,
    get_stats_collector,
    reset_stats_collector,
    log_api_request,
    log_data_quality,
)

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
    # JQLogAdapter
    "JQLogAdapter",
    "create_jq_log_adapter",
    "LogAdapter",  # backward compat alias
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

# ============================================================================
# Formatters
# ============================================================================

STANDARD_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
SIMPLE_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
STRATEGY_FORMAT = "%(asctime)s - %(message)s"


class StructuredFormatter(logging.Formatter):
    """Custom formatter that outputs logs in structured JSON format."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if hasattr(record, "error_code"):
            log_entry["error_code"] = record.error_code

        if hasattr(record, "context"):
            log_entry["context"] = record.context
        else:
            log_entry["context"] = {}

        if record.exc_info:
            exc_info = record.exc_info
            log_entry["exception"] = {
                "type": exc_info[0].__name__ if exc_info[0] else None,
                "message": str(exc_info[1]) if exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }
            if (
                exc_info[1]
                and hasattr(exc_info[1], "error_code")
                and exc_info[1].error_code
            ):
                log_entry["error_code"] = exc_info[1].error_code.value

        log_entry["location"] = {
            "file": record.filename,
            "line": record.lineno,
            "function": record.funcName,
        }

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class StandardFormatter(logging.Formatter):
    """Standard log formatter using predefined format constants."""

    def __init__(self, fmt=None):
        super().__init__(fmt or STANDARD_FORMAT)


class JSONFormatter(logging.Formatter):
    """JSON log formatter outputting structured log records."""

    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_data["exception"] = self.formatException(record.exc_info)
        try:
            return json.dumps(log_data)
        except (TypeError, ValueError):
            log_data["message"] = str(log_data["message"])
            return json.dumps(log_data)


def get_formatter(format_type: str = "standard") -> logging.Formatter:
    """Return a formatter based on the specified format type."""
    formatters = {
        "standard": lambda: StandardFormatter(),
        "simple": lambda: logging.Formatter(SIMPLE_FORMAT),
        "json": lambda: JSONFormatter(),
        "structured": lambda: StructuredFormatter(),
        "strategy": lambda: logging.Formatter(STRATEGY_FORMAT),
    }
    if format_type not in formatters:
        raise ValueError(
            f"Unsupported format type: {format_type}. Choose from {list(formatters.keys())}"
        )
    return formatters[format_type]()


# ============================================================================
# Context Filter
# ============================================================================


class ContextFilter(logging.Filter):
    """Filter that adds default context to log records."""

    def __init__(self, default_context: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.default_context = default_context or {}

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "context"):
            record.context = {}
        record.context = {**self.default_context, **record.context}
        return True


# ============================================================================
# Handlers
# ============================================================================


def create_rotating_file_handler(
    log_file: str,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    level: str = "INFO",
) -> logging.handlers.RotatingFileHandler:
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    handler.setLevel(getattr(logging, level.upper()))
    return handler


def create_timed_rotating_file_handler(
    log_file: str,
    when: str = "midnight",
    backup_count: int = 7,
    level: str = "INFO",
) -> logging.handlers.TimedRotatingFileHandler:
    handler = logging.handlers.TimedRotatingFileHandler(
        log_file, when=when, backupCount=backup_count, encoding="utf-8"
    )
    handler.setLevel(getattr(logging, level.upper()))
    return handler


def create_strategy_log_file(filepath: str, mode: str = "a") -> logging.FileHandler:
    handler = logging.FileHandler(filepath, mode=mode, encoding="utf-8")
    handler.setLevel(logging.INFO)
    return handler


def close_handler_safely(handler: Optional[logging.Handler]) -> None:
    if handler is not None and hasattr(handler, "close"):
        handler.close()


# ============================================================================
# JQLogAdapter
# ============================================================================


class JQLogAdapter:
    """Unified JoinQuant-style log adapter."""

    def __init__(self, strategy=None, logger_name: str = "akshare_data"):
        self._strategy = strategy
        self._logger = logging.getLogger(logger_name)
        self._log_levels = {
            "order": "info",
            "trade": "info",
            "debug": "info",
        }

    def info(self, *args, **kwargs):
        msg = self._format_message(args, kwargs)
        self._dispatch("info", msg)

    def warn(self, *args, **kwargs):
        msg = self._format_message(args, kwargs)
        self._dispatch("warn", msg)

    def warning(self, *args, **kwargs):
        msg = self._format_message(args, kwargs)
        self._dispatch("warn", msg)

    def error(self, *args, **kwargs):
        msg = self._format_message(args, kwargs)
        self._dispatch("error", msg)

    def debug(self, *args, **kwargs):
        level = self._log_levels.get("debug", "info")
        if level == "debug":
            msg = self._format_message(args, kwargs)
            self._dispatch("debug", msg)

    def critical(self, *args, **kwargs):
        msg = self._format_message(args, kwargs)
        self._dispatch("critical", msg)

    def set_level(self, module: str, level: str):
        self._log_levels[module] = level

    def _dispatch(self, method: str, msg: str):
        if self._strategy is not None:
            prefix = ""
            if method == "warn":
                prefix = "[WARN] "
            elif method == "error":
                prefix = "[ERROR] "
            elif method == "debug":
                prefix = "[DEBUG] "
            self._strategy.log(f"{prefix}{msg}")
        else:
            level_prefix = method.upper()
            print(f"[{level_prefix}] {msg}")

    def _format_message(self, args, kwargs):
        parts = []
        for arg in args:
            if isinstance(arg, (pd.DataFrame, pd.Series)):
                parts.append("\n" + str(arg))
            else:
                parts.append(str(arg))
        msg = " ".join(parts)
        if kwargs:
            kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            msg = f"{msg} {kwargs_str}" if msg else kwargs_str
        return msg

    def __repr__(self):
        return f"<JQLogAdapter(strategy={self._strategy})>"


def create_jq_log_adapter(
    strategy=None, logger_name: str = "akshare_data"
) -> JQLogAdapter:
    return JQLogAdapter(strategy=strategy, logger_name=logger_name)


# ============================================================================
# Logging Config
# ============================================================================


@dataclass
class LoggingConfig:
    """Logging configuration dataclass."""

    level: str = "INFO"
    log_file: str = "logs/akshare_data.log"
    format: str = "structured"
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    suppress_third_party: bool = True

    @classmethod
    def from_dict(cls, data: dict) -> "LoggingConfig":
        valid_fields = {
            "level",
            "log_file",
            "format",
            "max_bytes",
            "backup_count",
            "suppress_third_party",
        }
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


def apply_config(config: LoggingConfig) -> logging.Logger:
    """Apply logging configuration to the root logger."""
    log_level = getattr(logging, config.level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    if not root_logger.handlers:
        formatter = get_formatter(config.format)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        if config.log_file:
            file_handler = create_rotating_file_handler(
                config.log_file,
                max_bytes=config.max_bytes,
                backup_count=config.backup_count,
                level=config.level,
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

    if config.suppress_third_party:
        for logger_name in ("matplotlib", "PIL", "urllib3", "requests"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    return root_logger


def get_default_config() -> LoggingConfig:
    return LoggingConfig(
        level=os.environ.get("AKSHARE_DATA_LOG_LEVEL", "INFO"),
        log_file=os.environ.get("AKSHARE_DATA_LOG_FILE", "logs/akshare_data.log"),
    )


# ============================================================================
# LogManager
# ============================================================================


class LogManager:
    """Unified logging manager (singleton)."""

    _instance = None
    _init_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self._config = None
        self._stats_collector = None
        self._initialized = True

    @classmethod
    def get_instance(cls) -> "LogManager":
        return cls()

    def initialize(self, config: Optional[LoggingConfig] = None):
        if config is None:
            config = get_default_config()
        apply_config(config)
        self._config = config
        self._stats_collector = get_stats_collector()

    def get_jq_adapter(
        self, strategy=None, logger_name: str = "akshare_data"
    ) -> JQLogAdapter:
        return JQLogAdapter(strategy=strategy, logger_name=logger_name)

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        return logging.getLogger(name or "akshare_data")

    def get_stats_collector(self) -> StatsCollector:
        return self._stats_collector

    def shutdown(self):
        if self._stats_collector:
            self._stats_collector.log_summary()
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            try:
                handler.close()
                root_logger.removeHandler(handler)
            except Exception:
                pass

    def reset(self):
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        if self._stats_collector:
            self._stats_collector.reset()
        self._config = None


# ============================================================================
# setup_logging
# ============================================================================


def _get_default_log_dir() -> Path:
    return Path("/tmp") / "akshare_data" / "logs"


def setup_logging(
    log_level: str = "INFO",
    log_dir: Optional[str] = None,
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_file: bool = False,
    format_type: str = "structured",
    json_format: bool = True,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 7,
    default_context: Optional[Dict[str, Any]] = None,
    suppress_third_party: bool = True,
    force: bool = False,
) -> logging.Logger:
    """Set up structured logging configuration."""
    if force:
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    if json_format and format_type == "structured":
        format_type = "structured"
    elif json_format:
        format_type = "json"

    if log_file is None:
        if log_dir is None:
            log_dir = _get_default_log_dir()
        else:
            log_dir = Path(log_dir)

        file_logging_enabled = False
        if enable_file:
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
                test_file = log_dir / ".write_test"
                test_file.touch()
                test_file.unlink()
                file_logging_enabled = True
                log_file = str(log_dir / "akshare_data.log")
            except (PermissionError, OSError) as e:
                import warnings

                warnings.warn(
                    f"Cannot create log directory '{log_dir}': {e}. Falling back to console-only logging.",
                    RuntimeWarning,
                    stacklevel=2,
                )
        else:
            file_logging_enabled = False
    else:
        file_logging_enabled = enable_file
        if enable_file:
            try:
                Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            except (PermissionError, OSError):
                file_logging_enabled = False

    logger = logging.getLogger("akshare_data")
    logger.setLevel(getattr(logging, log_level.upper()))

    if force:
        logger.handlers = []

    formatter = get_formatter(format_type)

    context_filter = ContextFilter(default_context)
    logger.addFilter(context_filter)

    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if file_logging_enabled and log_file:
        try:
            file_handler = TimedRotatingFileHandler(
                filename=log_file,
                when="midnight",
                interval=1,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except (PermissionError, OSError) as e:
            import warnings

            warnings.warn(
                f"Cannot create log file '{log_file}': {e}. Falling back to console-only logging.",
                RuntimeWarning,
                stacklevel=2,
            )

    if suppress_third_party:
        for logger_name in ("matplotlib", "PIL", "urllib3", "requests"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger.propagate = False

    # Initialize LogManager singleton
    manager = LogManager.get_instance()
    manager.initialize(
        LoggingConfig(
            level=log_level,
            log_file=log_file or "",
            format=format_type,
            max_bytes=max_bytes,
            backup_count=backup_count,
            suppress_third_party=suppress_third_party,
        )
    )

    return logger


# ============================================================================
# get_logger
# ============================================================================


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module."""
    if not name.startswith("akshare_data"):
        name = f"akshare_data.{name}"

    logger = logging.getLogger(name)

    root_logger = logging.getLogger("akshare_data")
    if not root_logger.handlers:
        try:
            setup_logging()
        except Exception:
            if not root_logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                handler.setLevel(logging.INFO)
                handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                    )
                )
                root_logger.addHandler(handler)
                root_logger.setLevel(logging.INFO)
                root_logger.propagate = False

    return logger


# ============================================================================
# LogContext
# ============================================================================


class LogContext:
    """Context manager for adding temporary context to logs."""

    def __init__(self, logger: logging.Logger, context: Dict[str, Any]):
        self.logger = logger
        self.context = context
        self.old_context = None

    def __enter__(self):
        for filter_obj in self.logger.filters:
            if isinstance(filter_obj, ContextFilter):
                self.old_context = filter_obj.default_context.copy()
                filter_obj.default_context.update(self.context)
                break
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for filter_obj in self.logger.filters:
            if isinstance(filter_obj, ContextFilter):
                if self.old_context is not None:
                    filter_obj.default_context = self.old_context
                break
        return False


# ============================================================================
# Helper logging functions
# ============================================================================


def log_exception(
    logger: logging.Logger,
    exception: Exception,
    source: Optional[str] = None,
    endpoint: Optional[str] = None,
    symbol: Optional[str] = None,
    additional_context: Optional[Dict[str, Any]] = None,
) -> None:
    """Log an exception with structured context and error code."""
    context: Dict[str, Any] = {"log_type": "exception"}
    if source:
        context["source"] = source
    if endpoint:
        context["endpoint"] = endpoint
    if symbol:
        context["symbol"] = symbol
    if additional_context:
        context.update(additional_context)

    error_code = None
    if hasattr(exception, "error_code") and exception.error_code:
        error_code = exception.error_code.value
        context["error_code"] = error_code

    if hasattr(exception, "context") and exception.context:
        context.update(exception.context)

    extra = {"context": context}
    if error_code:
        extra["error_code"] = error_code

    logger.error(
        f"Exception occurred: {exception}",
        extra=extra,
        exc_info=True,
    )


# ============================================================================
# Convenience functions
# ============================================================================


def setup_logging_simple(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_type: str = "standard",
    force: bool = False,
) -> LogManager:
    """Convenience function to set up logging."""
    setup_logging(
        log_level=level,
        log_file=log_file,
        format_type=format_type,
        enable_console=True,
        enable_file=bool(log_file),
        force=force,
    )
    return LogManager.get_instance()


def get_jq_log(strategy=None, logger_name: str = "akshare_data") -> JQLogAdapter:
    """Convenience function to get a JQLogAdapter."""
    return LogManager.get_instance().get_jq_adapter(
        strategy=strategy, logger_name=logger_name
    )


def get_default_logger() -> logging.Logger:
    """Convenience function to get the default logger."""
    return LogManager.get_instance().get_logger("akshare_data")


# Backward compatibility alias
LogAdapter = JQLogAdapter
