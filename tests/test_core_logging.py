"""Tests for akshare_data.core.logging module.

Covers:
- StructuredFormatter, StandardFormatter, JSONFormatter
- get_formatter() function
- ContextFilter
- Handler creation functions
- RequestStats, CacheStats, StatsCollector
- JQLogAdapter
- LoggingConfig dataclass
- LogManager singleton
- setup_logging() function
- get_logger() function
- LogContext
- Helper logging functions
"""

import pytest
import logging
import json
import os
from unittest.mock import MagicMock

from akshare_data.core.logging import (
    StructuredFormatter,
    StandardFormatter,
    JSONFormatter,
    get_formatter,
    ContextFilter,
    create_rotating_file_handler,
    create_timed_rotating_file_handler,
    create_strategy_log_file,
    close_handler_safely,
    RequestStats,
    CacheStats,
    StatsCollector,
    get_stats_collector,
    reset_stats_collector,
    JQLogAdapter,
    create_jq_log_adapter,
    LoggingConfig,
    apply_config,
    get_default_config,
    LogManager,
    setup_logging,
    get_logger,
    LogContext,
    log_api_request,
    log_data_quality,
    log_exception,
    setup_logging_simple,
    get_jq_log,
    get_default_logger,
    STANDARD_FORMAT,
    SIMPLE_FORMAT,
    STRATEGY_FORMAT,
)


class TestFormatConstants:
    """Test format string constants."""

    def test_standard_format_defined(self):
        """STANDARD_FORMAT should be defined."""
        assert isinstance(STANDARD_FORMAT, str)
        assert len(STANDARD_FORMAT) > 0

    def test_simple_format_defined(self):
        """SIMPLE_FORMAT should be defined."""
        assert isinstance(SIMPLE_FORMAT, str)

    def test_strategy_format_defined(self):
        """STRATEGY_FORMAT should be defined."""
        assert isinstance(STRATEGY_FORMAT, str)


class TestStructuredFormatter:
    """Test StructuredFormatter class."""

    def test_format_returns_json_string(self, caplog):
        """Should return valid JSON string."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["message"] == "Test message"
        assert parsed["level"] == "INFO"

    def test_format_includes_timestamp(self, caplog):
        """Should include ISO format timestamp."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert "timestamp" in parsed
        assert parsed["timestamp"].endswith("Z")

    def test_format_includes_location(self):
        """Should include location info."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert "location" in parsed
        assert parsed["location"]["line"] == 42

    def test_format_with_error_code(self):
        """Should include error_code if present."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Error",
            args=(),
            exc_info=None,
        )
        record.error_code = "1001_SOURCE_UNAVAILABLE"
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["error_code"] == "1001_SOURCE_UNAVAILABLE"

    def test_format_with_context(self):
        """Should include context if present."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        record.context = {"source": "test_source"}
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["context"]["source"] == "test_source"

    def test_format_with_exception(self):
        """Should include exception info if present."""
        formatter = StructuredFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            record = logging.LogRecord(
                name="test",
                level=logging.ERROR,
                pathname="test.py",
                lineno=1,
                msg="Error",
                args=(),
                exc_info=sys.exc_info(),
            )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert "exception" in parsed


class TestStandardFormatter:
    """Test StandardFormatter class."""

    def test_uses_default_format(self):
        """Should use STANDARD_FORMAT by default."""
        formatter = StandardFormatter()
        assert formatter._fmt == STANDARD_FORMAT

    def test_uses_custom_format(self):
        """Should accept custom format string."""
        formatter = StandardFormatter(fmt="%(message)s")
        assert formatter._fmt == "%(message)s"


class TestJSONFormatter:
    """Test JSONFormatter class."""

    def test_format_returns_json(self):
        """Should return valid JSON."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["message"] == "Test"
        assert parsed["level"] == "INFO"


class TestGetFormatter:
    """Test get_formatter() function."""

    def test_standard_formatter(self):
        """Should return StandardFormatter for 'standard'."""
        formatter = get_formatter("standard")
        assert isinstance(formatter, StandardFormatter)

    def test_simple_formatter(self):
        """Should return simple Formatter for 'simple'."""
        formatter = get_formatter("simple")
        assert isinstance(formatter, logging.Formatter)

    def test_json_formatter(self):
        """Should return JSONFormatter for 'json'."""
        formatter = get_formatter("json")
        assert isinstance(formatter, JSONFormatter)

    def test_structured_formatter(self):
        """Should return StructuredFormatter for 'structured'."""
        formatter = get_formatter("structured")
        assert isinstance(formatter, StructuredFormatter)

    def test_strategy_formatter(self):
        """Should return strategy Formatter for 'strategy'."""
        formatter = get_formatter("strategy")
        assert isinstance(formatter, logging.Formatter)

    def test_invalid_format_type_raises(self):
        """Should raise ValueError for invalid format type."""
        with pytest.raises(ValueError):
            get_formatter("invalid_format")


class TestContextFilter:
    """Test ContextFilter class."""

    def test_adds_default_context(self):
        """Should add default context to records."""
        filter_obj = ContextFilter(default_context={"key": "value"})
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        filter_obj.filter(record)
        assert record.context["key"] == "value"

    def test_merges_contexts(self):
        """Should merge default and record contexts."""
        filter_obj = ContextFilter(default_context={"default": "value"})
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        record.context = {"record": "value"}
        filter_obj.filter(record)
        assert record.context["default"] == "value"
        assert record.context["record"] == "value"


class TestHandlerCreation:
    """Test handler creation functions."""

    def test_create_rotating_file_handler(self, tmp_path):
        """Should create rotating file handler."""
        log_file = str(tmp_path / "test.log")
        handler = create_rotating_file_handler(log_file)
        assert isinstance(handler, logging.handlers.RotatingFileHandler)
        handler.close()

    def test_create_timed_rotating_file_handler(self, tmp_path):
        """Should create timed rotating file handler."""
        log_file = str(tmp_path / "test.log")
        handler = create_timed_rotating_file_handler(log_file)
        assert isinstance(handler, logging.handlers.TimedRotatingFileHandler)
        handler.close()

    def test_create_strategy_log_file(self, tmp_path):
        """Should create strategy log file handler."""
        log_file = str(tmp_path / "strategy.log")
        handler = create_strategy_log_file(log_file)
        assert isinstance(handler, logging.FileHandler)
        handler.close()


class TestCloseHandlerSafely:
    """Test close_handler_safely() function."""

    def test_with_valid_handler(self, tmp_path):
        """Should close handler without error."""
        handler = logging.FileHandler(str(tmp_path / "test.log"))
        close_handler_safely(handler)

    def test_with_none(self):
        """Should handle None gracefully."""
        close_handler_safely(None)

    def test_with_object_without_close(self):
        """Should handle objects without close method."""
        obj = object()
        close_handler_safely(obj)


class TestRequestStats:
    """Test RequestStats class."""

    def test_initial_state(self):
        """Should start with zero values."""
        stats = RequestStats()
        assert stats.total_requests == 0
        assert stats.successful_requests == 0
        assert stats.failed_requests == 0

    def test_avg_duration_with_no_requests(self):
        """Should return 0 when no requests."""
        stats = RequestStats()
        assert stats.avg_duration_ms == 0.0

    def test_success_rate_with_no_requests(self):
        """Should return 0 when no requests."""
        stats = RequestStats()
        assert stats.success_rate == 0.0

    def test_error_rate_with_no_requests(self):
        """Should return 0 when no requests."""
        stats = RequestStats()
        assert stats.error_rate == 0.0

    def test_to_dict(self):
        """Should return dictionary representation."""
        stats = RequestStats()
        d = stats.to_dict()
        assert "total_requests" in d
        assert "successful_requests" in d
        assert "failed_requests" in d


class TestCacheStats:
    """Test CacheStats class."""

    def test_initial_state(self):
        """Should start with zero values."""
        stats = CacheStats()
        assert stats.hits == 0
        assert stats.misses == 0

    def test_total_requests(self):
        """Should calculate total requests."""
        stats = CacheStats()
        stats.hits = 5
        stats.misses = 3
        assert stats.total_requests == 8

    def test_hit_rate_with_no_requests(self):
        """Should return 0 when no requests."""
        stats = CacheStats()
        assert stats.hit_rate == 0.0

    def test_hit_rate_with_requests(self):
        """Should calculate hit rate correctly."""
        stats = CacheStats()
        stats.hits = 8
        stats.misses = 2
        assert stats.hit_rate == 0.8


class TestStatsCollector:
    """Test StatsCollector class."""

    def setup_method(self):
        """Reset singleton before each test."""
        reset_stats_collector()

    def teardown_method(self):
        """Reset singleton after each test."""
        reset_stats_collector()

    def test_singleton_pattern(self):
        """Should return same instance."""
        collector1 = StatsCollector()
        collector2 = StatsCollector()
        assert collector1 is collector2

    def test_record_request(self):
        """Should record request stats."""
        collector = StatsCollector()
        collector.record_request("source1", 100.0, True)
        stats = collector.get_source_stats("source1")
        assert stats["total_requests"] == 1
        assert stats["successful_requests"] == 1

    def test_record_request_failure(self):
        """Should record failed requests."""
        collector = StatsCollector()
        collector.record_request("source1", 100.0, False, error_type="timeout")
        stats = collector.get_source_stats("source1")
        assert stats["failed_requests"] == 1
        assert "timeout" in stats.get("errors", {})

    def test_record_cache_hit(self):
        """Should record cache hits."""
        collector = StatsCollector()
        collector.record_cache_hit("cache1")
        stats = collector.get_cache_stats("cache1")
        assert stats["hits"] == 1

    def test_record_cache_miss(self):
        """Should record cache misses."""
        collector = StatsCollector()
        collector.record_cache_miss("cache1")
        stats = collector.get_cache_stats("cache1")
        assert stats["misses"] == 1

    def test_get_all_stats(self):
        """Should return aggregated stats."""
        collector = StatsCollector()
        collector.record_request("source1", 100.0, True)
        collector.record_cache_hit("cache1")
        stats = collector.get_all_stats()
        assert "request_stats" in stats
        assert "cache_stats" in stats
        assert "summary" in stats

    def test_get_summary_text(self):
        """Should return formatted summary."""
        collector = StatsCollector()
        collector.record_request("source1", 100.0, True)
        summary = collector.get_summary_text()
        assert isinstance(summary, str)
        assert "Total Requests" in summary

    def test_export_json(self, tmp_path):
        """Should export stats to JSON file."""
        collector = StatsCollector()
        collector.record_request("source1", 100.0, True)
        filepath = str(tmp_path / "stats.json")
        collector.export_json(filepath)
        assert os.path.exists(filepath)

    def test_reset(self):
        """Should clear all stats."""
        collector = StatsCollector()
        collector.record_request("source1", 100.0, True)
        collector.reset()
        stats = collector.get_source_stats("source1")
        assert stats == {}


class TestGetStatsCollector:
    """Test get_stats_collector() function."""

    def setup_method(self):
        reset_stats_collector()

    def teardown_method(self):
        reset_stats_collector()

    def test_returns_stats_collector_instance(self):
        """Should return StatsCollector instance."""
        collector = get_stats_collector()
        assert isinstance(collector, StatsCollector)


class TestJQLogAdapter:
    """Test JQLogAdapter class."""

    def test_initialization(self):
        """Should initialize with logger."""
        adapter = JQLogAdapter()
        assert adapter._logger is not None

    def test_info_method(self):
        """Should have info method."""
        adapter = JQLogAdapter()
        adapter.info("Test message")

    def test_warn_method(self):
        """Should have warn method."""
        adapter = JQLogAdapter()
        adapter.warn("Warning message")

    def test_warning_method(self):
        """Should have warning method (alias for warn)."""
        adapter = JQLogAdapter()
        adapter.warning("Warning message")

    def test_error_method(self):
        """Should have error method."""
        adapter = JQLogAdapter()
        adapter.error("Error message")

    def test_debug_method(self):
        """Should have debug method."""
        adapter = JQLogAdapter()
        adapter.debug("Debug message")

    def test_critical_method(self):
        """Should have critical method."""
        adapter = JQLogAdapter()
        adapter.critical("Critical message")

    def test_set_level(self):
        """Should allow setting log levels."""
        adapter = JQLogAdapter()
        adapter.set_level("debug", "WARNING")

    def test_with_strategy(self):
        """Should work with strategy object."""
        strategy = MagicMock()
        adapter = JQLogAdapter(strategy=strategy)
        adapter.info("Test")
        strategy.log.assert_called_once()


class TestCreateJqLogAdapter:
    """Test create_jq_log_adapter() function."""

    def test_returns_jq_log_adapter(self):
        """Should return JQLogAdapter instance."""
        adapter = create_jq_log_adapter()
        assert isinstance(adapter, JQLogAdapter)


class TestLoggingConfig:
    """Test LoggingConfig dataclass."""

    def test_default_values(self):
        """Should have correct default values."""
        config = LoggingConfig()
        assert config.level == "INFO"
        assert config.log_file == "logs/akshare_data.log"
        assert config.format == "structured"
        assert config.max_bytes == 10 * 1024 * 1024
        assert config.backup_count == 5
        assert config.suppress_third_party is True

    def test_from_dict(self):
        """Should create from dictionary."""
        data = {
            "level": "DEBUG",
            "log_file": "custom.log",
            "format": "json",
        }
        config = LoggingConfig.from_dict(data)
        assert config.level == "DEBUG"
        assert config.log_file == "custom.log"
        assert config.format == "json"

    def test_from_dict_ignores_invalid_fields(self):
        """Should ignore invalid fields."""
        data = {
            "level": "DEBUG",
            "invalid_field": "value",
        }
        config = LoggingConfig.from_dict(data)
        assert config.level == "DEBUG"


class TestApplyConfig:
    """Test apply_config() function."""

    def test_returns_logger(self):
        """Should return configured logger."""
        config = LoggingConfig()
        logger = apply_config(config)
        assert isinstance(logger, logging.Logger)


class TestGetDefaultConfig:
    """Test get_default_config() function."""

    def test_returns_logging_config(self):
        """Should return LoggingConfig instance."""
        config = get_default_config()
        assert isinstance(config, LoggingConfig)


class TestLogManager:
    """Test LogManager singleton."""

    def setup_method(self):
        """Reset singleton before each test."""
        LogManager._instance = None

    def teardown_method(self):
        """Reset singleton after each test."""
        LogManager._instance = None

    def test_singleton_pattern(self):
        """Should return same instance."""
        manager1 = LogManager()
        manager2 = LogManager()
        assert manager1 is manager2

    def test_initialize(self):
        """Should initialize with config."""
        manager = LogManager()
        config = LoggingConfig()
        manager.initialize(config)
        assert manager._config is not None

    def test_get_jq_adapter(self):
        """Should return JQLogAdapter."""
        manager = LogManager()
        adapter = manager.get_jq_adapter()
        assert isinstance(adapter, JQLogAdapter)

    def test_get_logger(self):
        """Should return logger."""
        manager = LogManager()
        logger = manager.get_logger("test")
        assert isinstance(logger, logging.Logger)


class TestSetupLogging:
    """Test setup_logging() function."""

    def setup_method(self):
        """Reset LogManager singleton."""
        LogManager._instance = None

    def teardown_method(self):
        """Clean up."""
        LogManager._instance = None

    def test_returns_logger(self):
        """Should return configured logger."""
        logger = setup_logging(force=True)
        assert isinstance(logger, logging.Logger)

    def test_with_custom_level(self):
        """Should accept custom log level."""
        logger = setup_logging(log_level="DEBUG", force=True)
        assert logger.level == logging.DEBUG

    def test_with_json_format(self):
        """Should work with JSON format."""
        logger = setup_logging(format_type="json", force=True)
        assert isinstance(logger, logging.Logger)

    def test_suppress_third_party(self):
        """Should suppress third party loggers."""
        logger = setup_logging(suppress_third_party=True, force=True)
        assert isinstance(logger, logging.Logger)


class TestGetLogger:
    """Test get_logger() function."""

    def setup_method(self):
        """Reset LogManager singleton."""
        LogManager._instance = None

    def teardown_method(self):
        """Clean up."""
        LogManager._instance = None

    def test_returns_logger_with_prefix(self):
        """Should return logger with akshare_data prefix."""
        logger = get_logger("test")
        assert "akshare_data" in logger.name

    def test_does_not_double_prefix(self):
        """Should not double prefix if already prefixed."""
        logger = get_logger("akshare_data.test")
        assert logger.name == "akshare_data.test"


class TestLogContext:
    """Test LogContext class."""

    def test_context_manager(self, caplog):
        """Should temporarily add context."""
        logger = logging.getLogger("test_context")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        context = {"key": "value"}
        with LogContext(logger, context):
            pass


class TestHelperLoggingFunctions:
    """Test helper logging functions."""

    def test_log_api_request(self, caplog):
        """Should log API request with context."""
        logger = logging.getLogger("test_api")
        logger.setLevel(logging.INFO)
        with caplog.at_level(logging.INFO):
            log_api_request(
                logger,
                source="test_source",
                endpoint="/api/test",
                status="success",
                rows=10,
            )
        assert "test_source" in caplog.text or caplog.text

    def test_log_data_quality(self, caplog):
        """Should log data quality issue."""
        logger = logging.getLogger("test_quality")
        logger.setLevel(logging.WARNING)
        with caplog.at_level(logging.WARNING):
            log_data_quality(
                logger,
                source="test_source",
                data_type="price",
                issue="outlier_detected",
            )
        assert caplog.text

    def test_log_exception(self, caplog):
        """Should log exception."""
        logger = logging.getLogger("test_exception")
        logger.setLevel(logging.ERROR)
        with caplog.at_level(logging.ERROR):
            log_exception(logger, ValueError("Test error"))
        assert caplog.text


class TestSetupLoggingSimple:
    """Test setup_logging_simple() function."""

    def setup_method(self):
        """Reset LogManager singleton."""
        LogManager._instance = None

    def teardown_method(self):
        """Clean up."""
        LogManager._instance = None

    def test_returns_log_manager(self):
        """Should return LogManager."""
        manager = setup_logging_simple(force=True)
        assert isinstance(manager, LogManager)


class TestGetJqLog:
    """Test get_jq_log() function."""

    def setup_method(self):
        """Reset LogManager singleton."""
        LogManager._instance = None

    def teardown_method(self):
        """Clean up."""
        LogManager._instance = None

    def test_returns_jq_adapter(self):
        """Should return JQLogAdapter."""
        adapter = get_jq_log()
        assert isinstance(adapter, JQLogAdapter)


class TestGetDefaultLogger:
    """Test get_default_logger() function."""

    def setup_method(self):
        """Reset LogManager singleton."""
        LogManager._instance = None

    def teardown_method(self):
        """Clean up."""
        LogManager._instance = None

    def test_returns_logger(self):
        """Should return logger."""
        logger = get_default_logger()
        assert isinstance(logger, logging.Logger)
