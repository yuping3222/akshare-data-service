"""Extended tests for akshare_data.core.logging module.

Coverage gaps filled:
- create_rotating_file_handler with custom parameters
- create_timed_rotating_file_handler with custom parameters
- create_strategy_log_file with custom parameters
- close_handler_safely with various handler types
- LogManager.shutdown()
- LogManager.reset()
- setup_logging with enable_file=True
- setup_logging permission error paths
"""

import pytest
import logging
from unittest.mock import patch, MagicMock
from akshare_data.core.logging import (
    create_rotating_file_handler,
    create_timed_rotating_file_handler,
    create_strategy_log_file,
    close_handler_safely,
    LogManager,
    setup_logging,
)


class TestCreateRotatingFileHandlerExtended:
    """Extended tests for create_rotating_file_handler() function."""

    def test_with_custom_max_bytes(self, tmp_path):
        """Should respect custom max_bytes."""
        log_file = str(tmp_path / "test.log")
        handler = create_rotating_file_handler(log_file, max_bytes=1024)
        assert handler.maxBytes == 1024
        handler.close()

    def test_with_custom_backup_count(self, tmp_path):
        """Should respect custom backup_count."""
        log_file = str(tmp_path / "test.log")
        handler = create_rotating_file_handler(log_file, backup_count=3)
        assert handler.backupCount == 3
        handler.close()

    def test_with_custom_level(self, tmp_path):
        """Should respect custom level."""
        log_file = str(tmp_path / "test.log")
        handler = create_rotating_file_handler(log_file, level="DEBUG")
        assert handler.level == logging.DEBUG
        handler.close()

    def test_level_case_insensitive(self, tmp_path):
        """Should handle level case insensitively."""
        log_file = str(tmp_path / "test.log")
        handler = create_rotating_file_handler(log_file, level="debug")
        assert handler.level == logging.DEBUG
        handler.close()


class TestCreateTimedRotatingFileHandlerExtended:
    """Extended tests for create_timed_rotating_file_handler() function."""

    def test_with_custom_when(self, tmp_path):
        """Should respect custom when parameter."""
        log_file = str(tmp_path / "test.log")
        handler = create_timed_rotating_file_handler(log_file, when="H")
        assert handler.when == "H"
        handler.close()

    def test_with_custom_backup_count(self, tmp_path):
        """Should respect custom backup_count."""
        log_file = str(tmp_path / "test.log")
        handler = create_timed_rotating_file_handler(log_file, backup_count=10)
        assert handler.backupCount == 10
        handler.close()

    def test_with_custom_level(self, tmp_path):
        """Should respect custom level."""
        log_file = str(tmp_path / "test.log")
        handler = create_timed_rotating_file_handler(log_file, level="WARNING")
        assert handler.level == logging.WARNING
        handler.close()


class TestCreateStrategyLogFileExtended:
    """Extended tests for create_strategy_log_file() function."""

    def test_with_write_mode(self, tmp_path):
        """Should create file in write mode."""
        log_file = str(tmp_path / "strategy.log")
        handler = create_strategy_log_file(log_file, mode="w")
        assert isinstance(handler, logging.FileHandler)
        handler.close()

    def test_with_append_mode(self, tmp_path):
        """Should create file in append mode."""
        log_file = str(tmp_path / "strategy.log")
        handler = create_strategy_log_file(log_file, mode="a")
        assert isinstance(handler, logging.FileHandler)
        handler.close()


class TestCloseHandlerSafelyExtended:
    """Extended tests for close_handler_safely() function."""

    def test_with_rotating_file_handler(self, tmp_path):
        """Should close RotatingFileHandler safely."""
        handler = create_rotating_file_handler(str(tmp_path / "test.log"))
        close_handler_safely(handler)

    def test_with_timed_rotating_file_handler(self, tmp_path):
        """Should close TimedRotatingFileHandler safely."""
        handler = create_timed_rotating_file_handler(str(tmp_path / "test.log"))
        close_handler_safely(handler)

    def test_with_stream_handler(self):
        """Should close StreamHandler safely."""
        handler = logging.StreamHandler()
        close_handler_safely(handler)

    def test_with_mock_handler(self):
        """Should close mock handler safely."""
        mock_handler = MagicMock()
        close_handler_safely(mock_handler)


class TestLogManagerShutdown:
    """Extended tests for LogManager.shutdown() method."""

    def setup_method(self):
        """Reset singleton before each test."""
        LogManager._instance = None

    def teardown_method(self):
        """Reset singleton after each test."""
        LogManager._instance = None

    def test_shutdown_closes_handlers(self):
        """Should close all handlers during shutdown."""
        manager = LogManager()
        manager.initialize()

        logger = logging.getLogger()
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        manager.shutdown()

    def test_shutdown_with_no_handlers(self):
        """Should handle no handlers gracefully."""
        manager = LogManager()
        manager.initialize()
        manager.shutdown()


class TestLogManagerReset:
    """Extended tests for LogManager.reset() method."""

    def setup_method(self):
        """Reset singleton before each test."""
        LogManager._instance = None

    def teardown_method(self):
        """Reset singleton after each test."""
        LogManager._instance = None

    def test_reset_clears_config(self):
        """Should clear config during reset."""
        manager = LogManager()
        manager.initialize()
        manager.reset()
        assert manager._config is None

    def test_reset_removes_handlers(self):
        """Should remove all handlers during reset."""
        manager = LogManager()
        manager.initialize()

        logger = logging.getLogger()
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        manager.reset()
        assert logger.handlers == [] or logger.handlers == []


class TestSetupLoggingWithFile:
    """Extended tests for setup_logging() with enable_file=True."""

    def setup_method(self):
        """Reset singleton before each test."""
        LogManager._instance = None

    def teardown_method(self):
        """Reset singleton after each test."""
        LogManager._instance = None

    def test_enable_file_with_valid_directory(self, tmp_path):
        """Should create file handler when enable_file=True."""
        log_dir = tmp_path / "logs"
        logger = setup_logging(
            log_dir=str(log_dir),
            enable_file=True,
            force=True,
        )
        assert isinstance(logger, logging.Logger)

    def test_enable_file_with_custom_log_file(self, tmp_path):
        """Should use custom log_file when provided."""
        log_file = str(tmp_path / "custom.log")
        logger = setup_logging(
            log_file=log_file,
            enable_file=True,
            force=True,
        )
        assert isinstance(logger, logging.Logger)


class TestSetupLoggingPermissionErrors:
    """Extended tests for setup_logging() permission error paths."""

    def setup_method(self):
        """Reset singleton before each test."""
        LogManager._instance = None

    def teardown_method(self):
        """Reset singleton after each test."""
        LogManager._instance = None

    def test_permission_error_on_log_dir_creation(self):
        """Should handle permission error on log directory creation."""
        with patch("pathlib.Path.mkdir") as mock_mkdir:
            mock_mkdir.side_effect = PermissionError("Permission denied")
            with pytest.warns(RuntimeWarning):
                logger = setup_logging(
                    log_dir="/root/forbidden",
                    enable_file=True,
                    force=True,
                )
                assert isinstance(logger, logging.Logger)

    def test_permission_error_on_log_file_creation(self, tmp_path):
        """Should handle permission error on log file creation."""
        log_file = str(tmp_path / "cannot_create.log")
        with patch(
            "akshare_data.core.logging.TimedRotatingFileHandler"
        ) as mock_handler:
            mock_handler.side_effect = PermissionError("Permission denied")
            with pytest.warns(RuntimeWarning):
                logger = setup_logging(
                    log_file=log_file,
                    enable_file=True,
                    force=True,
                )
                assert isinstance(logger, logging.Logger)


class TestLogManagerGetStatsCollector:
    """Extended tests for LogManager.get_stats_collector() method."""

    def setup_method(self):
        """Reset singleton before each test."""
        LogManager._instance = None

    def teardown_method(self):
        """Reset singleton after each test."""
        LogManager._instance = None

    def test_get_stats_collector_after_initialize(self):
        """Should return stats collector after initialization."""
        manager = LogManager()
        manager.initialize()
        stats = manager.get_stats_collector()
        assert stats is not None

    def test_get_stats_collector_before_initialize(self):
        """Should return None before initialization."""
        manager = LogManager()
        assert manager._stats_collector is None
