"""Tests for AccessLogger and CallStatsAnalyzer"""

import json
import time
import pytest
from pathlib import Path
from datetime import datetime

from akshare_data.offline.access_logger import AccessLogger
from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer


@pytest.fixture
def tmp_log_dir(tmp_path):
    return str(tmp_path / "logs")


@pytest.fixture
def tmp_output(tmp_path):
    return str(tmp_path / "config" / "download_priority.yaml")


class TestAccessLogger:
    def test_record_writes_to_log(self, tmp_log_dir):
        logger = AccessLogger(log_dir=tmp_log_dir, flush_interval=0.1)
        try:
            logger.record(
                interface="equity_daily",
                symbol="000001",
                cache_hit=False,
                latency_ms=450.5,
            )
            time.sleep(0.3)

            log_file = Path(tmp_log_dir) / "access.log"
            assert log_file.exists()
            content = log_file.read_text().strip()
            entry = json.loads(content)
            assert entry["interface"] == "equity_daily"
            assert entry["symbol"] == "000001"
            assert entry["cache_hit"] is False
            assert entry["latency_ms"] == 450.5
            assert "ts" in entry
        finally:
            logger.shutdown()

    def test_record_is_non_blocking(self, tmp_log_dir):
        logger = AccessLogger(log_dir=tmp_log_dir, flush_interval=10.0)
        try:
            start = time.monotonic()
            for i in range(50):
                logger.record(
                    interface="test",
                    symbol=f"symbol_{i}",
                    cache_hit=i % 2 == 0,
                    latency_ms=10.0,
                )
            elapsed = time.monotonic() - start
            assert elapsed < 0.1
        finally:
            logger.shutdown()

    def test_multiple_entries(self, tmp_log_dir):
        logger = AccessLogger(log_dir=tmp_log_dir, flush_interval=0.1)
        try:
            logger.record("equity_daily", "000001", False, 100)
            logger.record("equity_daily", "000002", True, 5)
            logger.record("equity_minute", "000001", False, 300)
            time.sleep(0.3)

            log_file = Path(tmp_log_dir) / "access.log"
            lines = log_file.read_text().strip().split("\n")
            assert len(lines) == 3
        finally:
            logger.shutdown()

    def test_shutdown_flushes_queue(self, tmp_log_dir):
        logger = AccessLogger(log_dir=tmp_log_dir, flush_interval=60.0)
        logger.record("equity_daily", "000001", False, 100)
        logger.shutdown()

        log_file = Path(tmp_log_dir) / "access.log"
        assert log_file.exists()
        content = log_file.read_text().strip()
        assert len(content) > 0


class TestCallStatsAnalyzer:
    def _write_test_logs(self, log_dir: str, entries: list):
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_file = Path(log_dir) / "access.log"
        with open(log_file, "w", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def test_analyze_generates_config(self, tmp_log_dir, tmp_output):
        now = datetime.now()
        entries = [
            {
                "ts": now.isoformat(),
                "interface": "equity_daily",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 450,
            },
            {
                "ts": now.isoformat(),
                "interface": "equity_daily",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 400,
            },
            {
                "ts": now.isoformat(),
                "interface": "equity_daily",
                "symbol": "000002",
                "cache_hit": True,
                "latency_ms": 5,
            },
            {
                "ts": now.isoformat(),
                "interface": "equity_minute",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 300,
            },
        ]
        self._write_test_logs(tmp_log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=tmp_log_dir, output_path=tmp_output)
        config = analyzer.analyze(window_days=7)

        assert "priorities" in config
        assert "equity_daily" in config["priorities"]
        assert "equity_minute" in config["priorities"]
        assert config["priorities"]["equity_daily"]["call_count_7d"] == 3
        assert config["global"]["total_calls_7d"] == 4

    def test_analyze_empty_logs(self, tmp_log_dir, tmp_output):
        Path(tmp_log_dir).mkdir(parents=True, exist_ok=True)

        analyzer = CallStatsAnalyzer(log_dir=tmp_log_dir, output_path=tmp_output)
        config = analyzer.analyze(window_days=7)

        assert config == {}

    def test_score_calculation(self, tmp_log_dir, tmp_output):
        now = datetime.now()
        entries = []
        for i in range(100):
            entries.append(
                {
                    "ts": now.isoformat(),
                    "interface": "hot_interface",
                    "symbol": "000001",
                    "cache_hit": False,
                    "latency_ms": 200,
                }
            )
        for i in range(5):
            entries.append(
                {
                    "ts": now.isoformat(),
                    "interface": "cold_interface",
                    "symbol": "000001",
                    "cache_hit": True,
                    "latency_ms": 10,
                }
            )
        self._write_test_logs(tmp_log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=tmp_log_dir, output_path=tmp_output)
        config = analyzer.analyze(window_days=7)

        hot_score = config["priorities"]["hot_interface"]["score"]
        cold_score = config["priorities"]["cold_interface"]["score"]
        assert hot_score > cold_score
        assert (
            config["priorities"]["hot_interface"]["rank"]
            < config["priorities"]["cold_interface"]["rank"]
        )

    def test_recommendation_strategy(self, tmp_log_dir, tmp_output):
        now = datetime.now()
        entries = []
        for i in range(60):
            entries.append(
                {
                    "ts": now.isoformat(),
                    "interface": "high_miss",
                    "symbol": "000001",
                    "cache_hit": False,
                    "latency_ms": 200,
                }
            )
        self._write_test_logs(tmp_log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=tmp_log_dir, output_path=tmp_output)
        config = analyzer.analyze(window_days=7)

        rec = config["priorities"]["high_miss"]["recommendation"]
        assert rec["mode"] == "incremental"
        assert rec["frequency"] == "hourly"

    def test_output_file_created(self, tmp_log_dir, tmp_output):
        now = datetime.now()
        entries = [
            {
                "ts": now.isoformat(),
                "interface": "equity_daily",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 100,
            },
        ]
        self._write_test_logs(tmp_log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=tmp_log_dir, output_path=tmp_output)
        analyzer.analyze(window_days=7)

        assert Path(tmp_output).exists()
        content = Path(tmp_output).read_text()
        assert "priorities" in content
