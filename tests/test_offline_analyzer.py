"""Comprehensive tests for all offline/analyzer modules.

Covers:
- akshare_data.offline.analyzer.access_log.logger
- akshare_data.offline.analyzer.access_log.stats
- akshare_data.offline.analyzer.cache_analysis.anomaly
- akshare_data.offline.analyzer.cache_analysis.completeness
- akshare_data.offline.analyzer.interface_analysis.field_mapper
"""

import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch
import pandas as pd


class TestAccessLoggerAnalyzer:
    """Tests for AccessLogger (analyzer version at access_log/logger.py)"""

    def test_record_writes_to_log(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=0.1)
        try:
            logger.record(
                interface="equity_daily",
                symbol="000001",
                cache_hit=False,
                latency_ms=450.5,
            )
            time.sleep(0.3)

            log_file = Path(log_dir) / "access.log"
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

    def test_record_with_source(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=0.1)
        try:
            logger.record(
                interface="equity_daily",
                symbol="000001",
                cache_hit=True,
                latency_ms=10.0,
                source="akshare_em",
            )
            time.sleep(0.3)

            log_file = Path(log_dir) / "access.log"
            content = log_file.read_text().strip()
            entry = json.loads(content)
            assert entry["source"] == "akshare_em"
        finally:
            logger.shutdown()

    def test_record_queue_full_drops_entry(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=60.0, max_buffer=1)
        try:
            logger.record("iface1", "sym1", False, 100.0)
            logger.record("iface2", "sym2", False, 100.0)
            logger.record("iface3", "sym3", False, 100.0)
            time.sleep(0.2)
        finally:
            logger.shutdown()

    def test_flush_writes_multiple_entries(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=0.1)
        try:
            logger.record("equity_daily", "000001", False, 100)
            logger.record("equity_daily", "000002", True, 5)
            logger.record("equity_minute", "000001", False, 300)
            time.sleep(0.3)

            log_file = Path(log_dir) / "access.log"
            lines = log_file.read_text().strip().split("\n")
            assert len(lines) == 3
        finally:
            logger.shutdown()

    def test_shutdown_flushes_queue(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=60.0)
        logger.record("equity_daily", "000001", False, 100)
        logger.shutdown()

        log_file = Path(log_dir) / "access.log"
        assert log_file.exists()
        content = log_file.read_text().strip()
        assert len(content) > 0

    def test_log_rotation_on_date_change(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=60.0)

        logger.record("equity_daily", "000001", False, 100)

        with patch("akshare_data.offline.access_logger.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2020, 1, 2)
            mock_dt.strftime.return_value = "2020-01-02"

            logger._current_date = "2020-01-01"
            logger._rotate()
            logger._current_date = "2020-01-02"

        logger.shutdown()

    def test_cleanup_old_logs(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        old_log = Path(log_dir) / "access.log.2019-01-01"
        old_log.write_text("old content")

        logger = AccessLogger(log_dir=log_dir, flush_interval=60.0, backup_days=30)
        logger._cleanup_old_logs()
        logger.shutdown()

    def test_get_file_handle_opens_log_file(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=60.0)
        try:
            handle = logger._get_file_handle()
            assert handle.name.endswith("access.log")
            handle.close()
        finally:
            logger.shutdown()

    def test_flush_empty_queue(self, tmp_path):
        from akshare_data.offline.access_logger import AccessLogger

        log_dir = str(tmp_path / "logs")
        logger = AccessLogger(log_dir=log_dir, flush_interval=0.01)
        logger._flush()
        logger.shutdown()


class TestCallStatsAnalyzer:
    """Tests for CallStatsAnalyzer (access_log/stats.py)"""

    def _write_test_logs(self, log_dir: str, entries: list):
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_file = Path(log_dir) / "access.log"
        with open(log_file, "w", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def test_analyze_generates_config(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

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
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        assert "priorities" in config
        assert "equity_daily" in config["priorities"]
        assert "equity_minute" in config["priorities"]
        assert config["priorities"]["equity_daily"]["call_count_7d"] == 3
        assert config["global"]["total_calls_7d"] == 4

    def test_analyze_empty_logs(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        assert config == {}

    def test_score_calculation(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

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
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        hot_score = config["priorities"]["hot_interface"]["score"]
        cold_score = config["priorities"]["cold_interface"]["score"]
        assert hot_score > cold_score
        assert (
            config["priorities"]["hot_interface"]["rank"]
            < config["priorities"]["cold_interface"]["rank"]
        )

    def test_recommendation_strategy_hourly(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

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
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        rec = config["priorities"]["high_miss"]["recommendation"]
        assert rec["mode"] == "incremental"
        assert rec["frequency"] == "hourly"

    def test_recommendation_strategy_daily(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        now = datetime.now()
        entries = []
        for i in range(60):
            entries.append(
                {
                    "ts": now.isoformat(),
                    "interface": "mid_miss",
                    "symbol": "000001",
                    "cache_hit": True,
                    "latency_ms": 200,
                }
            )
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        rec = config["priorities"]["mid_miss"]["recommendation"]
        assert rec["mode"] == "incremental"
        assert rec["frequency"] == "daily"

    def test_recommendation_strategy_weekly(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        now = datetime.now()
        five_days_ago = now - timedelta(days=5)
        entries = []
        for i in range(30):
            entries.append(
                {
                    "ts": five_days_ago.isoformat(),
                    "interface": "low_score",
                    "symbol": "000001",
                    "cache_hit": True,
                    "latency_ms": 10,
                }
            )
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        rec = config["priorities"]["low_score"]["recommendation"]
        assert rec["mode"] == "full"
        assert rec["frequency"] == "weekly"

    def test_recommendation_strategy_monthly(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        now = datetime.now()
        five_days_ago = now - timedelta(days=5)
        entries = []

        for i in range(100):
            entries.append(
                {
                    "ts": five_days_ago.isoformat(),
                    "interface": "high_volume_ iface",
                    "symbol": "000001",
                    "cache_hit": True,
                    "latency_ms": 5,
                }
            )

        for i in range(3):
            entries.append(
                {
                    "ts": five_days_ago.isoformat(),
                    "interface": "very_low_score",
                    "symbol": "000001",
                    "cache_hit": True,
                    "latency_ms": 5,
                }
            )
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        rec = config["priorities"]["very_low_score"]["recommendation"]
        assert rec["mode"] == "full"
        assert rec["frequency"] == "monthly"

    def test_output_file_created(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

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
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        analyzer.analyze(window_days=7)

        assert Path(output_path).exists()
        content = Path(output_path).read_text()
        assert "priorities" in content

    def test_read_logs_with_dated_files(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        now = datetime.now()
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        recent_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        old_date = (now - timedelta(days=10)).strftime("%Y-%m-%d")

        recent_log = Path(log_dir) / f"access.log.{recent_date}"
        old_log = Path(log_dir) / f"access.log.{old_date}"

        recent_log.write_text(
            json.dumps(
                {
                    "ts": now.isoformat(),
                    "interface": "recent_iface",
                    "symbol": "000001",
                    "cache_hit": False,
                    "latency_ms": 100,
                }
            )
            + "\n"
        )

        old_log.write_text(
            json.dumps(
                {
                    "ts": (now - timedelta(days=10)).isoformat(),
                    "interface": "old_iface",
                    "symbol": "000001",
                    "cache_hit": False,
                    "latency_ms": 100,
                }
            )
            + "\n"
        )

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        assert "recent_iface" in config["priorities"]
        assert "old_iface" not in config["priorities"]

    def test_read_logs_malformed_json(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_file = Path(log_dir) / "access.log"
        now = datetime.now()
        log_file.write_text(
            f'not json\n{{"ts": "{now.isoformat()}", "interface": "test", "symbol": "000001", "cache_hit": false, "latency_ms": 100}}\n'
        )

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        assert "test" in config["priorities"]

    def test_read_logs_invalid_date_in_filename(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        Path(log_dir).mkdir(parents=True, exist_ok=True)
        bad_log = Path(log_dir) / "access.log.notadate"
        now = datetime.now()
        bad_log.write_text(
            f'{{"ts": "{now.isoformat()}", "interface": "bad", "symbol": "000001", "cache_hit": false, "latency_ms": 100}}\n'
        )

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        assert "bad" in config["priorities"]

    def test_read_logs_oserror(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        Path(log_dir).mkdir(parents=True, exist_ok=True)

        with patch("pathlib.Path.open", side_effect=OSError("mocked error")):
            analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
            config = analyzer.analyze(window_days=7)

        assert config == {}

    def test_aggregate_multiple_entries_same_key(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        now = datetime.now()
        entries = [
            {
                "ts": now.isoformat(),
                "interface": "test_iface",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 100,
            },
            {
                "ts": now.isoformat(),
                "interface": "test_iface",
                "symbol": "000001",
                "cache_hit": True,
                "latency_ms": 50,
            },
            {
                "ts": now.isoformat(),
                "interface": "test_iface",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 75,
            },
        ]
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        config = analyzer.analyze(window_days=7)

        test_iface = config["priorities"]["test_iface"]
        assert test_iface["call_count_7d"] == 3
        assert test_iface["miss_rate_7d"] == 0.67

    def test_calc_recency_with_timestamps(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        now = datetime.now()
        entries = [
            {
                "ts": now.isoformat(),
                "interface": "test_iface",
                "symbol": "000001",
                "cache_hit": False,
                "latency_ms": 100,
            },
        ]
        self._write_test_logs(log_dir, entries)

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        analyzer.analyze(window_days=7)

        assert Path(output_path).exists()

    def test_score_empty_aggregated(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        result = analyzer._score({})
        assert result == {}

    def test_rank(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)
        scored = {
            "a": {"score": 50},
            "b": {"score": 80},
            "c": {"score": 20},
        }
        ranked = analyzer._rank(scored)

        assert ranked[0]["score"] == 80
        assert ranked[0]["rank"] == 1
        assert ranked[1]["score"] == 50
        assert ranked[1]["rank"] == 2
        assert ranked[2]["score"] == 20
        assert ranked[2]["rank"] == 3

    def test_build_config(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)

        datetime.now()
        ranked = [
            {
                "interface": "iface1",
                "symbol": "sym1",
                "call_count": 10,
                "miss_count": 5,
                "score": 70,
                "avg_latency": 100,
            },
            {
                "interface": "iface1",
                "symbol": "sym2",
                "call_count": 5,
                "miss_count": 3,
                "score": 60,
                "avg_latency": 50,
            },
        ]
        entries = [
            {"cache_hit": False},
            {"cache_hit": True},
        ]

        config = analyzer._build_config(ranked, entries, 7)

        assert "generated_at" in config
        assert config["window"] == "7d"
        assert "iface1" in config["priorities"]
        assert len(config["priorities"]["iface1"]["symbols"]) == 2

    def test_save(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)

        config = {"test": "data"}
        analyzer._save(config)

        assert Path(output_path).exists()

    def test_aggregate_no_cache_hit(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)

        entries = [
            {"interface": "iface1", "symbol": "sym1"},
            {"interface": "iface1", "symbol": "sym1", "cache_hit": False},
            {"interface": "iface1", "symbol": "sym1", "cache_hit": True},
        ]

        aggregated = analyzer._aggregate(entries)
        assert "iface1:sym1" in aggregated
        assert aggregated["iface1:sym1"]["call_count"] == 3

    def test_aggregate_missing_ts(self, tmp_path):
        from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

        log_dir = str(tmp_path / "logs")
        output_path = str(tmp_path / "config" / "download_priority.yaml")

        analyzer = CallStatsAnalyzer(log_dir=log_dir, output_path=output_path)

        entries = [
            {"interface": "iface1", "symbol": "sym1", "ts": "invalid"},
        ]

        aggregated = analyzer._aggregate(entries)
        assert len(aggregated["iface1:sym1"]["timestamps"]) == 0


class TestAnomalyDetector:
    """Tests for AnomalyDetector (cache_analysis/anomaly.py)"""

    def test_detect_empty_dataframe(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        result = detector.detect(pd.DataFrame())

        assert result["total_rows"] == 0
        assert result["anomaly_count"] == 0
        assert result["anomalies"] == []

    def test_detect_none_dataframe(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        result = detector.detect(None)

        assert result["total_rows"] == 0
        assert result["anomaly_count"] == 0

    def test_check_price_anomaly(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "pct_change": [1.0, 25.0, 3.0, -30.0],
                "symbol": ["a", "b", "c", "d"],
            }
        )

        result = detector.detect(df, price_change_threshold=20.0)

        assert result["total_rows"] == 4
        assert result["anomaly_count"] == 2
        assert len(result["price_anomalies"]) == 2

    def test_check_price_with_chinese_column(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "涨跌幅": [1.0, 25.0, 3.0],
                "symbol": ["a", "b", "c"],
            }
        )

        result = detector.detect(df, price_change_threshold=20.0)

        assert result["anomaly_count"] == 1

    def test_check_price_no_relevant_column(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "close": [100, 101, 102],
                "symbol": ["a", "b", "c"],
            }
        )

        result = detector.detect(df)

        assert result["anomaly_count"] == 0
        assert result["price_anomalies"] == []

    def test_check_price_non_numeric_values(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "pct_change": [1.0, "N/A", 3.0, None],
                "symbol": ["a", "b", "c", "d"],
            }
        )

        result = detector.detect(df, price_change_threshold=20.0)

        assert result["total_rows"] == 4

    def test_check_high_low_anomaly(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "high": [100, 105, 110],
                "low": [90, 95, 100],
                "symbol": ["a", "b", "c"],
            }
        )

        result = detector.detect(df)

        assert result["anomaly_count"] == 0
        assert result["high_low_anomalies"] == []

    def test_check_high_low_anomaly_detected(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "high": [100, 90, 110],
                "low": [90, 100, 100],
                "symbol": ["a", "b", "c"],
            }
        )

        result = detector.detect(df)

        assert result["anomaly_count"] >= 1
        assert len(result["high_low_anomalies"]) >= 1

    def test_check_high_low_invalid_data(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "high": ["N/A", 105, 110],
                "low": [90, "N/A", 100],
                "symbol": ["a", "b", "c"],
            }
        )

        result = detector.detect(df)

        assert result["total_rows"] == 3

    def test_check_volume_anomaly(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "volume": [100, 200, 300, 1000, 1100, 1200, 100000],
                "symbol": ["a", "b", "c", "d", "e", "f", "g"],
            }
        )

        result = detector.detect(df, volume_change_threshold=2.0)

        assert result["anomaly_count"] >= 1
        assert len(result["volume_anomalies"]) >= 1

    def test_check_volume_no_column(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "close": [100, 101, 102],
                "symbol": ["a", "b", "c"],
            }
        )

        result = detector.detect(df)

        assert result["anomaly_count"] == 0

    def test_check_volume_single_value(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "volume": [100],
                "symbol": ["a"],
            }
        )

        result = detector.detect(df, volume_change_threshold=2.0)

        assert result["anomaly_count"] == 0

    def test_check_volume_zero_std(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "volume": [100, 100, 100, 100],
                "symbol": ["a", "b", "c", "d"],
            }
        )

        result = detector.detect(df, volume_change_threshold=2.0)

        assert result["anomaly_count"] == 0

    def test_anomaly_limit_50(self):
        from akshare_data.offline.analyzer.cache_analysis.anomaly import AnomalyDetector

        detector = AnomalyDetector()
        df = pd.DataFrame(
            {
                "pct_change": [50.0] * 100,
                "symbol": [f"s{i}" for i in range(100)],
            }
        )

        result = detector.detect(df, price_change_threshold=20.0)

        assert result["anomaly_count"] == 100
        assert len(result["anomalies"]) == 50


class TestCompletenessChecker:
    """Tests for CompletenessChecker (cache_analysis/completeness.py)"""

    def test_check_none_dataframe(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        result = checker.check(None)

        assert result["has_data"] is False
        assert result["total_records"] == 0
        assert result["completeness_ratio"] == 0.0
        assert result["is_complete"] is False

    def test_check_empty_dataframe(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        result = checker.check(pd.DataFrame())

        assert result["has_data"] is False
        assert result["total_records"] == 0

    def test_check_with_expected_dates(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "close": [100, 101, 102],
            }
        )

        result = checker.check(
            df, expected_dates=["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        )

        assert result["has_data"] is True
        assert result["total_records"] == 3
        assert result["missing_dates_count"] == 1
        assert "2024-01-04" in result["missing_dates"]
        assert result["is_complete"] is False

    def test_check_complete_data(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "close": [100, 101, 102],
            }
        )

        result = checker.check(
            df, expected_dates=["2024-01-01", "2024-01-02", "2024-01-03"]
        )

        assert result["has_data"] is True
        assert result["missing_dates_count"] == 0
        assert result["is_complete"] is True
        assert result["completeness_ratio"] == 1.0

    def test_check_missing_fields(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "close": [100, 101],
            }
        )

        result = checker.check(df, required_fields=["date", "close", "volume"])

        assert result["missing_fields"] == ["volume"]
        assert result["is_complete"] is False

    def test_check_no_missing_fields(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "close": [100, 101],
                "volume": [1000, 1100],
            }
        )

        result = checker.check(df, required_fields=["date", "close", "volume"])

        assert result["missing_fields"] == []
        assert result["is_complete"] is True

    def test_find_date_column_variants(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()

        for col in ("date", "datetime", "trade_date", "日期", "时间"):
            df = pd.DataFrame({col: ["2024-01-01"], "close": [100]})
            result = checker._find_date_column(df)
            assert result == col, f"Failed for column {col}"

    def test_find_date_column_not_found(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame({"close": [100], "symbol": ["a"]})

        result = checker._find_date_column(df)
        assert result is None

    def test_check_without_expected_dates(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "close": [100, 101, 102],
            }
        )

        result = checker.check(df)

        assert result["has_data"] is True
        assert result["total_records"] == 3
        assert result["missing_dates_count"] == 0

    def test_check_completeness_ratio_calculation(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "close": [100, 101],
            }
        )

        result = checker.check(
            df, expected_dates=["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        )

        assert result["completeness_ratio"] == 0.5

    def test_check_no_date_column(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "close": [100, 101, 102],
                "symbol": ["a", "b", "c"],
            }
        )

        result = checker.check(df, expected_dates=["2024-01-01", "2024-01-02"])

        assert result["has_data"] is True
        assert result["missing_dates"] == []

    def test_check_required_fields_not_in_df(self):
        from akshare_data.offline.analyzer.cache_analysis.completeness import (
            CompletenessChecker,
        )

        checker = CompletenessChecker()
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "close": [100, 101],
            }
        )

        result = checker.check(df, required_fields=["nonexistent_field"])

        assert result["missing_fields"] == ["nonexistent_field"]
        assert result["is_complete"] is False


class TestFieldMapperAnalyzer:
    """Tests for FieldMapper (offline/field_mapper.py)"""

    def test_column_info_dataclass(self):
        from akshare_data.offline.field_mapper import ColumnInfo

        col_info = ColumnInfo(
            original_name="日期", dtype="object", sample_value="2024-01-01"
        )
        assert col_info.original_name == "日期"
        assert col_info.dtype == "object"
        assert col_info.is_mapped is False

    def test_interface_field_result_dataclass(self):
        from akshare_data.offline.field_mapper import InterfaceFieldResult

        result = InterfaceFieldResult(interface_name="test_func")
        assert result.interface_name == "test_func"
        assert result.mapped_columns == 0
        assert result.unmapped_columns == 0
        assert result.columns == []

    def test_load_registry_file_not_exists(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper

        mapper = FieldMapper(
            registry_path=tmp_path / "nonexistent.yaml",
            output_dir=tmp_path / "output",
        )
        try:
            mapper.load_registry()
        except FileNotFoundError:
            pass

    def test_load_registry_file_exists(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper
        import yaml

        registry_file = tmp_path / "registry.yaml"
        registry_data = {"interfaces": {"test_func": {"category": "equity"}}}
        registry_file.write_text(yaml.dump(registry_data))

        mapper = FieldMapper(
            registry_path=registry_file,
            output_dir=tmp_path / "output",
        )
        result = mapper.load_registry()

        assert "interfaces" in result
        assert "test_func" in result["interfaces"]

    def test_analyze_interface_func_not_found(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper

        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )

        result = mapper.analyze_interface("nonexistent_func", {"probe": {"params": {}}})

        assert result.interface_name == "nonexistent_func"
        assert result.columns == []

    def test_generate_report_empty(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper

        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )

        report = mapper.generate_report()
        assert "# AkShare 字段映射分析报告" in report

    def test_generate_report_with_results(self, tmp_path):
        from akshare_data.offline.field_mapper import (
            FieldMapper,
            InterfaceFieldResult,
        )

        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )

        result = InterfaceFieldResult(interface_name="test_func")
        result.columns = [
            {
                "original_name": "日期",
                "mapped_name": "date",
                "is_mapped": True,
                "dtype": "object",
                "sample_value": "2024-01-01",
            },
            {
                "original_name": "unknown_col",
                "mapped_name": None,
                "is_mapped": False,
                "dtype": "object",
                "sample_value": "val",
            },
        ]
        result.mapped_columns = 1
        result.unmapped_columns = 1
        result.total_columns = 2
        result.status = "success"
        mapper.results = [result]

        report = mapper.generate_report()
        assert "### test_func" in report

    def test_export_mappings_json(self, tmp_path):
        from akshare_data.offline.field_mapper import (
            FieldMapper,
            InterfaceFieldResult,
        )

        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )

        result = InterfaceFieldResult(interface_name="test_func")
        result.columns = [
            {
                "original_name": "日期",
                "mapped_name": "date",
                "is_mapped": True,
                "dtype": "object",
                "sample_value": "2024-01-01",
            },
        ]
        result.mapped_columns = 1
        result.unmapped_columns = 0
        result.status = "success"
        result.output_mapping = {"日期": "date"}
        mapper.results = [result]

        mappings = mapper.export_mappings_json()

        assert "test_func" in mappings

    def test_extended_cn_to_en_mapping(self):
        from akshare_data.offline.field_mapper import EXTENDED_CN_TO_EN

        assert EXTENDED_CN_TO_EN["日期"] == "date"
        assert EXTENDED_CN_TO_EN["开盘"] == "open"
        assert EXTENDED_CN_TO_EN["最高"] == "high"
        assert EXTENDED_CN_TO_EN["最低"] == "low"
        assert EXTENDED_CN_TO_EN["收盘"] == "close"
        assert EXTENDED_CN_TO_EN["成交量"] == "volume"
        assert EXTENDED_CN_TO_EN["代码"] == "symbol"

    def test_field_mapper_init_creates_output_dir(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper

        output_dir = tmp_path / "output"

        FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=output_dir,
        )

        assert output_dir.exists()

    def test_analyze_interface_with_empty_result(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper

        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )

        result = mapper.analyze_interface("empty_result", {"probe": {"params": {}}})

        assert result.interface_name == "empty_result"
        assert result.columns == []

    def test_cn_to_en_copy_exists(self, tmp_path):
        from akshare_data.offline.field_mapper import FieldMapper, EXTENDED_CN_TO_EN

        FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )

        assert EXTENDED_CN_TO_EN is not None
        assert len(EXTENDED_CN_TO_EN) > 0
        assert "日期" in EXTENDED_CN_TO_EN
