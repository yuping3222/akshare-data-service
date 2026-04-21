"""Tests for akshare_data.core.logging StatsCollector.

Covers:
- StatsCollector class
- record_request, record_cache_hit, record_cache_miss methods
- get_all_stats / get_source_stats / get_cache_stats methods
- get_stats_collector singleton function
"""

import pytest
from akshare_data.core.logging import (
    StatsCollector,
    get_stats_collector,
    reset_stats_collector,
)


class TestStatsCollector:
    """Test StatsCollector class."""

    def setup_method(self):
        """Reset singleton for each test."""
        reset_stats_collector()
        self.collector = StatsCollector()

    def test_initial_state(self):
        """Should start with empty stats."""
        stats = self.collector.get_all_stats()
        assert stats["summary"]["total_requests"] == 0
        assert stats["summary"]["cache_hits"] == 0
        assert stats["summary"]["cache_misses"] == 0

    def test_record_request_increments_total(self):
        """record_request should increment request counter."""
        self.collector.record_request("test_source", 100.0, True)
        stats = self.collector.get_source_stats("test_source")
        assert stats["total_requests"] == 1
        assert stats["successful_requests"] == 1

    def test_record_request_tracks_failure(self):
        """record_request should track failed requests."""
        self.collector.record_request("test_source", 200.0, False, "TimeoutError")
        stats = self.collector.get_source_stats("test_source")
        assert stats["total_requests"] == 1
        assert stats["failed_requests"] == 1
        assert "TimeoutError" in stats["errors"]

    def test_record_request_tracks_duration(self):
        """record_request should track duration stats."""
        self.collector.record_request("test_source", 50.0, True)
        self.collector.record_request("test_source", 150.0, True)
        stats = self.collector.get_source_stats("test_source")
        assert stats["avg_duration_ms"] == 100.0
        assert stats["min_duration_ms"] == 50.0
        assert stats["max_duration_ms"] == 150.0

    def test_record_cache_hit_increments_hits(self):
        """record_cache_hit should increment cache hits."""
        self.collector.record_cache_hit("test_cache")
        stats = self.collector.get_cache_stats("test_cache")
        assert stats["hits"] == 1
        assert stats["misses"] == 0

    def test_record_cache_miss_increments_misses(self):
        """record_cache_miss should increment cache misses."""
        self.collector.record_cache_miss("test_cache")
        stats = self.collector.get_cache_stats("test_cache")
        assert stats["hits"] == 0
        assert stats["misses"] == 1

    def test_multiple_recordings(self):
        """Should accumulate multiple recordings."""
        self.collector.record_request("src_a", 10.0, True)
        self.collector.record_request("src_a", 20.0, True)
        self.collector.record_request("src_b", 30.0, False, "Error")
        self.collector.record_cache_hit("cache_x")
        self.collector.record_cache_hit("cache_x")
        self.collector.record_cache_miss("cache_x")

        all_stats = self.collector.get_all_stats()
        assert all_stats["summary"]["total_requests"] == 3
        assert all_stats["summary"]["total_success"] == 2
        assert all_stats["summary"]["total_failed"] == 1
        assert all_stats["summary"]["cache_hits"] == 2
        assert all_stats["summary"]["cache_misses"] == 1

    def test_get_all_stats_returns_dict(self):
        """get_all_stats should return a dictionary."""
        stats = self.collector.get_all_stats()
        assert isinstance(stats, dict)
        assert "request_stats" in stats
        assert "cache_stats" in stats
        assert "summary" in stats

    def test_cache_hit_rate_calculation(self):
        """Cache hit rate should be calculated correctly."""
        for _ in range(4):
            self.collector.record_cache_hit("cache")
        for _ in range(2):
            self.collector.record_cache_miss("cache")

        stats = self.collector.get_cache_stats("cache")
        assert stats["hit_rate"] == pytest.approx(0.6667, rel=0.01)

    def test_success_rate_calculation(self):
        """Success rate should be calculated correctly."""
        for _ in range(8):
            self.collector.record_request("src", 10.0, True)
        for _ in range(2):
            self.collector.record_request("src", 10.0, False)

        stats = self.collector.get_source_stats("src")
        assert stats["success_rate"] == pytest.approx(0.8, rel=0.01)

    def test_get_summary_text(self):
        """get_summary_text should return formatted string."""
        self.collector.record_request("src", 100.0, True)
        self.collector.record_cache_hit("cache")
        text = self.collector.get_summary_text()
        assert isinstance(text, str)
        assert "Stats Summary" in text

    def test_reset_clears_stats(self):
        """reset should clear all stats."""
        self.collector.record_request("src", 10.0, True)
        self.collector.record_cache_hit("cache")
        self.collector.reset()
        stats = self.collector.get_all_stats()
        assert stats["summary"]["total_requests"] == 0
        assert stats["summary"]["cache_hits"] == 0


class TestGetStatsCollectorSingleton:
    """Test get_stats_collector() singleton function."""

    def setup_method(self):
        reset_stats_collector()

    def test_returns_stats_collector_instance(self):
        """Should return StatsCollector instance."""
        collector = get_stats_collector()
        assert isinstance(collector, StatsCollector)

    def test_returns_same_instance(self):
        """Should return the same instance on multiple calls."""
        collector1 = get_stats_collector()
        collector2 = get_stats_collector()
        assert collector1 is collector2

    def test_accumulates_stats(self):
        """Should accumulate stats across calls."""
        collector1 = get_stats_collector()
        collector1.record_request("src", 10.0, True)

        collector2 = get_stats_collector()
        collector2.record_cache_hit("cache")

        stats = collector2.get_all_stats()
        assert stats["summary"]["total_requests"] == 1
        assert stats["summary"]["cache_hits"] == 1
