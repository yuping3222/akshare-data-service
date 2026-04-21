"""Integration tests for multi-source routing, failover, and circuit breaker behavior.

These tests use REAL MultiSourceRouter, DomainRateLimiter, and SourceHealthMonitor
classes, with MagicMock only for individual data source callables.

Test scenarios:
1. Primary source success: first provider returns data, backup sources NOT called
2. Primary failure -> backup success: first raises, second returns data
3. All sources fail: all raise -> aggregated error with correct ErrorCode
4. Circuit breaker trigger: N consecutive failures -> source circuit-opened
5. Circuit breaker recovery: after cooldown, probe succeeds -> source restored
6. Source priority ordering: providers tried in configured priority order
7. Rate limiter integration: requests are rate-limited per domain
8. Health tracking: SourceHealthMonitor correctly tracks and reports health status
"""

import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from akshare_data.sources.router import (
    DomainRateLimiter,
    EmptyDataPolicy,
    ExecutionResult,
    MultiSourceRouter,
    SourceHealthMonitor,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures specific to router failover integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df():
    """A simple non-empty DataFrame to use as successful source return value."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", "2024-01-05", freq="B"),
            "symbol": ["sh600000"] * 4,
            "open": [10.0, 10.1, 10.2, 10.3],
            "high": [11.0, 11.1, 11.2, 11.3],
            "low": [9.0, 9.1, 9.2, 9.3],
            "close": [10.5, 10.6, 10.7, 10.8],
            "volume": [100_000, 110_000, 120_000, 130_000],
        }
    )


@pytest.fixture
def empty_df():
    """An empty DataFrame to simulate a source that returns no data."""
    return pd.DataFrame()


# ===========================================================================
# 1. Primary source success: backup sources NOT called
# ===========================================================================


class TestPrimarySourceSuccess:
    """Verify that when the primary (first) provider succeeds,
    no backup providers are invoked."""

    def test_primary_returns_data_backups_not_called(self, sample_df):
        """Lixinger (primary) returns data; backup AKShare is never called."""
        primary = MagicMock(return_value=sample_df)
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("lixinger", primary), ("akshare", backup)]
        )
        result = router.execute(symbol="sh600000")

        assert result.success is True
        assert result.source == "lixinger"
        assert result.attempts == 1
        assert result.is_fallback is False
        primary.assert_called_once_with(symbol="sh600000")
        backup.assert_not_called()

    def test_primary_returns_data_health_recorded(self, sample_df):
        """Health monitor marks primary source as available after success."""
        primary = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(providers=[("lixinger", primary)])
        router.execute()

        assert router._health.is_available("lixinger") is True

    def test_primary_returns_data_stats_updated(self, sample_df):
        """Stats reflect a single successful call."""
        primary = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(providers=[("lixinger", primary)])
        router.execute()

        stats = router.get_stats()
        assert stats["total_calls"] == 1
        assert stats["successes"] == 1
        assert stats["fallbacks"] == 0
        assert stats["source_stats"]["lixinger"]["successes"] == 1

    def test_multiple_calls_primary_always_wins(self, sample_df):
        """Repeated calls always succeed on primary without touching backups."""
        primary = MagicMock(return_value=sample_df)
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("lixinger", primary), ("akshare", backup)]
        )
        for _ in range(5):
            router.execute()

        assert primary.call_count == 5
        backup.assert_not_called()


# ===========================================================================
# 2. Primary failure -> backup success
# ===========================================================================


class TestPrimaryFailureBackupSuccess:
    """When the primary provider raises an exception, the router falls over
    to the next provider in order."""

    def test_primary_fails_backup_succeeds(self, sample_df):
        """Lixinger raises, AKShare returns data."""
        error = RuntimeError("lixinger API down")
        primary = MagicMock(side_effect=error)
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("lixinger", primary), ("akshare", backup)]
        )
        result = router.execute(symbol="sh600000")

        assert result.success is True
        assert result.source == "akshare"
        assert result.attempts == 2
        assert result.is_fallback is True
        primary.assert_called_once()
        backup.assert_called_once()

    def test_error_details_contains_primary_failure(self, sample_df):
        """error_details records the primary source's exception message."""
        primary = MagicMock(side_effect=RuntimeError("connection refused"))
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("lixinger", primary), ("akshare", backup)]
        )
        result = router.execute()

        assert len(result.error_details) == 1
        assert result.error_details[0] == ("lixinger", "connection refused")

    def test_sources_tried_records_both_attempts(self, sample_df):
        """sources_tried includes metadata for each attempted provider."""
        primary = MagicMock(side_effect=RuntimeError("fail"))
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("lixinger", primary), ("akshare", backup)]
        )
        result = router.execute()

        assert len(result.sources_tried) == 2
        assert result.sources_tried[0]["name"] == "lixinger"
        assert result.sources_tried[0]["success"] is False
        assert result.sources_tried[1]["name"] == "akshare"
        assert result.sources_tried[1]["success"] is True

    def test_fallback_chain_two_failures_then_success(self, sample_df):
        """Two providers fail before the third succeeds."""
        src1 = MagicMock(side_effect=RuntimeError("src1 down"))
        src2 = MagicMock(side_effect=TimeoutError("src2 timeout"))
        src3 = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("src1", src1), ("src2", src2), ("src3", src3)]
        )
        result = router.execute()

        assert result.success is True
        assert result.source == "src3"
        assert result.attempts == 3
        assert result.is_fallback is True
        assert len(result.error_details) == 2

    def test_primary_returns_none_tries_backup(self, sample_df):
        """Primary returning None is treated as empty and router tries next."""
        primary = MagicMock(return_value=None)
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("lixinger", primary), ("akshare", backup)]
        )
        result = router.execute()

        assert result.success is True
        assert result.source == "akshare"
        assert result.is_fallback is True


# ===========================================================================
# 3. All sources fail
# ===========================================================================


class TestAllSourcesFail:
    """When every provider raises an exception, the router returns a failure
    result with aggregated error details."""

    def test_all_fail_returns_failure(self):
        """All providers raise -> result.success is False."""
        src1 = MagicMock(side_effect=RuntimeError("src1 down"))
        src2 = MagicMock(side_effect=TimeoutError("src2 timeout"))

        router = MultiSourceRouter(
            providers=[("lixinger", src1), ("akshare", src2)]
        )
        result = router.execute()

        assert result.success is False
        assert result.error == "all_providers_failed"
        assert result.data is None
        assert result.source is None

    def test_error_details_aggregates_all_failures(self):
        """error_details contains one entry per failed provider."""
        src1 = MagicMock(side_effect=RuntimeError("error1"))
        src2 = MagicMock(side_effect=ValueError("error2"))

        router = MultiSourceRouter(
            providers=[("lixinger", src1), ("akshare", src2)]
        )
        result = router.execute()

        assert len(result.error_details) == 2
        assert result.error_details[0] == ("lixinger", "error1")
        assert result.error_details[1] == ("akshare", "error2")

    def test_all_fail_health_records_failures(self):
        """Health monitor records failure for each attempted source."""
        monitor = SourceHealthMonitor()
        src1 = MagicMock(side_effect=RuntimeError("fail"))
        src2 = MagicMock(side_effect=RuntimeError("fail"))

        router = MultiSourceRouter(
            providers=[("src1", src1), ("src2", src2)]
        )
        router._health = monitor
        router.execute()

        # Both sources should have recorded a failure (not yet disabled, only 1 error)
        status = monitor.get_status()
        assert status["src1"]["error_count"] == 1
        assert status["src2"]["error_count"] == 1

    def test_all_fail_stats_reflect_failure(self):
        """Stats show failure with no successes."""
        src1 = MagicMock(side_effect=RuntimeError("fail"))
        src2 = MagicMock(side_effect=RuntimeError("fail"))

        router = MultiSourceRouter(
            providers=[("src1", src1), ("src2", src2)]
        )
        router.execute()

        stats = router.get_stats()
        assert stats["total_calls"] == 1
        assert stats["successes"] == 0
        assert stats["failures"] == 1

    def test_single_provider_fail(self):
        """Single provider failure still returns proper error."""
        src = MagicMock(side_effect=RuntimeError("only source down"))
        router = MultiSourceRouter(providers=[("only", src)])
        result = router.execute()

        assert result.success is False
        assert result.error == "all_providers_failed"
        assert len(result.error_details) == 1


# ===========================================================================
# 4. Circuit breaker trigger
# ===========================================================================


class TestCircuitBreakerTrigger:
    """After N consecutive failures (threshold=5), the SourceHealthMonitor
    marks the source as unavailable and the router skips it."""

    def test_five_failures_disable_source(self):
        """5 consecutive failures -> source becomes unavailable."""
        monitor = SourceHealthMonitor()
        for i in range(5):
            monitor.record_result("bad_src", success=False, error=f"err{i}")

        assert monitor.is_available("bad_src") is False

    def test_router_skips_disabled_source(self, sample_df):
        """Router bypasses a circuit-opened source entirely."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("bad_src", success=False, error="err")

        # Even though the callable would succeed, it should never be called
        good_func = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(providers=[("bad_src", good_func)])
        router._health = monitor

        result = router.execute()

        assert result.success is False
        good_func.assert_not_called()

    def test_router_tries_remaining_sources_when_first_disabled(
        self, sample_df
    ):
        """When primary is circuit-opened, router tries backup sources."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("primary", success=False, error="err")

        backup = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(
            providers=[("primary", MagicMock()), ("backup", backup)]
        )
        router._health = monitor

        result = router.execute()

        assert result.success is True
        assert result.source == "backup"
        # is_fallback is False because the skipped (circuit-opened) primary
        # was never attempted, so sources_tried is empty when backup runs.
        # The router only sets is_fallback=True when len(sources_tried) > 0.
        assert result.is_fallback is False
        assert result.attempts == 1

    def test_circuit_breaker_error_tracking_in_stats(self):
        """Stats track per-source failure count during circuit breaker buildup."""
        src1 = MagicMock(side_effect=RuntimeError("fail"))
        src2 = MagicMock(side_effect=RuntimeError("fail"))

        router = MultiSourceRouter(
            providers=[("src1", src1), ("src2", src2)]
        )
        # Run enough times to trip the circuit breaker on src1
        for _ in range(5):
            router.execute()

        stats = router.get_stats()
        assert stats["total_calls"] == 5
        # src1 failed 5 times, then was skipped
        assert stats["source_stats"]["src1"]["failures"] == 5

    def test_disabled_source_not_in_sources_tried(self, sample_df):
        """A circuit-opened source should not appear in sources_tried."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("disabled", success=False, error="err")

        backup = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(
            providers=[("disabled", MagicMock()), ("backup", backup)]
        )
        router._health = monitor

        result = router.execute()

        tried_names = [t["name"] for t in result.sources_tried]
        assert "disabled" not in tried_names
        assert "backup" in tried_names


# ===========================================================================
# 5. Circuit breaker recovery
# ===========================================================================


class TestCircuitBreakerRecovery:
    """After the disable duration (300s default) elapses, the health monitor
    re-enables the source. A successful probe call fully restores it."""

    def test_source_recovers_after_cooldown(self, sample_df):
        """After cooldown period, disabled source becomes available again."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="err")

        assert monitor.is_available("src1") is False

        # Advance time past the 300-second disable duration
        with patch.object(time, "time", return_value=time.time() + 400):
            assert monitor.is_available("src1") is True

    def test_probe_success_fully_restores_source(self, sample_df):
        """After recovery time, a successful probe call resets error count."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="err")

        func = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(providers=[("src1", func)])
        router._health = monitor

        # Advance time past cooldown
        with patch.object(time, "time", return_value=time.time() + 400):
            result = router.execute()

        assert result.success is True
        assert result.source == "src1"
        status = monitor.get_status()
        assert status["src1"]["error_count"] == 0
        assert status["src1"]["available"] is True

    def test_probe_failure_re_disables_source(self):
        """After recovery time, if probe fails again, source re-enters circuit-open."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="err")

        # Advance past cooldown
        with patch.object(time, "time", return_value=time.time() + 400):
            assert monitor.is_available("src1") is True
            # Simulate one more failure while "available" (time still advanced)
            monitor.record_result("src1", success=False, error="probe failed")
            # Need 4 more to trip again (already at 1 after reset... actually
            # the reset clears error_count to 0, then this adds 1)
            # We need to check the actual logic: after recovery time,
            # is_available resets error_count to 0, but record_result adds 1
            # So we need 4 more failures
            for _ in range(4):
                monitor.record_result("src1", success=False, error="err")
            assert monitor.is_available("src1") is False

    def test_recovery_integration_full_cycle(self, sample_df):
        """Full cycle: healthy -> disabled -> recovered -> healthy."""
        func = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(providers=[("src1", func)])
        monitor = router._health

        # Phase 1: healthy
        router.execute()
        assert monitor.is_available("src1") is True

        # Phase 2: simulate 5 failures to disable
        failing_func = MagicMock(side_effect=RuntimeError("down"))
        router.providers = [("src1", failing_func)]
        for _ in range(5):
            router.execute()
        assert monitor.is_available("src1") is False

        # Phase 3: advance time, restore
        router.providers = [("src1", func)]
        with patch.object(time, "time", return_value=time.time() + 400):
            result = router.execute()
            assert result.success is True
            assert monitor.is_available("src1") is True


# ===========================================================================
# 6. Source priority ordering
# ===========================================================================


class TestSourcePriorityOrdering:
    """Providers are tried in the exact order they appear in the providers list."""

    def test_providers_tried_in_order(self, sample_df):
        """First provider in list is attempted first."""
        call_order = []

        def src_a(**kwargs):
            call_order.append("A")
            return sample_df

        def src_b(**kwargs):
            call_order.append("B")
            return sample_df

        router = MultiSourceRouter(
            providers=[("source_a", src_a), ("source_b", src_b)]
        )
        router.execute()
        assert call_order == ["A"]

    def test_fallback_follows_list_order(self, sample_df):
        """When primary fails, secondary is tried next per list order."""
        call_order = []

        def src_a(**kwargs):
            call_order.append("A")
            raise RuntimeError("A down")

        def src_b(**kwargs):
            call_order.append("B")
            return sample_df

        def src_c(**kwargs):
            call_order.append("C")
            return sample_df

        router = MultiSourceRouter(
            providers=[
                ("source_a", src_a),
                ("source_b", src_b),
                ("source_c", src_c),
            ]
        )
        result = router.execute()

        assert call_order == ["A", "B"]
        assert result.source == "source_b"
        # source_c never tried
        assert "C" not in call_order

    def test_reversed_priority(self, sample_df):
        """Reversing provider order changes which source is tried first."""
        primary = MagicMock(return_value=sample_df)
        backup = MagicMock(return_value=sample_df)

        # Order 1: primary first
        router1 = MultiSourceRouter(
            providers=[("primary", primary), ("backup", backup)]
        )
        router1.execute()
        primary.assert_called_once()
        backup.assert_not_called()

        # Order 2: backup first (reset mocks)
        primary.reset_mock()
        backup.reset_mock()
        router2 = MultiSourceRouter(
            providers=[("backup", backup), ("primary", primary)]
        )
        result = router2.execute()

        assert result.source == "backup"
        backup.assert_called_once()
        primary.assert_not_called()

    def test_circuit_breaker_respects_priority(self, sample_df):
        """Disabled high-priority source is skipped, next priority is tried."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("high_pri", success=False, error="err")

        low_pri = MagicMock(return_value=sample_df)
        router = MultiSourceRouter(
            providers=[("high_pri", MagicMock()), ("low_pri", low_pri)]
        )
        router._health = monitor
        result = router.execute()

        assert result.source == "low_pri"


# ===========================================================================
# 7. Rate limiter integration
# ===========================================================================


class TestRateLimiterIntegration:
    """DomainRateLimiter enforces minimum intervals between requests per domain."""

    def test_first_request_no_wait(self):
        """First request to a domain never waits."""
        limiter = DomainRateLimiter(
            intervals={"default": 1.0},
            default_interval=1.0,
        )
        start = time.time()
        limiter.wait_if_needed("default")
        elapsed = time.time() - start
        assert elapsed < 0.1

    def test_second_request_within_interval_waits(self):
        """Second request before interval elapses must wait."""
        limiter = DomainRateLimiter(
            intervals={"test": 0.5},
            domain_map={"test.com": "test"},
            default_interval=0.5,
        )
        limiter.wait_if_needed("test.com")  # first request, no wait
        start = time.time()
        limiter.wait_if_needed("test.com")  # second request, must wait
        elapsed = time.time() - start
        assert elapsed >= 0.4  # allow small tolerance

    def test_different_domains_independent(self):
        """Rate limits for different domains are tracked independently."""
        limiter = DomainRateLimiter(
            intervals={"domain_a": 0.5, "domain_b": 0.5},
            domain_map={"a.com": "domain_a", "b.com": "domain_b"},
            default_interval=0.5,
        )
        limiter.wait_if_needed("a.com")
        start = time.time()
        limiter.wait_if_needed("b.com")  # different domain, no wait
        elapsed = time.time() - start
        assert elapsed < 0.1

    def test_rate_limiter_tracks_requests(self):
        """record_request updates internal tracking."""
        limiter = DomainRateLimiter(intervals={"test": 1.0}, default_interval=1.0)
        limiter.record_request("test")
        assert "test" in limiter._last_request

    def test_rate_limiter_reset_clears_state(self):
        """reset() clears all recorded request timestamps."""
        limiter = DomainRateLimiter(intervals={"test": 1.0}, default_interval=1.0)
        limiter.record_request("test")
        limiter.reset()
        assert len(limiter._last_request) == 0

    def test_domain_resolution_with_map(self):
        """Domain map correctly resolves hostnames to rate keys."""
        limiter = DomainRateLimiter(
            intervals={"em_push2his": 0.5, "default": 0.3},
            domain_map={"push2his.eastmoney.com": "em_push2his"},
            default_interval=0.3,
        )
        assert limiter.get_rate_key("push2his.eastmoney.com") == "em_push2his"
        assert limiter.get_rate_key("unknown.com") == "default"

    def test_extract_domain_from_url(self):
        """Static method correctly extracts domain from URLs."""
        assert (
            DomainRateLimiter.extract_domain("https://api.example.com/v1/data")
            == "api.example.com"
        )
        assert (
            DomainRateLimiter.extract_domain("http://localhost:8080/path")
            == "localhost:8080"
        )

    def test_set_interval_changes_rate(self):
        """set_interval() updates the rate limit for a key."""
        limiter = DomainRateLimiter(intervals={"default": 0.3}, default_interval=0.3)
        limiter.set_interval("default", 2.0)
        assert limiter.get_interval("default") == 2.0


# ===========================================================================
# 8. Health tracking
# ===========================================================================


class TestHealthTracking:
    """SourceHealthMonitor correctly tracks and reports health status."""

    def test_unknown_source_is_available(self):
        """A source with no history is considered available."""
        monitor = SourceHealthMonitor()
        assert monitor.is_available("never_seen_before") is True

    def test_single_success(self):
        """A single success marks the source as available with zero errors."""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=True)
        status = monitor.get_status()
        assert status["src1"]["available"] is True
        assert status["src1"]["error_count"] == 0
        assert status["src1"]["last_error"] is None

    def test_single_failure_below_threshold(self):
        """A few failures (below threshold) do not disable the source."""
        monitor = SourceHealthMonitor()
        for _ in range(4):
            monitor.record_result("src1", success=False, error="err")

        assert monitor.is_available("src1") is True
        status = monitor.get_status()
        assert status["src1"]["error_count"] == 4

    def test_exact_threshold_disables_source(self):
        """Exactly 5 consecutive failures disable the source."""
        monitor = SourceHealthMonitor()
        for i in range(5):
            monitor.record_result("src1", success=False, error=f"err{i}")

        assert monitor.is_available("src1") is False
        status = monitor.get_status()
        assert "disabled_at" in status["src1"]

    def test_success_resets_error_count(self):
        """A success after partial failures resets the error count."""
        monitor = SourceHealthMonitor()
        for _ in range(3):
            monitor.record_result("src1", success=False, error="err")

        assert monitor.get_status()["src1"]["error_count"] == 3

        monitor.record_result("src1", success=True)
        assert monitor.get_status()["src1"]["error_count"] == 0
        assert monitor.is_available("src1") is True

    def test_get_status_returns_deep_copy(self):
        """get_status() returns a copy; modifying it does not affect internal state."""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=True)

        status = monitor.get_status()
        status["src1"]["available"] = False

        assert monitor.get_status()["src1"]["available"] is True

    def test_mixed_success_failure(self):
        """Interleaved successes and failures: only consecutive failures count."""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=False, error="err1")
        monitor.record_result("src1", success=False, error="err2")
        monitor.record_result("src1", success=True)  # resets count
        monitor.record_result("src1", success=False, error="err3")
        monitor.record_result("src1", success=False, error="err4")

        # Only 2 consecutive failures, below threshold of 5
        assert monitor.is_available("src1") is True
        assert monitor.get_status()["src1"]["error_count"] == 2

    def test_multiple_sources_tracked_independently(self):
        """Each source's health is tracked independently."""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("bad", success=False, error="err")
        monitor.record_result("good", success=True)

        assert monitor.is_available("bad") is False
        assert monitor.is_available("good") is True

    def test_status_includes_error_history(self):
        """Status includes last_error and error_count."""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=False, error="connection timeout")

        status = monitor.get_status()
        assert status["src1"]["last_error"] == "connection timeout"
        assert status["src1"]["error_count"] == 1

    def test_router_execute_records_health(self, sample_df):
        """Router.execute() records health for both success and failure."""
        primary = MagicMock(side_effect=RuntimeError("fail"))
        backup = MagicMock(return_value=sample_df)

        router = MultiSourceRouter(
            providers=[("primary", primary), ("backup", backup)]
        )
        router.execute()

        assert router._health.get_status()["primary"]["error_count"] == 1
        assert router._health.get_status()["backup"]["error_count"] == 0
        assert router._health.get_status()["backup"]["available"] is True
