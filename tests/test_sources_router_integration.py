"""Integration tests for router.py and MultiSourceRouter with real data."""
import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from akshare_data.sources.router import (
    MultiSourceRouter,
    DomainRateLimiter,
    SourceHealthMonitor,
    create_simple_router,
    EmptyDataPolicy,
)
from akshare_data.core.errors import SourceUnavailableError


class TestDomainRateLimiter:
    """Test DomainRateLimiter with real config."""

    def test_default_limiter(self):
        limiter = DomainRateLimiter(intervals={"default": 0.1})
        limiter.wait_if_needed("default")

    def test_rate_limiter_sets_interval(self):
        limiter = DomainRateLimiter(intervals={"default": 0.1})
        limiter.set_interval("test", 0.05)
        limiter.wait_if_needed("test")

    def test_rate_limiter_respects_interval(self):
        """Test that rapid calls are properly rate-limited."""
        import time
        limiter = DomainRateLimiter(intervals={"test": 0.2})
        limiter.wait_if_needed("test")
        start = time.time()
        limiter.wait_if_needed("test")
        elapsed = time.time() - start
        assert elapsed >= 0.15

    def test_domain_map_routing(self):
        limiter = DomainRateLimiter(
            intervals={"default": 0.1, "api.example.com": 0.5},
            domain_map={"api.example.com": "api.example.com"}
        )
        limiter.wait_if_needed("api.example.com")

    def test_resolve_rate_key_fallback(self):
        limiter = DomainRateLimiter(
            intervals={"default": 0.1},
            domain_map={}
        )
        limiter.wait_if_needed("unknown.domain.com")


class TestSourceHealthMonitor:
    """Test SourceHealthMonitor."""

    def test_record_success(self):
        monitor = SourceHealthMonitor()
        monitor.record_result("lixinger", success=True)
        assert monitor.is_available("lixinger")

    def test_record_failure(self):
        monitor = SourceHealthMonitor()
        for _ in range(10):
            monitor.record_result("lixinger", success=False)

    def test_is_available_unknown_source(self):
        monitor = SourceHealthMonitor()
        assert monitor.is_available("unknown_source")

    def test_multiple_sources(self):
        monitor = SourceHealthMonitor()
        monitor.record_result("lixinger", success=True)
        monitor.record_result("akshare", success=False)
        assert monitor.is_available("lixinger")
        assert monitor.is_available("akshare")


class TestMultiSourceRouter:
    """Test MultiSourceRouter with real data sources."""

    @pytest.mark.integration
    @pytest.mark.network
    def test_router_execute_akshare_only(self):
        """Test router with only akshare source."""
        import akshare as ak
        def ak_daily():
            return ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20240105", adjust="qfq")
        router = MultiSourceRouter(
            providers=[("akshare", ak_daily)],
            policy=EmptyDataPolicy.STRICT,
        )
        try:
            result = router.execute()
            if result.data is not None:
                assert isinstance(result.data, pd.DataFrame)
        except SourceUnavailableError:
            pytest.skip("akshare equity_daily unavailable")

    @pytest.mark.integration
    @pytest.mark.network
    def test_router_execute_macro(self):
        """Test router with macro data."""
        import akshare as ak
        def ak_macro():
            return ak.macro_china_cpi_yearly()
        router = MultiSourceRouter(
            providers=[("akshare", ak_macro)],
            policy=EmptyDataPolicy.STRICT,
        )
        try:
            result = router.execute()
            assert result.data is not None
        except SourceUnavailableError:
            pytest.skip("akshare macro_cpi unavailable")

    @pytest.mark.integration
    @pytest.mark.network
    def test_router_best_effort_policy(self):
        """Test BEST_EFFORT policy returns even with empty result."""
        def bad_source():
            return pd.DataFrame()
        router = MultiSourceRouter(
            providers=[("bad", bad_source)],
            policy=EmptyDataPolicy.BEST_EFFORT,
        )
        result = router.execute()
        # BEST_EFFORT should not raise
        assert result is not None

    def test_router_all_sources_disabled(self):
        """Test router when all providers fail."""
        def bad():
            raise RuntimeError("disabled")
        router = MultiSourceRouter(
            providers=[("bad", bad)],
            policy=EmptyDataPolicy.STRICT,
        )
        result = router.execute()
        # STRICT policy should report all providers failed
        assert not result.success

    def test_router_stats_tracking(self):
        """Test that router tracks source stats."""
        def good():
            return pd.DataFrame({"a": [1]})
        router = MultiSourceRouter(
            providers=[("good", good)],
        )
        result = router.execute()
        assert router._stats["total_calls"] == 1
        assert router._stats["successes"] == 1


class TestCreateSimpleRouter:
    """Test create_simple_router factory."""

    def test_create_with_callables(self):
        def good():
            return pd.DataFrame({"a": [1]})
        router = create_simple_router(callables={"test": good})
        assert isinstance(router, MultiSourceRouter)

    def test_create_with_best_effort(self):
        def good():
            return pd.DataFrame({"a": [1]})
        router = create_simple_router(
            callables={"test": good},
            policy=EmptyDataPolicy.BEST_EFFORT,
        )
        assert isinstance(router, MultiSourceRouter)
        assert router.policy == EmptyDataPolicy.BEST_EFFORT
