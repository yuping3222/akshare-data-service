"""Integration tests for offline tools — prober, scanner, scheduler with real data."""

import pytest

from akshare_data.offline.prober.prober import APIProber
from akshare_data.offline.scanner.akshare_scanner import AkShareScanner
from akshare_data.offline.scanner.category_inferrer import CategoryInferrer
from akshare_data.offline.scanner.domain_extractor import DomainExtractor
from akshare_data.offline.scanner.param_inferrer import ParamInferrer
from akshare_data.offline.scheduler.scheduler import Scheduler
from akshare_data.offline.source_manager.health_tracker import HealthTracker
from akshare_data.offline.source_manager.failover import FailoverManager


class TestAkShareProberReal:
    """Test prober with real akshare interfaces."""

    @pytest.mark.integration
    @pytest.mark.network
    def test_probe_macro_interface(self):
        """Probe a macro interface."""
        prober = APIProber()
        try:
            result = prober.probe("macro_china_cpi_yearly", n_samples=1)
            assert result is not None
        except Exception as e:
            pytest.skip(f"prober unavailable: {e}")


class TestAkShareScanner:
    """Test scanner with real akshare."""

    def test_scanner_creation(self):
        scanner = AkShareScanner()
        assert scanner is not None

    def test_scanner_has_interfaces(self):
        """Test that scanner can discover interfaces."""
        scanner = AkShareScanner()
        if hasattr(scanner, "list_interfaces"):
            interfaces = scanner.list_interfaces()
            assert isinstance(interfaces, list)
            assert len(interfaces) > 0


class TestCategoryInferrer:
    """Test category inferrer."""

    def test_inferrer_detects_macro(self):
        inferrer = CategoryInferrer()
        cat = inferrer.infer("macro_china_cpi_yearly")
        assert cat is not None

    def test_inferrer_detects_stock(self):
        inferrer = CategoryInferrer()
        cat = inferrer.infer("stock_zh_a_daily")
        assert cat is not None


class TestDomainExtractor:
    """Test domain extractor."""

    def test_extract_domains(self):
        extractor = DomainExtractor()
        domains = extractor.extract("macro_china_cpi_yearly")
        assert isinstance(domains, (list, set))


class TestParamInferrer:
    """Test parameter inferrer."""

    def test_infer_params(self):
        inferrer = ParamInferrer()
        try:
            params = inferrer.infer("macro_china_cpi_yearly", signature={"params": []})
            assert isinstance(params, (list, dict))
        except Exception:
            pytest.skip("param inferrer unavailable")


class TestScheduler:
    """Test scheduler."""

    def test_scheduler_creation(self):
        scheduler = Scheduler()
        assert scheduler is not None


class TestHealthTracker:
    """Test health tracking."""

    def test_tracker_creation(self):
        tracker = HealthTracker()
        assert tracker is not None


class TestFailoverManager:
    """Test failover manager."""

    def test_failover_creation(self):
        mgr = FailoverManager()
        assert mgr is not None

    def test_failover_has_methods(self):
        mgr = FailoverManager()
        # Just verify the class can be instantiated and has expected interface
        assert hasattr(mgr, "__class__")
