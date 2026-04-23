"""tests/test_sources_router.py

多数据源路由测试
测试DomainRateLimiter、SourceHealthMonitor、MultiSourceRouter执行逻辑
"""

import time
from unittest.mock import patch, MagicMock

import pandas as pd

from akshare_data.ingestion.router import (
    MultiSourceRouter,
    DomainRateLimiter,
    SourceHealthMonitor,
    EmptyDataPolicy,
    ExecutionResult,
    create_simple_router,
)


class TestDomainRateLimiter:
    """测试域名级限速器"""

    def test_init_with_intervals_and_domain_map(self):
        """测试传入 intervals 和 domain_map"""
        limiter = DomainRateLimiter(
            intervals={"em_push2his": 0.5, "default": 0.3},
            domain_map={"push2his.eastmoney.com": "em_push2his"},
            default_interval=0.3,
        )
        assert limiter.get_interval("push2his.eastmoney.com") == 0.5
        assert limiter.get_interval("unknown.com") == 0.3

    def test_init_custom_intervals(self):
        """测试自定义间隔"""
        limiter = DomainRateLimiter(
            intervals={"test_key": 2.0, "default": 0.3},
            domain_map={"custom.domain": "test_key"},
            default_interval=0.3,
        )
        assert limiter.get_interval("custom.domain") == 2.0

    def test_rate_key_passthrough(self):
        """测试 rate_limit_key 直接作为 key 传入"""
        limiter = DomainRateLimiter(
            intervals={"em_push2": 0.5, "default": 0.3},
            domain_map={},
            default_interval=0.3,
        )
        assert limiter.get_interval("em_push2") == 0.5

    def test_record_request(self):
        """测试记录请求时间"""
        limiter = DomainRateLimiter(intervals={"test_key": 1.0, "default": 0.3})
        limiter.record_request("test_key")
        assert "test_key" in limiter._last_request

    def test_wait_if_needed_first_request(self):
        """测试首次请求无需等待"""
        limiter = DomainRateLimiter(intervals={"default": 0.3})
        start = time.time()
        limiter.wait_if_needed("any_domain")
        elapsed = time.time() - start
        assert elapsed < 0.1

    def test_wait_if_needed_within_interval(self):
        """测试在间隔内需要等待"""
        limiter = DomainRateLimiter(
            intervals={"test_key": 1.0, "default": 0.3},
            domain_map={"test.domain": "test_key"},
            default_interval=0.3,
        )
        limiter.record_request("test.domain")
        start = time.time()
        limiter.wait_if_needed("test.domain")
        elapsed = time.time() - start
        assert elapsed >= 0.9

    def test_wait_if_needed_after_interval(self):
        """测试超过间隔无需等待"""
        limiter = DomainRateLimiter(intervals={"test_key": 0.1, "default": 0.3})
        limiter.record_request("test_key")
        time.sleep(0.15)
        start = time.time()
        limiter.wait_if_needed("test_key")
        elapsed = time.time() - start
        assert elapsed < 0.05

    def test_set_interval(self):
        """测试设置间隔"""
        limiter = DomainRateLimiter(intervals={"default": 0.3})
        limiter.set_interval("new_key", 5.0)
        assert limiter.get_interval("new_key") == 5.0

    def test_reset(self):
        """测试重置限速器"""
        limiter = DomainRateLimiter(intervals={"default": 0.3})
        limiter.record_request("default")
        limiter.reset()
        assert len(limiter._last_request) == 0

    def test_extract_domain_from_url(self):
        """测试从URL提取域名"""
        assert (
            DomainRateLimiter.extract_domain("https://push2.eastmoney.com/a")
            == "push2.eastmoney.com"
        )
        assert (
            DomainRateLimiter.extract_domain("http://dce.com.cn/path") == "dce.com.cn"
        )

    def test_extract_domain_invalid_url(self):
        """测试无效URL提取域名"""
        assert DomainRateLimiter.extract_domain("invalid-url") == "invalid-url"

    def test_resolve_rate_key_fallback(self):
        """测试域名未匹配时回退到 default"""
        limiter = DomainRateLimiter(
            intervals={"em_push2": 0.5, "default": 0.3},
            domain_map={"push2.eastmoney.com": "em_push2"},
            default_interval=0.3,
        )
        assert limiter.get_rate_key("push2.eastmoney.com") == "em_push2"
        assert limiter.get_rate_key("unknown.com") == "default"


class TestSourceHealthMonitor:
    """测试数据源健康监控"""

    def test_initial_available(self):
        """测试未知源初始可用"""
        monitor = SourceHealthMonitor()
        assert monitor.is_available("new_source") is True

    def test_record_success_resets_error_count(self):
        """测试成功后重置错误计数"""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=False, error="error1")
        monitor.record_result("src1", success=False, error="error2")
        monitor.record_result("src1", success=True)
        status = monitor.get_status()
        assert status["src1"]["error_count"] == 0
        assert status["src1"]["available"] is True

    def test_error_threshold_triggers_disable(self):
        """测试错误阈值触发禁用"""
        monitor = SourceHealthMonitor()
        for i in range(5):
            monitor.record_result("src1", success=False, error=f"error{i}")
        assert monitor.is_available("src1") is False
        status = monitor.get_status()
        assert "disabled_at" in status["src1"]

    def test_source_recovers_after_duration(self):
        """测试源在禁用时长后恢复"""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="error")

        with patch.object(time, "time", return_value=time.time() + 400):
            assert monitor.is_available("src1") is True
            status = monitor.get_status()
            assert status["src1"]["error_count"] == 0

    def test_get_status_returns_copy(self):
        """测试get_status返回深拷贝"""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=True)
        status1 = monitor.get_status()
        status1["src1"]["available"] = False
        status2 = monitor.get_status()
        assert status2["src1"]["available"] is True


class TestExecutionResult:
    """测试执行结果"""

    def test_execution_result_creation(self):
        """测试创建执行结果"""
        df = pd.DataFrame({"a": [1]})
        result = ExecutionResult(
            success=True,
            data=df,
            source="test",
            error=None,
            attempts=1,
            is_empty=False,
            is_fallback=False,
            sources_tried=[],
        )
        assert result.success is True
        assert result.data is not None
        assert result.source == "test"

    def test_execution_result_default_values(self):
        """测试执行结果默认值"""
        result = ExecutionResult(
            success=False,
            data=None,
            source=None,
            error="failed",
            attempts=0,
        )
        assert result.is_empty is False
        assert result.is_fallback is False
        assert result.error_details is None


class TestMultiSourceRouterExecute:
    """测试MultiSourceRouter执行逻辑"""

    def test_execute_passes_kwargs_to_providers(self):
        """测试kwargs传递给providers"""
        mock_func = MagicMock(return_value=pd.DataFrame({"open": [100.0]}))
        router = MultiSourceRouter(providers=[("src1", mock_func)])
        router.execute(symbol="600000", start_date="2023-01-01")
        mock_func.assert_called_once_with(symbol="600000", start_date="2023-01-01")

    def test_execute_with_none_as_dataframe(self):
        """测试返回None被视为空"""
        none_func = MagicMock(return_value=None)
        success_func = MagicMock(return_value=pd.DataFrame({"open": [100.0]}))
        router = MultiSourceRouter(
            providers=[("none", none_func), ("success", success_func)]
        )
        result = router.execute()
        assert result.success is True
        assert result.source == "success"

    def test_execute_validation_failure_skips_provider(self):
        """测试验证失败跳过provider"""
        mock_func = MagicMock(return_value=pd.DataFrame({"open": [100.0]}))
        router = MultiSourceRouter(
            providers=[("src1", mock_func)], required_columns=["open", "close"]
        )
        result = router.execute()
        assert result.success is False

    def test_execute_min_rows_validation(self):
        """测试最小行数验证"""
        df = pd.DataFrame({"open": [100.0]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", mock_func)], min_rows=10)
        result = router.execute()
        assert result.success is False

    def test_execute_records_health_on_success(self):
        """测试成功时记录健康状态"""
        mock_func = MagicMock(return_value=pd.DataFrame({"open": [100.0]}))
        router = MultiSourceRouter(providers=[("src1", mock_func)])
        router.execute()
        assert router._health.is_available("src1") is True

    def test_execute_records_health_on_failure(self):
        """测试失败时记录健康状态"""
        mock_func = MagicMock(side_effect=Exception("error"))
        router = MultiSourceRouter(providers=[("src1", mock_func)])
        # Health monitor requires 5 errors before marking unavailable
        for _ in range(5):
            router.execute()
        assert router._health.is_available("src1") is False


class TestMultiSourceRouterPolicy:
    """测试多数据源路由策略"""

    def test_strict_policy_rejects_empty(self):
        """测试严格策略拒绝空数据"""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock(return_value=pd.DataFrame()))],
            policy=EmptyDataPolicy.STRICT,
        )
        result = router.execute()
        assert result.success is False
        assert result.is_empty is True

    def test_relaxed_policy_accepts_empty(self):
        """测试宽松策略接受空数据"""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock(return_value=pd.DataFrame()))],
            policy=EmptyDataPolicy.RELAXED,
        )
        result = router.execute()
        assert result.success is True
        assert result.is_empty is True

    def test_best_effort_policy_accepts_empty(self):
        """测试尽力而为策略接受空数据"""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock(return_value=pd.DataFrame()))],
            policy=EmptyDataPolicy.BEST_EFFORT,
        )
        result = router.execute()
        assert result.success is True
        assert result.is_empty is True


class TestMultiSourceRouterFallback:
    """测试多数据源路由fallback"""

    def test_is_fallback_when_second_succeeds(self):
        """测试第二个成功时标记为fallback"""
        fail_func = MagicMock(side_effect=Exception("error"))
        df = pd.DataFrame({"open": [100.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[("fail", fail_func), ("success", success_func)]
        )
        result = router.execute()
        assert result.is_fallback is True
        assert result.attempts == 2

    def test_is_fallback_when_cache_succeeds(self):
        """测试缓存成功时标记为fallback"""
        fail_func = MagicMock(side_effect=Exception("error"))
        cache_func = MagicMock(return_value=pd.DataFrame({"open": [100.0]}))
        router = MultiSourceRouter(
            providers=[("fail", fail_func), ("__cache__", cache_func)]
        )
        result = router.execute()
        assert result.is_fallback is True
        assert result.source == "__cache__"

    def test_sources_tried_tracked(self):
        """测试sources_tried记录"""
        fail_func1 = MagicMock(side_effect=Exception("error1"))
        fail_func2 = MagicMock(side_effect=Exception("error2"))
        df = pd.DataFrame({"open": [100.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[
                ("fail1", fail_func1),
                ("fail2", fail_func2),
                ("success", success_func),
            ]
        )
        result = router.execute()
        assert len(result.sources_tried) == 3
        assert result.sources_tried[0]["name"] == "fail1"
        assert result.sources_tried[0]["error"] == "error1"
        assert result.sources_tried[2]["success"] is True


class TestMultiSourceRouterCircuitBreaker:
    """测试多数据源路由熔断机制"""

    def test_unavailable_provider_skipped(self):
        """测试跳过不可用provider"""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("bad_src", success=False, error="error")

        df = pd.DataFrame({"open": [100.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("bad_src", success_func)])
        router._health = monitor

        result = router.execute()
        assert result.success is False
        success_func.assert_not_called()

    def test_circuit_breaker_resets_on_recovery(self):
        """测试熔断器恢复后重新可用"""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="error")

        df = pd.DataFrame({"open": [100.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", success_func)])
        router._health = monitor

        with patch.object(time, "time", return_value=time.time() + 400):
            result = router.execute()
            assert result.success is True
            assert result.source == "src1"


class TestMultiSourceRouterStats:
    """测试多数据源路由统计"""

    def test_stats_track_total_calls(self):
        """测试统计总调用次数"""
        router = MultiSourceRouter(
            providers=[
                ("src1", MagicMock(return_value=pd.DataFrame({"open": [100.0]})))
            ]
        )
        router.execute()
        router.execute()
        stats = router.get_stats()
        assert stats["total_calls"] == 2

    def test_stats_track_source_stats(self):
        """测试统计各源统计"""
        df = pd.DataFrame({"open": [100.0]})
        router = MultiSourceRouter(
            providers=[
                ("src1", MagicMock(return_value=df)),
                ("src2", MagicMock(side_effect=Exception("error"))),
            ]
        )
        router.execute()
        stats = router.get_stats()
        # src1 succeeds, src2 is never tried because src1 already succeeded
        assert stats["source_stats"]["src1"]["successes"] == 1
        assert "src2" not in stats["source_stats"]

    def test_stats_track_fallbacks(self):
        """测试统计fallback次数"""
        router = MultiSourceRouter(
            providers=[
                ("src1", MagicMock(side_effect=Exception("error"))),
                ("src2", MagicMock(return_value=pd.DataFrame({"open": [100.0]}))),
            ]
        )
        router.execute()
        stats = router.get_stats()
        assert stats["fallbacks"] == 1

    def test_stats_track_empty_results(self):
        """测试统计空结果次数"""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock(return_value=pd.DataFrame()))],
            policy=EmptyDataPolicy.RELAXED,
        )
        router.execute()
        stats = router.get_stats()
        assert stats["empty_results"] == 1


class TestCreateSimpleRouter:
    """测试简单路由工厂函数"""

    def test_create_with_cache_provider(self):
        """测试创建时添加缓存provider"""
        callables = {"src1": MagicMock(), "src2": MagicMock()}
        cache_func = MagicMock()
        router = create_simple_router(callables={**callables, "__cache__": cache_func})
        assert len(router.providers) == 3
        provider_names = [name for name, _ in router.providers]
        assert "__cache__" in provider_names

    def test_create_simple_router_all_params(self):
        """测试创建时传递所有参数"""
        router = create_simple_router(
            callables={"src1": MagicMock()},
            required_columns=["open", "high"],
            min_rows=5,
            policy=EmptyDataPolicy.BEST_EFFORT,
        )
        assert router.required_columns == ["open", "high"]
        assert router.min_rows == 5
        assert router.policy == EmptyDataPolicy.BEST_EFFORT


class TestMultiSourceRouterEdgeCases:
    """测试多数据源路由边界情况"""

    def test_empty_providers_list(self):
        """测试空providers列表"""
        router = MultiSourceRouter(providers=[])
        result = router.execute()
        assert result.success is False
        assert result.error == "all_providers_failed"

    def test_all_providers_return_empty(self):
        """测试所有provider都返回空"""
        router = MultiSourceRouter(
            providers=[
                ("src1", MagicMock(return_value=pd.DataFrame())),
                ("src2", MagicMock(return_value=pd.DataFrame())),
            ]
        )
        result = router.execute()
        assert result.success is False
        assert result.error == "all_providers_returned_empty"

    def test_error_details_collected(self):
        """测试错误详情收集"""
        router = MultiSourceRouter(
            providers=[
                ("src1", MagicMock(side_effect=ValueError("invalid"))),
                ("src2", MagicMock(side_effect=RuntimeError("timeout"))),
            ]
        )
        result = router.execute()
        assert len(result.error_details) == 2
        assert result.error_details[0] == ("src1", "invalid")
        assert result.error_details[1] == ("src2", "timeout")

    def test_provider_returns_non_dataframe(self):
        """测试provider返回非DataFrame — accepted when no validation requirements are set."""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock(return_value="not a dataframe"))]
        )
        result = router.execute()
        # With no min_rows/required_columns set, non-DF data is accepted
        assert result.success is True
        assert result.data == "not a dataframe"

    def test_provider_returns_dict(self):
        """测试provider返回字典 — accepted when no validation requirements are set."""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock(return_value={"key": "value"}))]
        )
        result = router.execute()
        # With no min_rows/required_columns set, non-DF data is accepted
        assert result.success is True
        assert result.data == {"key": "value"}
