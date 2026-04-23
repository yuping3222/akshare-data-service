"""tests/test_fetcher.py

数据获取器测试: AkShare 适配器、备份数据源、路由

参考 jk2bt tests/ 编写
"""

from unittest.mock import patch, MagicMock

import pandas as pd

from akshare_data.ingestion.router import (
    MultiSourceRouter,
    EmptyDataPolicy,
    ExecutionResult,
    SourceHealthMonitor,
    create_simple_router,
)
from akshare_data.sources.akshare_source import AkShareAdapter

from akshare_data.core.symbols import normalize_symbol, jq_code_to_ak


class TestAkShareAdapter:
    """测试 AkShare 适配器"""

    def test_adapter_init(self):
        """测试适配器初始化"""
        adapter = AkShareAdapter()
        assert adapter.name == "akshare"
        assert adapter.source_type == "real"

    def test_adapter_default_sources(self):
        """测试默认数据源"""
        adapter = AkShareAdapter()
        assert "sina" in adapter.DEFAULT_DATA_SOURCES
        assert "east_money" in adapter.DEFAULT_DATA_SOURCES

    def test_normalize_symbol(self):
        """测试股票代码标准化"""
        assert normalize_symbol("sh600000") == "600000"
        assert normalize_symbol("600000") == "600000"
        assert normalize_symbol("600000.XSHG") == "600000"

    def test_jq_code_to_ak(self):
        """测试聚宽代码转 AkShare 格式"""
        assert jq_code_to_ak("600000.XSHG") == "sh600000"
        assert jq_code_to_ak("000001.XSHE") == "sz000001"


class TestEmptyDataPolicy:
    """测试空数据策略"""

    def test_strict_policy(self):
        """测试严格策略"""
        assert EmptyDataPolicy.STRICT.value == "strict"

    def test_relaxed_policy(self):
        """测试宽松策略"""
        assert EmptyDataPolicy.RELAXED.value == "relaxed"

    def test_best_effort_policy(self):
        """测试尽力而为策略"""
        assert EmptyDataPolicy.BEST_EFFORT.value == "best_effort"


class TestExecutionResult:
    """测试执行结果"""

    def test_successful_result(self):
        """测试成功结果"""
        df = pd.DataFrame({"a": [1, 2]})
        result = ExecutionResult(
            success=True,
            data=df,
            source="test_source",
            error=None,
            attempts=1,
        )
        assert result.success is True
        assert result.data is not None
        assert result.source == "test_source"
        assert result.is_empty is False
        assert result.is_fallback is False

    def test_failed_result(self):
        """测试失败结果"""
        result = ExecutionResult(
            success=False,
            data=None,
            source=None,
            error="all_providers_failed",
            attempts=3,
            error_details=[("src1", "error1"), ("src2", "error2")],
        )
        assert result.success is False
        assert result.data is None
        assert result.error == "all_providers_failed"
        assert len(result.error_details) == 2

    def test_empty_result_with_fallback(self):
        """测试空结果但使用了备用源"""
        result = ExecutionResult(
            success=True,
            data=pd.DataFrame(),
            source="fallback",
            error=None,
            attempts=2,
            is_empty=True,
            is_fallback=True,
        )
        assert result.success is True
        assert result.is_empty is True
        assert result.is_fallback is True


class TestSourceHealthMonitor:
    """测试数据源健康监控"""

    def test_initial_state(self):
        """测试初始状态"""
        monitor = SourceHealthMonitor()
        assert monitor.is_available("unknown_source") is True

    def test_record_success(self):
        """测试记录成功"""
        monitor = SourceHealthMonitor()
        monitor.record_result("src1", success=True)
        status = monitor.get_status()
        assert status["src1"]["available"] is True
        assert status["src1"]["error_count"] == 0

    def test_record_failure_below_threshold(self):
        """测试记录失败但未达到阈值"""
        monitor = SourceHealthMonitor()
        for _ in range(4):
            monitor.record_result("src1", success=False, error="timeout")
        status = monitor.get_status()
        assert status["src1"]["available"] is True
        assert status["src1"]["error_count"] == 4

    def test_record_failure_reaches_threshold(self):
        """测试失败达到阈值后被禁用"""
        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="timeout")
        status = monitor.get_status()
        assert status["src1"]["available"] is False
        assert status["src1"]["error_count"] == 5

    def test_source_recovery_after_disable_duration(self):
        """测试源在禁用时间后恢复"""
        import time

        monitor = SourceHealthMonitor()
        for _ in range(5):
            monitor.record_result("src1", success=False, error="timeout")
        assert monitor.is_available("src1") is False

        with patch.object(time, "time", return_value=time.time() + 400):
            assert monitor.is_available("src1") is True

    def test_success_resets_error_count(self):
        """测试成功后重置错误计数"""
        monitor = SourceHealthMonitor()
        for _ in range(3):
            monitor.record_result("src1", success=False, error="error")
        monitor.record_result("src1", success=True)
        status = monitor.get_status()
        assert status["src1"]["error_count"] == 0
        assert status["src1"]["available"] is True


class TestMultiSourceRouter:
    """测试多数据源路由"""

    def test_init_with_providers(self):
        """测试使用 providers 初始化"""
        providers = [("src1", MagicMock()), ("src2", MagicMock())]
        router = MultiSourceRouter(providers=providers)
        assert len(router.providers) == 2
        assert router.required_columns == []
        assert router.min_rows == 0

    def test_init_with_validation_params(self):
        """测试带验证参数初始化"""
        router = MultiSourceRouter(
            providers=[("src1", MagicMock())],
            required_columns=["open", "close"],
            min_rows=10,
            policy=EmptyDataPolicy.RELAXED,
        )
        assert router.required_columns == ["open", "close"]
        assert router.min_rows == 10
        assert router.policy == EmptyDataPolicy.RELAXED

    def test_first_provider_succeeds(self):
        """测试第一个 provider 成功"""
        df = pd.DataFrame({"open": [10.0], "close": [11.0]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", mock_func)])
        result = router.execute()
        assert result.success is True
        assert result.source == "src1"
        assert result.attempts == 1
        assert result.is_fallback is False

    def test_first_fails_second_succeeds(self):
        """测试第一个失败后第二个成功"""
        fail_func = MagicMock(side_effect=Exception("connection error"))
        df = pd.DataFrame({"open": [10.0], "close": [11.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[("src1", fail_func), ("src2", success_func)]
        )
        result = router.execute()
        assert result.success is True
        assert result.source == "src2"
        assert result.attempts == 2
        assert result.is_fallback is True

    def test_all_providers_fail(self):
        """测试所有 provider 都失败"""
        fail_func1 = MagicMock(side_effect=Exception("error1"))
        fail_func2 = MagicMock(side_effect=Exception("error2"))
        router = MultiSourceRouter(
            providers=[("src1", fail_func1), ("src2", fail_func2)]
        )
        result = router.execute()
        assert result.success is False
        assert result.error == "all_providers_failed"

    def test_provider_returns_empty_dataframe(self):
        """测试 provider 返回空 DataFrame"""
        empty_func = MagicMock(return_value=pd.DataFrame())
        df = pd.DataFrame({"open": [10.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[("src1", empty_func), ("src2", success_func)]
        )
        result = router.execute()
        assert result.success is True
        assert result.source == "src2"

    def test_kwargs_passed_to_provider(self):
        """测试 kwargs 传递给 provider"""
        df = pd.DataFrame({"open": [10.0]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", mock_func)])
        router.execute(symbol="600519", start_date="2023-01-01")
        mock_func.assert_called_once_with(symbol="600519", start_date="2023-01-01")


class TestMultiSourceRouterValidation:
    """测试数据验证"""

    def test_validate_with_required_columns(self):
        """测试必需列验证"""
        df = pd.DataFrame({"open": [10.0], "close": [11.0], "volume": [100]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[("src1", mock_func)], required_columns=["open", "close"]
        )
        result = router.execute()
        assert result.success is True

    def test_validate_missing_required_columns(self):
        """测试缺少必需列"""
        df = pd.DataFrame({"open": [10.0], "high": [11.0]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[("src1", mock_func)], required_columns=["open", "close"]
        )
        result = router.execute()
        assert result.success is False

    def test_validate_min_rows(self):
        """测试最小行数验证"""
        df = pd.DataFrame({"open": [10.0]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", mock_func)], min_rows=5)
        result = router.execute()
        assert result.success is False


class TestMultiSourceRouterEmptyPolicy:
    """测试空数据策略"""

    def test_strict_policy_all_empty(self):
        """测试严格策略全部返回空"""
        empty_func = MagicMock(return_value=pd.DataFrame())
        router = MultiSourceRouter(
            providers=[("src1", empty_func)], policy=EmptyDataPolicy.STRICT
        )
        result = router.execute()
        assert result.success is False
        assert result.is_empty is True

    def test_relaxed_policy_all_empty(self):
        """测试宽松策略全部返回空"""
        empty_func = MagicMock(return_value=pd.DataFrame())
        router = MultiSourceRouter(
            providers=[("src1", empty_func)], policy=EmptyDataPolicy.RELAXED
        )
        result = router.execute()
        assert result.success is True
        assert result.is_empty is True


class TestMultiSourceRouterStats:
    """测试统计功能"""

    def test_initial_stats(self):
        """测试初始统计"""
        router = MultiSourceRouter(providers=[("src1", MagicMock())])
        stats = router.get_stats()
        assert stats["total_calls"] == 0
        assert stats["successes"] == 0
        assert stats["failures"] == 0

    def test_stats_updated_on_success(self):
        """测试成功后更新统计"""
        df = pd.DataFrame({"open": [10.0]})
        mock_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", mock_func)])
        router.execute()
        stats = router.get_stats()
        assert stats["total_calls"] == 1
        assert stats["successes"] == 1


class TestCreateSimpleRouter:
    """测试简单路由工厂"""

    def test_create_from_dict(self):
        """测试从字典创建"""
        callables = {"src1": MagicMock(), "src2": MagicMock()}
        router = create_simple_router(callables=callables)
        assert len(router.providers) == 2
        assert isinstance(router, MultiSourceRouter)

    def test_create_with_all_params(self):
        """测试创建时指定所有参数"""
        MagicMock()
        router = create_simple_router(
            callables={"src1": MagicMock()},
            required_columns=["open"],
            min_rows=1,
            policy=EmptyDataPolicy.BEST_EFFORT,
        )
        assert router.required_columns == ["open"]
        assert router.min_rows == 1
        assert router.policy == EmptyDataPolicy.BEST_EFFORT


class TestRouterEdgeCases:
    """测试路由边界情况"""

    def test_execute_with_no_providers(self):
        """测试无 provider 执行"""
        router = MultiSourceRouter(providers=[])
        result = router.execute()
        assert result.success is False

    def test_none_dataframe_treated_as_empty(self):
        """测试 None DataFrame 视为空"""
        none_func = MagicMock(return_value=None)
        df = pd.DataFrame({"open": [10.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(
            providers=[("src1", none_func), ("src2", success_func)]
        )
        result = router.execute()
        assert result.success is True
        assert result.source == "src2"

    def test_unavailable_provider_skipped(self):
        """测试跳过不可用的 provider"""
        df = pd.DataFrame({"open": [10.0]})
        success_func = MagicMock(return_value=df)
        router = MultiSourceRouter(providers=[("src1", success_func)])
        for _ in range(5):
            router._health.record_result("src1", success=False)
        result = router.execute()
        assert result.success is False
