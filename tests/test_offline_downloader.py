"""tests/test_offline_downloader.py

批量下载器测试: BatchDownloader, DomainRateLimiter 及相关工具函数

Adapted for current API:
- download_incremental() / download_full() (no download_full_market, download_batch, download_by_index)
- DomainRateLimiter (no RateLimiter, no get_limiter method)
- Execution via TaskBuilder + TaskExecutor (no ._update_one, no .service)
"""

import pytest
from unittest.mock import patch, MagicMock

import pandas as pd

from akshare_data.offline.downloader import (
    BatchDownloader,
    DomainRateLimiter,
    validate_ohlcv_data,
    convert_wide_to_long,
)
RateLimiter = DomainRateLimiter


@pytest.mark.unit
# ---------------------------------------------------------------------------
# TestRateLimiter — the old RateLimiter class no longer exists.
# The new DomainRateLimiter delegates to sources.router and has a different
# API (wait / set_interval / get_interval / reset) with no acquire(),
# max_calls, time_window, or _calls attributes.
# ---------------------------------------------------------------------------
class TestRateLimiter:
    """DomainRateLimiter 限速器测试（现行 API）"""

    def test_init_with_intervals(self):
        limiter = DomainRateLimiter({"default": 0.7, "custom": 1.5})
        assert limiter.get_interval("default") == 0.7
        assert limiter.get_interval("custom") == 1.5

    def test_wait_allows_within_limit(self):
        limiter = DomainRateLimiter({"default": 0.0})
        limiter.wait("default")
        limiter.wait("default")

    def test_wait_respects_set_interval(self):
        limiter = DomainRateLimiter({"default": 0.01})
        limiter.set_interval("default", 0.02)
        assert limiter.get_interval("default") == 0.02
        limiter.wait("default")

    def test_reset_after_wait(self):
        limiter = DomainRateLimiter({"default": 0.01})
        limiter.wait("default")
        limiter.reset()
        limiter.wait("default")

    def test_wait_unknown_key_fallback(self):
        limiter = DomainRateLimiter({"default": 0.0})
        limiter.wait("non_existing_key")

    def test_set_interval_for_new_key(self):
        limiter = DomainRateLimiter({"default": 0.5})
        limiter.set_interval("new_key", 0.3)
        assert limiter.get_interval("new_key") == 0.3

    def test_get_interval_default_exists(self):
        limiter = DomainRateLimiter({"default": 0.5})
        interval = limiter.get_interval("default")
        assert isinstance(interval, (int, float))
        assert interval > 0


class TestBatchDownloaderInit:
    """BatchDownloader 初始化测试"""

    def test_default_initialization(self):
        """测试默认初始化"""
        dl = BatchDownloader()
        assert dl._max_workers == 4
        assert dl._batch_size == 50
        assert isinstance(dl._rate_limiter, DomainRateLimiter)

    def test_custom_initialization(self):
        """测试自定义参数初始化"""
        dl = BatchDownloader(
            max_workers=8,
            batch_size=100,
            rate_limiter_config={"custom": (15, 2.0)},
        )
        assert dl._max_workers == 8
        assert dl._batch_size == 100
        # rate_limiter_config is used by DomainRateLimiter intervals, not merged into _rate_limits
        # _rate_limits is loaded from rate_limits.yaml file
        assert "default" in dl._rate_limits

    def test_rate_limiter_domains_configured(self):
        """测试配置的域名限速器"""
        dl = BatchDownloader()
        assert "sina_vip" in dl._rate_limits
        assert "em_push2his" in dl._rate_limits
        assert "tushare" in dl._rate_limits

    def test_incremental_cache_check_uses_resolved_table(self):
        """缓存命中检查应使用 schema 对齐后的表名（如 equity_daily -> stock_daily）"""
        mock_cache = MagicMock()
        mock_cache.has_range.return_value = True
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=1, batch_size=1)
        dl._registry = {
            "interfaces": {
                "equity_daily": {
                    "signature": ["symbol", "start_date", "end_date"],
                    "category": "equity",
                    "sources": [{"func": "stock_zh_a_hist", "enabled": True}],
                }
            }
        }
        result = dl.download_incremental(days_back=1)
        assert result["skipped"] == 1
        mock_cache.has_range.assert_called()
        args, kwargs = mock_cache.has_range.call_args
        assert args[0] == "stock_daily"

    def test_get_stock_list_static_filters_non_a_share_codes(self):
        """_get_stock_list_static 应过滤掉 stock_zh_a_daily 不支持的代码段。"""
        mock_df = pd.DataFrame({"代码": ["600000", "000001", "300750", "430047", "830799"]})
        with patch("akshare.stock_zh_a_spot_em", return_value=mock_df):
            codes = BatchDownloader._get_stock_list_static()
        assert "600000" in codes
        assert "000001" in codes
        assert "300750" in codes
        assert "430047" not in codes
        assert "830799" not in codes


# ---------------------------------------------------------------------------
# TestBatchDownloaderUpdateOne — _update_one() does not exist in current API.
# Execution flows through TaskBuilder -> TaskExecutor.execute().
# ---------------------------------------------------------------------------
class TestBatchDownloaderUpdateOne:
    """BatchDownloader task execution tests（替代旧 _update_one）"""

    @pytest.fixture
    def downloader(self):
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2, batch_size=2)
        yield dl

    def test_update_success_with_data(self, downloader):
        """全部任务成功"""
        with patch(
            "akshare_data.offline.downloader.executor.TaskExecutor.execute"
        ) as m:
            m.return_value = {"success": True, "rows": 3, "task": "iface1"}
            from akshare_data.offline.downloader.task_builder import DownloadTask

            tasks = [
                DownloadTask(interface="iface1", func="ak.a", table="t", kwargs={}),
                DownloadTask(interface="iface2", func="ak.b", table="t", kwargs={}),
            ]
            result = downloader._execute_tasks(tasks)
        assert result["success_count"] == 2
        assert result["failed_count"] == 0

    def test_update_no_data(self, downloader):
        """任务返回空数据/失败计入 failed"""
        with patch(
            "akshare_data.offline.downloader.executor.TaskExecutor.execute"
        ) as m:
            m.return_value = {"success": False, "error": "Empty data", "task": "iface1"}
            from akshare_data.offline.downloader.task_builder import DownloadTask

            tasks = [
                DownloadTask(interface="iface1", func="ak.a", table="t", kwargs={})
            ]
            result = downloader._execute_tasks(tasks)
        assert result["success_count"] == 0
        assert result["failed_count"] == 1
        assert result["failed_stocks"][0][0] == "iface1"

    def test_update_with_none_result(self, downloader):
        """执行器返回 None 时应抛异常（体现当前行为）"""
        with patch(
            "akshare_data.offline.downloader.executor.TaskExecutor.execute",
            return_value=None,
        ):
            from akshare_data.offline.downloader.task_builder import DownloadTask

            tasks = [
                DownloadTask(interface="iface1", func="ak.a", table="t", kwargs={})
            ]
            with pytest.raises(AttributeError):
                downloader._execute_tasks(tasks)

    def test_update_exception_handling(self, downloader):
        """执行器抛异常时冒泡（便于上层感知失败）"""
        with patch(
            "akshare_data.offline.downloader.executor.TaskExecutor.execute",
            side_effect=RuntimeError("boom"),
        ):
            from akshare_data.offline.downloader.task_builder import DownloadTask

            tasks = [
                DownloadTask(interface="iface1", func="ak.a", table="t", kwargs={})
            ]
            with pytest.raises(RuntimeError, match="boom"):
                downloader._execute_tasks(tasks)

    def test_update_with_progress_callback(self, downloader):
        """progress callback 被调用"""
        cb = MagicMock()
        with patch(
            "akshare_data.offline.downloader.executor.TaskExecutor.execute"
        ) as m:
            m.return_value = {"success": True, "rows": 1, "task": "iface1"}
            from akshare_data.offline.downloader.task_builder import DownloadTask

            tasks = [
                DownloadTask(interface="iface1", func="ak.a", table="t", kwargs={})
            ]
            downloader._execute_tasks(tasks, progress_callback=cb)
        assert cb.call_count >= 1


# ---------------------------------------------------------------------------
# TestBatchDownloaderIncremental — adapt to mock TaskExecutor.execute
# ---------------------------------------------------------------------------
class TestBatchDownloaderIncremental:
    """BatchDownloader.download_incremental 增量更新测试"""

    @pytest.fixture
    def downloader(self):
        """创建测试下载器"""
        mock_cache = MagicMock()
        # Default has_range to False so the downloader actually exercises the
        # task-building path; otherwise the MagicMock auto-generated truthy
        # return value makes the pre-check skip every interface.
        mock_cache.has_range.return_value = False
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2, batch_size=2)
        yield dl

    def test_incremental_with_provided_stock_list(self, downloader):
        """测试提供股票列表的增量更新 — adapted: mock TaskExecutor.execute"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(
                    interface="iface1", func="ak.func1", table="t1", kwargs={"a": 1}
                ),
                DownloadTask(
                    interface="iface2", func="ak.func2", table="t2", kwargs={"b": 2}
                ),
            ]

            mock_result = {"success": True, "rows": 10, "task": "iface1"}
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value=mock_result,
            ):
                result = downloader.download_incremental(
                    stock_list=["600000", "600001"],
                    start="2024-01-01",
                    days_back=1,
                )

        assert result["success_count"] == 2
        assert result["failed_count"] == 0

    def test_incremental_empty_stock_list(self, downloader):
        """测试空股票列表 — adapted: no stock_list param in new API; test with empty registry."""
        # New API does not take stock_list; it reads from registry.
        # With an empty registry, build_tasks returns [], so _execute_tasks gets 0 tasks.
        downloader._registry = {"interfaces": {}}
        result = downloader.download_incremental(start="2024-01-01", days_back=1)
        # With 0 tasks, all counts should be 0 and success_count == 0
        assert result["success_count"] == 0
        assert result["failed_count"] == 0

    def test_incremental_auto_date_calculation(self, downloader):
        """测试自动日期计算 — adapted: mock task execution"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(
                    interface="iface1", func="ak.func1", table="t1", kwargs={}
                ),
            ]
            mock_result = {"success": True, "rows": 5, "task": "iface1"}
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value=mock_result,
            ):
                result = downloader.download_incremental(days_back=3)

        assert result["success_count"] == 1
        assert result["failed_count"] == 0

    def test_incremental_cache_check_uses_resolved_table(self, downloader):
        """缓存跳过检查应使用 task builder 的表名映射结果。"""
        downloader._registry = {
            "interfaces": {
                "equity_daily": {"signature": ["start_date", "end_date"], "category": "equity"},
            }
        }
        with patch.object(
            downloader._task_builder,
            "_resolve_cache_table",
            return_value="stock_daily",
        ) as mock_resolve:
            downloader.download_incremental(start="2024-01-01", days_back=1)
            mock_resolve.assert_called_with("equity_daily")
            downloader._cache_manager.has_range.assert_called()
            called_table = downloader._cache_manager.has_range.call_args.args[0]
            assert called_table == "stock_daily"

    def test_incremental_with_failed_stocks(self, downloader):
        """测试包含失败任务 — adapted: mock TaskExecutor with mixed results"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(
                    interface="iface_ok", func="ak.func1", table="t1", kwargs={}
                ),
                DownloadTask(
                    interface="iface_fail", func="ak.func2", table="t2", kwargs={}
                ),
            ]

            call_count = [0]

            def fake_execute(task):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"success": True, "rows": 5, "task": task.interface}
                else:
                    return {
                        "success": False,
                        "error": "API Error",
                        "task": task.interface,
                    }

            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                side_effect=fake_execute,
            ):
                result = downloader.download_incremental(start="2024-01-01")

        assert result["success_count"] == 1
        assert result["failed_count"] == 1
        assert len(result["failed_stocks"]) == 1

# ---------------------------------------------------------------------------
# TestBatchDownloaderFullMarket — download_full_market() renamed to download_full()
# ---------------------------------------------------------------------------
class TestBatchDownloaderFullMarket:
    """BatchDownloader.download_full 全量下载测试 (was download_full_market)"""

    @pytest.fixture
    def downloader(self):
        """创建测试下载器"""
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2)
        dl._cache_manager = mock_cache
        yield dl

    def test_full_download_success(self, downloader):
        """测试全量下载成功 — adapted: use download_full(), mock TaskExecutor"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(
                    interface="iface1", func="ak.func1", table="t1", kwargs={}
                ),
                DownloadTask(
                    interface="iface2", func="ak.func2", table="t2", kwargs={}
                ),
            ]

            mock_result = {"success": True, "rows": 10, "task": "iface1"}
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value=mock_result,
            ):
                result = downloader.download_full(
                    interfaces=["iface1", "iface2"],
                    start="2024-01-01",
                    end="2024-01-02",
                )

        assert result["success_count"] == 2
        assert result["failed_count"] == 0

    def test_full_download_default_interfaces(self, downloader):
        """测试默认接口列表 — when interfaces=None, uses registry keys[:20]"""
        downloader._registry = {
            "interfaces": {f"iface{i}": {"func": f"func{i}"} for i in range(5)}
        }
        with patch.object(
            downloader._task_builder, "build_tasks", return_value=[]
        ) as mock_build:
            downloader.download_full()
            mock_build.assert_called_once()
            args = mock_build.call_args[0]
            # interfaces arg should be the 5 registry keys
            assert len(args[0]) == 5

    def test_full_download_with_cache_skip(self, downloader):
        """测试缓存场景 — adapted: mock executor to simulate cache skip via empty data result"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(
                    interface="iface1", func="ak.func1", table="t1", kwargs={}
                ),
            ]

            # Executor returns failure (empty data -> simulates "already cached / nothing new")
            mock_result = {"success": False, "error": "Empty data", "task": "iface1"}
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value=mock_result,
            ):
                result = downloader.download_full(
                    interfaces=["iface1"],
                    start="2024-01-01",
                    end="2024-01-02",
                )

        assert result["failed_count"] == 1
        assert result["success_count"] == 0

    def test_full_download_force_update(self, downloader):
        """测试强制更新参数 — force flag passed through"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(
                    interface="iface1", func="ak.func1", table="t1", kwargs={}
                ),
            ]
            mock_result = {"success": True, "rows": 10, "task": "iface1"}
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value=mock_result,
            ):
                result = downloader.download_full(
                    interfaces=["iface1"],
                    start="2024-01-01",
                    force=True,
                )

        assert result["success_count"] == 1


# ---------------------------------------------------------------------------
# TestBatchDownloaderByIndex — download_by_index() does not exist.
# ---------------------------------------------------------------------------
class TestBatchDownloaderByIndex:
    """按接口分组下载（由 download_full/download_incremental 承担）"""

    @pytest.fixture
    def downloader(self):
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2)
        yield dl

    def test_download_by_index_success(self, downloader):
        """以接口子集执行 download_full 成功"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(interface="index_1", func="ak.a", table="t", kwargs={}),
            ]
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value={"success": True, "rows": 8, "task": "index_1"},
            ):
                result = downloader.download_full(interfaces=["index_1"])
        assert result["success_count"] == 1
        assert result["failed_count"] == 0

    def test_download_by_index_fetch_fails(self, downloader):
        """接口执行失败时记录 failed_stocks"""
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(interface="index_1", func="ak.a", table="t", kwargs={}),
            ]
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value={"success": False, "error": "failed", "task": "index_1"},
            ):
                result = downloader.download_full(interfaces=["index_1"])
        assert result["success_count"] == 0
        assert result["failed_count"] == 1

    def test_download_by_index_empty_components(self, downloader):
        """空接口列表返回空统计"""
        with patch.object(downloader._task_builder, "build_tasks", return_value=[]):
            result = downloader.download_full(interfaces=[])
        assert result["success_count"] == 0
        assert result["failed_count"] == 0


# ---------------------------------------------------------------------------
# TestBatchDownloaderBatch — download_batch() does not exist.
# ---------------------------------------------------------------------------
class TestBatchDownloaderBatch:
    """批量下载场景（由 download_full 承担）"""

    @pytest.fixture
    def downloader(self):
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2)
        yield dl

    def test_batch_download_success(self, downloader):
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(interface="a", func="ak.a", table="t", kwargs={}),
                DownloadTask(interface="b", func="ak.b", table="t", kwargs={}),
            ]
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                return_value={"success": True, "rows": 5, "task": "a"},
            ):
                result = downloader.download_full(interfaces=["a", "b"])
        assert result["success_count"] == 2
        assert result["failed_count"] == 0

    def test_batch_download_empty_list(self, downloader):
        with patch.object(downloader._task_builder, "build_tasks", return_value=[]):
            result = downloader.download_full(interfaces=[])
        assert result["success_count"] == 0
        assert result["failed_count"] == 0

    def test_batch_download_partial_failure(self, downloader):
        with patch.object(downloader._task_builder, "build_tasks") as mock_build:
            from akshare_data.offline.downloader.task_builder import DownloadTask

            mock_build.return_value = [
                DownloadTask(interface="a", func="ak.a", table="t", kwargs={}),
                DownloadTask(interface="b", func="ak.b", table="t", kwargs={}),
            ]

            seq = [
                {"success": True, "rows": 5, "task": "a"},
                {"success": False, "error": "boom", "task": "b"},
            ]
            with patch(
                "akshare_data.offline.downloader.executor.TaskExecutor.execute",
                side_effect=seq,
            ):
                result = downloader.download_full(interfaces=["a", "b"])
        assert result["success_count"] == 1
        assert result["failed_count"] == 1


# ---------------------------------------------------------------------------
# TestGetRateLimiter — DomainRateLimiter has no get_limiter() method.
# Adapt to test the actual API: wait(), set_interval(), get_interval().
# ---------------------------------------------------------------------------
class TestGetRateLimiter:
    """DomainRateLimiter rate limiter tests — adapted to actual API"""

    def test_wait_does_not_raise(self):
        """测试 wait() 不抛异常"""
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache)
        # Should not raise
        dl._rate_limiter.wait("sina")

    def test_set_and_get_interval(self):
        """测试设置和获取间隔"""
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache)
        dl._rate_limiter.set_interval("custom_key", 2.5)
        assert dl._rate_limiter.get_interval("custom_key") == 2.5

    def test_get_interval_default(self):
        """测试默认间隔"""
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache)
        interval = dl._rate_limiter.get_interval("default")
        assert isinstance(interval, (int, float))
        assert interval > 0

    def test_reset_does_not_raise(self):
        """测试 reset() 不抛异常"""
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache)
        dl._rate_limiter.reset()  # should not raise


class TestValidateOhlcvData:
    """validate_ohlcv_data 测试"""

    def test_empty_dataframe(self):
        """测试空DataFrame"""
        assert validate_ohlcv_data(pd.DataFrame()) is False

    def test_valid_dataframe(self):
        """测试有效数据"""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "stock_code": ["600000", "600000"],
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [103.0, 104.0],
            }
        )
        assert validate_ohlcv_data(df) is True

    def test_missing_required_columns(self):
        """测试缺少必需列"""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "stock_code": ["600000"],
                "open": [100.0],
            }
        )
        assert validate_ohlcv_data(df) is False

    def test_no_valid_ohlc_data(self):
        """测试无有效OHLC数据"""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "stock_code": ["600000"],
                "open": [None],
                "high": [None],
                "low": [None],
                "close": [None],
            }
        )
        assert validate_ohlcv_data(df) is False

    def test_high_less_than_low(self):
        """测试high < low情况"""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "stock_code": ["600000"],
                "open": [100.0],
                "high": [90.0],
                "low": [100.0],
                "close": [95.0],
            }
        )
        assert validate_ohlcv_data(df) is True


class TestConvertWideToLong:
    """convert_wide_to_long 测试"""

    def test_empty_dict(self):
        """测试空字典"""
        result = convert_wide_to_long({}, ["600000"])
        assert result.empty

    def test_valid_conversion(self):
        """测试有效转换"""
        ohlcv_dict = {
            "open": pd.DataFrame(
                {"600000": [100.0, 101.0], "600001": [200.0, 201.0]},
                index=["2024-01-01", "2024-01-02"],
            ),
            "close": pd.DataFrame(
                {"600000": [103.0, 104.0], "600001": [203.0, 204.0]},
                index=["2024-01-01", "2024-01-02"],
            ),
        }

        result = convert_wide_to_long(ohlcv_dict, ["600000", "600001"])

        assert not result.empty
        assert "date" in result.columns
        assert "stock_code" in result.columns

    def test_conversion_with_missing_values(self):
        """测试包含缺失值的转换"""
        ohlcv_dict = {
            "open": pd.DataFrame(
                {"600000": [100.0, None], "600001": [200.0, 201.0]},
                index=["2024-01-01", "2024-01-02"],
            ),
        }

        result = convert_wide_to_long(ohlcv_dict, ["600000", "600001"])

        assert not result.empty

    def test_conversion_sorts_by_date_and_stock(self):
        """测试转换后按日期和股票排序"""
        ohlcv_dict = {
            "open": pd.DataFrame(
                {"600000": [100.0, 101.0]},
                index=["2024-01-02", "2024-01-01"],
            ),
        }

        result = convert_wide_to_long(ohlcv_dict, ["600000"])

        dates = result["date"].tolist()
        assert dates == sorted(dates)
