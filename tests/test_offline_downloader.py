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


# ---------------------------------------------------------------------------
# TestRateLimiter — the old RateLimiter class no longer exists.
# The new DomainRateLimiter delegates to sources.router and has a different
# API (wait / set_interval / get_interval / reset) with no acquire(),
# max_calls, time_window, or _calls attributes.
# ---------------------------------------------------------------------------
class TestRateLimiter:
    """RateLimiter 限速器测试 — skipped; old RateLimiter class removed."""

    @pytest.mark.skip(
        reason="TODO: RateLimiter class removed; DomainRateLimiter has different API (wait/set_interval/get_interval/reset). Rewrite against actual DomainRateLimiter."
    )
    def test_init_with_domain_config(self):
        limiter = RateLimiter("example.com", max_calls=10, time_window=1.0)
        assert limiter.domain == "example.com"

    @pytest.mark.skip(reason="TODO: RateLimiter.acquire() no longer exists.")
    def test_acquire_allows_within_limit(self):
        pass

    @pytest.mark.skip(reason="TODO: RateLimiter.acquire() no longer exists.")
    def test_acquire_blocks_at_limit(self):
        pass

    @pytest.mark.skip(reason="TODO: RateLimiter time-window semantics changed.")
    def test_acquire_resets_after_time_window(self):
        pass

    @pytest.mark.skip(
        reason="TODO: wait_if_needed replaced by DomainRateLimiter.wait()."
    )
    def test_wait_if_needed_returns_immediately_when_allowed(self):
        pass

    @pytest.mark.skip(
        reason="TODO: wait_if_needed replaced by DomainRateLimiter.wait()."
    )
    def test_wait_if_needed_blocks_when_limited(self):
        pass

    @pytest.mark.skip(reason="TODO: RateLimiter class removed.")
    def test_thread_safety(self):
        pass


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


# ---------------------------------------------------------------------------
# TestBatchDownloaderUpdateOne — _update_one() does not exist in current API.
# Execution flows through TaskBuilder -> TaskExecutor.execute().
# ---------------------------------------------------------------------------
class TestBatchDownloaderUpdateOne:
    """BatchDownloader._update_one — skipped; method does not exist."""

    @pytest.fixture
    def downloader(self):
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2, batch_size=2)
        yield dl

    @pytest.mark.skip(
        reason="TODO: _update_one() removed; execution now uses TaskExecutor.execute(). Adapt test to mock TaskExecutor or use download_incremental/download_full."
    )
    def test_update_success_with_data(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: _update_one() removed.")
    def test_update_no_data(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: _update_one() removed.")
    def test_update_with_none_result(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: _update_one() removed.")
    def test_update_exception_handling(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: _update_one() removed.")
    def test_update_with_progress_callback(self, downloader):
        pass


# ---------------------------------------------------------------------------
# TestBatchDownloaderIncremental — adapt to mock TaskExecutor.execute
# ---------------------------------------------------------------------------
class TestBatchDownloaderIncremental:
    """BatchDownloader.download_incremental 增量更新测试"""

    @pytest.fixture
    def downloader(self):
        """创建测试下载器"""
        mock_cache = MagicMock()
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
    """BatchDownloader.download_by_index — skipped; method does not exist."""

    @pytest.fixture
    def downloader(self):
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2)
        yield dl

    @pytest.mark.skip(
        reason="TODO: download_by_index() does not exist in current API. Equivalent functionality: download_full(interfaces=...) or download_incremental(). Adapt test."
    )
    def test_download_by_index_success(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: download_by_index() does not exist.")
    def test_download_by_index_fetch_fails(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: download_by_index() does not exist.")
    def test_download_by_index_empty_components(self, downloader):
        pass


# ---------------------------------------------------------------------------
# TestBatchDownloaderBatch — download_batch() does not exist.
# ---------------------------------------------------------------------------
class TestBatchDownloaderBatch:
    """BatchDownloader.download_batch — skipped; method does not exist."""

    @pytest.fixture
    def downloader(self):
        mock_cache = MagicMock()
        dl = BatchDownloader(cache_manager=mock_cache, max_workers=2)
        yield dl

    @pytest.mark.skip(
        reason="TODO: download_batch() does not exist in current API. Equivalent: download_full() or download_incremental(). Adapt test."
    )
    def test_batch_download_success(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: download_batch() does not exist.")
    def test_batch_download_empty_list(self, downloader):
        pass

    @pytest.mark.skip(reason="TODO: download_batch() does not exist.")
    def test_batch_download_partial_failure(self, downloader):
        pass


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
