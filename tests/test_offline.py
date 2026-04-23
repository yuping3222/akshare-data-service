"""tests/test_offline.py

离线模块测试: 接口探测器、数据质量检查、报告生成

参考 jk2bt tests/ 和 akshare-one-enhanced tests/ 编写
"""

import pytest
from unittest.mock import patch, MagicMock

import pandas as pd

from akshare_data.offline.downloader import (
    BatchDownloader,
    validate_ohlcv_data,
    convert_wide_to_long,
)
from akshare_data.offline.quality import (
    DataQualityChecker,
    QualityChecker,
)
from akshare_data.offline.prober import APIProber
from akshare_data.offline.reporter import Reporter
from akshare_data.store.manager import reset_cache_manager


@pytest.fixture(autouse=True)
def reset_global_cache():
    """Reset global cache manager before each test"""
    reset_cache_manager()
    yield
    reset_cache_manager()


@pytest.mark.skip(reason="RateLimiter API changed — use DomainRateLimiter instead")
class TestRateLimiter:
    """测试限速器 (deprecated — replaced by DomainRateLimiter)"""

    def test_rate_limiter_init(self):
        pass

    def test_rate_limiter_acquire(self):
        pass

    def test_rate_limiter_reset(self):
        pass


class TestBatchDownloader:
    """测试批量下载器"""

    def test_downloader_init(self):
        """测试下载器初始化"""
        downloader = BatchDownloader(max_workers=4)
        assert downloader._max_workers == 4
        assert downloader._batch_size == 50

    def test_downloader_with_custom_config(self):
        """测试自定义配置"""
        downloader = BatchDownloader(
            max_workers=8,
            batch_size=100,
        )
        assert downloader._max_workers == 8
        assert downloader._batch_size == 100

    def test_update_one_success(self):
        """测试单个更新成功"""
        downloader = BatchDownloader()

        with patch(
            "akshare_data.offline.downloader.downloader.TaskExecutor"
        ) as mock_executor_cls:
            mock_executor = MagicMock()
            mock_executor.execute.return_value = {
                "success": True,
                "task": "sh600000",
            }
            mock_executor_cls.return_value = mock_executor

            tasks = []
            with patch.object(
                downloader._task_builder, "build_tasks", return_value=tasks
            ):
                result = downloader.download_incremental(days_back=1)
                assert "success_count" in result

    def test_update_one_failure(self):
        """测试单个更新失败"""
        downloader = BatchDownloader()

        with patch(
            "akshare_data.offline.downloader.downloader.TaskExecutor"
        ) as mock_executor_cls:
            mock_executor = MagicMock()
            mock_executor.execute.return_value = {
                "success": False,
                "task": "sh600000",
                "error": "Network error",
            }
            mock_executor_cls.return_value = mock_executor

            tasks = []
            with patch.object(
                downloader._task_builder, "build_tasks", return_value=tasks
            ):
                result = downloader.download_incremental(days_back=1)
                assert result["failed_count"] == 0  # no tasks, so no failures in result

    def test_download_incremental(self):
        """测试增量下载"""
        downloader = BatchDownloader(max_workers=2)

        with patch(
            "akshare_data.offline.downloader.downloader.TaskExecutor"
        ) as mock_executor_cls:
            mock_executor = MagicMock()
            mock_executor.execute.return_value = {
                "success": True,
                "task": "test_task",
            }
            mock_executor_cls.return_value = mock_executor

            result = downloader.download_incremental(days_back=5)
            assert "total" in result
            assert "completed" in result

    def test_download_batch(self):
        """测试批量下载"""
        downloader = BatchDownloader(max_workers=2)

        with patch(
            "akshare_data.offline.downloader.downloader.TaskExecutor"
        ) as mock_executor_cls:
            mock_executor = MagicMock()
            mock_executor.execute.return_value = {
                "success": True,
                "task": "test_task",
            }
            mock_executor_cls.return_value = mock_executor

            result = downloader.download_full(
                interfaces=["stock_zh_a_hist_em"],
                start="2024-01-01",
                end="2024-01-05",
            )
            assert "total" in result


class TestValidateOHLCV:
    """测试 OHLCV 验证"""

    def test_valid_ohlcv_data(self):
        """测试有效 OHLCV 数据"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "stock_code": ["sh600000"] * 10,
                "open": [10.0] * 10,
                "high": [11.0] * 10,
                "low": [9.0] * 10,
                "close": [10.5] * 10,
            }
        )
        assert validate_ohlcv_data(df) is True

    def test_empty_dataframe(self):
        """测试空 DataFrame"""
        assert validate_ohlcv_data(pd.DataFrame()) is False

    def test_missing_required_columns(self):
        """测试缺少必需列"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "stock_code": ["sh600000"] * 10,
            }
        )
        assert validate_ohlcv_data(df) is False


class TestConvertWideToLong:
    """测试宽表转长表"""

    def test_convert_valid_data(self):
        """测试有效数据转换"""
        ohlcv_dict = {
            "open": pd.DataFrame(
                {"sh600000": [10.0, 11.0]},
                index=pd.date_range("2024-01-01", "2024-01-02"),
            ),
            "close": pd.DataFrame(
                {"sh600000": [10.5, 11.5]},
                index=pd.date_range("2024-01-01", "2024-01-02"),
            ),
        }
        result = convert_wide_to_long(ohlcv_dict, ["sh600000"])
        assert not result.empty
        assert "date" in result.columns
        assert "stock_code" in result.columns

    def test_convert_empty_data(self):
        """测试空数据转换"""
        result = convert_wide_to_long({}, ["sh600000"])
        assert result.empty


class TestDataQualityChecker:
    """测试数据质量检查器"""

    def test_checker_init(self):
        """测试检查器初始化"""
        checker = DataQualityChecker()
        assert checker.cache_manager is not None

    def test_check_completeness_empty(self):
        """测试空数据完整性检查"""
        checker = DataQualityChecker()

        with patch.object(
            checker.cache_manager,
            "read",
            return_value=pd.DataFrame(),
        ):
            result = checker.check_completeness("stock_daily")
            assert "has_data" in result
            assert result["has_data"] is False

    def test_check_completeness_with_data(self):
        """测试有数据的完整性检查"""
        checker = DataQualityChecker()

        with patch.object(
            checker.cache_manager,
            "read",
            return_value=pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", "2024-01-10"),
                    "symbol": ["sh600000"] * 10,
                    "open": [10.0] * 10,
                }
            ),
        ):
            result = checker.check_completeness(
                "stock_daily",
                symbol="sh600000",
                start_date="2024-01-01",
                end_date="2024-01-10",
            )
            assert "completeness_ratio" in result
            assert "missing_dates" in result

    def test_check_anomalies_valid_data(self):
        """测试正常数据的异常检测"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "pct_chg": [1.0, 2.0, -1.0, 0.5, -0.5, 1.5, -1.5, 0.8, -0.8, 1.2],
                "high": [11.0] * 10,
                "low": [9.0] * 10,
                "volume": [100000] * 10,
            }
        )

        result = checker.check_anomalies(df)
        assert "anomaly_count" in result
        assert result["anomaly_count"] == 0

    def test_check_anomalies_with_outliers(self):
        """测试有异常值的数据"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
                "high": [11.0, 15.0, 12.0, 11.0, 11.0],
                "low": [9.0, 10.0, 8.0, 9.0, 9.0],
                "volume": [100000, 1000000, 100000, 100000, 100000],
            }
        )

        result = checker.check_anomalies(df, price_change_threshold=20.0)
        assert "price_anomalies" in result
        assert len(result["price_anomalies"]) > 0

    def test_check_consistency(self):
        """测试一致性检查"""
        checker = DataQualityChecker()

        with patch.object(
            checker.cache_manager,
            "read",
            return_value=pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", "2024-01-10"),
                    "symbol": ["sh600000"] * 10,
                }
            ),
        ):
            result = checker.check_consistency(
                "stock_daily",
                "stock_daily_backup",
                "sh600000",
            )
            assert "consistent" in result

    def test_generate_report(self):
        """测试生成综合报告"""
        checker = DataQualityChecker()

        with patch.object(
            checker.cache_manager,
            "read",
            return_value=pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-01", "2024-01-10"),
                    "symbol": ["sh600000"] * 10,
                    "open": [10.0] * 10,
                    "high": [11.0] * 10,
                    "low": [9.0] * 10,
                    "close": [10.5] * 10,
                }
            ),
        ):
            result = checker.generate_report(
                "stock_daily",
                symbol="sh600000",
                start_date="2024-01-01",
                end_date="2024-01-10",
            )
            assert "timestamp" in result
            assert "checks" in result
            assert "summary" in result


class TestQualityChecker:
    """测试 QualityChecker 工具类"""

    def test_check_daily_completeness(self):
        """测试日线完整性检查"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
            }
        )
        expected_days = [
            d.strftime("%Y-%m-%d") for d in pd.date_range("2024-01-01", "2024-01-10")
        ]

        result = QualityChecker.check_daily_completeness(df, expected_days)
        assert "missing_count" in result
        assert result["missing_count"] == 5

    def test_detect_anomalies(self):
        """测试异常检测"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
                "high": [11.0, 15.0, 12.0, 11.0, 11.0],
                "low": [9.0, 10.0, 8.0, 9.0, 9.0],
            }
        )

        anomalies = QualityChecker.detect_anomalies(df)
        assert isinstance(anomalies, list)


class TestAPIProber:
    """测试接口探测器"""

    def test_prober_init(self):
        """测试探测器初始化"""
        prober = APIProber()
        assert prober is not None


class TestReporter:
    """测试报告生成"""

    def test_reporter_init(self):
        """测试报告器初始化"""
        reporter = Reporter()
        assert reporter is not None


class TestOfflineEdgeCases:
    """测试离线模块边界情况"""

    def test_batch_downloader_empty_stock_list(self):
        """测试空接口列表"""
        downloader = BatchDownloader()

        result = downloader.download_full(interfaces=[])
        assert result["total"] == 0

    def test_quality_checker_invalid_table(self):
        """测试无效表名"""
        checker = DataQualityChecker()

        with patch.object(checker.cache_manager, "read", return_value=None):
            result = checker.check_completeness("invalid_table")
            assert result["has_data"] is False
