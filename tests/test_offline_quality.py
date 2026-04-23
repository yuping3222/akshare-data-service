"""tests/test_offline_quality.py

数据质量检查器完整测试
"""

import pytest
from unittest.mock import patch, MagicMock

import pandas as pd

from akshare_data.offline.quality import (
    DataQualityChecker,
    QualityChecker,
)


class TestDataQualityChecker:
    """测试 DataQualityChecker 类"""

    def test_checker_init_with_cache_manager(self):
        """测试使用自定义 cache_manager 初始化"""
        mock_cache = MagicMock()
        checker = DataQualityChecker(cache_manager=mock_cache)
        assert checker.cache_manager is mock_cache

    def test_checker_init_default(self):
        """测试默认初始化"""
        checker = DataQualityChecker()
        assert checker.cache_manager is not None

    def test_find_date_column(self):
        """测试日期列查找"""
        checker = DataQualityChecker()

        df_with_date = pd.DataFrame({"date": [1, 2, 3]})
        assert checker._find_date_column(df_with_date) == "date"

        df_with_datetime = pd.DataFrame({"datetime": [1, 2, 3]})
        assert checker._find_date_column(df_with_datetime) == "datetime"

        df_with_trade_date = pd.DataFrame({"trade_date": [1, 2, 3]})
        assert checker._find_date_column(df_with_trade_date) == "trade_date"

        df_with_chinese = pd.DataFrame({"日期": [1, 2, 3]})
        assert checker._find_date_column(df_with_chinese) == "日期"

        df_no_date = pd.DataFrame({"symbol": [1, 2, 3]})
        assert checker._find_date_column(df_no_date) is None

    def test_get_required_fields(self):
        """测试获取必需字段"""
        checker = DataQualityChecker()

        assert checker._get_required_fields("stock_daily") == [
            "date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
        ]
        assert checker._get_required_fields("index_daily") == [
            "date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
        ]
        assert checker._get_required_fields("etf_daily") == [
            "date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
        ]
        assert checker._get_required_fields("unknown_table") == []


class TestCheckCompleteness:
    """测试 check_completeness 方法"""

    def test_completeness_empty_data(self):
        """测试空数据返回正确结构"""
        checker = DataQualityChecker()

        with patch.object(checker.cache_manager, "read", return_value=None):
            result = checker.check_completeness("stock_daily")
            assert result["has_data"] is False
            assert result["total_records"] == 0
            assert result["missing_dates_count"] == 0
            assert result["completeness_ratio"] == 0.0
            assert result["is_complete"] is False

    def test_completeness_empty_dataframe(self):
        """测试空 DataFrame"""
        checker = DataQualityChecker()

        with patch.object(checker.cache_manager, "read", return_value=pd.DataFrame()):
            result = checker.check_completeness("stock_daily")
            assert result["has_data"] is False

    def test_completeness_with_data(self):
        """测试有数据的完整性检查"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "open": [10.0] * 5,
                "high": [11.0] * 5,
                "low": [9.0] * 5,
                "close": [10.5] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.check_completeness(
                "stock_daily",
                symbol="sh600000",
                start_date="2024-01-01",
                end_date="2024-01-05",
            )
            assert result["has_data"] is True
            assert result["total_records"] == 5
            assert result["completeness_ratio"] == 1.0
            assert result["is_complete"] is True
            assert result["missing_dates_count"] == 0

    def test_completeness_with_missing_dates(self):
        """测试有缺失日期的情况"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-03"),
                "symbol": ["sh600000"] * 3,
                "open": [10.0] * 3,
                "high": [11.0] * 3,
                "low": [9.0] * 3,
                "close": [10.5] * 3,
            }
        )

        expected_days = [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ]
        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.check_completeness(
                "stock_daily",
                expected_trading_days=expected_days,
            )
            assert result["has_data"] is True
            assert result["missing_dates_count"] == 2
            assert len(result["missing_dates"]) == 2
            assert result["completeness_ratio"] < 1.0
            assert result["is_complete"] is False

    def test_completeness_with_expected_trading_days(self):
        """测试使用期望交易日列表"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                "symbol": ["sh600000"] * 3,
            }
        )

        expected_days = [
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
        ]

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.check_completeness(
                "stock_daily",
                expected_trading_days=expected_days,
            )
            assert result["missing_dates_count"] == 2
            assert "2024-01-05" in result["missing_dates"]
            assert "2024-01-08" in result["missing_dates"]

    def test_completeness_no_date_column(self):
        """测试没有日期列的情况"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "symbol": ["sh600000"] * 5,
                "open": [10.0] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.check_completeness("stock_daily")
            assert result["has_data"] is True
            assert result["missing_dates_count"] == 0

    def test_completeness_missing_fields(self):
        """测试缺失必需字段"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.check_completeness("stock_daily")
            assert "missing_fields" in result
            assert "open" in result["missing_fields"]
            assert "high" in result["missing_fields"]
            assert "low" in result["missing_fields"]
            assert "close" in result["missing_fields"]

    def test_completeness_with_symbol_filter(self):
        """测试按 symbol 过滤"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=6).tolist(),
                "symbol": ["sh600000", "sh600519"] * 3,
                "open": [10.0] * 6,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df) as mock_read:
            checker.check_completeness(
                "stock_daily",
                symbol="sh600000",
            )
            mock_read.assert_called_once()
            call_args = mock_read.call_args
            assert call_args[1]["where"]["symbol"] == "sh600000"


class TestCheckAnomalies:
    """测试 check_anomalies 方法"""

    def test_anomalies_empty_dataframe(self):
        """测试空 DataFrame"""
        checker = DataQualityChecker()
        result = checker.check_anomalies(pd.DataFrame())

        assert result["total_rows"] == 0
        assert result["anomaly_count"] == 0
        assert result["anomalies"] == []

    def test_anomalies_no_anomalies(self):
        """测试无异常数据"""
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

        assert result["anomaly_count"] == 0
        assert result["price_anomalies"] == []
        assert result["volume_anomalies"] == []
        assert result["high_low_anomalies"] == []

    def test_anomalies_price_threshold(self):
        """测试价格异常检测"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
                "high": [11.0] * 5,
                "low": [9.0] * 5,
                "volume": [100000] * 5,
            }
        )

        result = checker.check_anomalies(df, price_change_threshold=20.0)

        assert len(result["price_anomalies"]) == 2
        assert result["anomaly_count"] >= 2

        pct_anomaly_dates = [str(a["date"]) for a in result["price_anomalies"]]
        assert (
            "2024-01-02" in pct_anomaly_dates
            or "2024-01-02 00:00:00" in pct_anomaly_dates
        )
        assert (
            "2024-01-03" in pct_anomaly_dates
            or "2024-01-03 00:00:00" in pct_anomaly_dates
        )

    def test_anomalies_change_column(self):
        """测试使用 change 列而非 pct_chg"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "change": [1.0, 50.0, -30.0, 0.5, 1.2],
                "high": [11.0] * 5,
                "low": [9.0] * 5,
            }
        )

        result = checker.check_anomalies(df, price_change_threshold=20.0)
        assert len(result["price_anomalies"]) == 2

    def test_anomalies_high_low_invalid(self):
        """测试最高价低于最低价异常"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "high": [9.0, 8.0, 11.0, 10.0, 10.0],
                "low": [10.0, 10.0, 9.0, 9.0, 9.0],
                "volume": [100000] * 5,
            }
        )

        result = checker.check_anomalies(df)

        assert len(result["high_low_anomalies"]) == 2
        assert result["anomaly_count"] >= 2

    def test_anomalies_volume_zscore(self):
        """测试成交量异常（Z-score 方法）"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "volume": [
                    100000,
                    100000,
                    100000,
                    100000,
                    100000,
                    100000,
                    100000,
                    100000,
                    10000000,
                    100000,
                ],
            }
        )

        result = checker.check_anomalies(df, volume_change_threshold=2.0)

        assert len(result["volume_anomalies"]) >= 1
        volume_anomaly_dates = [str(a["date"]) for a in result["volume_anomalies"]]
        assert (
            "2024-01-09" in volume_anomaly_dates
            or "2024-01-09 00:00:00" in volume_anomaly_dates
        )

    def test_anomalies_combined(self):
        """测试综合异常检测"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
                "high": [11.0, 15.0, 12.0, 9.0, 11.0],
                "low": [9.0, 10.0, 8.0, 10.0, 9.0],
                "volume": [100000, 1000000, 100000, 100000, 100000],
            }
        )

        result = checker.check_anomalies(df, price_change_threshold=20.0)

        assert result["total_rows"] == 5
        assert result["anomaly_count"] > 0

    def test_anomalies_with_datetime_column(self):
        """测试使用 datetime 列名"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "datetime": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
            }
        )

        result = checker.check_anomalies(df, price_change_threshold=20.0)
        assert len(result["price_anomalies"]) == 2


class TestCheckConsistency:
    """测试 check_consistency 方法"""

    def test_consistency_identical_data(self):
        """测试完全一致的数据"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "close": [10.0] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.check_consistency(
                "stock_daily",
                "stock_daily_backup",
                "sh600000",
            )

            assert result["consistent"] is True
            assert result["record_count_1"] == 5
            assert result["record_count_2"] == 5
            assert result["common_dates"] == 5
            assert result["only_in_table1"] == []
            assert result["only_in_table2"] == []

    def test_consistency_different_data(self):
        """测试不一致的数据"""
        checker = DataQualityChecker()

        df1 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-07"),
                "symbol": ["sh600000"] * 7,
            }
        )
        df2 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
            }
        )

        def mock_read(table, where=None):
            if table == "stock_daily":
                return df1
            return df2

        with patch.object(checker.cache_manager, "read", side_effect=mock_read):
            result = checker.check_consistency(
                "stock_daily",
                "stock_daily_backup",
                "sh600000",
            )

            assert result["consistent"] is False
            assert result["record_count_1"] == 7
            assert result["record_count_2"] == 5
            assert result["common_dates"] == 5
            assert len(result["only_in_table1"]) == 2
            assert result["only_in_table2"] == []

    def test_consistency_empty_first_table(self):
        """测试第一个表为空"""
        checker = DataQualityChecker()

        df2 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
            }
        )

        def mock_read(table, where=None):
            if table == "stock_daily":
                return None
            return df2

        with patch.object(checker.cache_manager, "read", side_effect=mock_read):
            result = checker.check_consistency(
                "stock_daily",
                "stock_daily_backup",
                "sh600000",
            )

            # Empty first table (no date column) results in consistent=True
            # since date comparison is skipped
            assert result["consistent"] is True
            assert result["record_count_1"] == 0

    def test_consistency_no_date_column(self):
        """测试没有日期列"""
        checker = DataQualityChecker()

        df1 = pd.DataFrame(
            {
                "symbol": ["sh600000"] * 5,
                "close": [10.0] * 5,
            }
        )
        df2 = pd.DataFrame(
            {
                "symbol": ["sh600000"] * 5,
                "close": [10.5] * 5,
            }
        )

        def mock_read(table, where=None):
            return df1 if table == "stock_daily" else df2

        with patch.object(checker.cache_manager, "read", side_effect=mock_read):
            result = checker.check_consistency(
                "stock_daily",
                "stock_daily_backup",
                "sh600000",
            )

            assert result["common_dates"] == 0


class TestGenerateReport:
    """测试 generate_report 方法"""

    def test_generate_report_structure(self):
        """测试报告结构"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
                "high": [11.0] * 10,
                "low": [9.0] * 10,
                "close": [10.5] * 10,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.generate_report(
                "stock_daily",
                symbol="sh600000",
                start_date="2024-01-01",
                end_date="2024-01-10",
            )

            assert "timestamp" in result
            assert "table" in result
            assert "symbol" in result
            assert "checks" in result
            assert "summary" in result
            assert "completeness" in result["checks"]
            assert "anomalies" in result["checks"]

    def test_generate_report_with_issues(self):
        """测试包含问题的报告"""
        checker = DataQualityChecker()

        # Data missing required fields (high, low, close) to trigger issues
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "open": [10.0] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.generate_report(
                "stock_daily",
                symbol="sh600000",
            )

            assert result["summary"]["issues_count"] > 0
            assert "critical_issues" in result["summary"]

    def test_generate_report_no_data(self):
        """测试无数据的报告"""
        checker = DataQualityChecker()

        with patch.object(checker.cache_manager, "read", return_value=None):
            result = checker.generate_report("stock_daily")

            assert (
                "Data completeness issues detected"
                in result["summary"]["critical_issues"]
            )

    def test_generate_report_overall_score(self):
        """测试总体评分"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
                "open": [10.0] * 5,
                "high": [11.0] * 5,
                "low": [9.0] * 5,
                "close": [10.5] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            result = checker.generate_report(
                "stock_daily",
                start_date="2024-01-01",
                end_date="2024-01-10",
            )

            assert result["summary"]["overall_score"] == 100.0


class TestQualityChecker:
    """测试 QualityChecker 工具类"""

    def test_check_daily_completeness_empty(self):
        """测试空数据完整性"""
        expected_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        result = QualityChecker.check_daily_completeness(pd.DataFrame(), expected_days)

        assert result["missing_count"] == 3
        assert result["missing_days"] == expected_days
        assert result["completeness"] == 0.0

    def test_check_daily_completeness_full(self):
        """测试完整数据"""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            }
        )
        expected_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        result = QualityChecker.check_daily_completeness(df, expected_days)

        assert result["missing_count"] == 0
        assert result["completeness"] == 1.0

    def test_check_daily_completeness_partial(self):
        """测试部分完整"""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            }
        )
        expected_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        result = QualityChecker.check_daily_completeness(df, expected_days)

        assert result["missing_count"] == 1
        assert result["missing_days"] == ["2024-01-02"]
        assert result["completeness"] == pytest.approx(2 / 3)

    def test_check_daily_completeness_datetime_column(self):
        """测试 datetime 列"""
        df = pd.DataFrame(
            {
                "datetime": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            }
        )
        expected_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        result = QualityChecker.check_daily_completeness(df, expected_days)

        assert result["missing_count"] == 1

    def test_check_daily_completeness_trade_date_column(self):
        """测试 trade_date 列"""
        df = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            }
        )
        expected_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        result = QualityChecker.check_daily_completeness(df, expected_days)

        assert result["missing_count"] == 1

    def test_detect_anomalies_empty(self):
        """测试空数据异常检测"""
        result = QualityChecker.detect_anomalies(pd.DataFrame())
        assert result == []

    def test_detect_anomalies_no_issues(self):
        """测试无异常数据"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "pct_chg": [1.0, 2.0, -1.0, 0.5, 1.2],
                "high": [11.0] * 5,
                "low": [9.0] * 5,
            }
        )

        result = QualityChecker.detect_anomalies(df)
        assert result == []

    def test_detect_anomalies_price_change(self):
        """测试价格异常检测"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
            }
        )

        result = QualityChecker.detect_anomalies(df)

        assert len(result) == 2
        assert any("Abnormal price change" in r for r in result)

    def test_detect_anomalies_high_low(self):
        """测试最高价最低价异常"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "high": [9.0, 15.0, 11.0, 10.0, 10.0],
                "low": [10.0, 10.0, 9.0, 11.0, 9.0],
            }
        )

        result = QualityChecker.detect_anomalies(df)

        assert len(result) == 2
        assert any("High < Low" in r for r in result)

    def test_detect_anomalies_combined(self):
        """测试综合异常"""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "pct_chg": [1.0, 50.0, -30.0, 0.5, 1.2],
                "high": [9.0, 15.0, 11.0, 10.0, 10.0],
                "low": [10.0, 10.0, 9.0, 11.0, 9.0],
            }
        )

        result = QualityChecker.detect_anomalies(df)
        assert len(result) == 4


class TestDataQualityCheckerEdgeCases:
    """测试边界情况"""

    def test_completeness_with_exception(self):
        """测试异常处理"""
        checker = DataQualityChecker()

        with patch.object(
            checker.cache_manager,
            "read",
            side_effect=Exception("Database error"),
        ):
            result = checker.check_completeness("stock_daily")
            assert "error" in result

    def test_consistency_with_exception(self):
        """测试一致性检查异常"""
        checker = DataQualityChecker()

        with patch.object(
            checker.cache_manager,
            "read",
            side_effect=Exception("Connection failed"),
        ):
            result = checker.check_consistency(
                "stock_daily", "stock_daily_backup", "sh600000"
            )
            assert "error" in result

    def test_generate_report_with_anomaly_exception(self):
        """测试报告生成时异常处理"""
        checker = DataQualityChecker()

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["sh600000"] * 5,
            }
        )

        with patch.object(checker.cache_manager, "read", return_value=df):
            with patch.object(
                checker,
                "check_anomalies",
                side_effect=Exception("Analysis failed"),
            ):
                result = checker.generate_report("stock_daily")
                assert "error" in result["checks"]["anomalies"]
