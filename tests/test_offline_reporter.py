"""tests/test_offline_reporter.py

报告生成器完整测试
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from akshare_data.offline.reporter import Reporter


class TestReporterInit:
    """测试 Reporter 初始化"""

    def test_reporter_init_creates_report_dir(self):
        """测试初始化创建报告目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(Reporter, "REPORT_DIR", tmpdir):
                Reporter()
                assert os.path.exists(tmpdir)

    def test_reporter_init_existing_dir(self):
        """测试初始化已存在目录"""
        with tempfile.TemporaryDirectory():
            reporter = Reporter()
            assert os.path.exists(reporter.REPORT_DIR)


class TestToMarkdown:
    """测试 to_md 方法"""

    def test_to_md_empty_dataframe(self):
        """测试空 DataFrame"""
        reporter = Reporter()
        result = reporter.to_md(pd.DataFrame())
        assert result == ""

    def test_to_md_basic(self):
        """测试基本转换"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "name": ["foo", "bar"],
                "value": [1, 2],
            }
        )

        result = reporter.to_md(df)

        assert "|name|value|" in result
        assert "|---|---|" in result
        assert "|foo|1|" in result
        assert "|bar|2|" in result

    def test_to_md_with_last_check(self):
        """测试包含 last_check 列的时间戳转换"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "name": ["foo"],
                "last_check": [1704067200.0],
            }
        )

        result = reporter.to_md(df)

        assert "foo" in result
        assert "01-01" in result or "01-01" in result

    def test_to_md_with_exec_time(self):
        """测试包含 exec_time 列"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "name": ["foo", "bar"],
                "exec_time": [1.234, 5.678],
            }
        )

        result = reporter.to_md(df)

        assert "1.23s" in result
        assert "5.68s" in result

    def test_to_md_with_newlines(self):
        """测试包含换行符的值"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "name": ["foo\nbar", "baz"],
                "value": [1, 2],
            }
        )

        result = reporter.to_md(df)

        assert "foo bar" in result
        assert "baz" in result

    def test_to_md_with_time_column(self):
        """测试包含 Time 后缀的列"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "name": ["foo"],
                "updateTime": [1.5],
            }
        )

        result = reporter.to_md(df)

        assert "1.50s" in result


class TestGenerateHealthReport:
    """测试 generate_health_report 方法"""

    def test_health_report_empty_results(self):
        """测试空结果"""
        reporter = Reporter()

        result = reporter.generate_health_report({})
        assert result == ""

    def test_health_report_empty_dataframe(self):
        """测试空 DataFrame"""
        reporter = Reporter()

        result = reporter.generate_health_report({})
        assert result == ""

    def test_health_report_basic(self):
        """测试基本报告生成"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "func_name": ["func1", "func2", "func3"],
                "domain_group": ["stock", "fund", "bond"],
                "status": ["Success", "Success", "Failed"],
                "exec_time": [1.0, 2.0, 3.0],
                "last_check": [1704067200.0, 1704067300.0, 1704067400.0],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "HEALTH_REPORT_FILE", os.path.join(tmpdir, "health_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_health_report(df.to_dict("records"))

                assert "# Akshare Health Audit Report" in result
                assert "Total APIs:" in result
                assert "Available APIs:" in result
                assert "Top 20 Slowest APIs" in result
                assert "func3" in result

    def test_health_report_success_rate(self):
        """测试成功率计算"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "func_name": ["func1", "func2", "func3", "func4"],
                "domain_group": ["stock"] * 4,
                "status": ["Success", "Success", "Failed", "Success"],
                "exec_time": [1.0, 2.0, 3.0, 4.0],
                "last_check": [1704067200.0] * 4,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "HEALTH_REPORT_FILE", os.path.join(tmpdir, "health_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_health_report(df.to_dict("records"))

                assert "75.00%" in result

    def test_health_report_slowest_apis_sorted(self):
        """测试最慢 API 排序"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "func_name": ["fast", "slow", "medium"],
                "domain_group": ["stock"] * 3,
                "status": ["Success"] * 3,
                "exec_time": [1.0, 10.0, 5.0],
                "last_check": [1704067200.0] * 3,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "HEALTH_REPORT_FILE", os.path.join(tmpdir, "health_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_health_report(df.to_dict("records"))

                slow_pos = result.find("slow")
                medium_pos = result.find("medium")
                fast_pos = result.find("fast")

                assert slow_pos < medium_pos < fast_pos


class TestGenerateQualityReport:
    """测试 generate_quality_report 方法"""

    def test_quality_report_empty(self):
        """测试空数据"""
        reporter = Reporter()

        result = reporter.generate_quality_report(pd.DataFrame())
        assert result == ""

    def test_quality_report_none(self):
        """测试 None 数据"""
        reporter = Reporter()

        result = reporter.generate_quality_report(None)
        assert result == ""

    def test_quality_report_basic(self):
        """测试基本报告"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "interface_name": [
                    "stock_daily",
                    "fund_etf",
                    "bond_rate",
                    "stock_minute",
                    "index_daily",
                ],
                "分类": ["daily", "static", "monthly", "realtime", "daily"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter,
                "QUALITY_REPORT_FILE",
                os.path.join(tmpdir, "quality_report.md"),
            ):
                reporter = Reporter()
                result = reporter.generate_quality_report(df)

                assert "# Akshare Quality Report" in result
                assert "Total Interfaces:" in result
                assert "Data Interfaces:" in result

    def test_quality_report_module_extraction(self):
        """测试模块提取"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "interface_name": [
                    "stock_daily",
                    "fund_etf",
                    "bond_rate",
                    "futures_main",
                    "index_daily",
                ],
                "分类": ["daily", "static", "monthly", "daily", "daily"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter,
                "QUALITY_REPORT_FILE",
                os.path.join(tmpdir, "quality_report.md"),
            ):
                reporter = Reporter()
                result = reporter.generate_quality_report(df)

                assert "**Data Interfaces:** 5" in result
                assert "**Daily:** 3" in result
                assert "**Static:** 1" in result
                assert "**Monthly:** 1" in result

    def test_quality_report_update_frequency_distribution(self):
        """测试更新频率分布"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "interface_name": [
                    "stock_daily",
                    "stock_daily2",
                    "stock_minute",
                ],
                "分类": ["daily", "daily", "realtime"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter,
                "QUALITY_REPORT_FILE",
                os.path.join(tmpdir, "quality_report.md"),
            ):
                reporter = Reporter()
                result = reporter.generate_quality_report(df)

                assert "**Daily:** 2" in result
                assert "**Realtime:** 1" in result

    def test_quality_report_cache_strategy(self):
        """测试缓存策略建议"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "interface_name": ["stock_daily"],
                "分类": ["daily"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter,
                "QUALITY_REPORT_FILE",
                os.path.join(tmpdir, "quality_report.md"),
            ):
                reporter = Reporter()
                result = reporter.generate_quality_report(df)

                assert "Cache Strategy Recommendations" in result
                assert "**Realtime**" in result
                assert "**Daily**" in result
                assert "**Weekly**" in result
                assert "**Monthly**" in result

    def test_quality_report_non_data_interfaces(self):
        """测试非数据接口"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "interface_name": [
                    "stock_daily",
                    "exception_handler",
                    "tool_wrapper",
                ],
                "分类": ["daily", "exception", "tool"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter,
                "QUALITY_REPORT_FILE",
                os.path.join(tmpdir, "quality_report.md"),
            ):
                reporter = Reporter()
                result = reporter.generate_quality_report(df)

                # Only non-exception/tool/unknown interfaces are counted as data interfaces
                assert "**Data Interfaces:** 1" in result
                assert "**Daily:** 1" in result


class TestGenerateVolumeReport:
    """测试 generate_volume_report 方法"""

    def test_volume_report_empty(self):
        """测试空数据"""
        reporter = Reporter()

        result = reporter.generate_volume_report(pd.DataFrame())
        assert result == ""

    def test_volume_report_none(self):
        """测试 None 数据"""
        reporter = Reporter()

        result = reporter.generate_volume_report(None)
        assert result == ""

    def test_volume_report_basic(self):
        """测试基本报告"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "接口名称": ["stock_daily", "fund_etf"],
                "分类": ["daily", "static"],
                "数据行数": [1000, 500],
                "数据列数": [10, 5],
                "内存占用_KB": [100.0, 50.0],
                "估算方法": ["exact", "estimate"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "VOLUME_REPORT_FILE", os.path.join(tmpdir, "volume_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_volume_report(df)

                assert "# Akshare Data Volume Report" in result
                assert "Total Interfaces:" in result
                assert "Total Rows:" in result

    def test_volume_report_memory_calculation(self):
        """测试内存计算"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "接口名称": ["stock_daily"],
                "分类": ["daily"],
                "数据行数": [1000],
                "数据列数": [10],
                "内存占用_KB": [1024.0],
                "估算方法": ["exact"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "VOLUME_REPORT_FILE", os.path.join(tmpdir, "volume_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_volume_report(df)

                assert "1024.0 KB" in result
                assert "1.00 MB" in result

    def test_volume_report_category_stats(self):
        """测试分类统计"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "接口名称": ["stock_daily", "fund_etf", "bond_rate"],
                "分类": ["daily", "static", "daily"],
                "数据行数": [1000, 500, 200],
                "数据列数": [10, 5, 8],
                "内存占用_KB": [100.0, 50.0, 80.0],
                "估算方法": ["exact", "estimate", "exact"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "VOLUME_REPORT_FILE", os.path.join(tmpdir, "volume_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_volume_report(df)

                assert "### DAILY" in result
                assert "### STATIC" in result

    def test_volume_report_top20(self):
        """测试 Top 20 列表"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "接口名称": [f"interface_{i}" for i in range(25)],
                "分类": ["daily"] * 25,
                "数据行数": [1000] * 25,
                "数据列数": [10] * 25,
                "内存占用_KB": [100.0 * (25 - i) for i in range(25)],
                "估算方法": ["exact"] * 25,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "VOLUME_REPORT_FILE", os.path.join(tmpdir, "volume_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_volume_report(df)

                assert "Top 20 Largest Data Interfaces" in result
                assert "interface_0" in result

    def test_volume_report_cache_strategy_by_size(self):
        """测试按数据量的缓存策略"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "接口名称": ["large_data", "medium_data", "small_data"],
                "分类": ["daily", "daily", "daily"],
                "数据行数": [10000, 1000, 100],
                "数据列数": [10, 10, 10],
                "内存占用_KB": [2000.0, 500.0, 50.0],
                "估算方法": ["exact", "exact", "exact"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "VOLUME_REPORT_FILE", os.path.join(tmpdir, "volume_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_volume_report(df)

                assert "Large (>1000 KB)" in result
                assert "Medium (100-1000 KB)" in result
                assert "Small (<100 KB)" in result


class TestSaveJson:
    """测试 save_json 静态方法"""

    def test_save_json_basic(self):
        """测试基本 JSON 保存"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "output.json")

            Reporter.save_json({"key": "value"}, output_path)

            assert os.path.exists(output_path)

            with open(output_path, "r") as f:
                content = json.load(f)
                assert content["key"] == "value"

    def test_save_json_nested(self):
        """测试嵌套 JSON"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "output.json")

            data = {
                "outer": {
                    "inner": [1, 2, 3],
                },
                "list": [{"a": 1}, {"b": 2}],
            }

            Reporter.save_json(data, output_path)

            with open(output_path, "r") as f:
                content = json.load(f)
                assert content["outer"]["inner"] == [1, 2, 3]

    def test_save_json_creates_parent_dirs(self):
        """测试创建父目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "nested", "dir", "output.json")

            Reporter.save_json({"key": "value"}, output_path)

            assert os.path.exists(output_path)

    def test_save_json_chinese_characters(self):
        """测试中文内容"""
        import json

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "output.json")

            Reporter.save_json({"中文键": "中文值"}, output_path)

            with open(output_path, "r", encoding="utf-8") as f:
                content = json.load(f)
                assert content["中文键"] == "中文值"


class TestGenerateSummary:
    """测试 generate_summary 静态方法"""

    def test_generate_summary_all_success(self):
        """测试全部成功"""
        probe_results = {
            "func1": {"status": "SUCCESS"},
            "func2": {"status": "SUCCESS"},
            "func3": {"status": "SUCCESS"},
        }

        result = Reporter.generate_summary(probe_results)
        assert result == "Health Audit: 3/3 APIs passed."

    def test_generate_summary_partial_success(self):
        """测试部分成功"""
        probe_results = {
            "func1": {"status": "SUCCESS"},
            "func2": {"status": "FAILED"},
            "func3": {"status": "SUCCESS"},
        }

        result = Reporter.generate_summary(probe_results)
        assert result == "Health Audit: 2/3 APIs passed."

    def test_generate_summary_all_failed(self):
        """测试全部失败"""
        probe_results = {
            "func1": {"status": "FAILED"},
            "func2": {"status": "FAILED"},
        }

        result = Reporter.generate_summary(probe_results)
        assert result == "Health Audit: 0/2 APIs passed."

    def test_generate_summary_empty(self):
        """测试空结果"""
        probe_results = {}

        result = Reporter.generate_summary(probe_results)
        assert result == "Health Audit: 0/0 APIs passed."

    def test_generate_summary_other_status(self):
        """测试其他状态"""
        probe_results = {
            "func1": {"status": "SUCCESS"},
            "func2": {"status": "TIMEOUT"},
            "func3": {"status": "ERROR"},
        }

        result = Reporter.generate_summary(probe_results)
        assert result == "Health Audit: 1/3 APIs passed."


class TestIntegrateWithSummary:
    """测试 integrate_with_summary 方法"""

    def test_integrate_new_summary(self):
        """测试集成新摘要"""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / "reports"
            os.makedirs(report_dir, exist_ok=True)
            summary_path = report_dir / "final_summary.txt"

            with open(summary_path, "w", encoding="utf-8") as f:
                f.write("Overall Statistics:\n  Items: 10\n")

            with patch.object(Reporter, "REPORT_DIR", report_dir):
                reporter = Reporter()
                reporter.integrate_with_summary(100, 80, 80.0, 5.5)

                with open(summary_path, "r", encoding="utf-8") as f:
                    content = f.read()

                assert "Interface Health Audit" in content
                assert "Audited APIs: 100" in content
                assert "Available APIs: 80" in content

    def test_integrate_existing_summary(self):
        """测试更新已有摘要"""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / "reports"
            os.makedirs(report_dir, exist_ok=True)
            summary_path = report_dir / "final_summary.txt"

            content = """Interface Health Audit:
  Audited APIs: 50
  Available APIs: 40

Overall Statistics:
  Items: 10
"""
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(content)

            with patch.object(Reporter, "REPORT_DIR", report_dir):
                reporter = Reporter()
                reporter.integrate_with_summary(100, 80, 80.0, 5.5)

                with open(summary_path, "r", encoding="utf-8") as f:
                    new_content = f.read()

                assert "Audited APIs: 100" in new_content
                assert "Available APIs: 80 (80.0%)" in new_content

    def test_integrate_no_summary_file(self):
        """测试摘要文件不存在"""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir) / "reports"
            os.makedirs(report_dir, exist_ok=True)

            with patch.object(Reporter, "REPORT_DIR", report_dir):
                reporter = Reporter()
                reporter.integrate_with_summary(100, 80, 80.0, 5.5)


class TestReporterEdgeCases:
    """测试边界情况"""

    def test_health_report_handles_missing_columns(self):
        """测试处理缺失列"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "func_name": ["func1"],
                "exec_time": [1.5],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "HEALTH_REPORT_FILE", os.path.join(tmpdir, "health_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_health_report(df.to_dict("records"))

                assert "Top 20 Slowest APIs" in result

    def test_quality_report_handles_unknown_module(self):
        """测试处理未知模块"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "interface_name": ["unknown_api", "xyz_abc"],
                "分类": ["daily", "static"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter,
                "QUALITY_REPORT_FILE",
                os.path.join(tmpdir, "quality_report.md"),
            ):
                reporter = Reporter()
                result = reporter.generate_quality_report(df)

                assert "**Total Interfaces:** 2" in result
                assert "**Data Interfaces:** 2" in result
                assert "**Daily:** 1" in result
                assert "**Static:** 1" in result

    def test_volume_report_handles_zero_memory(self):
        """测试处理零内存"""
        reporter = Reporter()

        df = pd.DataFrame(
            {
                "接口名称": ["empty_data"],
                "分类": ["static"],
                "数据行数": [0],
                "数据列数": [0],
                "内存占用_KB": [0.0],
                "估算方法": ["exact"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                Reporter, "VOLUME_REPORT_FILE", os.path.join(tmpdir, "volume_report.md")
            ):
                reporter = Reporter()
                result = reporter.generate_volume_report(df)

                assert "Total Rows:" in result
