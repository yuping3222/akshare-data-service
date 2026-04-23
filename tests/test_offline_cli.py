"""tests/test_offline_cli.py - CLI tests for offline module"""

import pytest
import json
from unittest.mock import patch, MagicMock, mock_open

import pandas as pd


class TestSubParsers:
    """Test individual sub-parser registration functions"""

    def test_add_download_parser(self):
        """Test download subparser adds correct arguments"""
        from akshare_data.offline.cli.main import _add_download_parser

        parser = MagicMock()
        subparsers = MagicMock()
        subparsers.add_parser.return_value = parser

        _add_download_parser(subparsers)

        subparsers.add_parser.assert_called_once_with("download", help="Download data")
        assert parser.add_argument.call_count >= 6

    def test_add_probe_parser(self):
        """Test probe subparser adds correct arguments"""
        from akshare_data.offline.cli.main import _add_probe_parser

        parser = MagicMock()
        subparsers = MagicMock()
        subparsers.add_parser.return_value = parser

        _add_probe_parser(subparsers)

        subparsers.add_parser.assert_called_once_with(
            "probe", help="Probe interface health"
        )
        assert parser.add_argument.call_count >= 3

    def test_add_analyze_parser(self):
        """Test analyze subparser adds correct arguments"""
        from akshare_data.offline.cli.main import _add_analyze_parser

        parser = MagicMock()
        subparsers = MagicMock()
        subparsers.add_parser.return_value = parser

        _add_analyze_parser(subparsers)

        subparsers.add_parser.assert_called_once_with("analyze", help="Analyze data")
        assert parser.add_argument.call_count >= 4

    def test_add_report_parser(self):
        """Test report subparser adds correct arguments"""
        from akshare_data.offline.cli.main import _add_report_parser

        parser = MagicMock()
        subparsers = MagicMock()
        subparsers.add_parser.return_value = parser

        _add_report_parser(subparsers)

        subparsers.add_parser.assert_called_once_with("report", help="Generate reports")
        assert parser.add_argument.call_count >= 2

    def test_add_config_parser(self):
        """Test config subparser adds correct arguments"""
        from akshare_data.offline.cli.main import _add_config_parser

        parser = MagicMock()
        subparsers = MagicMock()
        subparsers.add_parser.return_value = parser

        _add_config_parser(subparsers)

        subparsers.add_parser.assert_called_once_with(
            "config", help="Manage configuration"
        )
        assert parser.add_argument.call_count >= 1


class TestHandleProbe:
    """Test _handle_probe function"""

    def test_handle_probe_with_status(self, capsys):
        """Test probe with --status flag"""
        from akshare_data.offline.cli.main import _handle_probe

        mock_prober = MagicMock()
        mock_prober.checkpoint_mgr.get_all_results.return_value = {
            "func1": {"status": "Success", "exec_time": 1.0, "data_size": 100}
        }
        mock_prober.get_summary.return_value = "5/5 APIs available"

        with patch("akshare_data.offline.prober.APIProber", return_value=mock_prober):
            args = MagicMock()
            args.all = False
            args.interface = None
            args.status = True

            _handle_probe(args)


class TestHandleAnalyze:
    """Test _handle_analyze function"""

    def test_handle_analyze_logs(self, capsys):
        """Test analyze logs type"""
        from akshare_data.offline.cli.main import _handle_analyze

        mock_analyzer = MagicMock()
        mock_analyzer.analyze.return_value = {"priorities": {"func1": 1}, "total": 1}

        with patch(
            "akshare_data.offline.analyzer.CallStatsAnalyzer",
            return_value=mock_analyzer,
        ):
            args = MagicMock()
            args.type = "logs"
            args.window = 7
            args.table = None
            args.category = None

            _handle_analyze(args)

    def test_handle_analyze_cache_without_table(self):
        """Test analyze cache without required --table"""
        from akshare_data.offline.cli.main import _handle_analyze

        args = MagicMock()
        args.type = "cache"
        args.table = None

        with pytest.raises(SystemExit) as exc_info:
            _handle_analyze(args)

        assert exc_info.value.code == 1

    def test_handle_analyze_cache_with_empty_data(self):
        """Test analyze cache with empty table data"""
        from akshare_data.offline.cli.main import _handle_analyze

        with patch(
            "akshare_data.offline.core.data_loader.load_table",
            return_value=pd.DataFrame(),
        ):
            args = MagicMock()
            args.type = "cache"
            args.table = "stock_daily"

            with pytest.raises(SystemExit) as exc_info:
                _handle_analyze(args)

            assert exc_info.value.code == 0

    def test_handle_analyze_cache_with_data(self, capsys):
        """Test analyze cache with actual data"""
        from akshare_data.offline.cli.main import _handle_analyze

        mock_checker = MagicMock()
        mock_checker._find_date_column.return_value = "date"
        mock_checker.check.return_value = {
            "total_records": 100,
            "completeness_ratio": 0.95,
            "is_complete": True,
        }

        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10),
                "symbol": ["sh600000"] * 10,
            }
        )

        with patch(
            "akshare_data.offline.analyzer.cache_analysis.CompletenessChecker",
            return_value=mock_checker,
        ):
            with patch(
                "akshare_data.offline.core.data_loader.load_table", return_value=test_df
            ):
                args = MagicMock()
                args.type = "cache"
                args.table = "stock_daily"

                _handle_analyze(args)

    def test_handle_analyze_fields(self, capsys):
        """Test analyze fields type"""
        from akshare_data.offline.cli.main import _handle_analyze

        mock_mapper = MagicMock()
        mock_mapper.analyze_all.return_value = []
        mock_mapper.generate_report.return_value = "# Report"
        mock_mapper.export_mappings_json.return_value = "/path/to/mappings.json"

        with patch(
            "akshare_data.offline.analyzer.FieldMapper", return_value=mock_mapper
        ):
            args = MagicMock()
            args.type = "fields"
            args.category = "equity"
            args.table = None

            _handle_analyze(args)


class TestHandleReport:
    """Test _handle_report function"""

    def test_handle_report_health_no_state_file(self):
        """Test health report when no probe data exists"""
        from akshare_data.offline.cli.main import _handle_report

        mock_paths = MagicMock()
        mock_paths.prober_state_file.exists.return_value = False

        with patch("akshare_data.offline.cli.main.paths", mock_paths):
            args = MagicMock()
            args.type = "health"
            args.table = None

            with pytest.raises(SystemExit) as exc_info:
                _handle_report(args)

            assert exc_info.value.code == 1

    def test_handle_report_health_with_data(self, capsys):
        """Test health report with existing probe data"""
        from akshare_data.offline.cli.main import _handle_report

        probe_data = json.dumps(
            [
                {
                    "func_name": "func1",
                    "status": "Success",
                    "exec_time": 1.0,
                    "data_size": 100,
                }
            ]
        )

        mock_paths = MagicMock()
        mock_paths.prober_state_file.exists.return_value = True
        mock_paths.health_reports_dir = "/reports/health"

        mock_generator = MagicMock()
        mock_generator.generate.return_value = "# Health Report"

        with patch("akshare_data.offline.cli.main.paths", mock_paths):
            with patch(
                "akshare_data.offline.report.HealthReportGenerator",
                return_value=mock_generator,
            ):
                with patch("builtins.open", mock_open(read_data=probe_data)):
                    args = MagicMock()
                    args.type = "health"
                    args.table = None

                    _handle_report(args)

    def test_handle_report_quality_without_table(self):
        """Test quality report without required --table"""
        from akshare_data.offline.cli.main import _handle_report

        args = MagicMock()
        args.type = "quality"
        args.table = None

        with pytest.raises(SystemExit) as exc_info:
            _handle_report(args)

        assert exc_info.value.code == 1

    def test_handle_report_quality_with_table(self, capsys):
        """Test quality report with table"""
        from akshare_data.offline.cli.main import _handle_report

        mock_checker = MagicMock()
        mock_checker.check.return_value = {"completeness_ratio": 0.95}

        mock_detector = MagicMock()
        mock_detector.detect.return_value = []

        mock_generator = MagicMock()
        mock_generator.generate.return_value = "# Quality Report"

        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10),
                "symbol": ["sh600000"] * 10,
                "open": [10.0] * 10,
                "close": [10.5] * 10,
            }
        )

        with patch(
            "akshare_data.offline.analyzer.cache_analysis.CompletenessChecker",
            return_value=mock_checker,
        ):
            with patch(
                "akshare_data.offline.analyzer.cache_analysis.AnomalyDetector",
                return_value=mock_detector,
            ):
                with patch(
                    "akshare_data.offline.report.QualityReportGenerator",
                    return_value=mock_generator,
                ):
                    with patch(
                        "akshare_data.offline.cli.main._load_table_data",
                        return_value=test_df,
                    ):
                        args = MagicMock()
                        args.type = "quality"
                        args.table = "stock_daily"

                        _handle_report(args)

    def test_handle_report_dashboard_no_probe_data(self, capsys):
        """Test dashboard report with no probe data"""
        from akshare_data.offline.cli.main import _handle_report

        mock_renderer = MagicMock()
        mock_renderer.render_markdown.return_value = "# Dashboard"
        mock_renderer.save.return_value = None

        mock_paths = MagicMock()
        mock_paths.prober_state_file.exists.return_value = False
        mock_paths.dashboard_dir = MagicMock()
        mock_paths.project_root = "/project"

        with patch("akshare_data.offline.cli.main.paths", mock_paths):
            with patch(
                "akshare_data.offline.report.renderer.ReportRenderer",
                return_value=mock_renderer,
            ):
                args = MagicMock()
                args.type = "dashboard"

                _handle_report(args)

    def test_handle_report_dashboard_with_probe_data(self, capsys):
        """Test dashboard report with probe data"""
        from akshare_data.offline.cli.main import _handle_report

        probe_data = json.dumps(
            [
                {"func_name": "func1", "status": "Success", "exec_time": 1.0},
                {"func_name": "func2", "status": "Failed", "exec_time": 0.5},
            ]
        )

        mock_renderer = MagicMock()
        mock_renderer.render_markdown.return_value = "# Dashboard"
        mock_renderer.save.return_value = None

        mock_paths = MagicMock()
        mock_paths.prober_state_file.exists.return_value = True
        mock_paths.dashboard_dir = MagicMock()
        mock_paths.project_root = "/project"

        with patch("akshare_data.offline.cli.main.paths", mock_paths):
            with patch(
                "akshare_data.offline.report.renderer.ReportRenderer",
                return_value=mock_renderer,
            ):
                with patch("builtins.open", mock_open(read_data=probe_data)):
                    args = MagicMock()
                    args.type = "dashboard"

                    _handle_report(args)


class TestLoadTableData:
    """Test _load_table_data function"""

    def test_load_table_data_success(self):
        """Test successful table load"""
        from akshare_data.offline.cli.main import _load_table_data

        test_df = pd.DataFrame({"col1": [1, 2, 3]})

        with patch(
            "akshare_data.offline.core.data_loader.load_table", return_value=test_df
        ):
            result = _load_table_data("stock_daily")

            assert not result.empty

    def test_load_table_data_exception(self):
        """Test table load with exception returns empty DataFrame"""
        from akshare_data.offline.cli.main import _load_table_data

        with patch(
            "akshare_data.offline.core.data_loader.load_table",
            side_effect=Exception("Not found"),
        ):
            result = _load_table_data("nonexistent_table")

            assert result.empty


class TestMain:
    """Test main CLI entry point"""

    def test_main_no_command(self):
        """Test main with no command prints help and exits"""
        from akshare_data.offline.cli.main import main

        parser = MagicMock()
        parser.parse_args.return_value = MagicMock(command=None)

        with patch("argparse.ArgumentParser", return_value=parser):
            with patch.object(parser, "print_help"):
                with pytest.raises(SystemExit) as exc_info:
                    main()

                assert exc_info.value.code == 1

    def test_main_download_command(self, capsys):
        """Test main with download command"""
        from akshare_data.offline.cli.main import main

        mock_cache_manager = MagicMock()

        mock_paths = MagicMock()
        mock_paths.ensure_dirs = MagicMock()

        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = MagicMock(
            command="download",
            mode="incremental",
            days=1,
            interface=None,
            start=None,
            end=None,
            workers=4,
            schedule=False,
        )

        with patch("argparse.ArgumentParser", return_value=mock_parser):
            with patch("akshare_data.offline.cli.main.paths", mock_paths):
                with patch(
                    "akshare_data.offline.core.data_loader.get_cache_manager_instance",
                    return_value=mock_cache_manager,
                ):
                    with patch(
                        "akshare_data.offline.downloader.BatchDownloader"
                    ) as mock_bd:
                        mock_bd.return_value.download_incremental.return_value = {
                            "success": 10
                        }
                        main()
