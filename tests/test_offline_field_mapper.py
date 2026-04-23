"""tests/test_offline_field_mapper.py - Tests for field_mapper module"""

import pytest
import json
from unittest.mock import patch, MagicMock

import pandas as pd

from akshare_data.offline.field_mapper import (
    FieldMapper,
    ColumnInfo,
    InterfaceFieldResult,
    EXTENDED_CN_TO_EN,
)


class TestColumnInfo:
    """Test ColumnInfo dataclass"""

    def test_column_info_defaults(self):
        """Test default values"""
        col = ColumnInfo(original_name="日期")
        assert col.original_name == "日期"
        assert col.mapped_name is None
        assert col.is_mapped is False
        assert col.dtype == ""
        assert col.sample_value == ""

    def test_column_info_with_values(self):
        """Test with all values"""
        col = ColumnInfo(
            original_name="日期",
            mapped_name="date",
            is_mapped=True,
            dtype="datetime64",
            sample_value="2024-01-01",
        )
        assert col.original_name == "日期"
        assert col.mapped_name == "date"
        assert col.is_mapped is True
        assert col.dtype == "datetime64"
        assert col.sample_value == "2024-01-01"


class TestInterfaceFieldResult:
    """Test InterfaceFieldResult dataclass"""

    def test_interface_field_result_defaults(self):
        """Test default values"""
        result = InterfaceFieldResult(interface_name="stock_zh_a_daily")
        assert result.interface_name == "stock_zh_a_daily"
        assert result.status == ""
        assert result.error_msg == ""
        assert result.total_columns == 0
        assert result.mapped_columns == 0
        assert result.unmapped_columns == 0
        assert result.columns == []
        assert result.output_mapping == {}
        assert result.row_count == 0
        assert result.exec_time == 0.0

    def test_interface_field_result_with_data(self):
        """Test with data"""
        result = InterfaceFieldResult(
            interface_name="stock_zh_a_daily",
            status="success",
            total_columns=5,
            mapped_columns=4,
            unmapped_columns=1,
            row_count=100,
            exec_time=1.5,
        )
        assert result.status == "success"
        assert result.total_columns == 5
        assert result.mapped_columns == 4
        assert result.unmapped_columns == 1
        assert result.row_count == 100
        assert result.exec_time == 1.5


class TestExtendedCnToEn:
    """Test EXTENDED_CN_TO_EN mapping dictionary"""

    def test_date_time_mappings(self):
        """Test date/time field mappings"""
        assert EXTENDED_CN_TO_EN["日期"] == "date"
        assert EXTENDED_CN_TO_EN["时间"] == "time"
        assert EXTENDED_CN_TO_EN["trade_date"] == "date"
        assert EXTENDED_CN_TO_EN["开盘"] == "open"
        assert EXTENDED_CN_TO_EN["最高"] == "high"
        assert EXTENDED_CN_TO_EN["最低"] == "low"
        assert EXTENDED_CN_TO_EN["收盘"] == "close"
        assert EXTENDED_CN_TO_EN["成交量"] == "volume"

    def test_code_name_mappings(self):
        """Test code/name field mappings"""
        assert EXTENDED_CN_TO_EN["代码"] == "symbol"
        assert EXTENDED_CN_TO_EN["名称"] == "name"
        assert EXTENDED_CN_TO_EN["股票代码"] == "symbol"
        assert EXTENDED_CN_TO_EN["股票名称"] == "stock_name"

    def test_financial_mappings(self):
        """Test financial field mappings"""
        assert EXTENDED_CN_TO_EN["总市值"] == "total_market_cap"
        assert EXTENDED_CN_TO_EN["市盈率"] == "pe_ratio"
        assert EXTENDED_CN_TO_EN["市净率"] == "pb_ratio"
        assert EXTENDED_CN_TO_EN["净利润"] == "net_profit"


class TestFieldMapper:
    """Test FieldMapper class"""

    def test_field_mapper_init(self, tmp_path):
        """Test FieldMapper initialization"""
        mapper = FieldMapper()
        assert mapper.registry_path is not None
        assert mapper.output_dir is not None
        assert mapper.registry == {}
        assert mapper.results == []

    def test_field_mapper_init_custom_paths(self, tmp_path):
        """Test FieldMapper with custom paths"""
        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml",
            output_dir=tmp_path / "output",
        )
        assert mapper.registry_path == tmp_path / "registry.yaml"
        assert mapper.output_dir == tmp_path / "output"

    def test_load_registry_not_found(self, tmp_path):
        """Test load_registry with non-existent file"""
        mapper = FieldMapper(registry_path=tmp_path / "nonexistent.yaml")

        with pytest.raises(FileNotFoundError):
            mapper.load_registry()

    def test_load_registry_success(self, tmp_path):
        """Test successful registry load"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  stock_zh_a_daily:
    category: equity
    probe:
      params: {}
""")

        mapper = FieldMapper(registry_path=registry_file)
        result = mapper.load_registry()

        assert "interfaces" in result
        assert "stock_zh_a_daily" in result["interfaces"]

    def test_get_interfaces_no_filter(self, tmp_path):
        """Test get_interfaces without filter"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
  func2:
    category: bond
  func3:
    category: equity
""")

        mapper = FieldMapper(registry_path=registry_file)
        mapper.load_registry()

        interfaces = mapper.get_interfaces()
        assert len(interfaces) == 3

    def test_get_interfaces_with_category_filter(self, tmp_path):
        """Test get_interfaces with category filter"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
  func2:
    category: bond
  func3:
    category: equity
""")

        mapper = FieldMapper(registry_path=registry_file)
        mapper.load_registry()

        interfaces = mapper.get_interfaces(category="equity")
        assert len(interfaces) == 2
        assert all(name == "func1" or name == "func3" for name, _ in interfaces)

    def test_get_interfaces_with_sample_size(self, tmp_path):
        """Test get_interfaces with sample_size limit"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
  func2:
    category: bond
  func3:
    category: equity
""")

        mapper = FieldMapper(registry_path=registry_file)
        mapper.load_registry()

        interfaces = mapper.get_interfaces(sample_size=2)
        assert len(interfaces) == 2

    def test_call_interface_akshare_not_loaded(self):
        """Test _call_interface when akshare not loaded"""
        mapper = FieldMapper.__new__(FieldMapper)
        mapper.ak = None

        data, error = mapper._call_interface("stock_zh_a_daily", {})
        assert data is None
        assert error == "AkShare not loaded"

    def test_call_interface_function_not_found(self):
        """Test _call_interface when function doesn't exist"""
        mock_ak = MagicMock()
        del mock_ak.func_not_exist

        mapper = FieldMapper.__new__(FieldMapper)
        mapper.ak = mock_ak

        data, error = mapper._call_interface("func_not_exist", {})
        assert data is None
        assert "not found" in error

    def test_call_interface_success(self):
        """Test successful _call_interface"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_func = MagicMock(return_value=mock_df)

        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = mock_func

        mapper = FieldMapper.__new__(FieldMapper)
        mapper.ak = mock_ak

        data, error = mapper._call_interface("stock_zh_a_daily", {"symbol": "sh600000"})
        assert error == ""
        assert data is mock_df

    def test_call_interface_exception(self):
        """Test _call_interface with exception"""
        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = MagicMock(side_effect=Exception("Network error"))

        mapper = FieldMapper.__new__(FieldMapper)
        mapper.ak = mock_ak

        data, error = mapper._call_interface("stock_zh_a_daily", {})
        assert data is None
        assert "Network error" in error

    def test_analyze_columns_with_mapped_chinese(self):
        """Test _analyze_columns with Chinese columns that can be mapped"""
        mapper = FieldMapper.__new__(FieldMapper)

        df = pd.DataFrame(
            {
                "日期": pd.date_range("2024-01-01", periods=3),
                "股票代码": ["sh600000", "sh600519", "sh600036"],
                "收盘": [10.0, 20.0, 30.0],
            }
        )

        columns = mapper._analyze_columns(df)

        assert len(columns) == 3

        assert columns[0].original_name == "日期"
        assert columns[0].mapped_name == "date"
        assert columns[0].is_mapped is True

        assert columns[1].original_name == "股票代码"
        assert columns[1].mapped_name == "symbol"
        assert columns[1].is_mapped is True

        assert columns[2].original_name == "收盘"
        assert columns[2].mapped_name == "close"
        assert columns[2].is_mapped is True

    def test_analyze_columns_with_english_columns(self):
        """Test _analyze_columns with already English columns"""
        mapper = FieldMapper.__new__(FieldMapper)

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=3),
                "symbol": ["sh600000", "sh600519", "sh600036"],
                "close": [10.0, 20.0, 30.0],
            }
        )

        columns = mapper._analyze_columns(df)

        assert all(c.is_mapped for c in columns)

    def test_analyze_columns_with_unmapped(self):
        """Test _analyze_columns with unmapped columns"""
        mapper = FieldMapper.__new__(FieldMapper)

        df = pd.DataFrame(
            {
                "不明字段": [1, 2, 3],
                "unknown_col": [4, 5, 6],
            }
        )

        columns = mapper._analyze_columns(df)

        assert columns[0].original_name == "不明字段"
        assert columns[0].is_mapped is False

        assert columns[1].original_name == "unknown_col"
        assert columns[1].is_mapped is True
        assert columns[1].mapped_name == "unknown_col"

    def test_analyze_interface_success(self, tmp_path):
        """Test successful analyze_interface"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  stock_zh_a_daily:
    category: equity
    probe:
      params:
        symbol: sh600000
""")

        mock_df = pd.DataFrame(
            {
                "日期": pd.date_range("2024-01-01", periods=3),
                "股票代码": ["sh600000"] * 3,
                "收盘": [10.0, 20.0, 30.0],
            }
        )

        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = MagicMock(return_value=mock_df)

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        result = mapper.analyze_interface(
            "stock_zh_a_daily", {"probe": {"params": {"symbol": "sh600000"}}}
        )

        assert result.status == "success"
        assert result.row_count == 3
        assert result.total_columns == 3
        assert result.mapped_columns == 3
        assert result.unmapped_columns == 0

    def test_analyze_interface_failed(self, tmp_path):
        """Test analyze_interface with failed call"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  stock_zh_a_daily:
    category: equity
""")

        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = MagicMock(side_effect=Exception("API Error"))

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        result = mapper.analyze_interface("stock_zh_a_daily", {"probe": {"params": {}}})

        assert result.status == "failed"
        assert "API Error" in result.error_msg

    def test_analyze_interface_empty_data(self, tmp_path):
        """Test analyze_interface with empty data"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  stock_zh_a_daily:
    category: equity
""")

        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = MagicMock(return_value=pd.DataFrame())

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        result = mapper.analyze_interface("stock_zh_a_daily", {"probe": {"params": {}}})

        assert result.status == "empty"

    def test_analyze_interface_unsupported_type(self, tmp_path):
        """Test analyze_interface with unsupported data type"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  stock_zh_a_daily:
    category: equity
""")

        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = MagicMock(return_value="not a dataframe")

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        result = mapper.analyze_interface("stock_zh_a_daily", {"probe": {"params": {}}})

        assert result.status == "failed"

    def test_analyze_interface_list_to_dataframe(self, tmp_path):
        """Test analyze_interface converting list to DataFrame"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  stock_zh_a_daily:
    category: equity
""")

        mock_ak = MagicMock()
        mock_ak.stock_zh_a_daily = MagicMock(return_value=[{"col1": 1}, {"col1": 2}])

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        result = mapper.analyze_interface("stock_zh_a_daily", {"probe": {"params": {}}})

        assert result.status == "success"
        assert result.total_columns == 1

    def test_analyze_all(self, tmp_path):
        """Test analyze_all method"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
    probe:
      params: {}
  func2:
    category: bond
    probe:
      params: {}
""")

        mock_df = pd.DataFrame(
            {
                "日期": pd.date_range("2024-01-01", periods=3),
            }
        )

        mock_ak = MagicMock()
        mock_ak.func1 = MagicMock(return_value=mock_df)
        mock_ak.func2 = MagicMock(return_value=mock_df)

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        with patch("time.sleep"):
            results = mapper.analyze_all()

        assert len(results) == 2
        assert all(r.status == "success" for r in results)

    def test_analyze_all_with_skip_existing(self, tmp_path):
        """Test analyze_all with skip_existing"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
    sources:
      - name: akshare
        func: func1
        output_mapping:
          日期: date
""")

        mock_df = pd.DataFrame(
            {
                "日期": pd.date_range("2024-01-01", periods=3),
            }
        )

        mock_ak = MagicMock()
        mock_ak.func1 = MagicMock(return_value=mock_df)

        mapper = FieldMapper(registry_path=registry_file)
        mapper.ak = mock_ak

        with patch("time.sleep"):
            results = mapper.analyze_all(skip_existing=True)

        assert len(results) == 0

    def test_generate_report(self, tmp_path):
        """Test generate_report method"""
        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml", output_dir=tmp_path
        )

        result1 = InterfaceFieldResult(
            interface_name="func1",
            status="success",
            total_columns=3,
            mapped_columns=2,
            unmapped_columns=1,
            row_count=10,
            exec_time=1.0,
            columns=[
                {
                    "original_name": "日期",
                    "mapped_name": "date",
                    "is_mapped": True,
                    "dtype": "object",
                    "sample_value": "2024-01-01",
                },
                {
                    "original_name": "代码",
                    "mapped_name": "symbol",
                    "is_mapped": True,
                    "dtype": "object",
                    "sample_value": "sh600000",
                },
                {
                    "original_name": "未知",
                    "mapped_name": None,
                    "is_mapped": False,
                    "dtype": "object",
                    "sample_value": "val",
                },
            ],
            output_mapping={"日期": "date", "代码": "symbol"},
        )

        result2 = InterfaceFieldResult(
            interface_name="func2",
            status="failed",
            error_msg="API Error",
            total_columns=0,
            mapped_columns=0,
            unmapped_columns=0,
        )

        mapper.results = [result1, result2]
        mapper.global_column_stats = {"date": 1, "symbol": 1}
        mapper.global_unmapped = {"未知": ["func1"]}

        report = mapper.generate_report()

        assert "AkShare 字段映射分析报告" in report
        assert "func1" in report
        assert "func2" in report

    def test_export_unmapped_csv(self, tmp_path):
        """Test export_unmapped_csv method"""
        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml", output_dir=tmp_path
        )

        result = InterfaceFieldResult(
            interface_name="func1",
            status="success",
            total_columns=1,
            mapped_columns=0,
            unmapped_columns=1,
            columns=[
                {
                    "original_name": "未知字段",
                    "mapped_name": None,
                    "is_mapped": False,
                    "dtype": "object",
                    "sample_value": "test",
                },
            ],
        )

        mapper.results = [result]
        mapper.global_unmapped = {"未知字段": ["func1"]}

        mapper.export_unmapped_csv()

        csv_path = tmp_path / "unmapped_columns.csv"
        assert csv_path.exists()

    def test_export_mappings_json(self, tmp_path):
        """Test export_mappings_json method"""
        mapper = FieldMapper(
            registry_path=tmp_path / "registry.yaml", output_dir=tmp_path
        )

        result = InterfaceFieldResult(
            interface_name="func1",
            status="success",
            total_columns=2,
            mapped_columns=2,
            unmapped_columns=0,
            output_mapping={"日期": "date", "代码": "symbol"},
        )

        mapper.results = [result]

        mappings = mapper.export_mappings_json()

        assert "func1" in mappings
        assert mappings["func1"]["output_mapping"] == {"日期": "date", "代码": "symbol"}

    def test_merge_to_registry(self, tmp_path):
        """Test merge_to_registry method"""
        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
    sources:
      - name: akshare
        func: func1
        enabled: true
""")

        mapper = FieldMapper(registry_path=registry_file, output_dir=tmp_path)

        mappings = {
            "func1": {
                "output_mapping": {"日期": "date"},
                "total_columns": 1,
                "mapped_columns": 1,
                "unmapped_columns": 0,
            }
        }

        result_path = mapper.merge_to_registry(mappings)

        assert result_path == registry_file


class TestFieldMapperMain:
    """Test main function for field_mapper"""

    def test_main_help(self):
        """Test main with --help"""
        from akshare_data.offline.field_mapper import main

        with patch("sys.argv", ["field_mapper", "--help"]):
            with pytest.raises(SystemExit):
                main()

    def test_main_with_sample_size(self, tmp_path):
        """Test main with sample-size only (no actual analysis)"""
        from akshare_data.offline.field_mapper import main

        registry_file = tmp_path / "registry.yaml"
        registry_file.write_text("""
interfaces:
  func1:
    category: equity
    probe:
      params: {}
""")

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with patch(
            "sys.argv",
            [
                "field_mapper",
                "--registry",
                str(registry_file),
                "--output-dir",
                str(output_dir),
                "--sample-size",
                "1",
            ],
        ):
            with patch("time.sleep"):
                main()

    def test_main_report_only(self, tmp_path):
        """Test main with --report-only flag"""
        from akshare_data.offline.field_mapper import main

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mappings_file = output_dir / "field_mappings.json"
        mappings_file.write_text(
            json.dumps(
                {
                    "func1": {
                        "output_mapping": {"日期": "date"},
                        "total_columns": 1,
                        "mapped_columns": 1,
                        "unmapped_columns": 0,
                    }
                }
            )
        )

        with patch(
            "sys.argv",
            [
                "field_mapper",
                "--output-dir",
                str(output_dir),
                "--report-only",
            ],
        ):
            main()

    def test_main_report_only_no_file(self, tmp_path):
        """Test main with --report-only but no file exists"""
        from akshare_data.offline.field_mapper import main

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with patch(
            "sys.argv",
            [
                "field_mapper",
                "--output-dir",
                str(output_dir),
                "--report-only",
            ],
        ):
            with patch("akshare_data.offline.field_mapper.logger"):
                main()
