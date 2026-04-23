"""tests/test_offline_registry.py

注册表模块测试: builder.py, exporter.py, merger.py, validator.py
覆盖 100% 的 offline/registry/ 模块代码
"""

import pytest
import json
from unittest.mock import patch, MagicMock

import yaml

from akshare_data.offline.registry.builder import (
    RegistryBuilder,
    _load_rate_limits,
    _load_domain_mapping,
)
from akshare_data.offline.registry.exporter import RegistryExporter
from akshare_data.offline.registry.merger import RegistryMerger
from akshare_data.offline.registry.validator import RegistryValidator


class TestLoadRateLimits:
    """_load_rate_limits() 函数测试"""

    def test_load_rate_limits_file_exists(self, tmp_path):
        """测试加载存在的rate_limits文件"""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        rate_file = config_dir / "rate_limits.yaml"
        rate_file.write_text(
            "default:\n  interval: 0.5\n  description: '默认'\nem_push2his:\n  interval: 1.0\n"
        )

        with patch("akshare_data.offline.registry.builder.RATE_LIMITS_FILE", rate_file):
            result = _load_rate_limits()
            assert "default" in result
            assert result["default"]["interval"] == 0.5

    def test_load_rate_limits_file_not_exists(self, tmp_path):
        """测试rate_limits文件不存在时返回默认"""
        with patch(
            "akshare_data.offline.registry.builder.RATE_LIMITS_FILE",
            tmp_path / "nonexistent.yaml",
        ):
            result = _load_rate_limits()
            assert result["default"]["interval"] == 0.5


class TestLoadDomainMapping:
    """_load_domain_mapping() 函数测试"""

    def test_load_domain_mapping_populated(self):
        """测试域名映射表已填充"""
        mapping = _load_domain_mapping()
        assert len(mapping) > 0
        assert "push2his.eastmoney.com" in mapping
        assert mapping["push2his.eastmoney.com"] == "em_push2his"

    def test_load_domain_mapping_all_strings(self):
        """测试所有键值都是字符串"""
        mapping = _load_domain_mapping()
        for domain, rate_key in mapping.items():
            assert isinstance(domain, str)
            assert isinstance(rate_key, str)


class TestRegistryBuilder:
    """RegistryBuilder 类测试"""

    def test_init_creates_scanner_and_extractors(self):
        """测试初始化创建扫描器和提取器"""
        builder = RegistryBuilder()
        assert builder.scanner is not None
        assert builder.domain_extractor is not None
        assert builder.category_inferrer is not None
        assert builder.param_inferrer is not None

    def test_build_with_provided_scan_results(self):
        """测试使用提供的扫描结果构建"""
        builder = RegistryBuilder()
        scan_results = {
            "test_func": {
                "doc": "Test function",
                "signature": ["symbol"],
            }
        }

        with (
            patch.object(
                builder,
                "_build_interface",
                return_value={
                    "name": "test_func",
                    "category": "test",
                    "domains": [],
                    "rate_limit_key": "default",
                },
            ),
            patch.object(builder, "domain_extractor", MagicMock()),
        ):
            result = builder.build(scan_results=scan_results)

        assert "version" in result
        assert result["version"] == "2.0"
        assert "interfaces" in result
        assert "domains" in result
        assert "rate_limits" in result
        assert "generated_at" in result

    def test_build_without_scan_results(self):
        """测试不提供扫描结果时扫描所有"""
        builder = RegistryBuilder()

        mock_scanner = MagicMock()
        mock_scanner.scan_all.return_value = {}
        builder.scanner = mock_scanner

        with patch(
            "akshare_data.offline.registry.builder._load_rate_limits",
            return_value={"default": {"interval": 0.5}},
        ):
            result = builder.build()

        assert result["version"] == "2.0"
        mock_scanner.scan_all.assert_called_once()

    def test_build_interface_tracks_domains(self):
        """测试构建接口时追踪域名"""
        builder = RegistryBuilder()
        scan_results = {
            "func1": {"doc": "Doc 1", "signature": []},
            "func2": {"doc": "Doc 2", "signature": []},
        }

        def mock_build_interface(name, info):
            if name == "func1":
                return {
                    "name": name,
                    "category": "cat1",
                    "domains": ["example.com"],
                    "rate_limit_key": "default",
                }
            return {
                "name": name,
                "category": "cat2",
                "domains": ["example.com"],
                "rate_limit_key": "default",
            }

        with patch.object(
            builder, "_build_interface", side_effect=mock_build_interface
        ):
            result = builder.build(scan_results=scan_results)

        assert "example.com" in result["domains"]

    def test_get_func_obj_found(self):
        """测试获取存在的函数对象"""
        builder = RegistryBuilder()
        with patch("akshare_data.offline.registry.builder.getattr") as mock_getattr:
            mock_getattr.return_value = lambda: None
            builder._get_func_obj("test_func")
            mock_getattr.assert_called_once()

    def test_get_func_obj_not_found(self):
        """测试获取不存在的函数对象"""
        builder = RegistryBuilder()
        with patch("akshare_data.offline.registry.builder.getattr", return_value=None):
            result = builder._get_func_obj("nonexistent_func")
            assert result is None

    def test_infer_rate_limit_exact_match(self):
        """测试精确匹配域名"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(["push2his.eastmoney.com"])
        assert result == "em_push2his"

    def test_infer_rate_limit_suffix_match(self):
        """测试后缀匹配域名"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(["api.push2his.eastmoney.com"])
        assert result == "em_push2his"

    def test_infer_rate_limit_no_match(self):
        """测试无匹配时返回default"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(["unknown.domain.com"])
        assert result == "default"

    def test_infer_rate_limit_empty_list(self):
        """测试空域名列表"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit([])
        assert result == "default"

    def test_infer_rate_limit_multiple_domains_first_matches(self):
        """测试多个域名时返回第一个匹配"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(
            ["push2his.eastmoney.com", "push2.eastmoney.com"]
        )
        assert result == "em_push2his"

    def test_infer_rate_limit_second_domain_matches(self):
        """测试第一个不匹配但第二个匹配"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(["unknown.com", "push2.eastmoney.com"])
        assert result == "em_push2"

    def test_build_interface_with_func_obj(self):
        """测试有函数对象时构建接口"""
        builder = RegistryBuilder()

        mock_func = MagicMock()
        mock_func.__doc__ = "Test doc"

        with (
            patch.object(builder, "_get_func_obj", return_value=mock_func),
            patch.object(
                builder.domain_extractor, "extract", return_value=["example.com"]
            ),
            patch.object(
                builder.category_inferrer, "infer", return_value="test_category"
            ),
            patch.object(
                builder.param_inferrer, "infer", return_value={"symbol": "000001"}
            ),
        ):
            result = builder._build_interface(
                "test_func", {"doc": "Test", "signature": ["symbol"]}
            )

        assert result["name"] == "test_func"
        assert result["category"] == "test_category"
        assert result["domains"] == ["example.com"]
        assert result["description"] == "Test"
        assert result["signature"] == ["symbol"]
        assert result["rate_limit_key"] == "default"
        assert result["probe"]["params"] == {"symbol": "000001"}
        assert result["probe"]["skip"] is False

    def test_build_interface_without_func_obj(self):
        """测试无函数对象时构建接口"""
        builder = RegistryBuilder()

        with (
            patch.object(builder, "_get_func_obj", return_value=None),
            patch.object(builder.domain_extractor, "extract", return_value=[]),
            patch.object(builder.category_inferrer, "infer", return_value="unknown"),
            patch.object(builder.param_inferrer, "infer", return_value={}),
        ):
            result = builder._build_interface(
                "test_func", {"doc": "Test", "signature": []}
            )

        assert result["name"] == "test_func"
        assert result["domains"] == []
        assert "params" in result["probe"]


class TestRegistryExporter:
    """RegistryExporter 类测试"""

    @pytest.fixture
    def sample_registry(self):
        """示例注册表数据"""
        return {
            "version": "2.0",
            "generated_at": "2024-01-01T00:00:00",
            "description": "Test registry",
            "interfaces": {
                "func1": {
                    "name": "func1",
                    "category": "stock",
                    "description": "Func 1",
                    "signature": ["symbol"],
                    "domains": ["example.com"],
                    "rate_limit_key": "default",
                    "sources": [],
                    "probe": {"params": {}, "skip": False, "check_interval": 2592000},
                },
                "func2": {
                    "name": "func2",
                    "category": "fund",
                    "description": "Func 2",
                    "signature": [],
                    "domains": [],
                    "rate_limit_key": "default",
                    "sources": [],
                    "probe": {"params": {}, "skip": False, "check_interval": 2592000},
                },
            },
            "domains": {"example.com": {"rate_limit_key": "default"}},
            "rate_limits": {"default": {"interval": 0.5}},
        }

    def test_export_yaml_default_path(self, sample_registry, tmp_path):
        """测试导出YAML到默认路径"""
        with patch("akshare_data.offline.registry.exporter.paths") as mock_paths:
            mock_paths.legacy_registry_file = tmp_path / "registry.yaml"
            exporter = RegistryExporter()
            result = exporter.export_yaml(sample_registry)

            assert result.exists()
            content = yaml.safe_load(result.read_text())
            assert content["version"] == "2.0"

    def test_export_yaml_custom_path(self, sample_registry, tmp_path):
        """测试导出YAML到自定义路径"""
        output_file = tmp_path / "custom" / "registry.yaml"
        exporter = RegistryExporter()
        result = exporter.export_yaml(sample_registry, output_path=output_file)

        assert result == output_file
        assert output_file.exists()

    def test_export_yaml_creates_parent_dirs(self, sample_registry, tmp_path):
        """测试导出YAML时创建父目录"""
        output_file = tmp_path / "nested" / "dir" / "registry.yaml"
        exporter = RegistryExporter()
        exporter.export_yaml(sample_registry, output_path=output_file)

        assert output_file.exists()

    def test_export_split_default_dir(self, sample_registry, tmp_path):
        """测试拆分导出到默认目录"""
        with patch("akshare_data.offline.registry.exporter.paths") as mock_paths:
            mock_paths.registry_dir = tmp_path / "registry"
            exporter = RegistryExporter()
            result = exporter.export_split(sample_registry)

            assert result == tmp_path / "registry"
            assert (tmp_path / "registry" / "stock.yaml").exists()
            assert (tmp_path / "registry" / "fund.yaml").exists()
            assert (tmp_path / "registry" / "_base.yaml").exists()

    def test_export_split_custom_dir(self, sample_registry, tmp_path):
        """测试拆分导出到自定义目录"""
        output_dir = tmp_path / "custom_registry"
        exporter = RegistryExporter()
        result = exporter.export_split(sample_registry, output_dir=output_dir)

        assert result == output_dir
        assert output_dir.exists()
        assert (output_dir / "stock.yaml").exists()

    def test_export_split_creates_base_file(self, sample_registry, tmp_path):
        """测试拆分导出创建base文件"""
        with patch("akshare_data.offline.registry.exporter.paths") as mock_paths:
            mock_paths.registry_dir = tmp_path / "registry"
            exporter = RegistryExporter()
            exporter.export_split(sample_registry)

            base_file = tmp_path / "registry" / "_base.yaml"
            assert base_file.exists()
            content = yaml.safe_load(base_file.read_text())
            assert content["version"] == "2.0"
            assert content["description"] == "Test registry"

    def test_export_json_default_path(self, sample_registry, tmp_path):
        """测试导出JSON到默认路径"""
        with patch("akshare_data.offline.registry.exporter.paths") as mock_paths:
            mock_paths.config_dir = tmp_path / "config"
            mock_paths.config_dir.mkdir()
            exporter = RegistryExporter()
            result = exporter.export_json(sample_registry)

            assert result.exists()
            content = json.loads(result.read_text())
            assert content["version"] == "2.0"

    def test_export_json_custom_path(self, sample_registry, tmp_path):
        """测试导出JSON到自定义路径"""
        output_file = tmp_path / "custom" / "registry.json"
        exporter = RegistryExporter()
        result = exporter.export_json(sample_registry, output_path=output_file)

        assert result == output_file
        assert output_file.exists()
        content = json.loads(output_file.read_text())
        assert content["version"] == "2.0"

    def test_export_json_creates_parent_dirs(self, sample_registry, tmp_path):
        """测试导出JSON时创建父目录"""
        output_file = tmp_path / "nested" / "dir" / "registry.json"
        exporter = RegistryExporter()
        exporter.export_json(sample_registry, output_path=output_file)

        assert output_file.exists()


class TestRegistryMerger:
    """RegistryMerger 类测试"""

    @pytest.fixture
    def auto_generated_registry(self):
        """自动生成的注册表"""
        return {
            "interfaces": {
                "func1": {
                    "name": "func1",
                    "category": "stock",
                    "sources": [],
                    "probe": {"params": {}, "skip": False},
                },
                "func2": {
                    "name": "func2",
                    "category": "fund",
                    "sources": [],
                    "probe": {"params": {}, "skip": False},
                },
            },
            "rate_limits": {
                "default": {"interval": 0.5},
                "em_push2his": {"interval": 1.0},
            },
        }

    def test_merge_interfaces_no_manual_path(self, auto_generated_registry, tmp_path):
        """测试无手工配置路径"""
        with patch("akshare_data.offline.registry.merger.paths") as mock_paths:
            mock_paths.legacy_interfaces_dir = tmp_path / "nonexistent"
            merger = RegistryMerger()
            result = merger.merge_interfaces(auto_generated_registry)
            assert result == 0

    def test_merge_interfaces_manual_path_not_exists(
        self, auto_generated_registry, tmp_path
    ):
        """测试手工配置路径不存在"""
        merger = RegistryMerger()
        result = merger.merge_interfaces(
            auto_generated_registry, manual_config_path=tmp_path / "nonexistent"
        )
        assert result == 0

    def test_merge_interfaces_success(self, auto_generated_registry, tmp_path):
        """测试成功合并接口配置"""
        interfaces_dir = tmp_path / "interfaces"
        interfaces_dir.mkdir()

        manual_config = {
            "func1": {
                "sources": ["manual_source"],
                "category": "custom_category",
                "description": "Manual desc",
            }
        }
        with open(interfaces_dir / "manual.yaml", "w") as f:
            yaml.dump(manual_config, f)

        merger = RegistryMerger()
        result = merger.merge_interfaces(
            auto_generated_registry, manual_config_path=interfaces_dir
        )

        assert result == 1
        assert auto_generated_registry["interfaces"]["func1"]["sources"] == [
            "manual_source"
        ]
        assert (
            auto_generated_registry["interfaces"]["func1"]["category"]
            == "custom_category"
        )

    def test_merge_interfaces_with_input_output(
        self, auto_generated_registry, tmp_path
    ):
        """测试合并input/output配置"""
        interfaces_dir = tmp_path / "interfaces"
        interfaces_dir.mkdir()

        manual_config = {
            "func1": {
                "input": {"symbol": "000001"},
                "output": {"format": "DataFrame"},
            }
        }
        with open(interfaces_dir / "manual.yaml", "w") as f:
            yaml.dump(manual_config, f)

        merger = RegistryMerger()
        merger.merge_interfaces(
            auto_generated_registry, manual_config_path=interfaces_dir
        )

        assert auto_generated_registry["interfaces"]["func1"]["input"] == {
            "symbol": "000001"
        }
        assert auto_generated_registry["interfaces"]["func1"]["output"] == {
            "format": "DataFrame"
        }

    def test_merge_interfaces_override_rate_limit_key(
        self, auto_generated_registry, tmp_path
    ):
        """测试覆盖rate_limit_key"""
        interfaces_dir = tmp_path / "interfaces"
        interfaces_dir.mkdir()

        manual_config = {"func1": {"rate_limit_key": "custom_key"}}
        with open(interfaces_dir / "manual.yaml", "w") as f:
            yaml.dump(manual_config, f)

        merger = RegistryMerger()
        merger.merge_interfaces(
            auto_generated_registry, manual_config_path=interfaces_dir
        )

        assert (
            auto_generated_registry["interfaces"]["func1"]["rate_limit_key"]
            == "custom_key"
        )

    def test_merge_interfaces_multiple_files(self, auto_generated_registry, tmp_path):
        """测试合并多个文件"""
        interfaces_dir = tmp_path / "interfaces"
        interfaces_dir.mkdir()

        config1 = {"func1": {"sources": ["source1"]}}
        config2 = {"func2": {"sources": ["source2"]}}
        with open(interfaces_dir / "file1.yaml", "w") as f:
            yaml.dump(config1, f)
        with open(interfaces_dir / "file2.yaml", "w") as f:
            yaml.dump(config2, f)

        merger = RegistryMerger()
        result = merger.merge_interfaces(
            auto_generated_registry, manual_config_path=interfaces_dir
        )

        assert result == 2

    def test_merge_interfaces_file_parse_error(self, auto_generated_registry, tmp_path):
        """测试文件解析错误"""
        interfaces_dir = tmp_path / "interfaces"
        interfaces_dir.mkdir()

        invalid_file = interfaces_dir / "invalid.yaml"
        invalid_file.write_text("invalid: yaml: content:")

        merger = RegistryMerger()
        result = merger.merge_interfaces(
            auto_generated_registry, manual_config_path=interfaces_dir
        )

        assert result == 0

    def test_merge_rate_limits_no_path(self, auto_generated_registry, tmp_path):
        """测试无限制配置路径"""
        with patch("akshare_data.offline.registry.merger.paths") as mock_paths:
            mock_paths.legacy_rate_limits_file = tmp_path / "nonexistent"
            merger = RegistryMerger()
            result = merger.merge_rate_limits(auto_generated_registry)
            assert result == 0

    def test_merge_rate_limits_path_not_exists(self, auto_generated_registry, tmp_path):
        """测试限制配置路径不存在"""
        merger = RegistryMerger()
        result = merger.merge_rate_limits(
            auto_generated_registry, rate_limits_path=tmp_path / "nonexistent"
        )
        assert result == 0

    def test_merge_rate_limits_success(self, auto_generated_registry, tmp_path):
        """测试成功合并限速配置"""
        rate_file = tmp_path / "rate_limits.yaml"
        rate_file.write_text(
            "default:\n  interval: 1.0\n  burst: 10\nem_push2his:\n  interval: 2.0\n"
        )

        merger = RegistryMerger()
        result = merger.merge_rate_limits(
            auto_generated_registry, rate_limits_path=rate_file
        )

        assert result == 2
        assert auto_generated_registry["rate_limits"]["default"]["interval"] == 1.0
        assert auto_generated_registry["rate_limits"]["default"]["burst"] == 10

    def test_merge_rate_limits_exception(self, auto_generated_registry, tmp_path):
        """测试合并时异常"""
        merger = RegistryMerger()
        with patch("builtins.open", side_effect=IOError("Read error")):
            result = merger.merge_rate_limits(auto_generated_registry)
            assert result == 0

    def test_merge_single_interface_all_keys(self, auto_generated_registry):
        """测试合并单个接口的所有键"""
        merger = RegistryMerger()
        auto_iface = {"name": "test", "sources": [], "probe": {}}
        manual_iface = {
            "sources": ["source1", "source2"],
            "input": {"key": "value"},
            "output": {"format": "json"},
            "interface_name": "custom_name",
            "description": "desc",
            "category": "custom",
            "rate_limit_key": "custom",
        }

        merger._merge_single_interface(auto_iface, manual_iface)

        assert auto_iface["sources"] == ["source1", "source2"]
        assert auto_iface["input"] == {"key": "value"}
        assert auto_iface["output"] == {"format": "json"}
        assert auto_iface["interface_name"] == "custom_name"
        assert auto_iface["description"] == "desc"
        assert auto_iface["category"] == "custom"
        assert auto_iface["rate_limit_key"] == "custom"

    def test_merge_single_interface_partial_keys(self, auto_generated_registry):
        """测试合并单个接口的部分键"""
        merger = RegistryMerger()
        auto_iface = {"name": "test", "sources": []}
        manual_iface = {"sources": ["new_source"], "description": "desc"}

        merger._merge_single_interface(auto_iface, manual_iface)

        assert auto_iface["sources"] == ["new_source"]
        assert auto_iface["description"] == "desc"
        assert "input" not in auto_iface

    def test_merge_single_interface_empty_manual(self, auto_generated_registry):
        """测试空的手工配置"""
        merger = RegistryMerger()
        auto_iface = {"name": "test", "sources": ["original"]}
        manual_iface = {}

        merger._merge_single_interface(auto_iface, manual_iface)

        assert auto_iface["sources"] == ["original"]


class TestRegistryValidator:
    """RegistryValidator 类测试"""

    @pytest.fixture
    def valid_registry(self):
        """有效注册表"""
        return {
            "interfaces": {
                "func1": {
                    "name": "func1",
                    "category": "stock",
                    "description": "Test func",
                    "probe": {"params": {}, "skip": False},
                    "rate_limit_key": "default",
                },
            }
        }

    def test_validate_valid_registry(self, valid_registry):
        """测试验证有效注册表"""
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert len(errors) == 0

    def test_validate_missing_interfaces(self):
        """测试缺少interfaces部分"""
        validator = RegistryValidator()
        registry = {}
        errors = validator.validate(registry)
        assert len(errors) == 1
        assert "Missing 'interfaces'" in errors[0]

    def test_validate_interface_missing_name(self, valid_registry):
        """测试接口缺少name"""
        valid_registry["interfaces"]["func1"].pop("name")
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert any("missing 'name'" in e for e in errors)

    def test_validate_interface_missing_category(self, valid_registry):
        """测试接口缺少category"""
        valid_registry["interfaces"]["func1"].pop("category")
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert any("missing 'category'" in e for e in errors)

    def test_validate_interface_probe_params_not_dict(self, valid_registry):
        """测试probe.params不是字典"""
        valid_registry["interfaces"]["func1"]["probe"]["params"] = "not_dict"
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert any("probe.params must be a dict" in e for e in errors)

    def test_validate_interface_probe_skip_not_bool(self, valid_registry):
        """测试probe.skip不是布尔值"""
        valid_registry["interfaces"]["func1"]["probe"]["skip"] = "not_bool"
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert any("probe.skip must be a bool" in e for e in errors)

    def test_validate_interface_rate_limit_key_not_string(self, valid_registry):
        """测试rate_limit_key不是字符串"""
        valid_registry["interfaces"]["func1"]["rate_limit_key"] = 123
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert any("rate_limit_key must be a string" in e for e in errors)

    def test_validate_multiple_interfaces_multiple_errors(self):
        """测试多个接口多个错误"""
        validator = RegistryValidator()
        registry = {
            "interfaces": {
                "func1": {
                    "category": "stock",
                    "probe": {"params": "bad", "skip": "bad"},
                    "rate_limit_key": 123,
                },
                "func2": {
                    "name": "func2",
                },
            }
        }
        errors = validator.validate(registry)
        assert len(errors) > 2

    def test_validate_interface_no_probe(self, valid_registry):
        """测试接口无probe部分"""
        valid_registry["interfaces"]["func1"].pop("probe")
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert len(errors) == 0

    def test_validate_interface_no_rate_limit_key(self, valid_registry):
        """测试接口无rate_limit_key"""
        valid_registry["interfaces"]["func1"].pop("rate_limit_key")
        validator = RegistryValidator()
        errors = validator.validate(valid_registry)
        assert len(errors) == 0

    def test_validate_empty_interfaces(self):
        """测试空interfaces"""
        validator = RegistryValidator()
        registry = {"interfaces": {}}
        errors = validator.validate(registry)
        assert len(errors) == 0


class TestRegistryBuilderEdgeCases:
    """RegistryBuilder 边界情况测试"""

    def test_build_with_empty_scan_results(self):
        """测试空扫描结果"""
        builder = RegistryBuilder()
        with patch(
            "akshare_data.offline.registry.builder._load_rate_limits",
            return_value={"default": {"interval": 0.5}},
        ):
            result = builder.build(scan_results={})
        assert result["interfaces"] == {}

    def test_build_with_none_func_obj(self):
        """测试函数对象为None"""
        builder = RegistryBuilder()

        with (
            patch.object(builder, "_get_func_obj", return_value=None),
            patch.object(builder.domain_extractor, "extract", return_value=[]),
            patch.object(builder.category_inferrer, "infer", return_value="unknown"),
        ):
            result = builder._build_interface(
                "nonexistent", {"doc": "", "signature": []}
            )
            assert result["domains"] == []

    def test_infer_rate_limit_with_subdomain_match(self):
        """测试子域名匹配"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(["api.datacenter-web.eastmoney.com"])
        assert result == "em_datacenter"

    def test_infer_rate_limit_takes_first_match(self):
        """测试优先使用第一个匹配的域名"""
        builder = RegistryBuilder()
        result = builder._infer_rate_limit(
            ["hq.sinajs.cn", "vip.stock.finance.sina.com.cn"]
        )
        assert result == "sina_hq"


class TestRegistryExporterEdgeCases:
    """RegistryExporter 边界情况测试"""

    def test_export_split_empty_interfaces(self, tmp_path):
        """测试拆分导出空接口"""
        registry = {
            "version": "2.0",
            "generated_at": "2024-01-01",
            "description": "Empty",
            "interfaces": {},
        }
        exporter = RegistryExporter()
        output_dir = tmp_path / "empty"
        result = exporter.export_split(registry, output_dir=output_dir)

        assert result.exists()
        assert (output_dir / "_base.yaml").exists()

    def test_export_split_interfaces_without_category(self, tmp_path):
        """测试无category的接口"""
        registry = {
            "version": "2.0",
            "interfaces": {
                "uncategorized_func": {
                    "name": "uncategorized_func",
                    "category": "other",
                }
            },
        }
        exporter = RegistryExporter()
        output_dir = tmp_path / "registry"
        exporter.export_split(registry, output_dir=output_dir)

        assert (output_dir / "other.yaml").exists()

    def test_export_yaml_with_special_chars(self, tmp_path):
        """测试导出含特殊字符的YAML"""
        registry = {
            "version": "2.0",
            "interfaces": {
                "func": {
                    "name": "func",
                    "category": "test",
                    "description": "描述 with unicode: 中文",
                },
            },
        }
        exporter = RegistryExporter()
        output_file = tmp_path / "registry.yaml"
        exporter.export_yaml(registry, output_path=output_file)

        content = yaml.safe_load(output_file.read_text())
        assert "中文" in content["interfaces"]["func"]["description"]

    def test_export_json_with_unicode(self, tmp_path):
        """测试导出含unicode的JSON"""
        registry = {
            "version": "2.0",
            "interfaces": {
                "func": {
                    "name": "func",
                    "description": "描述: 中文",
                },
            },
        }
        exporter = RegistryExporter()
        output_file = tmp_path / "registry.json"
        exporter.export_json(registry, output_path=output_file)

        content = json.loads(output_file.read_text())
        assert "中文" in content["interfaces"]["func"]["description"]


class TestRegistryMergerEdgeCases:
    """RegistryMerger 边界情况测试"""

    def test_merge_interfaces_yaml_safe_load_returns_none(self, tmp_path):
        """测试yaml.safe_load返回None"""
        auto_generated_registry = {
            "interfaces": {
                "func1": {
                    "name": "func1",
                    "category": "stock",
                    "sources": [],
                    "probe": {"params": {}, "skip": False},
                },
            },
            "rate_limits": {
                "default": {"interval": 0.5},
                "em_push2his": {"interval": 1.0},
            },
        }
        interfaces_dir = tmp_path / "interfaces"
        interfaces_dir.mkdir()
        (interfaces_dir / "empty.yaml").write_text("")

        merger = RegistryMerger()
        result = merger.merge_interfaces(
            auto_generated_registry, manual_config_path=interfaces_dir
        )
        assert result == 0

    def test_merge_rate_limits_yaml_safe_load_returns_none(self, tmp_path):
        """测试限速配置yaml返回None"""
        registry = {"rate_limits": {}}
        rate_file = tmp_path / "empty.yaml"
        rate_file.write_text("")

        merger = RegistryMerger()
        result = merger.merge_rate_limits(registry, rate_limits_path=rate_file)
        assert result == 0

    def test_merge_rate_limits_non_dict_config(self, tmp_path):
        """测试限速配置非字典类型"""
        registry = {"rate_limits": {"default": {"interval": 0.5}}}
        rate_file = tmp_path / "bad_config.yaml"
        rate_file.write_text("default: 'not_a_dict'")

        merger = RegistryMerger()
        result = merger.merge_rate_limits(registry, rate_limits_path=rate_file)
        assert result == 1


class TestRegistryValidatorEdgeCases:
    """RegistryValidator 边界情况测试"""

    @pytest.fixture
    def valid_registry_for_edge(self):
        """有效注册表用于边界测试"""
        return {
            "interfaces": {
                "func1": {
                    "name": "func1",
                    "category": "stock",
                    "description": "Test func",
                    "probe": {"params": {}, "skip": False},
                    "rate_limit_key": "default",
                },
            }
        }

    def test_validate_interface_probe_params_missing(self, valid_registry_for_edge):
        """测试probe.params缺失"""
        valid_registry_for_edge["interfaces"]["func1"]["probe"]["params"] = None
        validator = RegistryValidator()
        errors = validator.validate(valid_registry_for_edge)
        assert any("probe.params must be a dict" in e for e in errors)

    def test_validate_interface_probe_skip_missing(self, valid_registry_for_edge):
        """测试probe.skip缺失"""
        valid_registry_for_edge["interfaces"]["func1"]["probe"]["skip"] = None
        validator = RegistryValidator()
        errors = validator.validate(valid_registry_for_edge)
        assert any("probe.skip must be a bool" in e for e in errors)

    def test_validate_empty_registry(self):
        """测试空注册表"""
        validator = RegistryValidator()
        errors = validator.validate({})
        assert len(errors) == 1
        assert "Missing 'interfaces'" in errors[0]

    def test_validate_none_registry(self):
        """测试None注册表 - 应该抛出TypeError"""
        validator = RegistryValidator()
        with pytest.raises(TypeError):
            validator.validate(None)

    def test_validate_underscore_category(self, valid_registry_for_edge):
        """测试category为下划线开头"""
        valid_registry_for_edge["interfaces"]["func1"]["category"] = "_private"
        validator = RegistryValidator()
        errors = validator.validate(valid_registry_for_edge)
        assert len(errors) == 0

    def test_validate_probe_with_extra_fields(self, valid_registry_for_edge):
        """测试probe含额外字段"""
        valid_registry_for_edge["interfaces"]["func1"]["probe"]["extra"] = "value"
        validator = RegistryValidator()
        errors = validator.validate(valid_registry_for_edge)
        assert len(errors) == 0
