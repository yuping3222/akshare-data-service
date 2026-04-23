"""tests/test_offline_core.py

离线核心模块测试: config_loader, data_loader, errors, paths, retry

覆盖 offline/core/ 全部模块 100%
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import io
import logging

import pandas as pd
import yaml

from akshare_data.offline.core import config_loader, data_loader, errors, paths, retry


# =============================================================================
# errors.py 测试
# =============================================================================


class TestOfflineErrors:
    """OfflineError 异常类测试"""

    def test_offline_error_is_exception(self):
        """测试 OfflineError 继承自 Exception"""
        err = errors.OfflineError("test")
        assert isinstance(err, Exception)

    def test_config_error(self):
        """测试 ConfigError"""
        err = errors.ConfigError("config error")
        assert isinstance(err, errors.OfflineError)
        assert str(err) == "config error"

    def test_download_error(self):
        """测试 DownloadError"""
        err = errors.DownloadError("download error")
        assert isinstance(err, errors.OfflineError)

    def test_probe_error(self):
        """测试 ProbeError"""
        err = errors.ProbeError("probe error")
        assert isinstance(err, errors.OfflineError)

    def test_analysis_error(self):
        """测试 AnalysisError"""
        err = errors.AnalysisError("analysis error")
        assert isinstance(err, errors.OfflineError)

    def test_source_error(self):
        """测试 SourceError"""
        err = errors.SourceError("source error")
        assert isinstance(err, errors.OfflineError)

    def test_retry_exhausted_error(self):
        """测试 RetryExhaustedError"""
        original_err = ValueError("original")
        err = errors.RetryExhaustedError("retry failed", original_err)
        assert isinstance(err, errors.OfflineError)
        assert str(err) == "retry failed"
        assert err.last_error is original_err


# =============================================================================
# paths.py 测试
# =============================================================================


@pytest.fixture(autouse=True)
def reset_paths_singleton():
    """在每个测试前后重置 Paths 单例"""
    paths.Paths._instance = None
    paths.Paths._project_root = None
    yield
    paths.Paths._instance = None
    paths.Paths._project_root = None


@pytest.fixture
def capture_akshare_logs():
    """
    Capture logs from the 'akshare_data' logger directly.

    This is needed because setup_logging() sets propagate=False on the
    'akshare_data' logger, preventing caplog from capturing its messages.
    """
    logger = logging.getLogger("akshare_data")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logger.addHandler(handler)
    yield stream
    logger.removeHandler(handler)
    stream.close()


class TestPathsSingleton:
    """Paths 单例测试"""

    def test_singleton_returns_same_instance(self):
        """测试单例返回同一实例"""
        p1 = paths.Paths()
        p2 = paths.Paths()
        assert p1 is p2

    def test_singleton_reinitialized_returns_same(self):
        """测试单例再次初始化返回同一实例"""
        p1 = paths.Paths(Path("/tmp"))
        p2 = paths.Paths(Path("/another"))
        assert p1 is p2

    def test_project_root_default(self):
        """测试默认项目根目录"""
        p = paths.Paths()
        assert p.project_root is not None
        assert isinstance(p.project_root, Path)

    def test_project_root_custom(self):
        """测试自定义项目根目录"""
        custom_root = Path("/custom/root")
        p = paths.Paths(custom_root)
        assert p.project_root == custom_root


class TestPathsConfigDirs:
    """Paths 配置目录属性测试"""

    def test_config_dir(self):
        """测试 config_dir"""
        p = paths.Paths(Path("/test"))
        assert p.config_dir == Path("/test/config")

    def test_registry_dir(self):
        """测试 registry_dir"""
        p = paths.Paths(Path("/test"))
        assert p.registry_dir == Path("/test/config/registry")

    def test_sources_dir(self):
        """测试 sources_dir"""
        p = paths.Paths(Path("/test"))
        assert p.sources_dir == Path("/test/config/sources")

    def test_download_dir(self):
        """测试 download_dir"""
        p = paths.Paths(Path("/test"))
        assert p.download_dir == Path("/test/config/download")

    def test_prober_dir(self):
        """测试 prober_dir"""
        p = paths.Paths(Path("/test"))
        assert p.prober_dir == Path("/test/config/prober")

    def test_fields_dir(self):
        """测试 fields_dir"""
        p = paths.Paths(Path("/test"))
        assert p.fields_dir == Path("/test/config/fields")

    def test_cache_config_dir(self):
        """测试 cache_config_dir"""
        p = paths.Paths(Path("/test"))
        assert p.cache_config_dir == Path("/test/config/cache")

    def test_logging_config_dir(self):
        """测试 logging_config_dir"""
        p = paths.Paths(Path("/test"))
        assert p.logging_config_dir == Path("/test/config/logging")


class TestPathsConfigFiles:
    """Paths 配置文件属性测试"""

    def test_registry_file_no_category(self):
        """测试 registry_file 无分类"""
        p = paths.Paths(Path("/test"))
        assert p.registry_file() == Path("/test/config/registry/_base.yaml")

    def test_registry_file_with_category(self):
        """测试 registry_file 有分类"""
        p = paths.Paths(Path("/test"))
        assert p.registry_file("stocks") == Path("/test/config/registry/stocks.yaml")

    def test_domains_file(self):
        """测试 domains_file"""
        p = paths.Paths(Path("/test"))
        assert p.domains_file == Path("/test/config/sources/domains.yaml")

    def test_sources_file(self):
        """测试 sources_file"""
        p = paths.Paths(Path("/test"))
        assert p.sources_file == Path("/test/config/sources/sources.yaml")

    def test_failover_file(self):
        """测试 failover_file"""
        p = paths.Paths(Path("/test"))
        assert p.failover_file == Path("/test/config/sources/failover.yaml")

    def test_priority_file(self):
        """测试 priority_file"""
        p = paths.Paths(Path("/test"))
        assert p.priority_file == Path("/test/config/download/priority.yaml")

    def test_schedule_file(self):
        """测试 schedule_file"""
        p = paths.Paths(Path("/test"))
        assert p.schedule_file == Path("/test/config/download/schedule.yaml")

    def test_prober_config_file(self):
        """测试 prober_config_file"""
        p = paths.Paths(Path("/test"))
        assert p.prober_config_file == Path("/test/config/prober/config.yaml")

    def test_prober_state_file(self):
        """测试 prober_state_file"""
        p = paths.Paths(Path("/test"))
        assert p.prober_state_file == Path("/test/config/prober/state.json")

    def test_prober_samples_dir(self):
        """测试 prober_samples_dir"""
        p = paths.Paths(Path("/test"))
        assert p.prober_samples_dir == Path("/test/config/prober/samples")

    def test_cn_to_en_file(self):
        """测试 cn_to_en_file"""
        p = paths.Paths(Path("/test"))
        assert p.cn_to_en_file == Path("/test/config/fields/cn_to_en.yaml")

    def test_type_hints_file(self):
        """测试 type_hints_file"""
        p = paths.Paths(Path("/test"))
        assert p.type_hints_file == Path("/test/config/fields/type_hints.yaml")

    def test_field_mappings_dir(self):
        """测试 field_mappings_dir"""
        p = paths.Paths(Path("/test"))
        assert p.field_mappings_dir == Path("/test/config/fields/mappings")

    def test_cache_strategies_file(self):
        """测试 cache_strategies_file"""
        p = paths.Paths(Path("/test"))
        assert p.cache_strategies_file == Path("/test/config/cache/strategies.yaml")

    def test_access_log_config_file(self):
        """测试 access_log_config_file"""
        p = paths.Paths(Path("/test"))
        assert p.access_log_config_file == Path("/test/config/logging/access.yaml")


class TestPathsDataDirs:
    """Paths 数据目录属性测试"""

    def test_logs_dir(self):
        """测试 logs_dir"""
        p = paths.Paths(Path("/test"))
        assert p.logs_dir == Path("/test/logs")

    def test_reports_dir(self):
        """测试 reports_dir"""
        p = paths.Paths(Path("/test"))
        assert p.reports_dir == Path("/test/reports")

    def test_health_reports_dir(self):
        """测试 health_reports_dir"""
        p = paths.Paths(Path("/test"))
        assert p.health_reports_dir == Path("/test/reports/health")

    def test_quality_reports_dir(self):
        """测试 quality_reports_dir"""
        p = paths.Paths(Path("/test"))
        assert p.quality_reports_dir == Path("/test/reports/quality")

    def test_dashboard_dir(self):
        """测试 dashboard_dir"""
        p = paths.Paths(Path("/test"))
        assert p.dashboard_dir == Path("/test/reports/dashboard")


class TestPathsLegacyDirs:
    """Paths 旧路径兼容测试"""

    def test_legacy_registry_file(self):
        """测试 legacy_registry_file"""
        p = paths.Paths(Path("/test"))
        assert p.legacy_registry_file == Path("/test/config/akshare_registry.yaml")

    def test_legacy_health_state_file(self):
        """测试 legacy_health_state_file"""
        p = paths.Paths(Path("/test"))
        assert p.legacy_health_state_file == Path("/test/config/health_state.json")

    def test_legacy_rate_limits_file(self):
        """测试 legacy_rate_limits_file"""
        p = paths.Paths(Path("/test"))
        assert p.legacy_rate_limits_file == Path("/test/config/rate_limits.yaml")

    def test_legacy_interfaces_dir(self):
        """测试 legacy_interfaces_dir"""
        p = paths.Paths(Path("/test"))
        assert p.legacy_interfaces_dir == Path("/test/config/interfaces")

    def test_legacy_health_samples_dir(self):
        """测试 legacy_health_samples_dir"""
        p = paths.Paths(Path("/test"))
        assert p.legacy_health_samples_dir == Path("/test/config/health_samples")

    def test_legacy_field_mappings_dir(self):
        """测试 legacy_field_mappings_dir"""
        p = paths.Paths(Path("/test"))
        assert p.legacy_field_mappings_dir == Path("/test/config/field_mappings")


class TestPathsEnsureDirs:
    """Paths.ensure_dirs 测试"""

    def test_ensure_dirs_creates_all_directories(self):
        """测试 ensure_dirs 创建所有目录"""
        test_root = Path("/test_ensure")
        p = paths.Paths(test_root)

        with patch.object(Path, "mkdir") as mock_mkdir:
            p.ensure_dirs()
            assert mock_mkdir.call_count > 0


# =============================================================================
# config_loader.py 测试
# =============================================================================


class TestConfigLoaderInit:
    """ConfigLoader 初始化测试"""

    def test_init_creates_empty_cache(self):
        """测试初始化创建空缓存"""
        loader = config_loader.ConfigLoader()
        assert loader._cache == {}


class TestConfigLoaderLoadYaml:
    """ConfigLoader.load_yaml 测试"""

    def test_load_yaml_uses_cache(self):
        """测试使用缓存"""
        loader = config_loader.ConfigLoader()
        test_path = Path("/test/config.yaml")
        loader._cache["/test/config.yaml"] = {"key": "value"}

        result = loader.load_yaml(test_path)
        assert result == {"key": "value"}

    def test_load_yaml_no_cache(self):
        """测试无缓存时加载文件"""
        loader = config_loader.ConfigLoader()
        test_path = Path("/test/config.yaml")
        loader._cache = {}

        yaml_content = "key: value"

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                with patch("yaml.safe_load", return_value={"key": "value"}):
                    result = loader.load_yaml(test_path, use_cache=False)
                    assert result == {"key": "value"}

    def test_load_yaml_file_not_found(self):
        """测试文件不存在"""
        loader = config_loader.ConfigLoader()
        test_path = Path("/nonexistent/config.yaml")

        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(errors.ConfigError, match="Config file not found"):
                loader.load_yaml(test_path)

    def test_load_yaml_parse_error(self):
        """测试 YAML 解析错误"""
        loader = config_loader.ConfigLoader()
        test_path = Path("/test/bad.yaml")

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data="invalid: yaml: content:")):
                with patch("yaml.safe_load", side_effect=yaml.YAMLError("parse error")):
                    with pytest.raises(
                        errors.ConfigError, match="Failed to parse YAML"
                    ):
                        loader.load_yaml(test_path)

    def test_load_yaml_caches_result(self):
        """测试加载后缓存结果"""
        loader = config_loader.ConfigLoader()
        test_path = Path("/test/config.yaml")
        loader._cache = {}
        yaml_content = "key: value"

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=yaml_content)):
                with patch("yaml.safe_load", return_value={"key": "value"}):
                    loader.load_yaml(test_path, use_cache=True)
                    assert "/test/config.yaml" in loader._cache


class TestConfigLoaderLoadRegistry:
    """ConfigLoader.load_registry 测试"""

    def test_load_registry_with_category(self):
        """测试带分类的加载"""
        loader = config_loader.ConfigLoader()

        with patch("akshare_data.offline.core.config_loader.ConfigCache") as mock_cache:
            mock_cache.load_registry.return_value = {"key": "value"}
            result = loader.load_registry("stocks")
            mock_cache.load_registry.assert_called_once()
            assert result == {"key": "value"}

    def test_load_registry_without_category(self):
        """测试不带分类的加载"""
        loader = config_loader.ConfigLoader()

        with patch("akshare_data.offline.core.config_loader.ConfigCache") as mock_cache:
            mock_cache.load_registry.return_value = {"key": "base"}
            result = loader.load_registry()
            mock_cache.load_registry.assert_called_once()
            assert result == {"key": "base"}


class TestConfigLoaderLoadAllRegistries:
    """ConfigLoader.load_all_registries 测试"""

    def test_load_all_registries_success(self):
        """测试成功加载所有注册表"""
        loader = config_loader.ConfigLoader()

        mock_yaml_file1 = MagicMock()
        mock_yaml_file1.stem = "stocks"
        mock_yaml_file1.name = "stocks.yaml"
        mock_yaml_file1.exists.return_value = True

        mock_yaml_file2 = MagicMock()
        mock_yaml_file2.stem = "funds"
        mock_yaml_file2.name = "funds.yaml"
        mock_yaml_file2.exists.return_value = True

        mock_glob_iterator = iter([mock_yaml_file1, mock_yaml_file2])
        mock_registry_dir = MagicMock()
        mock_registry_dir.exists.return_value = True
        mock_registry_dir.glob.return_value = mock_glob_iterator

        def load_yaml_side_effect(file_path, use_cache=True):
            return {"data": "value"}

        with patch.object(config_loader, "paths") as mock_paths_obj:
            mock_paths_obj.registry_dir = mock_registry_dir
            with patch.object(loader, "load_yaml", side_effect=load_yaml_side_effect):
                result = loader.load_all_registries()
                assert "stocks" in result
                assert "funds" in result

    def test_load_all_registries_skips_underscore_files(self):
        """测试跳过下划线开头的文件"""
        loader = config_loader.ConfigLoader()

        mock_yaml_file = MagicMock()
        mock_yaml_file.stem = "_base"
        mock_yaml_file.name = "_base.yaml"
        mock_yaml_file.exists.return_value = True

        mock_glob_iterator = iter([mock_yaml_file])
        mock_registry_dir = MagicMock()
        mock_registry_dir.exists.return_value = True
        mock_registry_dir.glob.return_value = mock_glob_iterator

        def load_yaml_side_effect(file_path, use_cache=True):
            return {"data": "value"}

        with patch.object(config_loader, "paths") as mock_paths_obj:
            mock_paths_obj.registry_dir = mock_registry_dir
            with patch.object(loader, "load_yaml", side_effect=load_yaml_side_effect):
                result = loader.load_all_registries()
                assert "_base" not in result

    def test_load_all_registries_dir_not_found(self):
        """测试注册表目录不存在"""
        loader = config_loader.ConfigLoader()

        mock_registry_dir = MagicMock()
        mock_registry_dir.exists.return_value = False

        with patch.object(config_loader, "paths") as mock_paths_obj:
            mock_paths_obj.registry_dir = mock_registry_dir
            with pytest.raises(
                errors.ConfigError, match="Registry directory not found"
            ):
                loader.load_all_registries()


class TestConfigLoaderLoadDomains:
    """ConfigLoader.load_domains 测试"""

    def test_load_domains(self):
        """测试加载域名配置"""
        loader = config_loader.ConfigLoader()

        with patch.object(loader, "load_yaml", return_value={"sina": 10}):
            with patch("akshare_data.offline.core.paths.paths") as mock_paths:
                mock_paths.domains_file = Path("/test/sources/domains.yaml")
                result = loader.load_domains()
                assert result == {"sina": 10}


class TestConfigLoaderLoadFailover:
    """ConfigLoader.load_failover 测试"""

    def test_load_failover(self):
        """测试加载切源配置"""
        loader = config_loader.ConfigLoader()

        with patch.object(
            loader, "load_yaml", return_value={"enabled": True}
        ):
            with patch("akshare_data.offline.core.paths.paths") as mock_paths:
                mock_paths.failover_file = Path("/test/sources/failover.yaml")
                result = loader.load_failover()
                assert result == {"enabled": True}


class TestConfigLoaderLoadPriority:
    """ConfigLoader.load_priority 测试"""

    def test_load_priority(self):
        """测试加载优先级配置"""
        loader = config_loader.ConfigLoader()

        with patch.object(
            loader, "load_yaml", return_value={"priority": 1}
        ):
            with patch("akshare_data.offline.core.paths.paths") as mock_paths:
                mock_paths.priority_file = Path("/test/download/priority.yaml")
                result = loader.load_priority()
                assert result == {"priority": 1}


class TestConfigLoaderLoadSchedule:
    """ConfigLoader.load_schedule 测试"""

    def test_load_schedule(self):
        """测试加载调度配置"""
        loader = config_loader.ConfigLoader()

        with patch.object(
            loader, "load_yaml", return_value={"schedule": "daily"}
        ):
            with patch("akshare_data.offline.core.paths.paths") as mock_paths:
                mock_paths.schedule_file = Path("/test/download/schedule.yaml")
                result = loader.load_schedule()
                assert result == {"schedule": "daily"}


class TestConfigLoaderLoadProberConfig:
    """ConfigLoader.load_prober_config 测试"""

    def test_load_prober_config(self):
        """测试加载探测配置"""
        loader = config_loader.ConfigLoader()

        with patch.object(
            loader, "load_yaml", return_value={"timeout": 30}
        ):
            with patch("akshare_data.offline.core.paths.paths") as mock_paths:
                mock_paths.prober_config_file = Path("/test/prober/config.yaml")
                result = loader.load_prober_config()
                assert result == {"timeout": 30}


class TestConfigLoaderLoadProberState:
    """ConfigLoader.load_prober_state 测试"""

    def test_load_prober_state(self):
        """测试加载探测状态"""
        loader = config_loader.ConfigLoader()

        with patch.object(
            loader, "load_yaml", return_value={"last_probe": "2024-01-01"}
        ):
            with patch("akshare_data.offline.core.paths.paths") as mock_paths:
                mock_paths.prober_state_file = Path("/test/prober/state.json")
                result = loader.load_prober_state()
                assert result == {"last_probe": "2024-01-01"}


class TestConfigLoaderInvalidateCache:
    """ConfigLoader.invalidate_cache 测试"""

    def test_invalidate_cache_specific_file(self):
        """测试清除特定文件缓存"""
        loader = config_loader.ConfigLoader()
        loader._cache = {"/test/file1.yaml": {}, "/test/file2.yaml": {}}

        loader.invalidate_cache(Path("/test/file1.yaml"))
        assert "/test/file1.yaml" not in loader._cache
        assert "/test/file2.yaml" in loader._cache

    def test_invalidate_cache_all(self):
        """测试清除所有缓存"""
        loader = config_loader.ConfigLoader()
        loader._cache = {"/test/file1.yaml": {}, "/test/file2.yaml": {}}

        loader.invalidate_cache()
        assert loader._cache == {}


# =============================================================================
# retry.py 测试
# =============================================================================


class TestRetryConfig:
    """RetryConfig 测试"""

    def test_default_init(self):
        """测试默认初始化"""
        config = retry.RetryConfig()
        assert config.max_retries == 3
        assert config.delay == 1.0
        assert config.backoff == 2.0
        assert config.exceptions == (Exception,)

    def test_custom_init(self):
        """测试自定义初始化"""
        config = retry.RetryConfig(
            max_retries=5,
            delay=2.0,
            backoff=3.0,
            exceptions=(ValueError, TypeError),
        )
        assert config.max_retries == 5
        assert config.delay == 2.0
        assert config.backoff == 3.0
        assert config.exceptions == (ValueError, TypeError)


class TestRetryDecorator:
    """retry 装饰器测试"""

    def test_retry_default_config(self):
        """测试默认配置重试"""
        call_count = 0

        @retry.retry()
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("fail")
            return "success"

        result = failing_func()
        assert result == "success"
        assert call_count == 3

    def test_retry_custom_config(self):
        """测试自定义配置重试"""
        call_count = 0

        custom_config = retry.RetryConfig(max_retries=2, delay=0.01, backoff=1.0)

        @retry.retry(config=custom_config)
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("fail")
            return "success"

        result = failing_func()
        assert result == "success"
        assert call_count == 3

    def test_retry_exhausted_raises_retry_exhausted_error(self):
        """测试重试耗尽抛出 RetryExhaustedError"""
        call_count = 0

        @retry.retry(config=retry.RetryConfig(max_retries=2, delay=0.01, backoff=1.0))
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("always fail")

        with pytest.raises(errors.RetryExhaustedError) as exc_info:
            always_fails()

        assert exc_info.value.last_error is not None
        assert "failed after 2 retries" in str(exc_info.value)

    def test_retry_no_exception_on_success(self):
        """测试成功时不重试"""
        call_count = 0

        @retry.retry()
        def success_func():
            nonlocal call_count
            call_count += 1
            return "done"

        result = success_func()
        assert result == "done"
        assert call_count == 1

    def test_retry_respects_exception_types(self):
        """测试只捕获指定的异常类型"""
        call_count = 0

        @retry.retry(
            config=retry.RetryConfig(
                max_retries=1, delay=0.01, exceptions=(ValueError,)
            )
        )
        def type_error_func():
            nonlocal call_count
            call_count += 1
            raise TypeError("not a ValueError")

        with pytest.raises(TypeError):
            type_error_func()
        assert call_count == 1

    def test_retry_logs_warning_on_failure(self, capture_akshare_logs):
        """测试失败时记录警告"""
        call_count = 0

        @retry.retry(config=retry.RetryConfig(max_retries=1, delay=0.01, backoff=1.0))
        def failing_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("fail")

        with pytest.raises(errors.RetryExhaustedError):
            failing_func()

        assert "failed" in capture_akshare_logs.getvalue()


# =============================================================================
# data_loader.py 测试
# =============================================================================


class TestLoadTable:
    """load_table 测试"""

    def test_load_table_with_specific_layer(self):
        """测试加载指定层"""
        mock_manager = MagicMock()
        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        mock_manager.read.return_value = mock_df

        with patch(
            "akshare_data.store.manager.get_cache_manager", return_value=mock_manager
        ):
            result = data_loader.load_table("test_table", layer="daily")
            assert not result.empty
            mock_manager.read.assert_called_once_with(
                "test_table", storage_layer="daily"
            )

    def test_load_table_with_empty_df(self):
        """测试加载返回空 DataFrame"""
        mock_manager = MagicMock()
        mock_manager.read.return_value = pd.DataFrame()

        with patch(
            "akshare_data.store.manager.get_cache_manager", return_value=mock_manager
        ):
            result = data_loader.load_table("test_table", layer="daily")
            assert result.empty

    def test_load_table_with_none_result(self):
        """测试加载返回 None"""
        mock_manager = MagicMock()
        mock_manager.read.return_value = None

        with patch(
            "akshare_data.store.manager.get_cache_manager", return_value=mock_manager
        ):
            result = data_loader.load_table("test_table", layer="daily")
            assert result.empty

    def test_load_table_falls_back_through_layers(self):
        """测试遍历所有层"""
        mock_manager = MagicMock()
        mock_manager.read.side_effect = [None, pd.DataFrame({"col": [1]})]

        with patch(
            "akshare_data.store.manager.get_cache_manager", return_value=mock_manager
        ):
            with patch("akshare_data.offline.core.data_loader.logger"):
                result = data_loader.load_table("test_table")
                assert not result.empty

    def test_load_table_no_data_returns_empty_with_warning(self, capture_akshare_logs):
        """测试无数据时返回空 DataFrame 并记录警告"""
        mock_manager = MagicMock()
        mock_manager.read.return_value = None

        with patch(
            "akshare_data.store.manager.get_cache_manager", return_value=mock_manager
        ):
            result = data_loader.load_table("nonexistent_table")
            assert result.empty
            assert "No cached data found" in capture_akshare_logs.getvalue()


class TestGetCacheManagerInstance:
    """get_cache_manager_instance 测试"""

    def test_returns_cache_manager_instance(self):
        """测试返回 CacheManager 实例"""
        mock_instance = MagicMock()

        with patch(
            "akshare_data.store.manager.CacheManager", return_value=mock_instance
        ):
            result = data_loader.get_cache_manager_instance()
            assert result is mock_instance
