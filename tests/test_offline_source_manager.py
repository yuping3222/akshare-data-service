"""tests/test_offline_source_manager.py

数据源管理模块测试
"""

import time
from unittest.mock import patch

from akshare_data.offline.source_manager.health_tracker import HealthTracker
from akshare_data.offline.source_manager.failover import FailoverManager


class TestHealthTrackerInit:
    """测试HealthTracker初始化"""

    def test_init_empty_sources(self):
        """测试初始化空源"""
        tracker = HealthTracker()
        assert tracker._sources == {}


class TestHealthTrackerRecordSuccess:
    """测试record_success方法"""

    def test_record_success_new_source(self):
        """测试记录新源成功"""
        tracker = HealthTracker()
        tracker.record_success("src1", latency=0.5)

        assert "src1" in tracker._sources
        src = tracker._sources["src1"]
        assert src["success_count"] == 1
        assert src["failure_count"] == 0
        assert src["total_latency"] == 0.5
        assert src["consecutive_failures"] == 0
        assert src["last_success"] is not None

    def test_record_success_existing_source(self):
        """测试记录已存在源成功"""
        tracker = HealthTracker()
        tracker.record_success("src1", latency=0.5)
        tracker.record_success("src1", latency=0.3)

        src = tracker._sources["src1"]
        assert src["success_count"] == 2
        assert src["total_latency"] == 0.8

    def test_record_success_zero_latency(self):
        """测试零延迟成功记录"""
        tracker = HealthTracker()
        tracker.record_success("src1", latency=0.0)

        assert tracker._sources["src1"]["total_latency"] == 0.0


class TestHealthTrackerRecordFailure:
    """测试record_failure方法"""

    def test_record_failure_new_source(self):
        """测试记录新源失败"""
        tracker = HealthTracker()
        tracker.record_failure("src1", error="connection timeout")

        assert "src1" in tracker._sources
        src = tracker._sources["src1"]
        assert src["failure_count"] == 1
        assert src["success_count"] == 0
        assert src["consecutive_failures"] == 1
        assert src["last_failure"] is not None

    def test_record_failure_existing_source(self):
        """测试记录已存在源失败"""
        tracker = HealthTracker()
        tracker.record_failure("src1", error="error1")
        tracker.record_failure("src1", error="error2")

        src = tracker._sources["src1"]
        assert src["failure_count"] == 2
        assert src["consecutive_failures"] == 2

    def test_record_failure_empty_error(self):
        """测试空错误消息"""
        tracker = HealthTracker()
        tracker.record_failure("src1", error="")

        assert tracker._sources["src1"]["failure_count"] == 1


class TestHealthTrackerUpdateHealth:
    """测试_update_health方法"""

    def test_update_health_perfect_score(self):
        """测试完美健康分数"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 100,
            "failure_count": 0,
            "total_latency": 100.0,
            "consecutive_failures": 0,
            "health_score": 100.0,
        }

        tracker._update_health("src1")

        score = tracker._sources["src1"]["health_score"]
        assert score > 90

    def test_update_health_zero_total(self):
        """测试零总数时不更新"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 0,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 100.0,
        }

        tracker._update_health("src1")

        assert tracker._sources["src1"]["health_score"] == 100.0

    def test_update_health_with_failures(self):
        """测试有失败时的健康分数"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 5,
            "total_latency": 5000.0,
            "consecutive_failures": 3,
            "health_score": 100.0,
        }

        tracker._update_health("src1")

        score = tracker._sources["src1"]["health_score"]
        assert score < 100

    def test_update_health_high_latency_penalty(self):
        """测试高延迟惩罚"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 30000.0,
            "consecutive_failures": 0,
            "health_score": 100.0,
        }

        tracker._update_health("src1")

        score = tracker._sources["src1"]["health_score"]
        assert score < 80


class TestHealthTrackerGetHealthScore:
    """测试get_health_score方法"""

    def test_get_health_score_existing_source(self):
        """测试获取已存在源健康分数"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 85.5,
        }

        assert tracker.get_health_score("src1") == 85.5

    def test_get_health_score_unknown_source(self):
        """测试获取未知源默认健康分数"""
        tracker = HealthTracker()
        assert tracker.get_health_score("unknown") == 100.0


class TestHealthTrackerGetBestSource:
    """测试get_best_source方法"""

    def test_get_best_source_empty_candidates(self):
        """测试空候选源列表"""
        tracker = HealthTracker()
        result = tracker.get_best_source("daily_bar", [])
        assert result is None

    def test_get_best_source_single_candidate(self):
        """测试单个候选源"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 90.0,
        }

        result = tracker.get_best_source("daily_bar", ["src1"])
        assert result == "src1"

    def test_get_best_source_multiple_candidates(self):
        """测试多个候选源返回最高分"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 5,
            "failure_count": 5,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 50.0,
        }
        tracker._sources["src2"] = {
            "name": "src2",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 100.0,
        }

        result = tracker.get_best_source("daily_bar", ["src1", "src2"])
        assert result == "src2"

    def test_get_best_source_with_unknown_candidates(self):
        """测试候选源包含未知源时未知源因为默认100分最高而选中"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 90.0,
        }

        result = tracker.get_best_source("daily_bar", ["src1", "unknown"])
        assert result == "unknown"


class TestHealthTrackerGetAllStatus:
    """测试get_all_status方法"""

    def test_get_all_status_empty(self):
        """测试空状态"""
        tracker = HealthTracker()
        status = tracker.get_all_status()
        assert status == {}

    def test_get_all_status_with_sources(self):
        """测试有源时的状态"""
        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 2,
            "total_latency": 50.0,
            "consecutive_failures": 0,
            "health_score": 85.555,
            "last_success": None,
            "last_failure": None,
        }

        status = tracker.get_all_status()

        assert "src1" in status
        assert status["src1"]["health_score"] == 85.56
        assert status["src1"]["success_count"] == 10
        assert status["src1"]["failure_count"] == 2
        assert status["src1"]["consecutive_failures"] == 0


class TestFailoverManagerInit:
    """测试FailoverManager初始化"""

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_init_with_custom_health_tracker(self, mock_yaml, mock_paths):
        """测试使用自定义健康追踪器初始化"""
        mock_paths.failover_file.exists.return_value = False
        custom_tracker = HealthTracker()

        manager = FailoverManager(health_tracker=custom_tracker)

        assert manager._health is custom_tracker

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_init_loads_default_config(self, mock_yaml, mock_paths):
        """测试加载默认配置"""
        mock_paths.failover_file.exists.return_value = False

        manager = FailoverManager()

        assert manager._failover_config["failure_threshold"] == 3
        assert manager._failover_config["cooldown_seconds"] == 300
        assert manager._failover_config["auto_recovery"]["enabled"] is True

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_init_loads_config_file(self, mock_yaml, mock_paths):
        """测试从文件加载配置"""
        mock_paths.failover_file.exists.return_value = True
        mock_yaml.return_value = {
            "failure_threshold": 5,
            "cooldown_seconds": 600,
            "auto_recovery": {"enabled": False},
            "source_priority": {"src1": 1, "src2": 2},
        }

        manager = FailoverManager()

        assert manager._failover_config["failure_threshold"] == 5
        assert manager._failover_config["cooldown_seconds"] == 600
        assert manager._failover_config["auto_recovery"]["enabled"] is False

    def test_init_config_file_not_exists(self):
        """测试配置文件不存在时使用默认"""
        with patch("akshare_data.offline.source_manager.failover.paths") as mock_paths:
            mock_paths.failover_file.exists.return_value = False

            manager = FailoverManager()

            assert manager._failover_config["failure_threshold"] == 3

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_init_yaml_exception(self, mock_yaml, mock_paths):
        """测试YAML解析异常"""
        mock_paths.failover_file.exists.return_value = True
        mock_yaml.side_effect = Exception("parse error")

        manager = FailoverManager()

        assert manager._failover_config == {}


class TestFailoverManagerShouldFailover:
    """测试should_failover方法"""

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_should_failover_low_health_score(self, mock_yaml, mock_paths):
        """测试低健康分触发切换"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 1,
            "failure_count": 10,
            "total_latency": 0.0,
            "consecutive_failures": 10,
            "health_score": 40.0,
        }
        manager._health = tracker

        assert manager.should_failover("src1") is True

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_should_failover_in_cooldown(self, mock_yaml, mock_paths):
        """测试在冷却期不切换"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 1,
            "failure_count": 5,
            "total_latency": 0.0,
            "consecutive_failures": 5,
            "health_score": 60.0,
        }
        manager._health = tracker

        manager._cooldowns["src1"] = time.time() - 100

        assert manager.should_failover("src1") is False

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_should_failover_healthy_source(self, mock_yaml, mock_paths):
        """测试健康源不切换"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 100,
            "failure_count": 1,
            "total_latency": 100.0,
            "consecutive_failures": 0,
            "health_score": 95.0,
        }
        manager._health = tracker

        assert manager.should_failover("src1") is False


class TestFailoverManagerFailover:
    """测试failover方法"""

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_failover_sets_cooldown(self, mock_yaml, mock_paths):
        """测试切换时设置冷却"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 100,
            "failure_count": 1,
            "total_latency": 100.0,
            "consecutive_failures": 0,
            "health_score": 95.0,
        }
        tracker._sources["src2"] = {
            "name": "src2",
            "success_count": 100,
            "failure_count": 1,
            "total_latency": 100.0,
            "consecutive_failures": 0,
            "health_score": 90.0,
        }
        manager._health = tracker

        manager.failover("daily_bar", "src1", ["src1", "src2"])

        assert "src1" in manager._cooldowns

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_failover_returns_best_source(self, mock_yaml, mock_paths):
        """测试返回最优源"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 5,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 60.0,
        }
        tracker._sources["src2"] = {
            "name": "src2",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 100.0,
        }
        manager._health = tracker

        result = manager.failover("daily_bar", "src1", ["src1", "src2"])

        assert result == "src2"

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_failover_no_alternative(self, mock_yaml, mock_paths):
        """测试无替代源时返回None"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 5,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 60.0,
        }
        manager._health = tracker

        result = manager.failover("daily_bar", "src1", ["src1"])

        assert result is None

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_failover_best_is_same_as_failed(self, mock_yaml, mock_paths):
        """测试最优源就是失败的源"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        tracker = HealthTracker()
        tracker._sources["src1"] = {
            "name": "src1",
            "success_count": 10,
            "failure_count": 0,
            "total_latency": 0.0,
            "consecutive_failures": 0,
            "health_score": 100.0,
        }
        manager._health = tracker

        result = manager.failover("daily_bar", "src1", ["src1"])

        assert result is None


class TestFailoverManagerRecover:
    """测试recover方法"""

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_recover_removes_cooldown(self, mock_yaml, mock_paths):
        """测试恢复移除冷却"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()
        manager._cooldowns["src1"] = time.time()

        manager.recover("src1")

        assert "src1" not in manager._cooldowns

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_recover_nonexistent_source(self, mock_yaml, mock_paths):
        """测试恢复不存在的源"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        manager.recover("nonexistent")

        assert "nonexistent" not in manager._cooldowns


class TestFailoverManagerGetPrioritySources:
    """测试get_priority_sources方法"""

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_get_priority_sources_empty(self, mock_yaml, mock_paths):
        """测试空优先级配置"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()

        result = manager.get_priority_sources("daily_bar")

        assert result == []

    @patch("akshare_data.offline.source_manager.failover.paths")
    @patch("akshare_data.offline.source_manager.failover.yaml.safe_load")
    def test_get_priority_sources_sorted(self, mock_yaml, mock_paths):
        """测试按优先级排序"""
        mock_paths.failover_file.exists.return_value = False
        manager = FailoverManager()
        manager._failover_config["source_priority"] = {
            "src3": 3,
            "src1": 1,
            "src2": 2,
        }
        # With source validation enabled, src1/src2/src3 must be in sources.yaml.
        # For this test, disable validation by clearing the source registry.
        manager._source_registry = {}

        result = manager.get_priority_sources("daily_bar")

        assert result == ["src1", "src2", "src3"]
