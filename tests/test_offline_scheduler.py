"""tests/test_offline_scheduler.py

调度器模块测试
"""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

from akshare_data.offline.scheduler.scheduler import Scheduler
from akshare_data.offline.core.errors import ConfigError


class TestSchedulerInit:
    """测试Scheduler初始化"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.yaml.safe_load")
    def test_init_loads_schedule_file(self, mock_yaml, mock_paths):
        """测试初始化加载schedule配置"""
        mock_yaml.return_value = {
            "schedules": [
                {
                    "name": "task1",
                    "interface": "daily_bar",
                    "cron": "0 0 * * *",
                    "mode": "full",
                    "days_back": 5,
                    "symbols": "all",
                    "enabled": True,
                }
            ]
        }
        mock_paths.schedule_file.exists.return_value = True

        scheduler = Scheduler()
        assert "task1" in scheduler._tasks
        assert scheduler._tasks["task1"]["interface"] == "daily_bar"
        assert scheduler._tasks["task1"]["mode"] == "full"
        assert scheduler._tasks["task1"]["days_back"] == 5

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_init_schedule_file_not_exists(self, mock_paths):
        """测试schedule文件不存在"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        assert scheduler._tasks == {}

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.yaml.safe_load")
    def test_init_with_empty_config(self, mock_yaml, mock_paths):
        """测试空配置加载"""
        mock_paths.schedule_file.exists.return_value = True
        mock_yaml.return_value = {}
        scheduler = Scheduler()
        assert scheduler._tasks == {}

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.yaml.safe_load")
    def test_init_load_schedule_exception(self, mock_yaml, mock_paths):
        """测试加载异常"""
        mock_paths.schedule_file.exists.return_value = True
        mock_yaml.side_effect = Exception("parse error")

        scheduler = Scheduler()
        assert scheduler._tasks == {}


class TestSchedulerCalcNextRun:
    """测试_calc_next_run方法"""

    def test_calc_next_run_no_cron(self):
        """测试无cron表达式返回None"""
        scheduler = Scheduler.__new__(Scheduler)
        scheduler._tasks = {}

        result = scheduler._calc_next_run(None)
        assert result is None

    def test_calc_next_run_invalid_cron(self):
        """测试无效cron表达式"""
        scheduler = Scheduler.__new__(Scheduler)
        scheduler._tasks = {}

        result = scheduler._calc_next_run("invalid cron")
        assert result is None

    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_calc_next_run_valid_cron(self, mock_croniter):
        """测试有效cron表达式"""
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler.__new__(Scheduler)
        scheduler._tasks = {}

        result = scheduler._calc_next_run("0 0 * * *")
        assert result == mock_next
        mock_croniter.assert_called_once()


class TestSchedulerAddTask:
    """测试add_task方法"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_add_task_basic(self, mock_croniter, mock_paths):
        """测试添加基本任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task("new_task", "0 0 * * *", "daily_bar")

        assert "new_task" in scheduler._tasks
        assert scheduler._tasks["new_task"]["name"] == "new_task"
        assert scheduler._tasks["new_task"]["interface"] == "daily_bar"
        assert scheduler._tasks["new_task"]["cron"] == "0 0 * * *"
        assert scheduler._tasks["new_task"]["mode"] == "incremental"
        assert scheduler._tasks["new_task"]["days_back"] == 1
        assert scheduler._tasks["new_task"]["symbols"] is None
        assert scheduler._tasks["new_task"]["enabled"] is True

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_add_task_with_all_params(self, mock_croniter, mock_paths):
        """测试添加完整参数任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task(
            "full_task",
            "0 0 * * *",
            "daily_bar",
            mode="full",
            days_back=10,
            symbols="600000,600001",
        )

        task = scheduler._tasks["full_task"]
        assert task["mode"] == "full"
        assert task["days_back"] == 10
        assert task["symbols"] == "600000,600001"

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_add_task_overwrites_existing(self, mock_croniter, mock_paths):
        """测试添加同名任务会覆盖"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task("task1", "0 0 * * *", "bar1")
        scheduler.add_task("task1", "0 1 * * *", "bar2")

        assert scheduler._tasks["task1"]["interface"] == "bar2"


class TestSchedulerRemoveTask:
    """测试remove_task方法"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_remove_task_exists(self, mock_paths):
        """测试移除存在的任务"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._tasks["task1"] = {"name": "task1"}

        scheduler.remove_task("task1")
        assert "task1" not in scheduler._tasks

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_remove_task_not_exists(self, mock_paths):
        """测试移除不存在的任务"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()

        scheduler.remove_task("nonexistent")
        assert scheduler._tasks == {}


class TestSchedulerStartStop:
    """测试start和stop方法"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_start_not_running(self, mock_paths):
        """测试启动调度器"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._running = False

        scheduler.start()
        assert scheduler._running is True
        assert scheduler._thread is not None
        assert scheduler._thread.daemon is True

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_start_already_running(self, mock_paths):
        """测试重复启动无效"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._running = True

        original_thread = scheduler._thread
        scheduler.start()
        assert scheduler._thread is original_thread

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_stop(self, mock_paths):
        """测试停止调度器"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._running = True
        scheduler._thread = MagicMock()

        scheduler.stop()
        assert scheduler._running is False
        scheduler._thread.join.assert_called_once_with(timeout=10)

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_stop_no_thread(self, mock_paths):
        """测试停止时无线程"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._running = False
        scheduler._thread = None

        scheduler.stop()
        assert scheduler._running is False


class TestSchedulerRunNow:
    """测试run_now方法"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_run_now_task_exists(self, mock_croniter, mock_paths):
        """测试运行存在的任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task("task1", "0 0 * * *", "daily_bar")
        scheduler._tasks["task1"]["status"] = "pending"

        callback = MagicMock()
        scheduler.run_now("task1", callback)

        callback.assert_called_once()
        call_args = callback.call_args[0][0]
        assert call_args["name"] == "task1"
        assert call_args["interface"] == "daily_bar"
        assert scheduler._tasks["task1"]["status"] == "completed"
        assert scheduler._tasks["task1"]["last_run"] is not None

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_run_now_task_not_found(self, mock_croniter, mock_paths):
        """测试运行不存在的任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task("task1", "0 0 * * *", "daily_bar")

        with pytest.raises(ConfigError, match="Task nonexistent not found"):
            scheduler.run_now("nonexistent")

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_run_now_without_callback(self, mock_croniter, mock_paths):
        """测试无回调函数运行任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task("task1", "0 0 * * *", "daily_bar")
        scheduler._tasks["task1"]["status"] = "pending"

        scheduler.run_now("task1")
        assert scheduler._tasks["task1"]["status"] == "completed"


class TestSchedulerGetStatus:
    """测试get_status方法"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_get_status(self, mock_croniter, mock_paths):
        """测试获取状态"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_next.isoformat.return_value = "2024-01-01T00:00:00"
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler._running = True
        scheduler.add_task("task1", "0 0 * * *", "daily_bar")
        scheduler._tasks["task1"]["last_run"] = "2024-01-01T00:00:00"

        status = scheduler.get_status()

        assert status["running"] is True
        assert "task1" in status["tasks"]
        assert status["tasks"]["task1"]["interface"] == "daily_bar"
        assert status["tasks"]["task1"]["status"] == "pending"

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    def test_get_status_empty_tasks(self, mock_paths):
        """测试空任务列表状态"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._running = False

        status = scheduler.get_status()

        assert status["running"] is False
        assert status["tasks"] == {}


class TestSchedulerRunLoop:
    """测试_run_loop方法"""

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.time.sleep")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_run_loop_skips_disabled_tasks(self, mock_croniter, mock_sleep, mock_paths):
        """测试循环跳过禁用任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        scheduler = Scheduler()
        scheduler.add_task("task1", "0 0 * * *", "daily_bar")
        scheduler._tasks["task1"]["enabled"] = False

        def side_effect(seconds):
            scheduler._running = False

        mock_sleep.side_effect = side_effect

        scheduler._running = True
        scheduler._run_loop()
        mock_sleep.assert_called_with(60)
        assert scheduler._tasks["task1"]["status"] == "pending"

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.time.sleep")
    @patch("akshare_data.offline.scheduler.scheduler.datetime")
    @patch("akshare_data.offline.scheduler.scheduler.croniter")
    def test_run_loop_executes_due_tasks(
        self, mock_croniter, mock_datetime, mock_sleep, mock_paths
    ):
        """测试循环执行到期任务"""
        mock_paths.schedule_file.exists.return_value = False
        mock_next = MagicMock()
        mock_croniter.return_value.get_next.return_value = mock_next

        due_time = datetime(2024, 1, 1, 0, 0, 0)
        mock_datetime.now.return_value = due_time

        scheduler = Scheduler()
        scheduler.add_task("task1", "0 0 * * *", "daily_bar")
        scheduler._tasks["task1"]["next_run"] = due_time

        def side_effect(seconds):
            scheduler._running = False

        mock_sleep.side_effect = side_effect

        scheduler._running = True
        scheduler._run_loop()
        assert scheduler._tasks["task1"]["status"] == "completed"

    @patch("akshare_data.offline.scheduler.scheduler.paths")
    @patch("akshare_data.offline.scheduler.scheduler.time.sleep")
    @patch("akshare_data.offline.scheduler.scheduler.datetime")
    def test_run_loop_stops_when_not_running(
        self, mock_datetime, mock_sleep, mock_paths
    ):
        """测试循环在_running为False时停止"""
        mock_paths.schedule_file.exists.return_value = False
        scheduler = Scheduler()
        scheduler._running = False

        scheduler._run_loop()
        mock_sleep.assert_not_called()
