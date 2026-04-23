"""定时任务调度器"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from croniter import croniter
import yaml

from akshare_data.offline.core.paths import paths
from akshare_data.offline.core.errors import ConfigError

logger = logging.getLogger("akshare_data")


class Scheduler:
    """定时任务调度器"""

    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._downloader: Any = None
        self._load_schedule()

    def _load_schedule(self):
        """加载调度配置"""
        schedule_file = paths.schedule_file
        if not schedule_file.exists():
            return

        try:
            with open(schedule_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}

            for task in config.get("schedules", []):
                self._tasks[task["name"]] = {
                    "name": task["name"],
                    "interface": task.get("interface"),
                    "cron": task.get("cron"),
                    "mode": task.get("mode", "incremental"),
                    "days_back": task.get("days_back", 1),
                    "symbols": task.get("symbols"),
                    "enabled": task.get("enabled", True),
                    "last_run": None,
                    "next_run": self._calc_next_run(task.get("cron")),
                    "status": "pending",
                }
        except Exception as e:
            logger.warning(f"Failed to load schedule: {e}")

    def _calc_next_run(self, cron_expr: Optional[str]) -> Optional[datetime]:
        if not cron_expr:
            return None
        try:
            return croniter(cron_expr, datetime.now()).get_next(datetime)
        except Exception:
            return None

    def add_task(
        self,
        name: str,
        cron: str,
        interface: str,
        mode: str = "incremental",
        days_back: int = 1,
        symbols: Optional[str] = None,
    ):
        """添加任务"""
        self._tasks[name] = {
            "name": name,
            "interface": interface,
            "cron": cron,
            "mode": mode,
            "days_back": days_back,
            "symbols": symbols,
            "enabled": True,
            "last_run": None,
            "next_run": self._calc_next_run(cron),
            "status": "pending",
        }

    def remove_task(self, name: str):
        """移除任务"""
        self._tasks.pop(name, None)

    def set_downloader(self, downloader: Any):
        """设置下载器，用于在任务触发时自动执行下载"""
        self._downloader = downloader

    def _run_download(self, task: Dict[str, Any], result: Dict[str, Any]):
        """根据任务配置触发下载"""
        interface = task.get("interface")
        mode = task.get("mode", "incremental")
        days_back = task.get("days_back", 1)
        task.get("symbols")

        if mode == "full":
            self._downloader.download_full(
                interfaces=[interface] if interface else None,
            )
        else:
            self._downloader.download_incremental(
                stock_list=None,
                days_back=days_back,
            )
        logger.info(f"Download triggered for task {task['name']} ({interface}, {mode})")

    def start(self):
        """启动调度器"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self):
        """停止调度器"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Scheduler stopped")

    def run_now(self, name: str, callback: Optional[Callable] = None):
        """立即执行任务"""
        task = self._tasks.get(name)
        if not task:
            raise ConfigError(f"Task {name} not found")

        task["status"] = "running"
        task["last_run"] = datetime.now().isoformat()
        result = {
            "name": name,
            "interface": task["interface"],
            "mode": task["mode"],
            "started_at": task["last_run"],
        }
        try:
            if callback:
                callback(task, result)
            elif self._downloader:
                self._run_download(task, result)
            else:
                logger.warning(
                    f"Task {name} executed with no callback or downloader registered; "
                    f"no download work will be performed."
                )
            task["status"] = "completed"
            logger.info(f"Task {name} completed")
        except Exception as e:
            task["status"] = f"failed: {e}"
            logger.error(f"Task {name} failed: {e}")

    def get_status(self) -> Dict[str, Any]:
        """获取任务状态"""
        return {
            "running": self._running,
            "tasks": {
                name: {
                    "interface": t["interface"],
                    "cron": t["cron"],
                    "status": t["status"],
                    "last_run": t["last_run"],
                    "next_run": t["next_run"].isoformat() if t["next_run"] else None,
                }
                for name, t in self._tasks.items()
            },
        }

    def _run_loop(self):
        """调度循环"""
        while self._running:
            now = datetime.now()
            for name, task in self._tasks.items():
                if not task["enabled"]:
                    continue
                if task["next_run"] and now >= task["next_run"]:
                    self.run_now(name)
                    task["next_run"] = self._calc_next_run(task["cron"])
            time.sleep(60)
