"""探测任务构建器"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import akshare as ak

from akshare_data.offline.prober.checkpoint import DEFAULT_STABLE_TTL


@dataclass
class ValidationResult:
    """验证结果"""

    func_name: str
    domain_group: str
    status: str
    error_msg: str
    exec_time: float
    data_size: int
    data: Optional[Any] = None
    last_check: float = 0.0
    check_count: int = 1


@dataclass
class ProbeTask:
    """探测任务"""

    func_name: str
    func: Callable
    params: Dict[str, Any]
    ttl: int = DEFAULT_STABLE_TTL
    skip: bool = False


class TaskBuilder:
    """探测任务构建器"""

    def build_tasks(self, config: Dict[str, Any]) -> List[ProbeTask]:
        """构建探测任务列表"""
        tasks = []
        for func_name, cfg in config.items():
            if cfg.get("skip", False):
                continue

            func = getattr(ak, func_name, None)
            if func is None:
                continue

            task = ProbeTask(
                func_name=func_name,
                func=func,
                params=cfg.get("params", {}),
                ttl=cfg.get("check_interval", DEFAULT_STABLE_TTL),
                skip=False,
            )
            tasks.append(task)

        return tasks
