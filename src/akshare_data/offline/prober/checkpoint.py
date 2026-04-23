"""探测检查点管理 - 加载/保存探测状态"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from akshare_data.offline.core.paths import paths

logger = logging.getLogger("akshare_data")

DEFAULT_STABLE_TTL = 30 * 24 * 3600


@dataclass
class ProbeResult:
    """单个探测结果"""

    func_name: str
    domain_group: str
    status: str
    error_msg: str
    exec_time: float
    data_size: int
    last_check: float = 0.0
    check_count: int = 1


class CheckpointManager:
    """探测检查点管理器"""

    def __init__(self, state_file: Optional[Path] = None):
        self._state_file = state_file or paths.prober_state_file
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._results: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self):
        """加载检查点"""
        if self._state_file.exists():
            try:
                with open(self._state_file, "r", encoding="utf-8") as f:
                    self._results = json.load(f)
                logger.info(f"Loaded checkpoint: {len(self._results)} entries")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
                self._results = {}

    def save(self):
        """保存检查点"""
        try:
            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump(self._results, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def get_result(self, func_name: str) -> Optional[Dict[str, Any]]:
        """获取探测结果"""
        return self._results.get(func_name)

    def set_result(self, result: ProbeResult):
        """设置探测结果"""
        self._results[result.func_name] = {
            "func_name": result.func_name,
            "domain_group": result.domain_group,
            "status": result.status,
            "error_msg": result.error_msg,
            "exec_time": result.exec_time,
            "data_size": result.data_size,
            "last_check": result.last_check,
            "check_count": result.check_count,
        }

    def get_all_results(self) -> Dict[str, Dict[str, Any]]:
        """获取所有探测结果"""
        return self._results.copy()

    def should_skip(self, func_name: str, ttl: int) -> bool:
        """检查是否应跳过（TTL 内）"""
        import time

        result = self._results.get(func_name)
        if not result:
            return False
        if result.get("status", "").startswith("Failed"):
            return False
        last_check = result.get("last_check", 0)
        return (time.time() - last_check) < ttl
