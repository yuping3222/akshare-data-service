"""数据源健康追踪"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional


logger = logging.getLogger("akshare_data")


class HealthTracker:
    """数据源健康追踪器"""

    def __init__(self):
        self._sources: Dict[str, Dict[str, Any]] = {}

    def record_success(self, source: str, latency: float = 0.0):
        """记录成功调用"""
        if source not in self._sources:
            self._sources[source] = {
                "name": source,
                "success_count": 0,
                "failure_count": 0,
                "total_latency": 0.0,
                "last_success": None,
                "last_failure": None,
                "consecutive_failures": 0,
                "health_score": 100.0,
            }

        s = self._sources[source]
        s["success_count"] += 1
        s["total_latency"] += latency
        s["last_success"] = time.time()
        s["consecutive_failures"] = 0
        self._update_health(source)

    def record_failure(self, source: str, error: str = ""):
        """记录失败调用"""
        if source not in self._sources:
            self._sources[source] = {
                "name": source,
                "success_count": 0,
                "failure_count": 0,
                "total_latency": 0.0,
                "last_success": None,
                "last_failure": None,
                "consecutive_failures": 0,
                "health_score": 100.0,
            }

        s = self._sources[source]
        s["failure_count"] += 1
        s["last_failure"] = time.time()
        s["consecutive_failures"] += 1
        self._update_health(source)

    def _update_health(self, source: str):
        """更新健康分数"""
        s = self._sources[source]
        total = s["success_count"] + s["failure_count"]
        if total == 0:
            return

        success_rate = s["success_count"] / total
        avg_latency = (
            s["total_latency"] / s["success_count"] if s["success_count"] > 0 else 0
        )

        latency_penalty = min(avg_latency / 1000, 0.3)
        failure_penalty = s["consecutive_failures"] * 0.1

        s["health_score"] = max(
            0, (success_rate * 100) - (latency_penalty * 100) - (failure_penalty * 100)
        )

    def get_health_score(self, source: str) -> float:
        """获取健康分数"""
        s = self._sources.get(source)
        return s["health_score"] if s else 100.0

    def get_best_source(self, interface: str, candidates: List[str]) -> Optional[str]:
        """获取当前最优源"""
        if not candidates:
            return None

        best = None
        best_score = -1
        for source in candidates:
            score = self.get_health_score(source)
            if score > best_score:
                best_score = score
                best = source

        return best

    def get_all_status(self) -> Dict[str, Any]:
        """获取所有源状态"""
        return {
            name: {
                "health_score": round(s["health_score"], 2),
                "success_count": s["success_count"],
                "failure_count": s["failure_count"],
                "consecutive_failures": s["consecutive_failures"],
            }
            for name, s in self._sources.items()
        }
