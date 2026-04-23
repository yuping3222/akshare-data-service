"""自动切源决策"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Set

import yaml

from akshare_data.offline.core.paths import paths
from akshare_data.offline.source_manager.health_tracker import HealthTracker

logger = logging.getLogger("akshare_data")


class FailoverManager:
    """自动切源管理器

    读取 sources.yaml 作为源名称的权威来源，校验 failover.yaml 中
    的 source_priority 是否都合法。
    """

    def __init__(self, health_tracker: Optional[HealthTracker] = None):
        self._health = health_tracker or HealthTracker()
        self._failover_config = self._load_config()
        self._source_registry = self._load_source_registry()
        self._validate_sources()
        self._cooldowns: Dict[str, float] = {}

    def _load_config(self) -> Dict[str, Any]:
        """加载切源配置"""
        failover_file = paths.failover_file
        if not failover_file.exists():
            return {
                "failure_threshold": 3,
                "cooldown_seconds": 300,
                "auto_recovery": {"enabled": True, "interval": 3600},
                "source_priority": {},
            }

        try:
            with open(failover_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Failed to load failover config: {e}")
            return {}

    def _load_source_registry(self) -> Dict[str, Any]:
        """加载数据源注册表"""
        sources_file = paths.sources_file
        if not sources_file.exists():
            return {}
        try:
            with open(sources_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data.get("sources", {})
        except Exception:
            return {}

    def _validate_sources(self):
        """校验 source_priority 中的源名是否都在 sources.yaml 中定义"""
        if not self._source_registry:
            logger.warning("sources.yaml not found, skipping source validation")
            return
        valid_sources: Set[str] = set(self._source_registry.keys())
        priority = self._failover_config.get("source_priority", {})
        for source_name in priority:
            if source_name not in valid_sources:
                logger.warning(
                    "source_priority references unknown source '%s' "
                    "(not in sources.yaml) — skipping",
                    source_name,
                )

    def _get_valid_sources(self) -> Set[str]:
        """返回所有合法源名称集合"""
        if self._source_registry:
            return set(self._source_registry.keys())
        return set(self._failover_config.get("source_priority", {}).keys())

    def should_failover(self, source: str) -> bool:
        """检查是否应切换源"""
        self._failover_config.get("failure_threshold", 3)
        score = self._health.get_health_score(source)

        if score < 50:
            return True

        cooldown = self._cooldowns.get(source, 0)
        if time.time() - cooldown < self._failover_config.get("cooldown_seconds", 300):
            return False

        return False

    def failover(
        self, interface: str, failed_source: str, candidates: List[str]
    ) -> Optional[str]:
        """执行切源"""
        self._cooldowns[failed_source] = time.time()
        logger.warning(
            "Failover triggered for %s: %s -> searching alternatives",
            interface,
            failed_source,
        )

        best = self._health.get_best_source(interface, candidates)
        if best and best != failed_source:
            logger.info("Failover to %s for %s", best, interface)
            return best

        logger.error("No alternative source available for %s", interface)
        return None

    def recover(self, source: str):
        """恢复源"""
        self._cooldowns.pop(source, None)
        logger.info("Source %s recovered", source)

    def get_priority_sources(
        self, interface: str = "", interface_sources: Optional[List[str]] = None
    ) -> List[str]:
        """获取按优先级排序的源列表

        如果提供了 interface_sources（从接口配置中取到的实际源列表），
        则只返回其中在 source_priority 中定义的源，按优先级排序。
        否则返回所有优先级源。
        """
        priority = self._failover_config.get("source_priority", {})
        valid = self._get_valid_sources()

        # 过滤掉不在合法源集合中的 key
        filtered = {k: v for k, v in priority.items() if k in valid}

        # 如果传入了接口实际配置的源列表，做交集
        if interface_sources:
            filtered = {k: v for k, v in filtered.items() if k in interface_sources}

        return sorted(filtered.keys(), key=lambda s: filtered.get(s, 999))

    def record_result(
        self, interface: str, source: str, success: bool, latency_ms: float = 0.0
    ):
        """记录源调用结果，供 fetcher.py 集成使用"""
        if success:
            self._health.record_success(source, latency_ms)
        else:
            self._health.record_failure(source)

    @property
    def cooldowns(self) -> Dict[str, float]:
        """当前冷却中的源及其时间戳"""
        return dict(self._cooldowns)
