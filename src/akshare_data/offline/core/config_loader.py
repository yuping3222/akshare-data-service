"""统一配置加载器 - 缓存解析结果"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from akshare_data.core.config_cache import ConfigCache
from akshare_data.offline.core.paths import paths
from akshare_data.offline.core.errors import ConfigError

logger = logging.getLogger("akshare_data")


class ConfigLoader:
    """配置加载器（带缓存）

    Shared config files (registry, interfaces, rate_limits, sources)
    delegate to the single ConfigCache so fetcher.py and ConfigLoader
    always see the same data.
    """

    def __init__(self):
        self._cache: Dict[str, Any] = {}

    def load_yaml(self, file_path: Path, use_cache: bool = True) -> Dict[str, Any]:
        """加载 YAML 配置"""
        key = str(file_path)
        if use_cache and key in self._cache:
            return self._cache[key]

        if not file_path.exists():
            raise ConfigError(f"Config file not found: {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if use_cache:
                self._cache[key] = data
            return data
        except yaml.YAMLError as e:
            raise ConfigError(f"Failed to parse YAML {file_path}: {e}")

    # ── Delegated to shared ConfigCache ──────────────────────────────

    def load_registry(self, category: Optional[str] = None) -> Dict[str, Any]:
        """加载注册表配置（共享缓存）"""
        return ConfigCache.load_registry()

    def load_all_registries(self) -> Dict[str, Any]:
        """加载所有分类的注册表"""
        registries = {}
        registry_dir = paths.registry_dir

        if not registry_dir.exists():
            raise ConfigError(f"Registry directory not found: {registry_dir}")

        for yaml_file in registry_dir.glob("*.yaml"):
            if yaml_file.name.startswith("_"):
                continue
            category = yaml_file.stem
            registries[category] = self.load_yaml(yaml_file)

        return registries

    def load_sources(self) -> Dict[str, Any]:
        """加载数据源注册表配置（共享缓存）"""
        return ConfigCache.load_sources()

    def load_domains(self) -> Dict[str, Any]:
        """加载域名限速配置"""
        return self.load_yaml(paths.domains_file)

    def load_rate_limits(self) -> Dict[str, Any]:
        """加载全局限速配置（共享缓存）"""
        return ConfigCache.load_rate_limits()

    def load_failover(self) -> Dict[str, Any]:
        """加载切源配置"""
        return self.load_yaml(paths.failover_file)

    def load_interfaces(self) -> Dict[str, Any]:
        """加载所有接口配置（共享缓存）"""
        return ConfigCache.load_interfaces()

    # ── Unique to ConfigLoader (not shared with fetcher) ─────────────
    def load_priority(self) -> Dict[str, Any]:
        """加载下载优先级配置"""
        return self.load_yaml(paths.priority_file)

    def load_schedule(self) -> Dict[str, Any]:
        """加载调度配置"""
        return self.load_yaml(paths.schedule_file)

    def load_prober_config(self) -> Dict[str, Any]:
        """加载探测配置"""
        return self.load_yaml(paths.prober_config_file)

    def load_prober_state(self) -> Dict[str, Any]:
        """加载探测状态"""
        return self.load_yaml(paths.prober_state_file)

    def invalidate_cache(self, file_path: Optional[Path] = None):
        """清除缓存（包括共享的 ConfigCache）"""
        if file_path:
            key = str(file_path)
            self._cache.pop(key, None)
            ConfigCache.invalidate(file_path)
        else:
            self._cache.clear()
            ConfigCache.invalidate()


# 全局单例
config_loader = ConfigLoader()
