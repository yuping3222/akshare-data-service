"""统一路径管理 - 所有路径常量集中定义"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from akshare_data.core.config_dir import get_config_dir, get_project_root


class Paths:
    """离线工具路径管理器（单例）"""

    _instance: Optional["Paths"] = None
    _project_root: Optional[Path] = None
    _config_dir: Optional[Path] = None

    def __new__(cls, project_root: Optional[Path] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, project_root: Optional[Path] = None):
        if self._initialized:
            return

        if project_root:
            self._project_root = project_root
            self._config_dir = project_root / "config"
        else:
            self._project_root = get_project_root()
            self._config_dir = get_config_dir()

        self._initialized = True

    @property
    def project_root(self) -> Optional[Path]:
        """Project root when running from a git checkout; None otherwise."""
        return self._project_root

    # ── 配置目录 ──────────────────────────────────────────────────────

    @property
    def config_dir(self) -> Path:
        return self._config_dir

    @property
    def registry_dir(self) -> Path:
        return self.config_dir / "registry"

    @property
    def sources_dir(self) -> Path:
        return self.config_dir / "sources"

    @property
    def download_dir(self) -> Path:
        return self.config_dir / "download"

    @property
    def prober_dir(self) -> Path:
        return self.config_dir / "prober"

    @property
    def fields_dir(self) -> Path:
        return self.config_dir / "fields"

    @property
    def cache_config_dir(self) -> Path:
        return self.config_dir / "cache"

    @property
    def logging_config_dir(self) -> Path:
        return self.config_dir / "logging"

    # ── 配置文件 ──────────────────────────────────────────────────────

    def registry_file(self, category: Optional[str] = None) -> Path:
        """获取注册表文件路径"""
        if category:
            return self.registry_dir / f"{category}.yaml"
        return self.registry_dir / "_base.yaml"

    @property
    def domains_file(self) -> Path:
        return self.sources_dir / "domains.yaml"

    @property
    def sources_file(self) -> Path:
        return self.sources_dir / "sources.yaml"

    @property
    def failover_file(self) -> Path:
        return self.sources_dir / "failover.yaml"

    @property
    def priority_file(self) -> Path:
        return self.download_dir / "priority.yaml"

    @property
    def schedule_file(self) -> Path:
        return self.download_dir / "schedule.yaml"

    @property
    def prober_config_file(self) -> Path:
        return self.prober_dir / "config.yaml"

    @property
    def prober_state_file(self) -> Path:
        return self.prober_dir / "state.json"

    @property
    def prober_samples_dir(self) -> Path:
        return self.prober_dir / "samples"

    @property
    def cn_to_en_file(self) -> Path:
        return self.fields_dir / "cn_to_en.yaml"

    @property
    def type_hints_file(self) -> Path:
        return self.fields_dir / "type_hints.yaml"

    @property
    def field_mappings_dir(self) -> Path:
        return self.fields_dir / "mappings"

    @property
    def cache_strategies_file(self) -> Path:
        return self.cache_config_dir / "strategies.yaml"

    @property
    def access_log_config_file(self) -> Path:
        return self.logging_config_dir / "access.yaml"

    # ── 数据目录 ──────────────────────────────────────────────────────

    @property
    def logs_dir(self) -> Path:
        if self._project_root:
            return self._project_root / "logs"
        return Path(os.environ.get("AKSHARE_DATA_LOGS_DIR",
                                   Path.home() / ".cache" / "akshare-data" / "logs"))

    @property
    def reports_dir(self) -> Path:
        if self._project_root:
            return self._project_root / "reports"
        return Path(os.environ.get("AKSHARE_DATA_REPORTS_DIR",
                                   Path.home() / ".local" / "share" / "akshare-data" / "reports"))

    @property
    def health_reports_dir(self) -> Path:
        return self.reports_dir / "health"

    @property
    def quality_reports_dir(self) -> Path:
        return self.reports_dir / "quality"

    @property
    def dashboard_dir(self) -> Path:
        return self.reports_dir / "dashboard"

    # ── 旧路径兼容（迁移期使用）────────────────────────────────────────

    @property
    def legacy_registry_file(self) -> Path:
        return self.config_dir / "akshare_registry.yaml"

    @property
    def legacy_health_state_file(self) -> Path:
        return self.config_dir / "health_state.json"

    @property
    def legacy_rate_limits_file(self) -> Path:
        return self.config_dir / "rate_limits.yaml"

    @property
    def legacy_interfaces_dir(self) -> Path:
        return self.config_dir / "interfaces"

    @property
    def legacy_health_samples_dir(self) -> Path:
        return self.config_dir / "health_samples"

    @property
    def legacy_field_mappings_dir(self) -> Path:
        return self.config_dir / "field_mappings"

    def ensure_dirs(self):
        """确保所有目录存在"""
        dirs = [
            self.config_dir,
            self.registry_dir,
            self.sources_dir,
            self.download_dir,
            self.prober_dir,
            self.fields_dir,
            self.field_mappings_dir,
            self.cache_config_dir,
            self.logging_config_dir,
            self.logs_dir,
            self.reports_dir,
            self.health_reports_dir,
            self.quality_reports_dir,
            self.dashboard_dir,
            self.prober_samples_dir,
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)


# 全局单例
paths = Paths()
