"""注册表合并器 - 合并手工配置与扫描结果"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from akshare_data.offline.core.paths import paths

logger = logging.getLogger("akshare_data")


class RegistryMerger:
    """合并手工配置与自动扫描结果"""

    def merge_interfaces(
        self,
        auto_generated: Dict[str, Any],
        manual_config_path: Optional[Path] = None,
    ) -> int:
        """合并手工接口配置"""
        if manual_config_path is None:
            manual_config_path = paths.legacy_interfaces_dir

        if not manual_config_path.exists():
            return 0

        merged_count = 0
        for yaml_file in manual_config_path.glob("*.yaml"):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    manual_data = yaml.safe_load(f) or {}

                for iface_name, manual_iface in manual_data.items():
                    if iface_name in auto_generated.get("interfaces", {}):
                        auto_iface = auto_generated["interfaces"][iface_name]
                        self._merge_single_interface(auto_iface, manual_iface)
                        merged_count += 1
            except Exception as e:
                logger.warning(f"Failed to merge {yaml_file}: {e}")

        logger.info(f"Merged {merged_count} manual interface configurations")
        return merged_count

    def merge_rate_limits(
        self,
        auto_generated: Dict[str, Any],
        rate_limits_path: Optional[Path] = None,
    ) -> int:
        """合并手工限速配置"""
        if rate_limits_path is None:
            rate_limits_path = paths.legacy_rate_limits_file

        if not rate_limits_path.exists():
            return 0

        try:
            with open(rate_limits_path, "r", encoding="utf-8") as f:
                manual_limits = yaml.safe_load(f) or {}

            merged_count = 0
            for key, manual_config in manual_limits.items():
                if key in auto_generated.get("rate_limits", {}):
                    auto_config = auto_generated["rate_limits"][key]
                    if isinstance(manual_config, dict):
                        auto_config.update(manual_config)
                    merged_count += 1

            logger.info(f"Merged {merged_count} manual rate limit configurations")
            return merged_count
        except Exception as e:
            logger.warning(f"Failed to merge rate limits: {e}")
            return 0

    def _merge_single_interface(
        self, auto_iface: Dict[str, Any], manual_iface: Dict[str, Any]
    ):
        """合并单个接口配置"""
        for key in (
            "sources",
            "input",
            "output",
            "interface_name",
            "description",
            "category",
        ):
            if key in manual_iface:
                auto_iface[key] = manual_iface[key]

        if "rate_limit_key" in manual_iface:
            auto_iface["rate_limit_key"] = manual_iface["rate_limit_key"]
