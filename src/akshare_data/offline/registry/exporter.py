"""注册表导出器 - 导出 YAML/JSON/多文件"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from akshare_data.offline.core.paths import paths

logger = logging.getLogger("akshare_data")


class RegistryExporter:
    """注册表导出器"""

    def export_yaml(
        self, registry: Dict[str, Any], output_path: Optional[Path] = None
    ) -> Path:
        """导出为单个 YAML 文件（兼容旧格式）"""
        if output_path is None:
            output_path = paths.legacy_registry_file

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(registry, f, default_flow_style=False, allow_unicode=True)

        logger.info(f"Exported registry to {output_path}")
        return output_path

    def export_split(
        self,
        registry: Dict[str, Any],
        output_dir: Optional[Path] = None,
    ) -> Path:
        """按分类拆分为多个 YAML 文件"""
        if output_dir is None:
            output_dir = paths.registry_dir

        output_dir.mkdir(parents=True, exist_ok=True)

        interfaces = registry.get("interfaces", {})
        by_category = defaultdict(dict)

        for name, iface in interfaces.items():
            category = iface.get("category", "other")
            by_category[category][name] = iface

        for category, cat_interfaces in by_category.items():
            output_file = output_dir / f"{category}.yaml"
            with open(output_file, "w", encoding="utf-8") as f:
                yaml.dump(
                    {"category": category, "interfaces": cat_interfaces},
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                )
            logger.info(f"Exported {category} registry to {output_file}")

        base_file = output_dir / "_base.yaml"
        with open(base_file, "w", encoding="utf-8") as f:
            yaml.dump(
                {
                    "version": registry.get("version", "2.0"),
                    "generated_at": registry.get("generated_at", ""),
                    "description": registry.get("description", ""),
                },
                f,
                default_flow_style=False,
                allow_unicode=True,
            )

        return output_dir

    def export_json(
        self, registry: Dict[str, Any], output_path: Optional[Path] = None
    ) -> Path:
        """导出为 JSON 文件"""
        if output_path is None:
            output_path = paths.config_dir / "akshare_registry.json"

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2, ensure_ascii=False)

        logger.info(f"Exported registry to {output_path}")
        return output_path
