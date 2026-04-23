"""注册表验证器 - 验证注册表完整性/一致性"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger("akshare_data")


class RegistryValidator:
    """注册表验证器"""

    def validate(self, registry: Dict[str, Any]) -> List[str]:
        """验证注册表，返回错误列表"""
        errors = []

        if "interfaces" not in registry:
            errors.append("Missing 'interfaces' section")
            return errors

        for name, iface in registry["interfaces"].items():
            errors.extend(self._validate_interface(name, iface))

        if errors:
            logger.warning(f"Registry validation found {len(errors)} issues")
        else:
            logger.info("Registry validation passed")

        return errors

    def _validate_interface(self, name: str, iface: Dict[str, Any]) -> List[str]:
        """验证单个接口"""
        errors = []

        if "name" not in iface:
            errors.append(f"Interface {name}: missing 'name'")

        if "category" not in iface:
            errors.append(f"Interface {name}: missing 'category'")

        if "probe" in iface:
            probe = iface["probe"]
            if not isinstance(probe.get("params"), dict):
                errors.append(f"Interface {name}: probe.params must be a dict")
            if not isinstance(probe.get("skip"), bool):
                errors.append(f"Interface {name}: probe.skip must be a bool")

        if "rate_limit_key" in iface:
            if not isinstance(iface["rate_limit_key"], str):
                errors.append(f"Interface {name}: rate_limit_key must be a string")

        return errors
