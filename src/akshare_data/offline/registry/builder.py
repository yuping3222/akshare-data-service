"""注册表构建器 - 从扫描结果构建注册表数据结构"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from akshare_data.core.config_dir import get_config_dir
from akshare_data.offline.scanner import (
    AkShareScanner,
    DomainExtractor,
    CategoryInferrer,
    ParamInferrer,
)

logger = logging.getLogger("akshare_data")

_CONFIG_DIR = get_config_dir()
RATE_LIMITS_FILE = _CONFIG_DIR / "rate_limits.yaml"


def _load_domain_mapping() -> Dict[str, str]:
    """Load domain -> rate_limit_key mapping from config/sources/domains.yaml.

    Returns a dict of {url_pattern: rate_limit_key} for each domain entry.
    If url_pattern is missing, the domain key itself is used as the pattern.
    """
    domains_file = _CONFIG_DIR / "sources" / "domains.yaml"
    if not domains_file.exists():
        logger.warning("domains.yaml not found at %s, returning empty mapping", domains_file)
        return {}

    with open(domains_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    mapping: Dict[str, str] = {}
    for domain_key, info in data.get("domains", {}).items():
        if not isinstance(info, dict):
            continue
        rate_limit_key = info.get("rate_limit_key", "default")
        url_pattern = info.get("url_pattern", domain_key)
        mapping[url_pattern] = rate_limit_key

    return mapping


def _load_rate_limits() -> Dict[str, Any]:
    if RATE_LIMITS_FILE.exists():
        with open(RATE_LIMITS_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {"default": {"interval": 0.5, "description": "默认"}}


class RegistryBuilder:
    """从扫描结果构建注册表"""

    def __init__(self):
        self.scanner = AkShareScanner()
        self.domain_extractor = DomainExtractor()
        self.category_inferrer = CategoryInferrer()
        self.param_inferrer = ParamInferrer()

    def build(self, scan_results: Optional[Dict[str, Dict]] = None) -> Dict[str, Any]:
        """构建完整注册表"""
        if scan_results is None:
            scan_results = self.scanner.scan_all()

        interfaces = {}
        domains = {}
        rate_limits = _load_rate_limits()

        for func_name, func_info in scan_results.items():
            interface = self._build_interface(func_name, func_info)
            interfaces[func_name] = interface
            for domain in interface.get("domains", []):
                if domain not in domains:
                    domains[domain] = {"rate_limit_key": interface.get("rate_limit_key", "default")}

        return {
            "version": "2.0",
            "generated_at": datetime.now().isoformat(),
            "description": "AkShare 接口注册表 - 按分类拆分",
            "interfaces": interfaces,
            "domains": domains,
            "rate_limits": rate_limits,
        }

    def _build_interface(self, func_name: str, func_info: Dict) -> Dict[str, Any]:
        """构建单个接口定义"""
        func_obj = self._get_func_obj(func_name)
        domains = self.domain_extractor.extract(func_obj) if func_obj else []
        category = self.category_inferrer.infer(func_name)
        params = self.param_inferrer.infer(func_obj, func_info.get("signature", [])) if func_obj else {}
        rate_limit_key = self._infer_rate_limit(domains)

        return {
            "name": func_name,
            "category": category,
            "description": func_info.get("doc", ""),
            "signature": func_info.get("signature", []),
            "domains": domains,
            "rate_limit_key": rate_limit_key,
            "sources": [],
            "probe": {
                "params": params,
                "skip": False,
                "check_interval": 2592000,
            },
        }

    def _get_func_obj(self, func_name: str):
        """获取函数对象"""
        import akshare as ak
        return getattr(ak, func_name, None)

    def _infer_rate_limit(self, domains: List[str]) -> str:
        """推断限速键"""
        domain_map = _load_domain_mapping()
        for domain in domains:
            if domain in domain_map:
                return domain_map[domain]
            for known_domain, rate_key in domain_map.items():
                if domain.endswith("." + known_domain):
                    return rate_key
        return "default"
