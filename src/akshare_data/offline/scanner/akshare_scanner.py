"""AkShare 模块扫描器 - 提取函数签名/文档"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, List

import akshare as ak

logger = logging.getLogger("akshare_data")

SKIP_FUNCTIONS = {
    "__dir__",
    "__getattr__",
    "update_all_data",
    "version",
    "__all__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
}


class AkShareScanner:
    """扫描 akshare 模块，提取所有公开函数元信息"""

    def scan_all(self) -> Dict[str, Dict[str, Any]]:
        """扫描所有公开函数"""
        results = {}
        for name, func in inspect.getmembers(ak, inspect.isfunction):
            if name.startswith("_") or name in SKIP_FUNCTIONS:
                continue
            results[name] = self._analyze_function(name, func)
        logger.info(f"Scanned {len(results)} functions from akshare")
        return results

    def _analyze_function(self, name: str, func: Callable) -> Dict[str, Any]:
        """分析单个函数"""
        return {
            "name": name,
            "signature": self._extract_signature(func),
            "doc": self._extract_doc(func),
            "module": func.__module__,
        }

    def _extract_signature(self, func: Callable) -> List[str]:
        """提取函数签名"""
        try:
            sig = inspect.signature(func)
            return [
                param.name for param in sig.parameters.values() if param.name != "self"
            ]
        except (ValueError, TypeError):
            return []

    def _extract_doc(self, func: Callable) -> str:
        """提取文档字符串"""
        return inspect.getdoc(func) or ""
