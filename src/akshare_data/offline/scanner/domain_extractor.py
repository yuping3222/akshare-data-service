"""域名提取器 - 从源码提取 URL"""

from __future__ import annotations

import inspect
import re
from typing import Callable, List


class DomainExtractor:
    """从函数源码中提取域名"""

    URL_PATTERN = re.compile(
        r'https?://([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    )

    def extract(self, func: Callable) -> List[str]:
        """从函数源码提取域名列表"""
        try:
            source = inspect.getsource(func)
            return self._extract_domains(source)
        except (OSError, TypeError):
            return []

    def _extract_domains(self, source: str) -> List[str]:
        """从文本中提取域名"""
        matches = self.URL_PATTERN.findall(source)
        return list(set(matches))
