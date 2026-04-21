"""参数推断器 - 智能推断探测参数"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List

SIZE_LIMIT_PARAMS = {"limit", "count", "top", "recent", "size", "page_size"}
SYMBOL_FALLBACKS = ["000001", "sh000001", "USD", "1.0"]


class ParamInferrer:
    """智能推断探测参数"""

    def infer(self, func: Callable, signature: List[str]) -> Dict[str, Any]:
        """推断探测参数"""
        kwargs = {}

        for param_name in signature:
            value = self._infer_param(param_name)
            if value is not None:
                kwargs[param_name] = value

        doc = self._extract_doc(func)
        doc_params = self._parse_doc_params(doc)
        kwargs.update(doc_params)

        return kwargs

    def _infer_param(self, param_name: str) -> Any:
        """推断单个参数"""
        if param_name in SIZE_LIMIT_PARAMS:
            return 1
        if param_name == "symbol":
            return "000001"
        if param_name == "period":
            return "daily"
        if param_name == "year":
            return datetime.now().year
        if param_name == "start_date":
            return (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
        if param_name == "end_date":
            return datetime.now().strftime("%Y%m%d")
        return None

    def _extract_doc(self, func: Callable) -> str:
        """提取文档字符串"""
        return func.__doc__ or ""

    def _parse_doc_params(self, doc: str) -> Dict[str, Any]:
        """从文档中解析参数示例"""
        params = {}
        pattern = r'(\w+)\s*=\s*["\']?([^"\',\s]+)["\']?'
        for match in re.finditer(pattern, doc):
            name, value = match.groups()
            if name not in ("type", "rtype", "param"):
                params[name] = value
        return params
