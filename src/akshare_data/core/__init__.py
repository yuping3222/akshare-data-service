"""Core module: compatibility shell for common/ capabilities.

DEPRECATED: Use `akshare_data.common` instead.
This module re-exports from common/ for backward compatibility.
"""
# ruff: noqa: E402

import warnings

warnings.warn(
    "akshare_data.core is deprecated. Use akshare_data.common instead.",
    DeprecationWarning,
    stacklevel=2,
)

from akshare_data.core.tokens import TokenManager, get_token, set_token
from akshare_data.core.config_dir import get_config_dir, get_project_root

__all__ = ["TokenManager", "get_token", "set_token", "get_config_dir", "get_project_root"]
