# ruff: noqa: E402
"""Token management - compatibility shell.

DEPRECATED: Use `akshare_data.common.config` instead.
This module re-exports from common/config.py for backward compatibility.
"""
# ruff: noqa: E402

import warnings

warnings.warn(
    "akshare_data.core.tokens is deprecated. Use akshare_data.common.config instead.",
    DeprecationWarning,
    stacklevel=2,
)

from akshare_data.common.config import (
    TokenManager,
    get_token,
    set_token,
    _SOURCE_ENV_MAP,
    _SOURCE_CFG_KEYS,
)

__all__ = ["TokenManager", "get_token", "set_token", "_SOURCE_ENV_MAP", "_SOURCE_CFG_KEYS"]
