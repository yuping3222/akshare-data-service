"""Config cache - compatibility shell.

DEPRECATED: Use `akshare_data.common.config` instead.
This module re-exports from common/config.py for backward compatibility.
"""

import warnings

warnings.warn(
    "akshare_data.core.config_cache is deprecated. Use akshare_data.common.config instead.",
    DeprecationWarning,
    stacklevel=2,
)

from akshare_data.common.config import ConfigCache

__all__ = ["ConfigCache"]
