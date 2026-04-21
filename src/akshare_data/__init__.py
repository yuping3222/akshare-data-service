"""统一金融数据服务入口 (Cache-First 策略, Lixinger 优先)"""

from akshare_data.api import DataService, get_service
from akshare_data.store.manager import CacheManager
from akshare_data.offline import (
    BatchDownloader,
    APIProber,
)
from akshare_data.offline.analyzer.cache_analysis.completeness import CompletenessChecker as DataQualityChecker
from akshare_data.offline.report.renderer import ReportRenderer as Reporter

__version__ = "0.2.0"

__all__ = [
    "DataService",
    "get_service",
    "CacheManager",
    "BatchDownloader",
    "APIProber",
    "DataQualityChecker",
    "Reporter",
]

# 内部属性/命名空间，不转发
_EXCLUDE = frozenset(
    {
        "cache",
        "router",
        "lixinger",
        "akshare",
        "adapters",
        "access_logger",
        "cn",
        "hk",
        "us",
        "macro",
    }
)


# 预计算已知方法集合，避免 __getattr__ 每次触发 get_service()
_KNOWN = frozenset(set(__all__) | {
    name for name in dir(DataService)
    if not name.startswith("_") and name not in _EXCLUDE
})


def __getattr__(name: str):
    """动态转发模块级属性访问到 DataService 实例方法。

    例如: from akshare_data import get_daily
          get_daily("000001", "2024-01-01", "2024-12-31")
    等价于:
          get_service().get_daily("000001", "2024-01-01", "2024-12-31")
    """
    if name in _EXCLUDE:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    # Short-circuit for unknown names — avoids triggering full service init
    # for typos, hasattr checks, etc.
    if name not in _KNOWN:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    service = get_service()
    attr = getattr(service, name, None)
    if attr is not None:
        return attr

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """支持 IDE 自动补全和 dir(akshare_data)。"""
    service = get_service()
    methods = {
        name
        for name in dir(service)
        if not name.startswith("_") and name not in _EXCLUDE
    }
    return sorted(set(__all__) | methods)
