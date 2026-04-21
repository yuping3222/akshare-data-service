"""数据源层: 适配器、多源备份、路由"""

from akshare_data.sources.akshare_source import AkShareAdapter
from akshare_data.sources.router import (
    MultiSourceRouter,
    DomainRateLimiter,
    SourceHealthMonitor,
    ExecutionResult,
    EmptyDataPolicy,
    create_simple_router,
)
from akshare_data.sources.lixinger_source import LixingerAdapter
from akshare_data.sources.tushare_source import TushareAdapter, set_tushare_token
from akshare_data.sources.lixinger_client import get_lixinger_client, set_lixinger_token

__all__ = [
    "AkShareAdapter",
    "MultiSourceRouter",
    "DomainRateLimiter",
    "SourceHealthMonitor",
    "ExecutionResult",
    "EmptyDataPolicy",
    "create_simple_router",
    "LixingerAdapter",
    "TushareAdapter",
    "set_tushare_token",
    "set_lixinger_token",
    "get_lixinger_client",
]
