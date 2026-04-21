"""存储层: 内存缓存、Parquet 持久化、DuckDB 查询、增量更新引擎、统一缓存执行器"""

from .manager import CacheManager
from .fetcher import CachedFetcher, FetchConfig
from .strategies import CacheStrategy, FullCacheStrategy, IncrementalStrategy
from .aggregator import Aggregator, run_aggregation
from .validator import SchemaValidator, SchemaValidationError

__all__ = [
    "CacheManager",
    "CachedFetcher",
    "FetchConfig",
    "CacheStrategy",
    "FullCacheStrategy",
    "IncrementalStrategy",
    "Aggregator",
    "run_aggregation",
    "SchemaValidator",
    "SchemaValidationError",
]
