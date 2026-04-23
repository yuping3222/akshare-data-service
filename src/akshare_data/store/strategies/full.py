from typing import Any

import pandas as pd

from .base import CacheStrategy


class FullCacheStrategy(CacheStrategy):
    """全量缓存策略 — 适用于 meta/snapshot 数据

    逻辑：缓存存在即命中，不存在则全量拉取并替换
    适用表：index_components, securities_list, industry_stocks, suspended_stocks 等
    """

    def __init__(self, filter_keys: list[str] | None = None):
        self.filter_keys = filter_keys or []

    def should_fetch(self, cached: pd.DataFrame | None, **params) -> bool:
        return cached is None or cached.empty

    def merge(
        self, cached: pd.DataFrame | None, fresh: pd.DataFrame, **params
    ) -> pd.DataFrame:
        return fresh

    def build_where(self, **params) -> dict[str, Any]:
        return {k: v for k, v in params.items() if k in self.filter_keys}
