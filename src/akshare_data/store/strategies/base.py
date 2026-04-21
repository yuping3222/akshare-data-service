from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class CacheStrategy(ABC):
    """缓存策略基类 — 定义缓存读取、判断、合并的接口"""

    @abstractmethod
    def should_fetch(self, cached: pd.DataFrame | None, **params) -> bool:
        """判断是否需要拉取新数据"""

    @abstractmethod
    def merge(self, cached: pd.DataFrame | None, fresh: pd.DataFrame, **params) -> pd.DataFrame:
        """合并缓存数据和新数据"""

    @abstractmethod
    def build_where(self, **params) -> dict[str, Any]:
        """构建缓存查询条件"""
