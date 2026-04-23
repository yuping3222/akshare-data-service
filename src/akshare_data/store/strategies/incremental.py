from typing import Any

import pandas as pd

from ..missing_ranges import find_missing_ranges
from .base import CacheStrategy


class IncrementalStrategy(CacheStrategy):
    """增量缓存策略 — 适用于时序数据

    逻辑：读已有区间 → 计算缺失 → 只拉缺失 → 合并
    适用表：stock_daily, stock_minute, index_daily, etf_daily, north_money_flow
    """

    def __init__(
        self,
        date_col: str = "date",
        filter_keys: list[str] | None = None,
    ):
        self.date_col = date_col
        self.filter_keys = filter_keys or []

    def should_fetch(self, cached: pd.DataFrame | None, **params) -> bool:
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        if start_date is None or end_date is None:
            return cached is None or cached.empty
        return not self._is_complete(cached, start_date, end_date)

    def merge(
        self, cached: pd.DataFrame | None, fresh: pd.DataFrame, **params
    ) -> pd.DataFrame:
        if cached is None or cached.empty:
            result = fresh
        else:
            result = pd.concat([cached, fresh], ignore_index=True)

        if self.date_col in result.columns:
            result = (
                result.sort_values(self.date_col)
                .drop_duplicates(subset=[self.date_col], keep="last")
                .reset_index(drop=True)
            )

        return result

    def build_where(self, **params) -> dict[str, Any]:
        where = {k: v for k, v in params.items() if k in self.filter_keys}
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        if start_date and end_date:
            where[self.date_col] = (start_date, end_date)
        return where

    def find_missing_ranges(
        self,
        cached: pd.DataFrame | None,
        start_date: str,
        end_date: str,
    ) -> list[tuple[str, str]]:
        if cached is None or cached.empty or self.date_col not in cached.columns:
            return [(start_date, end_date)]

        existing_ranges = self._extract_ranges(cached)
        return find_missing_ranges(start_date, end_date, existing_ranges)

    def _is_complete(
        self, cached: pd.DataFrame | None, start_date: str, end_date: str
    ) -> bool:
        if cached is None or cached.empty or self.date_col not in cached.columns:
            return False

        min_date = pd.to_datetime(cached[self.date_col].min()).tz_localize(None)
        max_date = pd.to_datetime(cached[self.date_col].max()).tz_localize(None)
        target_start = pd.to_datetime(start_date).tz_localize(None)
        target_end = pd.to_datetime(end_date).tz_localize(None)

        return min_date <= target_start and max_date >= target_end

    def _extract_ranges(self, df: pd.DataFrame) -> list[tuple[str, str]]:
        if df is None or df.empty or self.date_col not in df.columns:
            return []

        min_date = str(pd.to_datetime(df[self.date_col].min()).date())
        max_date = str(pd.to_datetime(df[self.date_col].max()).date())
        return [(min_date, max_date)]
