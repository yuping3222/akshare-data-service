"""统一缓存执行器 — 替代 _fetch_with_cache 和 _fetch_time_series_with_cache"""

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from ..store.manager import CacheManager
from ..store.strategies import CacheStrategy, FullCacheStrategy, IncrementalStrategy

logger = logging.getLogger(__name__)


@dataclass
class FetchConfig:
    table: str
    storage_layer: str | None = None
    strategy: CacheStrategy | None = None
    partition_by: str | None = None
    partition_value: str | None = None
    date_col: str = "date"
    interface_name: str | None = None
    filter_keys: list[str] = field(default_factory=list)


class CachedFetcher:
    """统一缓存执行器

    执行流程：
    1. 自动推断或使用指定策略
    2. 构建查询条件并读缓存
    3. 判断是否需要拉取
    4. 执行拉取（全量或增量）
    5. 合并 + 写回
    6. 返回结果
    """

    def __init__(self, cache: CacheManager):
        self.cache = cache

    def execute(
        self,
        config: FetchConfig,
        fetch_fn: Callable[[], pd.DataFrame | None],
        **params,
    ) -> pd.DataFrame:
        strategy = config.strategy or self._infer_strategy(config, params)

        where = strategy.build_where(**params)

        cached = self.cache.read(
            config.table,
            storage_layer=config.storage_layer,
            partition_by=config.partition_by,
            partition_value=config.partition_value,
            where=where,
        )

        if not strategy.should_fetch(cached, **params):
            logger.debug(
                "Cache hit for table=%s params=%s", config.table, params
            )
            return cached

        if isinstance(strategy, IncrementalStrategy):
            return self._execute_incremental(
                config, strategy, cached, fetch_fn, params
            )

        return self._execute_full(
            config, strategy, cached, fetch_fn, params
        )

    def _execute_full(
        self,
        config: FetchConfig,
        strategy: FullCacheStrategy,
        cached: pd.DataFrame | None,
        fetch_fn: Callable[[], pd.DataFrame | None],
        params: dict[str, Any],
    ) -> pd.DataFrame:
        fresh = fetch_fn()
        if fresh is None or fresh.empty:
            return cached if cached is not None and not cached.empty else pd.DataFrame()

        for k, v in params.items():
            if isinstance(v, (str, int, float)) and k not in fresh.columns:
                fresh[k] = v

        self.cache.write(
            config.table,
            fresh,
            storage_layer=config.storage_layer,
            partition_by=config.partition_by,
            partition_value=config.partition_value,
        )

        logger.info(
            "Cache miss for table=%s, fetched %d rows", config.table, len(fresh)
        )
        return fresh

    def _execute_incremental(
        self,
        config: FetchConfig,
        strategy: IncrementalStrategy,
        cached: pd.DataFrame | None,
        fetch_fn: Callable[[], pd.DataFrame | None],
        params: dict[str, Any],
    ) -> pd.DataFrame:
        start_date = params.get("start_date", "1970-01-01")
        end_date = params.get("end_date")

        if end_date is None:
            end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

        missing_ranges = strategy.find_missing_ranges(cached, start_date, end_date)

        if not missing_ranges:
            result = self.cache.read(
                config.table,
                storage_layer=config.storage_layer,
                partition_by=config.partition_by,
                partition_value=config.partition_value,
                where={config.date_col: (start_date, end_date)},
            )
            return result if result is not None else pd.DataFrame()

        fetched_parts = []
        for m_start, m_end in missing_ranges:
            logger.info(
                "Fetching missing range [%s ~ %s] for table=%s",
                m_start,
                m_end,
                config.table,
            )

            original_start = params.get("start_date")
            original_end = params.get("end_date")
            try:
                params["start_date"] = m_start
                params["end_date"] = m_end
                df = fetch_fn()
            finally:
                if original_start is not None:
                    params["start_date"] = original_start
                else:
                    params.pop("start_date", None)
                if original_end is not None:
                    params["end_date"] = original_end
                else:
                    params.pop("end_date", None)

            if df is not None and not df.empty:
                for k, v in params.items():
                    if isinstance(v, (str, int, float)) and k not in df.columns:
                        df[k] = v

                self.cache.write(
                    config.table,
                    df,
                    storage_layer=config.storage_layer,
                    partition_by=config.partition_by,
                    partition_value=config.partition_value,
                )
                fetched_parts.append(df)

        all_parts = []
        if cached is not None and not cached.empty:
            all_parts.append(cached)
        all_parts.extend(fetched_parts)

        if not all_parts:
            return pd.DataFrame()

        result = pd.concat(all_parts, ignore_index=True)
        if config.date_col in result.columns:
            result = (
                result.sort_values(config.date_col)
                .drop_duplicates(subset=[config.date_col], keep="last")
                .reset_index(drop=True)
            )

        logger.info(
            "Incremental fetch for table=%s, total %d rows",
            config.table,
            len(result),
        )
        return result

    def _infer_strategy(
        self, config: FetchConfig, params: dict[str, Any]
    ) -> CacheStrategy:
        if "start_date" in params or "end_date" in params:
            filter_keys = config.filter_keys or self._guess_filter_keys(params)
            return IncrementalStrategy(
                date_col=config.date_col,
                filter_keys=filter_keys,
            )
        filter_keys = config.filter_keys or self._guess_filter_keys(params)
        return FullCacheStrategy(filter_keys=filter_keys)

    @staticmethod
    def _guess_filter_keys(params: dict[str, Any]) -> list[str]:
        skip = {"start_date", "end_date", "date_col", "adjust", "source"}
        return [k for k in params if k not in skip and isinstance(params[k], (str, int, float))]
