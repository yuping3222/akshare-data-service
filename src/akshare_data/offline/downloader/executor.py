"""下载任务执行器。"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd

from akshare_data.ingestion.executor.base import (
    BaseTaskExecutor,
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    Executor,
    ExecutorContext,
)
from akshare_data.offline.core.errors import DownloadError
from akshare_data.offline.core.retry import RetryConfig, retry
from akshare_data.offline.downloader.rate_limiter import DomainRateLimiter
from akshare_data.offline.downloader.task_builder import DownloadTask
from akshare_data.offline.field_mapper import EXTENDED_CN_TO_EN
from akshare_data.core.schema import get_table_schema
from akshare_data.core.symbols import normalize_symbol

logger = logging.getLogger("akshare_data")

_RETRY_CONFIG = RetryConfig(max_retries=4, delay=1.0, backoff=1.0)


class TaskExecutor(
    Executor[DownloadTask, pd.DataFrame],
    BaseTaskExecutor[DownloadTask, pd.DataFrame],
):
    """下载任务执行器。"""

    mode = ExecutionMode.SYNC
    # 常见同义字段 -> schema 字段名
    COLUMN_ALIASES: Dict[str, str] = {
        "cash_dividend": "dividend_cash",
        "stock_dividend": "dividend_stock",
        "announcement_date": "announce_date",
        "pre_close": "prev_close",
        "total_revenue": "revenue",
        "open_interest": "open_interest",
    }

    def __init__(self, rate_limiter: DomainRateLimiter, cache_manager=None):
        self._rate_limiter = rate_limiter
        self._cache_manager = cache_manager
        self._current_task: Optional[DownloadTask] = None

    def execute(
        self,
        task: DownloadTask,
        context: ExecutionContext | None = None,
    ) -> Dict[str, Any]:
        """兼容旧调用方：返回 dict 结构。"""
        if context is None:
            context = ExecutionContext(
                request_id=f"download-{task.interface}",
                batch_id=f"batch-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                source="akshare",
                dataset=task.table,
            )

        legacy_context = ExecutorContext(
            batch_id=context.batch_id,
            run_id=context.request_id,
            trigger=context.source,
            metadata=context.tags,
        )

        result = self.run(task, context=legacy_context)
        return {
            "success": result.success,
            "rows": result.rows,
            "task": task.interface,
            "error": result.error,
        }

    def execute_structured(
        self,
        task: DownloadTask,
        *,
        context: ExecutionContext,
    ) -> ExecutionResult[pd.DataFrame]:
        """新执行接口：返回结构化统一结果。"""
        legacy_context = ExecutorContext(
            batch_id=context.batch_id,
            run_id=context.request_id,
            trigger=context.source,
            metadata=context.tags,
        )
        return self.run(task, context=legacy_context)

    def run(
        self,
        task: DownloadTask,
        *,
        context: Optional[ExecutorContext] = None,
    ) -> ExecutionResult[pd.DataFrame]:
        """执行单个下载任务并返回统一结果对象。"""
        started_at = datetime.now(timezone.utc)
        metadata: Dict[str, Any] = {
            "interface": task.interface,
            "table": task.table,
            "rate_limit_key": task.rate_limit_key,
            "task": task.interface,
        }
        if context is not None:
            metadata.update(
                {
                    "batch_id": context.batch_id,
                    "run_id": context.run_id,
                    "trigger": context.trigger,
                }
            )

        try:
            self._rate_limiter.wait(task.rate_limit_key)
            self._current_task = task
            df = self._call_akshare(task, **task.kwargs)
        except Exception as exc:
            logger.error("Task %s failed: %s", task.interface, exc)
            return self.result(
                success=False,
                task_name=task.interface,
                error=str(exc),
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                metadata=metadata,
            )

        if df is None or df.empty:
            return self.result(
                success=False,
                task_name=task.interface,
                error="Empty data",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                metadata=metadata,
            )

        if self._cache_manager:
            self._write_to_cache(task, df)

        return self.result(
            success=True,
            task_name=task.interface,
            rows=len(df),
            payload=df,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            metadata=metadata,
        )

    @retry(_RETRY_CONFIG)
    def _call_akshare(self, task: DownloadTask, **kwargs) -> Optional[pd.DataFrame]:
        """调用数据源（支持多源切换）"""
        if task.use_multi_source:
            from akshare_data.sources.akshare.fetcher import fetch as akshare_fetch
            return akshare_fetch(task.interface, **kwargs)
        else:
            import akshare as ak
            func = getattr(ak, task.func, None)
            if func is None:
                raise DownloadError(f"Function {task.func} not found")
            return func(**kwargs)

    def _map_columns(self, table: str, df: pd.DataFrame) -> pd.DataFrame:
        """将中文列名映射为统一英文字段。"""
        if df is None or df.empty:
            return df

        return df.rename(
            columns={cn: en for cn, en in EXTENDED_CN_TO_EN.items() if cn in df.columns}
        )

    def _fill_required_keys_from_kwargs(
        self,
        normalized: pd.DataFrame,
        task: DownloadTask,
    ) -> pd.DataFrame:
        """从任务参数补齐关键主键/日期字段，减少 schema 与落盘字段不一致。"""
        kwargs = task.kwargs or {}
        table_schema = get_table_schema(task.table)
        if table_schema is None:
            return normalized

        schema_fields = set(table_schema.schema.keys())

        def _first_non_empty(*keys: str):
            for key in keys:
                value = kwargs.get(key)
                if value is not None and value != "":
                    return value
            return None

        symbol = _first_non_empty("symbol", "code", "ts_code")
        date_val = _first_non_empty("date", "start_date", "start", "begin", "end_date")

        # 常见代码类字段兜底
        if symbol is not None:
            normalized_symbol = normalize_symbol(str(symbol))
            code_defaults = {
                "symbol": normalized_symbol,
                "stock_code": normalized_symbol,
                "fund_code": str(symbol),
                "bond_code": str(symbol),
                "index_code": str(symbol),
                "industry_code": str(symbol),
                "concept_code": str(symbol),
            }
            for field, value in code_defaults.items():
                if field in schema_fields and field not in normalized.columns:
                    normalized[field] = value

        # 常见日期类字段兜底
        if date_val is not None:
            for field in (
                "date",
                "report_date",
                "announce_date",
                "release_date",
                "rating_date",
                "status_date",
                "transaction_date",
            ):
                if field in schema_fields and field not in normalized.columns:
                    normalized[field] = pd.to_datetime(date_val, errors="coerce")

        if "adjust" in schema_fields and "adjust" not in normalized.columns:
            adjust = kwargs.get("adjust")
            if adjust is not None:
                normalized["adjust"] = str(adjust)

        if "period" in schema_fields and "period" not in normalized.columns:
            period = kwargs.get("period")
            if period is not None:
                normalized["period"] = str(period)

        if "week" in schema_fields and "week" not in normalized.columns:
            if "datetime" in normalized.columns:
                dt = pd.to_datetime(normalized["datetime"], errors="coerce")
                normalized["week"] = dt.dt.strftime("%Y-%U")
            elif date_val is not None:
                dt = pd.to_datetime(date_val, errors="coerce")
                if not pd.isna(dt):
                    normalized["week"] = dt.strftime("%Y-%U")

        # trade_calendar 兜底：接口通常只返回 date 列。
        if "is_trading_day" in schema_fields and "is_trading_day" not in normalized.columns:
            normalized["is_trading_day"] = True

        # 如果 schema 定义了字段但当前不存在，则补空列（保持列契约稳定）
        for field in schema_fields:
            if field not in normalized.columns:
                normalized[field] = pd.NA

        return normalized

    def _write_to_cache(self, task: DownloadTask, df: pd.DataFrame) -> None:
        """写入缓存（先做字段规范化）。"""
        if self._cache_manager is None:
            return

        normalized = self._map_columns(task.table, df)
        # 统一同义字段命名，避免 schema 字段与接口字段不一致。
        for old, new in self.COLUMN_ALIASES.items():
            if old in normalized.columns and new not in normalized.columns:
                normalized = normalized.rename(columns={old: new})

        normalized = self._fill_required_keys_from_kwargs(normalized, task)

        # 根据表名确定存储层
        table_schema = get_table_schema(task.table)
        storage_layer = table_schema.storage_layer if table_schema else "daily"
        partition_by = table_schema.partition_by if table_schema else None

        self._cache_manager.write(
            table=task.table,
            data=normalized,
            storage_layer=storage_layer,
            partition_by=partition_by,
            schema=None,
            primary_key=None,
        )
