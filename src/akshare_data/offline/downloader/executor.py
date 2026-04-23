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
    ExecutionResult as UnifiedExecutionResult,
    Executor,
    ExecutorContext,
    ExecutorStats,
    TaskExecutionResult,
)
from akshare_data.offline.core.errors import DownloadError
from akshare_data.offline.core.retry import RetryConfig, retry
from akshare_data.offline.downloader.rate_limiter import DomainRateLimiter
from akshare_data.offline.downloader.task_builder import DownloadTask
from akshare_data.offline.field_mapper import EXTENDED_CN_TO_EN

logger = logging.getLogger("akshare_data")

_RETRY_CONFIG = RetryConfig(max_retries=2, delay=1.0, backoff=1.0)


class TaskExecutor(Executor[DownloadTask, pd.DataFrame], BaseTaskExecutor[DownloadTask, pd.DataFrame]):
    """下载任务执行器。"""

    mode = ExecutionMode.SYNC

    def __init__(self, rate_limiter: DomainRateLimiter, cache_manager=None):
        self._rate_limiter = rate_limiter
        self._cache_manager = cache_manager

    def execute(
        self,
        task: DownloadTask,
        *,
        context: ExecutionContext | None = None,
    ) -> Dict[str, Any]:
        """兼容旧调用方：返回 dict 结构。"""
        legacy_context = None
        if context is not None:
            legacy_context = ExecutorContext(
                batch_id=context.batch_id,
                run_id=context.request_id,
                trigger=context.source,
                metadata=context.tags,
            )
        return self.run(task, context=legacy_context).to_dict()

    def execute_structured(
        self,
        task: DownloadTask,
        *,
        context: ExecutionContext,
    ) -> UnifiedExecutionResult[pd.DataFrame]:
        start = datetime.now(timezone.utc)
        result = self.run(
            task,
            context=ExecutorContext(
                batch_id=context.batch_id,
                run_id=context.request_id,
                trigger=context.source,
                metadata=context.tags,
            ),
        )

        if result.success:
            return UnifiedExecutionResult.success(
                payload=result.payload,
                stats=ExecutorStats(
                    latency_ms=(result.finished_at - start).total_seconds() * 1000,
                    input_count=1,
                    output_count=result.rows,
                ),
                metadata=result.metadata,
            )

        return UnifiedExecutionResult.failure(
            error_code="download_failed",
            error_message=result.error,
            stats=ExecutorStats(
                latency_ms=(result.finished_at - start).total_seconds() * 1000,
            ),
            metadata=result.metadata,
        )

    def run(
        self,
        task: DownloadTask,
        *,
        context: Optional[ExecutorContext] = None,
    ) -> TaskExecutionResult[pd.DataFrame]:
        """执行单个下载任务，返回统一结果对象。"""
        started_at = datetime.now(timezone.utc)
        metadata: Dict[str, Any] = {
            "interface": task.interface,
            "table": task.table,
            "rate_limit_key": task.rate_limit_key,
        }
        if context:
            metadata.update(
                {
                    "batch_id": context.batch_id,
                    "run_id": context.run_id,
                    "trigger": context.trigger,
                }
            )

        try:
            self._rate_limiter.wait(task.rate_limit_key)
            df = self._call_akshare(task.func, **task.kwargs)
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
    def _call_akshare(self, func_name: str, **kwargs) -> Optional[pd.DataFrame]:
        """调用 AkShare 函数"""
        import akshare as ak

        func = getattr(ak, func_name, None)
        if func is None:
            raise DownloadError(f"Function {func_name} not found")
        return func(**kwargs)

    def _write_to_cache(self, task: DownloadTask, df: pd.DataFrame):
        """写入缓存（先做字段规范化）"""
        mapped = self._map_columns(task.table, df)
        try:
            if hasattr(self._cache_manager, "write"):
                self._cache_manager.write(table=task.table, data=mapped, partition_value=None)
            elif hasattr(self._cache_manager, "write_data"):
                self._cache_manager.write_data(table=task.table, data=mapped)
            else:
                logger.warning("cache_manager has no write/write_data method")
        except Exception as e:
            logger.warning(f"Cache write failed for {task.table}: {e}")

    def _map_columns(self, table: str, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        from akshare_data.core.schema import get_table_schema

        schema = get_table_schema(table)
        if schema is None:
            return df

        target_cols = set(schema.schema.keys())
        rename_map: Dict[str, str] = {}

        for col in df.columns:
            if col in target_cols:
                continue
            mapped = EXTENDED_CN_TO_EN.get(col)
            if mapped and mapped in target_cols:
                rename_map[col] = mapped

        if rename_map:
            df = df.rename(columns=rename_map)

        drop_cols = [c for c in df.columns if c not in target_cols]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        return df
