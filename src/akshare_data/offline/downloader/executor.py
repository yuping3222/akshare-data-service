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

logger = logging.getLogger("akshare_data")

_RETRY_CONFIG = RetryConfig(max_retries=2, delay=1.0, backoff=1.0)


class TaskExecutor(
    Executor[DownloadTask, pd.DataFrame],
    BaseTaskExecutor[DownloadTask, pd.DataFrame],
):
    """下载任务执行器。"""

    mode = ExecutionMode.SYNC

    def __init__(self, rate_limiter: DomainRateLimiter, cache_manager=None):
        self._rate_limiter = rate_limiter
        self._cache_manager = cache_manager

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
        """调用 AkShare 函数。"""
        import akshare as ak

        func = getattr(ak, func_name, None)
        if func is None:
            raise DownloadError(f"Function {func_name} not found")
        return func(**kwargs)

    def _map_columns(self, table: str, df: pd.DataFrame) -> pd.DataFrame:
        """将中文列名映射为统一英文字段。"""
        if df is None or df.empty:
            return df

        mapping = EXTENDED_CN_TO_EN.get(table, {})
        if not mapping:
            return df

        return df.rename(columns={cn: en for cn, en in mapping.items() if cn in df.columns})

    def _write_to_cache(self, task: DownloadTask, df: pd.DataFrame) -> None:
        """写入缓存（先做字段规范化）。"""
        if self._cache_manager is None:
            return

        normalized = self._map_columns(task.table, df)
        kwargs = task.kwargs or {}

        partition_by = None
        partition_value = None
        for key in ("symbol", "code", "ts_code"):
            if key in kwargs and kwargs[key]:
                partition_by = "symbol"
                partition_value = str(kwargs[key])
                break

        start_date = kwargs.get("start_date") or kwargs.get("start") or kwargs.get("begin")
        end_date = kwargs.get("end_date") or kwargs.get("end") or kwargs.get("finish")

        try:
            self._cache_manager.write(
                table=task.table,
                df=normalized,
                partition_by=partition_by,
                partition_value=partition_value,
                start_date=start_date,
                end_date=end_date,
            )
        except TypeError:
            # Backward compatibility for alternate cache manager signatures.
            self._cache_manager.write(
                table=task.table,
                data=normalized,
                storage_layer="duckdb",
                partition_by=partition_by or "date",
            )
