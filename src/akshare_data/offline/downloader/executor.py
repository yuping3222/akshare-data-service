"""下载任务执行器。"""

from __future__ import annotations

import logging
import time
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

    def execute(self, task: DownloadTask, context: ExecutionContext | None = None) -> Dict[str, Any]:
        """兼容旧调用方：返回 dict 结构。"""
        if context is None:
            context = ExecutionContext(
                request_id=f"download-{task.interface}",
                batch_id=f"batch-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                source="akshare",
                dataset=task.table,
            )

        result = self.execute_structured(task, context=context)
        rows = len(result.payload) if result.payload is not None else 0
        return {
            "success": result.ok,
            "rows": rows,
            "task": task.interface,
            "error": result.error_message or result.error_code or "",
        }

    def run(
        self,
        task: DownloadTask,
        *,
        context: Optional[ExecutorContext] = None,
    ) -> TaskExecutionResult[pd.DataFrame]:
        """执行单个下载任务，返回统一结果对象。"""
    ) -> ExecutionResult[pd.DataFrame]:
        """兼容旧任务执行接口：返回 ExecutionResult。"""
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
        except Exception as e:
            logger.error("Task %s failed: %s", task.interface, e)
            return self.result(
                success=False,
                task_name=task.interface,
                error=str(e),
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

    def execute_structured(
        self,
        task: DownloadTask,
        *,
        context: ExecutionContext,
    ) -> ExecutionResult[pd.DataFrame]:
        """新执行接口：返回结构化统一结果。"""
        start = time.perf_counter()

        try:
            self._rate_limiter.wait(task.rate_limit_key)
            df = self._call_akshare(task.func, **task.kwargs)
        except Exception as exc:
            return ExecutionResult.create_failure(
                error_code="download_failed",
                error_message=str(exc),
                stats=ExecutorStats(latency_ms=(time.perf_counter() - start) * 1000),
                metadata={
                    "task": task.interface,
                    "request_id": context.request_id,
                    "batch_id": context.batch_id,
                },
            )

        if df is None or df.empty:
            return ExecutionResult.create_failure(
                error_code="empty_data",
                error_message="Empty data",
                stats=ExecutorStats(latency_ms=(time.perf_counter() - start) * 1000),
                metadata={
                    "task": task.interface,
                    "request_id": context.request_id,
                    "batch_id": context.batch_id,
                },
            )

        if self._cache_manager:
            self._write_to_cache(task, df)

        return ExecutionResult.create_success(
            payload=df,
            stats=ExecutorStats(
                latency_ms=(time.perf_counter() - start) * 1000,
                input_count=1,
                output_count=len(df),
            ),
            metadata={
                "task": task.interface,
                "request_id": context.request_id,
                "batch_id": context.batch_id,
            },
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

        exec_context = ExecutionContext(
            request_id=f"download-{task.interface}",
            batch_id=metadata.get("batch_id", f"batch-{int(time.time())}"),
            source="akshare",
            dataset=task.table,
        )

        result = self.execute_structured(task, context=exec_context)

        if not result.ok:
            return self.result(
                success=False,
                task_name=task.interface,
                error=result.error_message or result.error_code or "unknown",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                metadata=metadata,
            )

        if self._cache_manager:
            self._write_to_cache(task, df)

        df = result.payload
        assert df is not None
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

    def _write_to_cache(self, task: DownloadTask, df: pd.DataFrame) -> None:
        """写入缓存（先做字段规范化）。"""
        if not self._cache_manager:
            return

        try:
            normalized = self._normalize_columns(task, df)
            self._cache_manager.write(
                table=task.table,
                data=normalized,
                storage_layer="duckdb",
                partition_by="date",
            )
        except Exception as exc:
            logger.warning("Failed to write cache for %s: %s", task.table, exc)

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """标准化 DataFrame 列名（中文->英文）。"""
        if df is None or df.empty:
            return df
        rename_map = {
            c: EXTENDED_CN_TO_EN.get(c, c)
            for c in df.columns
            if isinstance(c, str) and c in EXTENDED_CN_TO_EN
        }
        if rename_map:
            return df.rename(columns=rename_map)
        return df

    def _write_to_cache(self, task: DownloadTask, df: pd.DataFrame) -> None:
        """写入缓存。"""
        if self._cache_manager is None:
            return

        normalized = self._normalize_columns(df)
        table = task.table
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

        self._cache_manager.write(
            table=table,
            df=normalized,
            partition_by=partition_by,
            partition_value=partition_value,
            start_date=start_date,
            end_date=end_date,
        )
