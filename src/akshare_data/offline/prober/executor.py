"""探测任务执行器 - 重试+回退。"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from akshare_data.ingestion.executor.base import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult as UnifiedExecutionResult,
    Executor,
    ExecutorStats,
)
from akshare_data.offline.core.errors import RetryExhaustedError
from akshare_data.offline.core.retry import RetryConfig, retry
from akshare_data.offline.prober.task_builder import ProbeTask, ValidationResult

logger = logging.getLogger("akshare_data")

SYMBOL_FALLBACKS = ["000001", "sh000001", "USD", "1.0"]
TIMEOUT_LIMIT = 20

_RETRY_CONFIG = RetryConfig(max_retries=2, delay=1.0, backoff=1.0)


class TaskExecutor(Executor[ProbeTask, ValidationResult]):
    """探测任务执行器。"""

    mode = ExecutionMode.SYNC

    def execute(self, task: ProbeTask, context: ExecutionContext | None = None) -> Optional[ValidationResult]:
        """执行探测任务（兼容旧接口）。"""
        if context is None:
            context = ExecutionContext(
                request_id=f"probe-{task.func_name}",
                batch_id=f"batch-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                source="akshare",
                dataset=task.func_name,
            )

        result = self.execute_structured(task, context=context)
        return result.payload if result.ok else ValidationResult(
            func_name=task.func_name,
            domain_group=task.func_name.split("_")[0] if "_" in task.func_name else "default",
            status=f"Failed ({result.error_message})",
            error_msg=result.error_message or result.error_code or "unknown",
            exec_time=result.stats.latency_ms / 1000,
            data_size=0,
            data=None,
            last_check=time.time(),
            check_count=1,
        )

    def execute_structured(
        self,
        task: ProbeTask,
        *,
        context: ExecutionContext,
    ) -> UnifiedExecutionResult[ValidationResult]:
        """执行探测任务并返回统一结构结果。"""
        start_time = time.perf_counter()
        try:
            data = self._call_with_retry(task.func, task.params)
            elapsed = (time.perf_counter() - start_time) * 1000
            data_size = len(data) if hasattr(data, "__len__") else 1
            status = "Success" if data_size > 0 else "Success (Empty)"
            payload = ValidationResult(
                func_name=task.func_name,
                domain_group=task.func_name.split("_")[0]
                if "_" in task.func_name
                else "default",
                status=status,
                error_msg="",
                exec_time=elapsed / 1000,
                data_size=data_size,
                data=data,
                last_check=time.time(),
                check_count=1,
            )
            return UnifiedExecutionResult.success(
                payload=payload,
                stats=ExecutorStats(
                    latency_ms=elapsed,
                    input_count=1,
                    output_count=data_size,
                ),
                metadata={
                    "request_id": context.request_id,
                    "batch_id": context.batch_id,
                    "func_name": task.func_name,
                },
            )
        except RetryExhaustedError as e:
            elapsed = (time.perf_counter() - start_time) * 1000
            err = str(e.__cause__) if e.__cause__ else str(e)
            return UnifiedExecutionResult.failure(
                error_code="probe_retry_exhausted",
                error_message=err,
                stats=ExecutorStats(latency_ms=elapsed),
                metadata={
                    "request_id": context.request_id,
                    "batch_id": context.batch_id,
                    "func_name": task.func_name,
                },
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start_time) * 1000
            return UnifiedExecutionResult.failure(
                error_code="probe_failed",
                error_message=str(e),
                stats=ExecutorStats(latency_ms=elapsed),
                metadata={
                    "request_id": context.request_id,
                    "batch_id": context.batch_id,
                    "func_name": task.func_name,
                },
            )

    @retry(_RETRY_CONFIG)
    def _call_with_retry(self, func: Callable, kwargs: Dict[str, Any]) -> Any:
        """带重试和符号回退的调用"""
        try:
            return func(**kwargs)
        except Exception as e:
            error_msg = str(e)
            if "symbol" in kwargs and any(
                kw in error_msg.lower() for kw in ["symbol", "code", "参数"]
            ):
                for fallback in SYMBOL_FALLBACKS:
                    try:
                        kwargs["symbol"] = fallback
                        return func(**kwargs)
                    except Exception:
                        continue
            raise
