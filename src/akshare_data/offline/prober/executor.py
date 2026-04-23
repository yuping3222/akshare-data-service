"""探测任务执行器 - 重试+回退"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional

import pandas as pd

from akshare_data.offline.core.errors import RetryExhaustedError
from akshare_data.offline.core.retry import RetryConfig, retry
from akshare_data.offline.prober.task_builder import ProbeTask, ValidationResult

logger = logging.getLogger("akshare_data")

SYMBOL_FALLBACKS = ["000001", "sh000001", "USD", "1.0"]
TIMEOUT_LIMIT = 20

_RETRY_CONFIG = RetryConfig(max_retries=2, delay=1.0, backoff=1.0)


class TaskExecutor:
    """探测任务执行器"""

    def execute(self, task: ProbeTask) -> Optional[ValidationResult]:
        """执行探测任务"""
        start_time = time.time()
        try:
            data = self._call_with_retry(task.func, task.params)
            exec_time = time.time() - start_time
            data_size = len(data) if hasattr(data, "__len__") else 1
            status = "Success" if data_size > 0 else "Success (Empty)"
            error = None
        except RetryExhaustedError as e:
            exec_time = time.time() - start_time
            data = None
            data_size = 0
            status = f"Failed ({e.__cause__})" if e.__cause__ else "Failed"
            error = str(e.__cause__) if e.__cause__ else str(e)
        except Exception as e:
            exec_time = time.time() - start_time
            data = None
            data_size = 0
            status = f"Failed ({e})"
            error = str(e)

        return ValidationResult(
            func_name=task.func_name,
            domain_group=task.func_name.split("_")[0]
            if "_" in task.func_name
            else "default",
            status=status,
            error_msg=error or "",
            exec_time=exec_time,
            data_size=data_size,
            data=data,
            last_check=time.time(),
            check_count=1,
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
