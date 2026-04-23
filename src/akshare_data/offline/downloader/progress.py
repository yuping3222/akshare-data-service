"""进度跟踪与回调"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("akshare_data")


class ProgressTracker:
    """进度跟踪器"""

    def __init__(
        self,
        total: int,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        batch_size: int = 50,
    ):
        self._total = total
        self._callback = callback
        self._batch_size = batch_size
        self._completed = 0
        self._failed = 0
        self._start_time = time.time()

    def update(self, success: bool = True):
        """更新进度"""
        if success:
            self._completed += 1
        else:
            self._failed += 1

        if (self._completed + self._failed) % self._batch_size == 0:
            self._report()

    def _report(self):
        """报告进度"""
        total_done = self._completed + self._failed
        pct = (total_done / self._total * 100) if self._total > 0 else 0
        elapsed = time.time() - self._start_time
        rate = total_done / elapsed if elapsed > 0 else 0

        msg = f"Progress: {total_done}/{self._total} ({pct:.1f}%) - {rate:.1f} tasks/s"
        logger.info(msg)

        if self._callback:
            self._callback(
                {
                    "total": self._total,
                    "completed": self._completed,
                    "failed": self._failed,
                    "percentage": pct,
                    "rate": rate,
                    "elapsed": elapsed,
                }
            )

    def finish(self) -> Dict[str, Any]:
        """完成并返回汇总"""
        self._report()
        elapsed = time.time() - self._start_time
        return {
            "total": self._total,
            "completed": self._completed,
            "failed": self._failed,
            "elapsed": elapsed,
        }
