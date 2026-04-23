"""异步访问日志记录器: 在线模块每次 API 调用写一行日志

日志格式 (每行一条 JSON):
{"ts": "2024-01-15T10:30:00.123", "interface": "equity_daily", "symbol": "000001", "cache_hit": false, "latency_ms": 450, "source": "akshare_em"}

特性:
- 异步批量刷盘，不阻塞 API 请求
- 按天轮转，保留 N 天
- 线程安全
"""

import json
import queue
import threading
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger("akshare_data")


class AccessLogger:
    """异步访问日志记录器"""

    def __init__(
        self,
        log_dir: str = "logs",
        max_buffer: int = 100,
        flush_interval: float = 5.0,
        backup_days: int = 30,
    ):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._queue: queue.Queue = queue.Queue(maxsize=max_buffer)
        self._flush_interval = flush_interval
        self._backup_days = backup_days
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._flush_loop, daemon=True)
        self._worker.start()
        self._current_date = datetime.now().strftime("%Y-%m-%d")
        self._file_handle = None

    def record(
        self,
        interface: str,
        symbol: Optional[str] = None,
        cache_hit: bool = False,
        latency_ms: float = 0,
        source: Optional[str] = None,
    ):
        """记录一次 API 调用 (非阻塞)"""
        entry = {
            "ts": datetime.now().isoformat(),
            "interface": interface,
            "symbol": symbol,
            "cache_hit": cache_hit,
            "latency_ms": round(latency_ms, 2),
        }
        if source:
            entry["source"] = source

        try:
            self._queue.put_nowait(entry)
        except queue.Full:
            logger.warning("Access log queue full, dropping entry")

    def _flush_loop(self):
        """后台刷盘循环"""
        while not self._stop_event.is_set():
            self._flush()
            self._stop_event.wait(self._flush_interval)

        self._flush()

    def _flush(self):
        """将队列中的日志写入文件"""
        entries = []
        while True:
            try:
                entries.append(self._queue.get_nowait())
            except queue.Empty:
                break

        if not entries:
            return

        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._current_date:
            self._rotate()
            self._current_date = today

        try:
            with self._get_file_handle() as f:
                for entry in entries:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error("Failed to write access log: %s", e)

    def _get_file_handle(self):
        """获取当前日志文件句柄"""
        log_path = self._log_dir / "access.log"
        return open(log_path, "a", encoding="utf-8")

    def _rotate(self):
        """日志轮转: 重命名旧文件"""
        old_path = self._log_dir / "access.log"
        if old_path.exists():
            rotated = self._log_dir / f"access.log.{self._current_date}"
            try:
                old_path.rename(rotated)
            except Exception:
                pass

        self._cleanup_old_logs()

    def _cleanup_old_logs(self):
        """清理超过保留天数的日志"""
        from datetime import timedelta

        cutoff = datetime.now() - timedelta(days=self._backup_days)
        for f in self._log_dir.glob("access.log.*"):
            try:
                date_str = f.name.replace("access.log.", "")
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < cutoff:
                    f.unlink()
            except (ValueError, OSError):
                pass

    def shutdown(self):
        """停止日志记录器"""
        self._stop_event.set()
        self._worker.join(timeout=10)
        self._flush()
