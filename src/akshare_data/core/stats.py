"""
Stats collection module - Request statistics, cache statistics, and stats collector.

Provides classes and functions for collecting and exporting runtime statistics
including API request metrics and cache hit/miss rates.
"""

import csv
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional


class RequestStats:
    """单个数据源的请求统计"""

    def __init__(self):
        self.total_requests: int = 0
        self.successful_requests: int = 0
        self.failed_requests: int = 0
        self.total_duration_ms: float = 0.0
        self.errors: Dict[str, int] = {}
        self.min_duration_ms: Optional[float] = None
        self.max_duration_ms: Optional[float] = None
        self._durations: List[float] = []

    @property
    def avg_duration_ms(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.total_duration_ms / self.total_requests

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests

    @property
    def error_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "avg_duration_ms": round(self.avg_duration_ms, 2),
            "min_duration_ms": self.min_duration_ms,
            "max_duration_ms": self.max_duration_ms,
            "success_rate": round(self.success_rate, 4),
            "error_rate": round(self.error_rate, 4),
        }
        if self.errors:
            result["errors"] = dict(self.errors)
        return result


class CacheStats:
    """单个缓存的统计"""

    def __init__(self):
        self.hits: int = 0
        self.misses: int = 0

    @property
    def total_requests(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        total = self.total_requests
        if total == 0:
            return 0.0
        return self.hits / total

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "total_requests": self.total_requests,
            "hit_rate": round(self.hit_rate, 4),
        }


class StatsCollector:
    """统计收集器（单例）"""

    _instance: Optional["StatsCollector"] = None
    _init_lock = threading.Lock()

    def __new__(cls) -> "StatsCollector":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self._lock = threading.Lock()
        self._request_stats: Dict[str, RequestStats] = {}
        self._cache_stats: Dict[str, CacheStats] = {}
        self._initialized = True

    @classmethod
    def get_instance(cls) -> "StatsCollector":
        return cls()

    @classmethod
    def reset_instance(cls):
        with cls._init_lock:
            cls._instance = None

    def record_request(
        self,
        source: str,
        duration_ms: float,
        success: bool,
        error_type: Optional[str] = None,
        endpoint: Optional[str] = None,
    ):
        with self._lock:
            if source not in self._request_stats:
                self._request_stats[source] = RequestStats()
            stats = self._request_stats[source]
            stats.total_requests += 1
            stats.total_duration_ms += duration_ms
            stats._durations.append(duration_ms)
            if stats.min_duration_ms is None or duration_ms < stats.min_duration_ms:
                stats.min_duration_ms = duration_ms
            if stats.max_duration_ms is None or duration_ms > stats.max_duration_ms:
                stats.max_duration_ms = duration_ms
            if success:
                stats.successful_requests += 1
            else:
                stats.failed_requests += 1
                if error_type:
                    stats.errors[error_type] = stats.errors.get(error_type, 0) + 1

    def record_cache_hit(self, cache_name: str):
        with self._lock:
            if cache_name not in self._cache_stats:
                self._cache_stats[cache_name] = CacheStats()
            self._cache_stats[cache_name].hits += 1

    def record_cache_miss(self, cache_name: str):
        with self._lock:
            if cache_name not in self._cache_stats:
                self._cache_stats[cache_name] = CacheStats()
            self._cache_stats[cache_name].misses += 1

    def get_source_stats(self, source: str) -> Dict[str, Any]:
        with self._lock:
            stats = self._request_stats.get(source)
            if stats is None:
                return {}
            return stats.to_dict()

    def get_cache_stats(self, cache_name: str) -> Dict[str, Any]:
        with self._lock:
            stats = self._cache_stats.get(cache_name)
            if stats is None:
                return {}
            return stats.to_dict()

    def get_all_stats(self) -> Dict[str, Any]:
        with self._lock:
            result = {
                "request_stats": {
                    source: stats.to_dict()
                    for source, stats in self._request_stats.items()
                },
                "cache_stats": {
                    name: stats.to_dict() for name, stats in self._cache_stats.items()
                },
            }
            total_requests = sum(s.total_requests for s in self._request_stats.values())
            total_success = sum(
                s.successful_requests for s in self._request_stats.values()
            )
            total_failed = sum(s.failed_requests for s in self._request_stats.values())
            total_duration = sum(
                s.total_duration_ms for s in self._request_stats.values()
            )
            cache_hits = sum(s.hits for s in self._cache_stats.values())
            cache_misses = sum(s.misses for s in self._cache_stats.values())
            cache_total = cache_hits + cache_misses
            result["summary"] = {
                "total_requests": total_requests,
                "total_success": total_success,
                "total_failed": total_failed,
                "overall_success_rate": round(total_success / total_requests, 4)
                if total_requests > 0
                else 0.0,
                "avg_duration_ms": round(total_duration / total_requests, 2)
                if total_requests > 0
                else 0.0,
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "cache_hit_rate": round(cache_hits / cache_total, 4)
                if cache_total > 0
                else 0.0,
            }
            return result

    def get_summary_text(self) -> str:
        stats = self.get_all_stats()
        summary = stats.get("summary", {})
        lines = []
        lines.append("=" * 50)
        lines.append("  Stats Summary")
        lines.append("=" * 50)
        lines.append(f"  Total Requests:    {summary.get('total_requests', 0)}")
        lines.append(
            f"  Success Rate:      {summary.get('overall_success_rate', 0) * 100:.1f}%"
        )
        lines.append(f"  Avg Duration:      {summary.get('avg_duration_ms', 0):.1f}ms")
        lines.append(
            f"  Cache Hit Rate:    {summary.get('cache_hit_rate', 0) * 100:.1f}%"
        )
        lines.append("-" * 50)
        request_stats = stats.get("request_stats", {})
        if request_stats:
            lines.append("  Request Stats by Source:")
            for source, s in sorted(request_stats.items()):
                lines.append(
                    f"    {source:20s} | reqs={s['total_requests']:5d} | "
                    f"ok={s['success_rate'] * 100:5.1f}% | "
                    f"avg={s['avg_duration_ms']:7.1f}ms"
                )
            lines.append("-" * 50)
        cache_stats = stats.get("cache_stats", {})
        if cache_stats:
            lines.append("  Cache Stats:")
            for name, s in sorted(cache_stats.items()):
                lines.append(
                    f"    {name:20s} | hits={s['hits']:5d} | "
                    f"misses={s['misses']:5d} | "
                    f"hit_rate={s['hit_rate'] * 100:5.1f}%"
                )
            lines.append("-" * 50)
        lines.append("")
        return "\n".join(lines)

    def print_summary(self, force: bool = False):
        summary_text = self.get_summary_text()
        if force:
            logging.getLogger(__name__).info("\n" + summary_text)
        else:
            logging.getLogger(__name__).debug("\n" + summary_text)

    def log_summary(self, logger=None, level=logging.INFO):
        if logger is None:
            logger = logging.getLogger(__name__)
        summary_text = self.get_summary_text()
        logger.log(level, "\n" + summary_text)

    def export_json(self, filepath: str):
        stats = self.get_all_stats()
        stats["exported_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)

    def export_csv(self, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        with self._lock:
            for source, stats in self._request_stats.items():
                filepath = os.path.join(output_dir, f"{source}_stats.csv")
                with open(filepath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["metric", "value"])
                    data = stats.to_dict()
                    for key, value in data.items():
                        if key == "errors" and isinstance(value, dict):
                            for error_type, count in value.items():
                                writer.writerow([f"error:{error_type}", count])
                        else:
                            writer.writerow([key, value])

    def reset(self):
        with self._lock:
            self._request_stats.clear()
            self._cache_stats.clear()


# Module-level singleton
_stats_collector: Optional[StatsCollector] = None


def get_stats_collector() -> StatsCollector:
    """Get the global stats collector singleton."""
    global _stats_collector
    if _stats_collector is None:
        _stats_collector = StatsCollector.get_instance()
    return _stats_collector


def reset_stats_collector():
    """Reset the global stats collector singleton."""
    global _stats_collector
    StatsCollector.reset_instance()
    _stats_collector = None


def log_api_request(
    logger: logging.Logger,
    source: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
    status: str = "success",
    rows: Optional[int] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
) -> None:
    """Log an API request with structured context."""
    context = {
        "log_type": "api_request",
        "source": source,
        "endpoint": endpoint,
        "status": status,
    }
    if params:
        context["params"] = params
    if duration_ms is not None:
        context["duration_ms"] = round(duration_ms, 2)
    if rows is not None:
        context["rows"] = rows
    if error:
        context["error"] = error

    extra = {"context": context}
    if error_code:
        extra["error_code"] = error_code

    if status == "success":
        logger.info(f"API request to {source} completed", extra=extra)
    elif status == "error":
        logger.error(f"API request to {source} failed: {error}", extra=extra)
    else:
        logger.warning(f"API request to {source} {status}", extra=extra)


def log_data_quality(
    logger: logging.Logger,
    source: str,
    data_type: str,
    issue: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Log a data quality issue."""
    context = {
        "log_type": "data_quality",
        "source": source,
        "data_type": data_type,
        "issue": issue,
    }
    if details:
        context["details"] = details
    logger.warning(f"Data quality issue: {issue}", extra={"context": context})


__all__ = [
    "RequestStats",
    "CacheStats",
    "StatsCollector",
    "get_stats_collector",
    "reset_stats_collector",
    "log_api_request",
    "log_data_quality",
    "_stats_collector",
]
