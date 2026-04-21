"""Multi-source data router with automatic failover.

Adapted from akshare-one-enhanced for akshare-data-service project.
Provides automatic failover across multiple data sources.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd

logger = logging.getLogger(__name__)


class EmptyDataPolicy(Enum):
    """Policy for handling empty results."""

    STRICT = "strict"
    RELAXED = "relaxed"
    BEST_EFFORT = "best_effort"


@dataclass
class ExecutionResult:
    """Result of a multi-source execution."""

    success: bool
    data: Optional[pd.DataFrame]
    source: Optional[str]
    error: Optional[str]
    attempts: int
    error_details: Optional[List[Tuple[str, str]]] = None
    is_empty: bool = False
    is_fallback: bool = False
    sources_tried: List[Dict[str, Any]] = field(default_factory=list)


class DomainRateLimiter:
    """Domain-level rate limiter that maps real domains to abstract rate limit keys.

    Loads domain-to-rate-key mapping from config/sources/domains.yaml and
    interval values from config/rate_limits.yaml. All rate limiting uses
    abstract keys (e.g. 'em_push2his') rather than raw hostnames.
    """

    def __init__(
        self,
        intervals: Optional[Dict[str, float]] = None,
        domain_map: Optional[Dict[str, str]] = None,
        default_interval: float = 0.5,
    ):
        # intervals: {rate_limit_key: seconds}
        self._intervals = intervals or {}
        # domain_map: {hostname: rate_limit_key}
        self._domain_map = domain_map or {}
        self._default_interval = default_interval
        self._last_request: Dict[str, float] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_config(
        cls,
        domains_path: Optional[str] = None,
        rate_limits_path: Optional[str] = None,
        domains_data: Optional[Dict] = None,
        rate_limits_data: Optional[Dict] = None,
    ) -> "DomainRateLimiter":
        """Build a DomainRateLimiter from YAML config data.

        Accepts either file paths to load, or already-parsed dicts.
        """
        import yaml

        if domains_data is None:
            if domains_path is None:
                raise ValueError("Need either domains_path or domains_data")
            with open(domains_path, "r") as f:
                domains_data = yaml.safe_load(f)

        if rate_limits_data is None:
            if rate_limits_path is None:
                raise ValueError("Need either rate_limits_path or rate_limits_data")
            with open(rate_limits_path, "r") as f:
                rate_limits_data = yaml.safe_load(f)

        # Build domain -> rate_limit_key mapping
        domain_map: Dict[str, str] = {}
        domains_section = domains_data.get("domains", domains_data)
        for key, value in domains_section.items():
            if isinstance(value, dict):
                rate_key = value.get("rate_limit_key", key)
                url_pattern = value.get("url_pattern")
                if url_pattern:
                    domain_map[url_pattern] = rate_key

        # Build rate_limit_key -> interval mapping
        intervals: Dict[str, float] = {}
        for key, value in rate_limits_data.items():
            if isinstance(value, dict):
                intervals[key] = value.get("interval", 0.5)
            elif isinstance(value, (int, float)):
                intervals[key] = value

        default_interval = intervals.get("default", 0.5)
        return cls(intervals=intervals, domain_map=domain_map, default_interval=default_interval)

    def _resolve_rate_key(self, domain: str) -> str:
        """Resolve a hostname to its rate limit key."""
        # Try exact match first
        if domain in self._domain_map:
            return self._domain_map[domain]
        # If domain is already a rate_limit_key, use it directly
        if domain in self._intervals:
            return domain
        # Fallback: check if any url_pattern is a substring
        for pattern, key in self._domain_map.items():
            if pattern in domain or domain in pattern:
                return key
        return "default"

    def wait_if_needed(self, domain: str) -> None:
        """Wait if the last request to this domain was too recent."""
        rate_key = self._resolve_rate_key(domain)
        interval = self._intervals.get(rate_key, self._default_interval)
        with self._lock:
            last_time = self._last_request.get(rate_key, 0)
            elapsed = time.time() - last_time
            if elapsed < interval:
                sleep_time = interval - elapsed
                logger.debug("Rate limit %s: sleeping %.2fs", rate_key, sleep_time)
                time.sleep(sleep_time)
            self._last_request[rate_key] = time.time()

    def record_request(self, domain: str) -> None:
        """Record a request timestamp for the given domain."""
        rate_key = self._resolve_rate_key(domain)
        with self._lock:
            self._last_request[rate_key] = time.time()

    def set_interval(self, rate_key: str, interval: float) -> None:
        """Set the minimum interval for a rate limit key."""
        with self._lock:
            self._intervals[rate_key] = interval

    def get_interval(self, domain: str) -> float:
        """Get the current interval for a domain."""
        rate_key = self._resolve_rate_key(domain)
        return self._intervals.get(rate_key, self._default_interval)

    def get_rate_key(self, domain: str) -> str:
        """Get the resolved rate limit key for a domain."""
        return self._resolve_rate_key(domain)

    def reset(self) -> None:
        """Reset all rate limiting state."""
        with self._lock:
            self._last_request.clear()

    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from a URL string."""
        try:
            parsed = urlparse(url)
            return parsed.netloc or url
        except Exception:
            return url


class SourceHealthMonitor:
    """Monitor health of data sources with simple circuit breaker."""

    _ERROR_THRESHOLD = 5
    _DISABLE_DURATION = 300

    def __init__(self):
        self._status: Dict[str, Dict[str, Any]] = {}

    def record_result(
        self, source: str, success: bool, error: Optional[str] = None
    ) -> None:
        if source not in self._status:
            self._status[source] = {
                "available": True,
                "last_error": None,
                "error_count": 0,
            }
        status = self._status[source]
        if success:
            status["available"] = True
            status["error_count"] = 0
            status["last_error"] = None
        else:
            status["error_count"] += 1
            status["last_error"] = error
            if status["error_count"] >= self._ERROR_THRESHOLD:
                status["available"] = False
                status["disabled_at"] = time.time()
                logger.warning("数据源 %s 被临时禁用（错误次数过多）", source)

    def is_available(self, source: str) -> bool:
        if source not in self._status:
            return True
        status = self._status[source]
        if not status["available"]:
            disabled_at = status.get("disabled_at")
            if disabled_at is not None:
                elapsed = time.time() - disabled_at
                if elapsed > self._DISABLE_DURATION:
                    status["available"] = True
                    status["error_count"] = 0
                    logger.info("数据源 %s 已恢复", source)
        return status["available"]

    def get_status(self) -> Dict[str, Dict[str, Any]]:
        import copy

        return copy.deepcopy(self._status)


class MultiSourceRouter:
    """Routes data requests across multiple sources with automatic failover.

    Accepts a list of (name, callable) tuples. Each callable should return
    a pandas DataFrame or raise an exception on failure.
    """

    def __init__(
        self,
        providers: List[Tuple[str, Callable]],
        required_columns: Optional[List[str]] = None,
        min_rows: int = 0,
        policy: EmptyDataPolicy = EmptyDataPolicy.STRICT,
        stats_collector=None,
    ):
        self.providers = list(providers)
        self.required_columns = required_columns or []
        self.min_rows = min_rows
        self.policy = policy
        self._health = SourceHealthMonitor()
        self._stats_collector = stats_collector
        self._stats: Dict[str, Any] = {
            "total_calls": 0,
            "successes": 0,
            "failures": 0,
            "empty_results": 0,
            "fallbacks": 0,
            "source_stats": {},
        }

    def execute(self, *args, **kwargs) -> ExecutionResult:
        """Execute data fetch across providers with automatic failover.

        Tries each provider in order until one returns valid data.
        """
        self._stats["total_calls"] += 1
        error_details: List[Tuple[str, str]] = []
        sources_tried: List[Dict[str, Any]] = []
        last_result: Optional[pd.DataFrame] = None
        last_source: Optional[str] = None
        last_empty: bool = False

        for name, func in self.providers:
            if not self._health.is_available(name):
                logger.debug("Provider %s is unavailable, skipping", name)
                continue

            source_info = {
                "name": name,
                "attempted": False,
                "success": False,
                "error": None,
                "elapsed": 0.0,
            }
            source_info["attempted"] = True
            start = time.time()

            try:
                logger.debug("Trying provider: %s", name)
                data = func(*args, **kwargs)
                elapsed = time.time() - start
                source_info["elapsed"] = elapsed
                self._health.record_result(name, success=True)

                if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                    logger.debug("Provider %s returned empty data", name)
                    source_info["success"] = True
                    sources_tried.append(source_info)
                    last_result = data if isinstance(data, pd.DataFrame) else None
                    last_source = name
                    last_empty = True
                    continue

                if not self._validate_result(data):
                    logger.debug("Provider %s returned invalid data", name)
                    source_info["error"] = "validation_failed"
                    sources_tried.append(source_info)
                    continue

                is_fallback = len(sources_tried) > 0 or name == "__cache__"
                self._update_stats(
                    name,
                    success=True,
                    empty=False,
                    fallback=is_fallback,
                    duration_ms=elapsed * 1000,
                )
                source_info["success"] = True
                sources_tried.append(source_info)

                return ExecutionResult(
                    success=True,
                    data=data,
                    source=name,
                    error=None,
                    attempts=len(sources_tried),
                    error_details=error_details,
                    is_empty=False,
                    is_fallback=is_fallback,
                    sources_tried=sources_tried,
                )

            except Exception as e:
                elapsed = time.time() - start
                source_info["elapsed"] = elapsed
                source_info["error"] = str(e)
                sources_tried.append(source_info)
                error_details.append((name, str(e)))
                logger.warning("Provider %s failed: %s", name, e)
                self._health.record_result(name, success=False, error=str(e))
                self._update_stats(
                    name,
                    success=False,
                    empty=False,
                    fallback=False,
                    duration_ms=elapsed * 1000,
                    error_type=type(e).__name__,
                )

        # No provider returned valid data
        if last_empty and self.policy in (
            EmptyDataPolicy.BEST_EFFORT,
            EmptyDataPolicy.RELAXED,
        ):
            is_fallback = len(sources_tried) > 1 or last_source == "__cache__"
            self._update_stats(
                last_source, success=True, empty=True, fallback=is_fallback
            )
            return ExecutionResult(
                success=True,
                data=last_result,
                source=last_source,
                error="all_providers_returned_empty"
                if self.policy == EmptyDataPolicy.RELAXED
                else None,
                attempts=len(sources_tried),
                error_details=error_details,
                is_empty=True,
                is_fallback=is_fallback,
                sources_tried=sources_tried,
            )

        self._stats["failures"] += 1
        error_msg = (
            "all_providers_failed" if not last_empty else "all_providers_returned_empty"
        )
        return ExecutionResult(
            success=False,
            data=None,
            source=None,
            error=error_msg,
            attempts=len(sources_tried),
            error_details=error_details,
            is_empty=last_empty,
            is_fallback=False,
            sources_tried=sources_tried,
        )

    def _validate_result(self, data: Any) -> bool:
        """Validate that the result meets requirements."""
        # When no specific DataFrame requirements are set (default case),
        # accept any non-None data (including non-DataFrame types like lists).
        if not self.required_columns and not self.min_rows:
            return data is not None
        if not isinstance(data, pd.DataFrame):
            return False
        if len(data) < self.min_rows:
            return False
        for col in self.required_columns:
            if col not in data.columns:
                return False
        return True

    def _update_stats(
        self,
        source: str,
        success: bool,
        empty: bool,
        fallback: bool,
        duration_ms: float = 0.0,
        error_type: str = None,
    ) -> None:
        """Update internal statistics and report to global StatsCollector."""
        if success:
            self._stats["successes"] += 1
        if empty:
            self._stats["empty_results"] += 1
        if fallback:
            self._stats["fallbacks"] += 1

        if source:
            if source not in self._stats["source_stats"]:
                self._stats["source_stats"][source] = {
                    "successes": 0,
                    "failures": 0,
                    "empty": 0,
                }
            if success:
                self._stats["source_stats"][source]["successes"] += 1
            else:
                self._stats["source_stats"][source]["failures"] += 1
            if empty:
                self._stats["source_stats"][source]["empty"] += 1

        # Report to injected StatsCollector
        if self._stats_collector is not None:
            self._stats_collector.record_request(
                source, duration_ms, success, error_type=error_type
            )

    def get_stats(self) -> Dict[str, Any]:
        """Return current statistics."""
        return dict(self._stats)


def create_simple_router(
    callables: Dict[str, Callable],
    required_columns: Optional[List[str]] = None,
    min_rows: int = 0,
    policy: EmptyDataPolicy = EmptyDataPolicy.STRICT,
    stats_collector=None,
) -> MultiSourceRouter:
    """Create a MultiSourceRouter from a dict of {name: callable}.

    Args:
        callables: Dict mapping source names to callable functions.
        required_columns: List of column names that must be present.
        min_rows: Minimum number of rows required.
        policy: Policy for handling empty results.
        stats_collector: Optional StatsCollector for metrics.

    Returns:
        A configured MultiSourceRouter instance.
    """
    providers = [(name, func) for name, func in callables.items()]
    return MultiSourceRouter(
        providers=providers,
        required_columns=required_columns,
        min_rows=min_rows,
        policy=policy,
        stats_collector=stats_collector,
    )
