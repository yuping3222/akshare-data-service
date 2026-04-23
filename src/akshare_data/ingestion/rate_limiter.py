"""Unified rate limiter for ingestion.

Supports rate limiting at ``source + interface + domain`` granularity,
loading configuration from YAML and providing thread-safe token-bucket /
sliding-window control.

Rate keys are composed of three dimensions so that:
- Two interfaces hitting the same domain share the same bucket.
- Two sources hitting different domains get separate buckets.
- A single source+interface+domain combination can have its own override.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "config"
    / "ingestion"
    / "rate_limits.yaml"
)

_FALLBACK_CONFIG_PATH = (
    Path(__file__).parent.parent.parent.parent.parent / "config" / "rate_limits.yaml"
)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _resolve_config_path(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit)
    if _DEFAULT_CONFIG_PATH.exists():
        return _DEFAULT_CONFIG_PATH
    if _FALLBACK_CONFIG_PATH.exists():
        return _FALLBACK_CONFIG_PATH
    return _DEFAULT_CONFIG_PATH


# ---------------------------------------------------------------------------
# Rate rule
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RateRule:
    """A single rate-limit rule.

    Attributes
    ----------
    key : str
        Composite rate key, e.g. ``"lixinger|equity_daily|em_push2his"``.
    interval : float
        Minimum seconds between two consecutive calls.
    max_concurrent : int
        Maximum concurrent in-flight calls (0 = unlimited).
    description : str
        Human-readable description.
    """

    key: str
    interval: float = 0.5
    max_concurrent: int = 0
    description: str = ""


# ---------------------------------------------------------------------------
# Token bucket (per key)
# ---------------------------------------------------------------------------


class _TokenBucket:
    """Simple token bucket that enforces a minimum interval between calls."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Block until a token is available. Returns wait time in seconds."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.interval:
                wait = self.interval - elapsed
                self._last_call = now + wait
                return wait
            self._last_call = now
            return 0.0


# ---------------------------------------------------------------------------
# Concurrent semaphore (per key)
# ---------------------------------------------------------------------------


class _ConcurrentGuard:
    """Limits the number of concurrent in-flight calls for a rate key."""

    def __init__(self, max_concurrent: int) -> None:
        self._sem = threading.Semaphore(max_concurrent)

    def acquire(self, timeout: Optional[float] = None) -> bool:
        return self._sem.acquire(timeout=timeout)

    def release(self) -> None:
        self._sem.release()


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """Thread-safe rate limiter with composite key support.

    Rate keys are built from ``(source_name, interface_name, domain)``
    triplets. The limiter resolves the most specific matching rule:

    1. ``source|interface|domain``  (most specific)
    2. ``source|interface|*``
    3. ``source|*|domain``
    4. ``*|interface|domain``
    5. ``source|*|*``
    6. ``*|interface|*``
    7. ``*|*|domain``
    8. ``default``                  (least specific)
    """

    def __init__(
        self,
        rules: Optional[Dict[str, RateRule]] = None,
        config_path: Optional[str] = None,
    ) -> None:
        self._rules: Dict[str, RateRule] = rules or {}
        self._buckets: Dict[str, _TokenBucket] = {}
        self._guards: Dict[str, _ConcurrentGuard] = {}
        self._lock = threading.Lock()
        self._config_path = config_path

        if not self._rules:
            self._load_from_config()

    # -- config loading --------------------------------------------------

    def _load_from_config(self) -> None:
        path = _resolve_config_path(self._config_path)
        raw = _load_yaml(path)
        for key, cfg in raw.items():
            if isinstance(cfg, dict):
                self._rules[key] = RateRule(
                    key=key,
                    interval=float(cfg.get("interval", 0.5)),
                    max_concurrent=int(cfg.get("max_concurrent", 0)),
                    description=cfg.get("description", ""),
                )
            elif isinstance(cfg, (int, float)):
                self._rules[key] = RateRule(key=key, interval=float(cfg))

    # -- key resolution --------------------------------------------------

    @staticmethod
    def compose_key(
        source_name: str,
        interface_name: str,
        domain: str,
    ) -> str:
        return f"{source_name}|{interface_name}|{domain}"

    def _resolve_rule(self, source: str, interface: str, domain: str) -> RateRule:
        """Find the most specific matching rule."""
        candidates = [
            f"{source}|{interface}|{domain}",
            f"{source}|{interface}|*",
            f"{source}|*|{domain}",
            f"*|{interface}|{domain}",
            f"{source}|*|*",
            f"*|{interface}|*",
            f"*|*|{domain}",
            "default",
        ]
        for candidate in candidates:
            if candidate in self._rules:
                return self._rules[candidate]
        return RateRule(key="default", interval=0.5)

    # -- acquire / release -----------------------------------------------

    def acquire(
        self,
        source_name: str,
        interface_name: str,
        domain: str = "",
        timeout: Optional[float] = None,
    ) -> float:
        """Wait until the call is allowed.

        Returns the total wait time in seconds (rate-limit wait only,
        not including concurrent-guard wait).
        """
        rule = self._resolve_rule(source_name, interface_name, domain)
        rate_key = rule.key

        # Concurrent guard
        if rule.max_concurrent > 0:
            with self._lock:
                if rate_key not in self._guards:
                    self._guards[rate_key] = _ConcurrentGuard(rule.max_concurrent)
                guard = self._guards[rate_key]
            acquired = guard.acquire(timeout=timeout)
            if not acquired:
                raise TimeoutError(
                    f"Rate key {rate_key!r} concurrent limit reached "
                    f"(max_concurrent={rule.max_concurrent})"
                )

        # Token bucket
        with self._lock:
            if rate_key not in self._buckets:
                self._buckets[rate_key] = _TokenBucket(rule.interval)
            bucket = self._buckets[rate_key]

        wait = bucket.acquire()
        if wait > 0:
            time.sleep(wait)
        return wait

    def release(
        self,
        source_name: str,
        interface_name: str,
        domain: str = "",
    ) -> None:
        """Release a concurrent slot after the call completes."""
        rule = self._resolve_rule(source_name, interface_name, domain)
        rate_key = rule.key
        with self._lock:
            guard = self._guards.get(rate_key)
        if guard is not None:
            guard.release()

    # -- management ------------------------------------------------------

    def set_rule(self, rule: RateRule) -> None:
        with self._lock:
            self._rules[rule.key] = rule
            self._buckets.pop(rule.key, None)
            self._guards.pop(rule.key, None)

    def get_rule(self, key: str) -> Optional[RateRule]:
        return self._rules.get(key)

    def list_rules(self) -> Dict[str, RateRule]:
        with self._lock:
            return dict(self._rules)

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()
            self._guards.clear()

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Return current limiter state for observability."""
        with self._lock:
            stats: Dict[str, Dict[str, Any]] = {}
            for key, rule in self._rules.items():
                bucket = self._buckets.get(key)
                stats[key] = {
                    "interval": rule.interval,
                    "max_concurrent": rule.max_concurrent,
                    "description": rule.description,
                    "last_call_age": (
                        round(time.monotonic() - bucket._last_call, 3)
                        if bucket and bucket._last_call > 0
                        else None
                    ),
                }
            return stats
