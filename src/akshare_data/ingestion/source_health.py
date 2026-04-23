"""Source health tracking with circuit breaker.

Records per-source and per-domain health state including:
- Failure reason and error type
- Recovery time (when a previously-failing source came back)
- Degradation reason (why a source was downgraded / bypassed)
- Circuit breaker state (closed / open / half-open)

Health records are thread-safe and serialisable for observability and
downstream alerting.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Circuit breaker states
# ---------------------------------------------------------------------------


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ---------------------------------------------------------------------------
# Health record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HealthRecord:
    """Snapshot of health for a single source or domain.

    Attributes
    ----------
    target : str
        Source name (e.g. ``"lixinger"``) or domain key
        (e.g. ``"em_push2his"``).
    kind : str
        ``"source"`` or ``"domain"``.
    circuit_state : CircuitState
        Current circuit breaker state.
    is_healthy : bool
        True when the circuit is closed or half-open and recent calls succeed.
    consecutive_failures : int
        Number of consecutive failures since last success.
    last_success_at : datetime | None
        Timestamp of the most recent successful call.
    last_failure_at : datetime | None
        Timestamp of the most recent failure.
    last_failure_reason : str | None
        Human-readable error description from the last failure.
    last_failure_type : str | None
        Exception class name or error category from the last failure.
    recovery_at : datetime | None
        When the source recovered from a failure (transitioned to healthy).
    degraded_at : datetime | None
        When the source was first marked degraded.
    degradation_reason : str | None
        Why the source was downgraded (e.g. ``"circuit_open"``,
        ``"rate_limited"``, ``"schema_mismatch"``).
    total_calls : int
        Lifetime call count.
    total_failures : int
        Lifetime failure count.
    avg_latency_ms : float | None
        Rolling average latency of recent calls.
    updated_at : datetime
        Last update timestamp.
    """

    target: str
    kind: str = "source"
    circuit_state: CircuitState = CircuitState.CLOSED
    is_healthy: bool = True
    consecutive_failures: int = 0
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    last_failure_reason: Optional[str] = None
    last_failure_type: Optional[str] = None
    recovery_at: Optional[datetime] = None
    degraded_at: Optional[datetime] = None
    degradation_reason: Optional[str] = None
    total_calls: int = 0
    total_failures: int = 0
    avg_latency_ms: Optional[float] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # -- helpers ---------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target": self.target,
            "kind": self.kind,
            "circuit_state": self.circuit_state.value,
            "is_healthy": self.is_healthy,
            "consecutive_failures": self.consecutive_failures,
            "last_success_at": _dt_iso(self.last_success_at),
            "last_failure_at": _dt_iso(self.last_failure_at),
            "last_failure_reason": self.last_failure_reason,
            "last_failure_type": self.last_failure_type,
            "recovery_at": _dt_iso(self.recovery_at),
            "degraded_at": _dt_iso(self.degraded_at),
            "degradation_reason": self.degradation_reason,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "avg_latency_ms": self.avg_latency_ms,
            "updated_at": _dt_iso(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HealthRecord":
        return cls(
            target=data["target"],
            kind=data.get("kind", "source"),
            circuit_state=CircuitState(data.get("circuit_state", "closed")),
            is_healthy=data.get("is_healthy", True),
            consecutive_failures=data.get("consecutive_failures", 0),
            last_success_at=_parse_dt(data.get("last_success_at")),
            last_failure_at=_parse_dt(data.get("last_failure_at")),
            last_failure_reason=data.get("last_failure_reason"),
            last_failure_type=data.get("last_failure_type"),
            recovery_at=_parse_dt(data.get("recovery_at")),
            degraded_at=_parse_dt(data.get("degraded_at")),
            degradation_reason=data.get("degradation_reason"),
            total_calls=data.get("total_calls", 0),
            total_failures=data.get("total_failures", 0),
            avg_latency_ms=data.get("avg_latency_ms"),
            updated_at=_parse_dt(data.get("updated_at")) or datetime.now(timezone.utc),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, raw: str) -> "HealthRecord":
        return cls.from_dict(json.loads(raw))


# ---------------------------------------------------------------------------
# Circuit breaker config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout_seconds: float = 300.0
    half_open_max_calls: int = 1


_DEFAULT_CB_CONFIG = CircuitBreakerConfig()


# ---------------------------------------------------------------------------
# Internal mutable state
# ---------------------------------------------------------------------------


@dataclass
class _HealthState:
    """Mutable internal state for a single target."""

    circuit_state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    last_failure_reason: Optional[str] = None
    last_failure_type: Optional[str] = None
    recovery_at: Optional[datetime] = None
    degraded_at: Optional[datetime] = None
    degradation_reason: Optional[str] = None
    total_calls: int = 0
    total_failures: int = 0
    _latencies: List[float] = field(default_factory=list)
    _last_open_at: Optional[float] = None  # monotonic


# ---------------------------------------------------------------------------
# SourceHealthTracker
# ---------------------------------------------------------------------------


class SourceHealthTracker:
    """Thread-safe tracker for source and domain health.

    Maintains per-target ``HealthRecord`` snapshots and a circuit breaker
    that transitions through ``closed -> open -> half_open -> closed``.

    Usage::

        tracker = SourceHealthTracker()
        tracker.record_success("lixinger", latency_ms=120.0)
        tracker.record_failure("lixinger", reason="timeout", error_type="TimeoutError")
        record = tracker.get("lixinger")
        if not record.is_healthy:
            ...  # skip or fallback
    """

    def __init__(
        self,
        config: Optional[CircuitBreakerConfig] = None,
        state_path: Optional[str] = None,
    ) -> None:
        self._config = config or _DEFAULT_CB_CONFIG
        self._states: Dict[str, _HealthState] = {}
        self._kinds: Dict[str, str] = {}  # target -> "source" | "domain"
        self._lock = threading.Lock()
        self._state_path = state_path
        self._load_persisted()

    # -- persistence -----------------------------------------------------

    def _load_persisted(self) -> None:
        if not self._state_path:
            return
        path = Path(self._state_path)
        if not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            for target, data in raw.items():
                record = HealthRecord.from_dict(data)
                state = _HealthState(
                    circuit_state=record.circuit_state,
                    consecutive_failures=record.consecutive_failures,
                    last_success_at=record.last_success_at,
                    last_failure_at=record.last_failure_at,
                    last_failure_reason=record.last_failure_reason,
                    last_failure_type=record.last_failure_type,
                    recovery_at=record.recovery_at,
                    degraded_at=record.degraded_at,
                    degradation_reason=record.degradation_reason,
                    total_calls=record.total_calls,
                    total_failures=record.total_failures,
                )
                self._states[target] = state
                self._kinds[target] = record.kind
        except Exception:
            pass

    def persist(self, path: Optional[str] = None) -> None:
        target_path = path or self._state_path
        if not target_path:
            return
        with self._lock:
            snapshot = {
                t: self._snapshot(t, s).to_dict() for t, s in self._states.items()
            }
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, ensure_ascii=False, indent=2)

    # -- record success / failure ----------------------------------------

    def record_success(
        self,
        target: str,
        kind: str = "source",
        latency_ms: Optional[float] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.get(target)
            if state is None:
                state = _HealthState()
                self._states[target] = state
            self._kinds[target] = kind

            state.total_calls += 1
            state.last_success_at = now
            state.consecutive_failures = 0

            if latency_ms is not None:
                state._latencies.append(latency_ms)
                if len(state._latencies) > 100:
                    state._latencies = state._latencies[-100:]

            # Recovery transition
            if state.circuit_state in (CircuitState.OPEN, CircuitState.HALF_OPEN):
                state.recovery_at = now
                state.degraded_at = None
                state.degradation_reason = None

            state.circuit_state = CircuitState.CLOSED
            state.updated_at = now

    def record_failure(
        self,
        target: str,
        kind: str = "source",
        reason: Optional[str] = None,
        error_type: Optional[str] = None,
        latency_ms: Optional[float] = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.get(target)
            if state is None:
                state = _HealthState()
                self._states[target] = state
            self._kinds[target] = kind

            state.total_calls += 1
            state.total_failures += 1
            state.consecutive_failures += 1
            state.last_failure_at = now
            state.last_failure_reason = reason
            state.last_failure_type = error_type

            if latency_ms is not None:
                state._latencies.append(latency_ms)
                if len(state._latencies) > 100:
                    state._latencies = state._latencies[-100:]

            # Circuit breaker transition
            if (
                state.circuit_state == CircuitState.CLOSED
                and state.consecutive_failures >= self._config.failure_threshold
            ):
                state.circuit_state = CircuitState.OPEN
                state._last_open_at = time.monotonic()
                state.degraded_at = now
                state.degradation_reason = "circuit_open"

            elif state.circuit_state == CircuitState.HALF_OPEN:
                state.circuit_state = CircuitState.OPEN
                state._last_open_at = time.monotonic()
                state.degraded_at = now
                state.degradation_reason = "circuit_open_half_open_retry_failed"

            state.updated_at = now

    def record_degradation(
        self,
        target: str,
        kind: str = "source",
        reason: str = "manual_degradation",
    ) -> None:
        """Explicitly mark a target as degraded without a failure event."""
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.get(target)
            if state is None:
                state = _HealthState()
                self._states[target] = state
            self._kinds[target] = kind

            state.degraded_at = now
            state.degradation_reason = reason
            state.circuit_state = CircuitState.OPEN
            state._last_open_at = time.monotonic()
            state.updated_at = now

    # -- query -----------------------------------------------------------

    def is_available(self, target: str) -> bool:
        """Return True if the target's circuit allows calls."""
        with self._lock:
            state = self._states.get(target)
            if state is None:
                return True
            if state.circuit_state == CircuitState.CLOSED:
                return True
            if state.circuit_state == CircuitState.OPEN:
                if state._last_open_at is not None and (
                    time.monotonic() - state._last_open_at
                    >= self._config.recovery_timeout_seconds
                ):
                    state.circuit_state = CircuitState.HALF_OPEN
                    return True
                return False
            # HALF_OPEN: allow limited calls
            return True

    def get(self, target: str) -> Optional[HealthRecord]:
        with self._lock:
            state = self._states.get(target)
            if state is None:
                return None
            return self._snapshot(target, state)

    def get_all(self) -> Dict[str, HealthRecord]:
        with self._lock:
            return {t: self._snapshot(t, s) for t, s in self._states.items()}

    def get_unhealthy(self) -> Dict[str, HealthRecord]:
        with self._lock:
            return {
                t: self._snapshot(t, s)
                for t, s in self._states.items()
                if not self._snapshot(t, s).is_healthy
            }

    def get_by_kind(self, kind: str) -> Dict[str, HealthRecord]:
        with self._lock:
            return {
                t: self._snapshot(t, s)
                for t, s in self._states.items()
                if self._kinds.get(t) == kind
            }

    def get_sources(self) -> Dict[str, HealthRecord]:
        return self.get_by_kind("source")

    def get_domains(self) -> Dict[str, HealthRecord]:
        return self.get_by_kind("domain")

    # -- reset -----------------------------------------------------------

    def reset(self, target: Optional[str] = None) -> None:
        with self._lock:
            if target:
                self._states.pop(target, None)
                self._kinds.pop(target, None)
            else:
                self._states.clear()
                self._kinds.clear()

    # -- internal --------------------------------------------------------

    def _snapshot(self, target: str, state: _HealthState) -> HealthRecord:
        avg_lat: Optional[float] = None
        if state._latencies:
            avg_lat = round(sum(state._latencies) / len(state._latencies), 2)

        is_healthy = (
            state.circuit_state == CircuitState.CLOSED
            or state.circuit_state == CircuitState.HALF_OPEN
        )

        return HealthRecord(
            target=target,
            kind=self._kinds.get(target, "source"),
            circuit_state=state.circuit_state,
            is_healthy=is_healthy,
            consecutive_failures=state.consecutive_failures,
            last_success_at=state.last_success_at,
            last_failure_at=state.last_failure_at,
            last_failure_reason=state.last_failure_reason,
            last_failure_type=state.last_failure_type,
            recovery_at=state.recovery_at,
            degraded_at=state.degraded_at,
            degradation_reason=state.degradation_reason,
            total_calls=state.total_calls,
            total_failures=state.total_failures,
            avg_latency_ms=avg_lat,
            updated_at=state.updated_at,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dt_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.isoformat()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
