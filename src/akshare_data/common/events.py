"""
Events module for akshare-data-service.

Provides a lightweight event system for cross-module communication and
observability hooks. Events carry context (dataset, batch_id, release_version)
and can be consumed by monitoring, logging, or alerting systems.

See docs/design/100-observability-metrics.md for metric-event correlation.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class EventSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EventDomain(Enum):
    INGESTION = "ingestion"
    QUALITY = "quality"
    SERVED = "served"
    SERVICE = "service"
    STORAGE = "storage"
    SYSTEM = "system"


@dataclass
class EventContext:
    """Correlation context shared across events and metrics."""

    dataset: str | None = None
    batch_id: str | None = None
    release_version: str | None = None
    source_name: str | None = None
    domain: str | None = None
    layer: str | None = None

    def to_labels(self) -> dict[str, str]:
        labels: dict[str, str] = {}
        if self.dataset:
            labels["dataset"] = self.dataset
        if self.batch_id:
            labels["batch_id"] = self.batch_id
        if self.release_version:
            labels["release_version"] = self.release_version
        if self.source_name:
            labels["source_name"] = self.source_name
        if self.domain:
            labels["domain"] = self.domain
        if self.layer:
            labels["layer"] = self.layer
        return labels


@dataclass
class Event:
    """Immutable event record."""

    event_id: str
    event_type: str
    domain: EventDomain
    severity: EventSeverity
    timestamp: float
    message: str
    context: EventContext
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        event_type: str,
        domain: EventDomain,
        severity: EventSeverity,
        message: str,
        context: EventContext | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            domain=domain,
            severity=severity,
            timestamp=time.time(),
            message=message,
            context=context or EventContext(),
            payload=payload or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "domain": self.domain.value,
            "severity": self.severity.value,
            "timestamp": self.timestamp,
            "message": self.message,
            "context": {
                "dataset": self.context.dataset,
                "batch_id": self.context.batch_id,
                "release_version": self.context.release_version,
                "source_name": self.context.source_name,
                "domain": self.context.domain,
                "layer": self.context.layer,
            },
            "payload": self.payload,
        }


EventHandler = Callable[[Event], None]


class EventBus:
    """Thread-safe event bus with handler registration and emission."""

    def __init__(self) -> None:
        self._handlers: dict[str, list[EventHandler]] = {}
        self._global_handlers: list[EventHandler] = []
        self._lock = threading.Lock()
        self._event_log: list[Event] = []
        self._max_log_size = 10000

    def on(self, event_type: str, handler: EventHandler) -> None:
        with self._lock:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(handler)

    def on_any(self, handler: EventHandler) -> None:
        with self._lock:
            self._global_handlers.append(handler)

    def emit(self, event: Event) -> None:
        handlers_to_call: list[EventHandler] = []
        with self._lock:
            handlers_to_call.extend(self._global_handlers)
            handlers_to_call.extend(self._handlers.get(event.event_type, []))
            self._event_log.append(event)
            if len(self._event_log) > self._max_log_size:
                self._event_log = self._event_log[-self._max_log_size // 2 :]

        for handler in handlers_to_call:
            try:
                handler(event)
            except Exception as e:
                logger.error(
                    "Event handler error for %s: %s", event.event_type, e
                )

    def get_events(
        self,
        event_type: str | None = None,
        domain: EventDomain | None = None,
        severity: EventSeverity | None = None,
        dataset: str | None = None,
        limit: int = 100,
    ) -> list[Event]:
        with self._lock:
            result = list(self._event_log)

        if event_type:
            result = [e for e in result if e.event_type == event_type]
        if domain:
            result = [e for e in result if e.domain == domain]
        if severity:
            result = [e for e in result if e.severity == severity]
        if dataset:
            result = [e for e in result if e.context.dataset == dataset]

        return result[-limit:]

    def clear(self) -> None:
        with self._lock:
            self._event_log.clear()


_global_bus = EventBus()


def get_event_bus() -> EventBus:
    return _global_bus


def emit_event(
    event_type: str,
    domain: EventDomain,
    severity: EventSeverity,
    message: str,
    context: EventContext | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    event = Event.create(
        event_type=event_type,
        domain=domain,
        severity=severity,
        message=message,
        context=context,
        payload=payload,
    )
    _global_bus.emit(event)
    return event


def emit_ingestion_event(
    event_type: str,
    severity: EventSeverity,
    message: str,
    dataset: str | None = None,
    batch_id: str | None = None,
    source_name: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    return emit_event(
        event_type=event_type,
        domain=EventDomain.INGESTION,
        severity=severity,
        message=message,
        context=EventContext(
            dataset=dataset,
            batch_id=batch_id,
            source_name=source_name,
            domain="ingestion",
        ),
        payload=payload,
    )


def emit_quality_event(
    event_type: str,
    severity: EventSeverity,
    message: str,
    dataset: str | None = None,
    batch_id: str | None = None,
    rule_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    return emit_event(
        event_type=event_type,
        domain=EventDomain.QUALITY,
        severity=severity,
        message=message,
        context=EventContext(
            dataset=dataset,
            batch_id=batch_id,
            domain="quality",
        ),
        payload={"rule_id": rule_id, **(payload or {})},
    )


def emit_served_event(
    event_type: str,
    severity: EventSeverity,
    message: str,
    dataset: str | None = None,
    batch_id: str | None = None,
    release_version: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    return emit_event(
        event_type=event_type,
        domain=EventDomain.SERVED,
        severity=severity,
        message=message,
        context=EventContext(
            dataset=dataset,
            batch_id=batch_id,
            release_version=release_version,
            domain="served",
        ),
        payload=payload,
    )


def emit_service_event(
    event_type: str,
    severity: EventSeverity,
    message: str,
    endpoint: str | None = None,
    dataset: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    return emit_event(
        event_type=event_type,
        domain=EventDomain.SERVICE,
        severity=severity,
        message=message,
        context=EventContext(
            dataset=dataset,
            domain="service",
        ),
        payload={"endpoint": endpoint, **(payload or {})},
    )


def emit_storage_event(
    event_type: str,
    severity: EventSeverity,
    message: str,
    dataset: str | None = None,
    layer: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    return emit_event(
        event_type=event_type,
        domain=EventDomain.STORAGE,
        severity=severity,
        message=message,
        context=EventContext(
            dataset=dataset,
            layer=layer,
            domain="storage",
        ),
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Predefined event types
# ---------------------------------------------------------------------------

class IngestionEvents:
    TASK_STARTED = "ingestion.task_started"
    TASK_COMPLETED = "ingestion.task_completed"
    TASK_FAILED = "ingestion.task_failed"
    TASK_RETRYING = "ingestion.task_retrying"
    CIRCUIT_BREAKER_TRIPPED = "ingestion.circuit_breaker_tripped"
    CIRCUIT_BREAKER_RESET = "ingestion.circuit_breaker_reset"
    RATE_LIMIT_HIT = "ingestion.rate_limit_hit"
    EXTRACT_COMPLETE = "ingestion.extract_complete"


class QualityEvents:
    CHECK_STARTED = "quality.check_started"
    CHECK_COMPLETED = "quality.check_completed"
    GATE_PASSED = "quality.gate_passed"
    GATE_FAILED = "quality.gate_failed"
    RULE_FAILED = "quality.rule_failed"
    QUARANTINE_WRITE = "quality.quarantine_write"
    SCHEMA_DRIFT_DETECTED = "quality.schema_drift_detected"


class ServedEvents:
    PUBLISH_STARTED = "served.publish_started"
    PUBLISH_COMPLETED = "served.publish_completed"
    PUBLISH_FAILED = "served.publish_failed"
    ROLLBACK_STARTED = "served.rollback_started"
    ROLLBACK_COMPLETED = "served.rollback_completed"
    VERSION_DEPRECATED = "served.version_deprecated"


class ServiceEvents:
    REQUEST_ERROR = "service.request_error"
    MISSING_DATA = "service.missing_data"
    CACHE_MISS_SPIKE = "service.cache_miss_spike"
    SLOW_QUERY = "service.slow_query"


class StorageEvents:
    COMPACTION_STARTED = "storage.compaction_started"
    COMPACTION_COMPLETED = "storage.compaction_completed"
    SMALL_FILE_THRESHOLD = "storage.small_file_threshold"
    STORAGE_QUOTA_WARNING = "storage.storage_quota_warning"
