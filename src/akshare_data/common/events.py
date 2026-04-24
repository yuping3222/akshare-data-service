"""
Events module for akshare-data-service.

Provides a lightweight event system for cross-module communication and
observability hooks. Events carry context (dataset, batch_id, release_version)
and can be consumed by monitoring, logging, or alerting systems.

See docs/design/100-observability-metrics.md for metric-event correlation.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
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


# ---------------------------------------------------------------------------
# Pipeline lifecycle event types
# ---------------------------------------------------------------------------


class PipelineEventType(str, Enum):
    """Standard pipeline lifecycle event types."""

    BATCH_STARTED = "batch_started"
    RAW_WRITTEN = "raw_written"
    STANDARDIZED_WRITTEN = "standardized_written"
    QUALITY_EVALUATED = "quality_evaluated"
    GATE_DECIDED = "gate_decided"
    RELEASED = "released"
    GATE_BLOCKED = "gate_blocked"
    PIPELINE_FAILED = "pipeline_failed"


@dataclass
class PipelineEvent:
    """A single pipeline lifecycle event.

    Every event MUST carry batch_id and dataset for traceability.
    """

    event_type: PipelineEventType
    batch_id: str
    dataset: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source_name: str = ""
    layer: str = ""  # "raw" / "standardized" / "quality" / "served"
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type.value,
            "batch_id": self.batch_id,
            "dataset": self.dataset,
            "timestamp": self.timestamp.isoformat(),
            "source_name": self.source_name,
            "layer": self.layer,
            "details": self.details,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class EventBus:
    """Thread-safe event bus with handler registration and emission."""

    def __init__(self, output_dir: str | Path | None = None) -> None:
        self._handlers: dict[str, list[EventHandler]] = {}
        self._global_handlers: list[EventHandler] = []
        self._lock = threading.Lock()
        self._event_log: list[Event] = []
        self._max_log_size = 10000
        # Pipeline event bus state
        self._pipeline_handlers: list[Callable[[PipelineEvent], None]] = []
        self._pipeline_event_log: list[PipelineEvent] = []
        self._output_dir: Path | None = Path(output_dir) if output_dir else None
        self._file_lock = threading.Lock()

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
                logger.error("Event handler error for %s: %s", event.event_type, e)

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

    # ------------------------------------------------------------------
    # Pipeline event bus interface
    # ------------------------------------------------------------------

    def subscribe(self, handler: Callable[[PipelineEvent], None]) -> None:
        """Register a pipeline event handler."""
        with self._lock:
            self._pipeline_handlers.append(handler)

    def publish(self, event: PipelineEvent) -> None:
        """Publish a pipeline event to all registered handlers."""
        handlers_to_call: list[Callable[[PipelineEvent], None]] = []
        with self._lock:
            handlers_to_call.extend(self._pipeline_handlers)
            self._pipeline_event_log.append(event)

        for handler in handlers_to_call:
            try:
                handler(event)
            except Exception as e:
                logger.error(
                    "Pipeline event handler error for %s: %s", event.event_type, e
                )

        self._write_to_file(event)

    def _write_to_file(self, event: PipelineEvent) -> None:
        """Append event to a date-partitioned JSONL file (thread-safe)."""
        if self._output_dir is None:
            return
        try:
            date_str = event.timestamp.strftime("%Y-%m-%d")
            filepath = self._output_dir / f"{date_str}.jsonl"
            with self._file_lock:
                self._output_dir.mkdir(parents=True, exist_ok=True)
                with filepath.open("a", encoding="utf-8") as f:
                    f.write(event.to_json() + "\n")
        except Exception as e:
            logger.error("Failed to write pipeline event to file: %s", e)

    def publish_batch_started(
        self, batch_id: str, dataset: str, source_name: str = "", **details: Any
    ) -> None:
        """Convenience: publish BATCH_STARTED event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.BATCH_STARTED,
                batch_id=batch_id,
                dataset=dataset,
                source_name=source_name,
                details=dict(details),
            )
        )

    def publish_raw_written(
        self, batch_id: str, dataset: str, raw_path: str = "", **details: Any
    ) -> None:
        """Convenience: publish RAW_WRITTEN event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.RAW_WRITTEN,
                batch_id=batch_id,
                dataset=dataset,
                layer="raw",
                details={"raw_path": raw_path, **details},
            )
        )

    def publish_standardized_written(
        self, batch_id: str, dataset: str, **details: Any
    ) -> None:
        """Convenience: publish STANDARDIZED_WRITTEN event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.STANDARDIZED_WRITTEN,
                batch_id=batch_id,
                dataset=dataset,
                layer="standardized",
                details=dict(details),
            )
        )

    def publish_quality_evaluated(
        self,
        batch_id: str,
        dataset: str,
        passed_count: int = 0,
        failed_count: int = 0,
        **details: Any,
    ) -> None:
        """Convenience: publish QUALITY_EVALUATED event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.QUALITY_EVALUATED,
                batch_id=batch_id,
                dataset=dataset,
                layer="quality",
                details={
                    "passed_count": passed_count,
                    "failed_count": failed_count,
                    **details,
                },
            )
        )

    def publish_gate_decided(
        self,
        batch_id: str,
        dataset: str,
        decision: str,
        blocking_rules: list | None = None,
        **details: Any,
    ) -> None:
        """Convenience: publish GATE_DECIDED event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.GATE_DECIDED,
                batch_id=batch_id,
                dataset=dataset,
                details={
                    "decision": decision,
                    "blocking_rules": blocking_rules or [],
                    **details,
                },
            )
        )

    def publish_released(
        self, batch_id: str, dataset: str, release_version: str = "", **details: Any
    ) -> None:
        """Convenience: publish RELEASED event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.RELEASED,
                batch_id=batch_id,
                dataset=dataset,
                layer="served",
                details={"release_version": release_version, **details},
            )
        )

    def publish_gate_blocked(
        self,
        batch_id: str,
        dataset: str,
        blocking_rules: list | None = None,
        **details: Any,
    ) -> None:
        """Convenience: publish GATE_BLOCKED event."""
        self.publish(
            PipelineEvent(
                event_type=PipelineEventType.GATE_BLOCKED,
                batch_id=batch_id,
                dataset=dataset,
                details={"blocking_rules": blocking_rules or [], **details},
            )
        )

    def get_events_for_batch(self, batch_id: str) -> list[PipelineEvent]:
        """Return all in-memory pipeline events for a given batch_id."""
        with self._lock:
            return [e for e in self._pipeline_event_log if e.batch_id == batch_id]

    def format_batch_summary(self, batch_id: str) -> str:
        """Format a human-readable markdown summary of pipeline events for a batch."""
        events = self.get_events_for_batch(batch_id)
        if not events:
            return f"No events found for batch_id={batch_id}"

        lines = [
            f"## Pipeline Trace: {batch_id}",
            "",
            "| 步骤 | 时间 | 数据集 | 详情 |",
            "|-----|------|--------|------|",
        ]
        for ev in sorted(events, key=lambda e: e.timestamp):
            lines.append(
                f"| {ev.event_type.value} | {ev.timestamp.strftime('%H:%M:%S')} "
                f"| {ev.dataset} | {json.dumps(ev.details, ensure_ascii=False)} |"
            )
        return "\n".join(lines)


_default_bus: EventBus | None = None
_bus_lock = threading.Lock()


def get_event_bus(output_dir: str | None = None) -> EventBus:
    """Get the global EventBus singleton (lazy-initialized, thread-safe)."""
    global _default_bus
    if _default_bus is None:
        with _bus_lock:
            if _default_bus is None:
                _default_bus = EventBus(output_dir=output_dir or "reports/events")
    return _default_bus


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
    get_event_bus().emit(event)
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
