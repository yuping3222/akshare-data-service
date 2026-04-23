"""
Metrics module for akshare-data-service.

Provides a unified metrics framework with Counter, Gauge, and Histogram types.
All metrics support labels for dataset, batch_id, release_version correlation.

See docs/design/100-observability-metrics.md for the full metric specification.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


@dataclass(frozen=True)
class MetricKey:
    """Immutable key combining metric name and labels."""

    name: str
    labels: frozenset[tuple[str, str]]

    @classmethod
    def create(cls, name: str, labels: dict[str, str] | None = None) -> MetricKey:
        label_tuples = frozenset((labels or {}).items())
        return cls(name=name, labels=label_tuples)


@dataclass
class CounterMetric:
    """Monotonically increasing counter."""

    name: str
    description: str
    _value: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def inc(self, value: float = 1.0) -> None:
        with self._lock:
            self._value += value

    @property
    def value(self) -> float:
        with self._lock:
            return self._value

    def reset(self) -> None:
        with self._lock:
            self._value = 0.0


@dataclass
class GaugeMetric:
    """Point-in-time value that can go up and down."""

    name: str
    description: str
    _value: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def inc(self, value: float = 1.0) -> None:
        with self._lock:
            self._value += value

    def dec(self, value: float = 1.0) -> None:
        with self._lock:
            self._value -= value

    @property
    def value(self) -> float:
        with self._lock:
            return self._value


@dataclass
class HistogramMetric:
    """Tracks value distribution with configurable buckets."""

    name: str
    description: str
    buckets: tuple[float, ...] = (
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float("inf")
    )
    _counts: dict[float, int] = field(default_factory=dict, repr=False)
    _sum: float = 0.0
    _count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        for bucket in self.buckets:
            self._counts[bucket] = 0

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._count += 1
            for bucket in self.buckets:
                if value <= bucket:
                    self._counts[bucket] += 1

    @property
    def sum(self) -> float:
        with self._lock:
            return self._sum

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    def get_bucket_counts(self) -> dict[float, int]:
        with self._lock:
            return dict(self._counts)

    @property
    def avg(self) -> float:
        with self._lock:
            return self._sum / self._count if self._count > 0 else 0.0

    def reset(self) -> None:
        with self._lock:
            self._sum = 0.0
            self._count = 0
            for bucket in self.buckets:
                self._counts[bucket] = 0


class Timer:
    """Context manager and standalone timer for histogram observation."""

    def __init__(self, histogram: HistogramMetric, labels: dict[str, str] | None = None):
        self._histogram = histogram
        self._labels = labels or {}
        self._start: float | None = None

    def __enter__(self) -> Timer:
        self._start = time.monotonic()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._start is not None:
            elapsed = time.monotonic() - self._start
            self._histogram.observe(elapsed)

    def elapsed(self) -> float:
        if self._start is None:
            return 0.0
        return time.monotonic() - self._start


@dataclass
class MetricDefinition:
    """Metadata for a registered metric."""

    name: str
    metric_type: MetricType
    description: str
    labels: list[str] = field(default_factory=list)
    buckets: tuple[float, ...] | None = None


class MetricRegistry:
    """Thread-safe registry for all metrics with label support."""

    def __init__(self) -> None:
        self._counters: dict[MetricKey, CounterMetric] = {}
        self._gauges: dict[MetricKey, GaugeMetric] = {}
        self._histograms: dict[MetricKey, HistogramMetric] = {}
        self._definitions: dict[str, MetricDefinition] = {}
        self._lock = threading.Lock()

    def register(self, definition: MetricDefinition) -> None:
        with self._lock:
            self._definitions[definition.name] = definition

    def counter(
        self, name: str, labels: dict[str, str] | None = None
    ) -> CounterMetric:
        key = MetricKey.create(name, labels)
        if key not in self._counters:
            with self._lock:
                if key not in self._counters:
                    desc = self._definitions.get(name, MetricDefinition(name, MetricType.COUNTER, "")).description
                    self._counters[key] = CounterMetric(name=name, description=desc)
        return self._counters[key]

    def gauge(
        self, name: str, labels: dict[str, str] | None = None
    ) -> GaugeMetric:
        key = MetricKey.create(name, labels)
        if key not in self._gauges:
            with self._lock:
                if key not in self._gauges:
                    desc = self._definitions.get(name, MetricDefinition(name, MetricType.GAUGE, "")).description
                    self._gauges[key] = GaugeMetric(name=name, description=desc)
        return self._gauges[key]

    def histogram(
        self, name: str, labels: dict[str, str] | None = None
    ) -> HistogramMetric:
        key = MetricKey.create(name, labels)
        if key not in self._histograms:
            with self._lock:
                if key not in self._histograms:
                    defn = self._definitions.get(name)
                    buckets = defn.buckets if defn and defn.buckets else (
                        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float("inf")
                    )
                    desc = defn.description if defn else ""
                    self._histograms[key] = HistogramMetric(
                        name=name, description=desc, buckets=buckets
                    )
        return self._histograms[key]

    def timer(
        self, name: str, labels: dict[str, str] | None = None
    ) -> Timer:
        return Timer(self.histogram(name, labels))

    def get_all(self) -> dict[str, Any]:
        with self._lock:
            result: dict[str, Any] = {}
            for key, metric in self._counters.items():
                result[key] = metric.value
            for key, metric in self._gauges.items():
                result[key] = metric.value
            for key, metric in self._histograms.items():
                result[key] = {
                    "sum": metric.sum,
                    "count": metric.count,
                    "avg": metric.avg,
                    "buckets": metric.get_bucket_counts(),
                }
            return result

    def reset_all(self) -> None:
        with self._lock:
            for metric in self._counters.values():
                metric.reset()
            for metric in self._histograms.values():
                metric.reset()


_global_registry = MetricRegistry()


def get_registry() -> MetricRegistry:
    return _global_registry


def emit_counter(name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
    _global_registry.counter(name, labels).inc(value)


def emit_gauge(name: str, value: float, labels: dict[str, str] | None = None) -> None:
    _global_registry.gauge(name, labels).set(value)


def emit_histogram(
    name: str, value: float, labels: dict[str, str] | None = None
) -> None:
    _global_registry.histogram(name, labels).observe(value)


def record_timer(name: str, labels: dict[str, str] | None = None) -> Timer:
    return _global_registry.timer(name, labels)


# ---------------------------------------------------------------------------
# Pre-registered metric definitions (aligned with 100-observability-metrics.md)
# ---------------------------------------------------------------------------

INGESTION_METRICS: list[MetricDefinition] = [
    MetricDefinition(
        name="akshare_data.ingestion.task_total",
        metric_type=MetricType.COUNTER,
        description="Total task executions",
        labels=["dataset", "status", "source_name"],
    ),
    MetricDefinition(
        name="akshare_data.ingestion.task_duration_seconds",
        metric_type=MetricType.HISTOGRAM,
        description="Task execution duration",
        labels=["dataset", "source_name"],
    ),
    MetricDefinition(
        name="akshare_data.ingestion.task_retry_total",
        metric_type=MetricType.COUNTER,
        description="Total task retries",
        labels=["dataset", "source_name", "error_code"],
    ),
    MetricDefinition(
        name="akshare_data.ingestion.records_extracted_total",
        metric_type=MetricType.COUNTER,
        description="Total records extracted",
        labels=["dataset", "batch_id", "source_name"],
    ),
    MetricDefinition(
        name="akshare_data.ingestion.circuit_breaker_trips_total",
        metric_type=MetricType.COUNTER,
        description="Circuit breaker trips",
        labels=["source_name"],
    ),
    MetricDefinition(
        name="akshare_data.ingestion.rate_limit_hits_total",
        metric_type=MetricType.COUNTER,
        description="Rate limit hits",
        labels=["source_name"],
    ),
]

QUALITY_METRICS: list[MetricDefinition] = [
    MetricDefinition(
        name="akshare_data.quality.rule_executions_total",
        metric_type=MetricType.COUNTER,
        description="Total rule executions",
        labels=["dataset", "batch_id", "rule_id", "result"],
    ),
    MetricDefinition(
        name="akshare_data.quality.gate_pass_rate",
        metric_type=MetricType.GAUGE,
        description="Gate pass rate (0-1)",
        labels=["dataset", "batch_id", "layer"],
    ),
    MetricDefinition(
        name="akshare_data.quality.failed_rules_total",
        metric_type=MetricType.COUNTER,
        description="Total failed rules",
        labels=["dataset", "batch_id", "rule_id", "severity"],
    ),
    MetricDefinition(
        name="akshare_data.quality.quarantine_records_total",
        metric_type=MetricType.COUNTER,
        description="Quarantined records",
        labels=["dataset", "batch_id"],
    ),
    MetricDefinition(
        name="akshare_data.quality.check_duration_seconds",
        metric_type=MetricType.HISTOGRAM,
        description="Quality check duration",
        labels=["dataset", "batch_id"],
    ),
    MetricDefinition(
        name="akshare_data.quality.schema_drifts_total",
        metric_type=MetricType.COUNTER,
        description="Schema drift occurrences",
        labels=["dataset", "batch_id"],
    ),
]

SERVED_METRICS: list[MetricDefinition] = [
    MetricDefinition(
        name="akshare_data.served.publish_total",
        metric_type=MetricType.COUNTER,
        description="Total publish attempts",
        labels=["dataset", "release_version", "status"],
    ),
    MetricDefinition(
        name="akshare_data.served.publish_duration_seconds",
        metric_type=MetricType.HISTOGRAM,
        description="Publish duration",
        labels=["dataset", "release_version"],
    ),
    MetricDefinition(
        name="akshare_data.served.rollback_total",
        metric_type=MetricType.COUNTER,
        description="Total rollbacks",
        labels=["dataset", "release_version"],
    ),
    MetricDefinition(
        name="akshare_data.served.active_release_version",
        metric_type=MetricType.GAUGE,
        description="Active release version (numeric)",
        labels=["dataset"],
    ),
    MetricDefinition(
        name="akshare_data.served.release_manifest_complete",
        metric_type=MetricType.GAUGE,
        description="Manifest completeness (0/1)",
        labels=["dataset", "release_version"],
    ),
]

SERVICE_METRICS: list[MetricDefinition] = [
    MetricDefinition(
        name="akshare_data.service.request_total",
        metric_type=MetricType.COUNTER,
        description="Total requests",
        labels=["endpoint", "status"],
    ),
    MetricDefinition(
        name="akshare_data.service.request_duration_seconds",
        metric_type=MetricType.HISTOGRAM,
        description="Request duration",
        labels=["endpoint", "http_method"],
    ),
    MetricDefinition(
        name="akshare_data.service.error_total",
        metric_type=MetricType.COUNTER,
        description="Total errors",
        labels=["endpoint", "error_code"],
    ),
    MetricDefinition(
        name="akshare_data.service.cache_hit_rate",
        metric_type=MetricType.GAUGE,
        description="Cache hit rate (0-1)",
        labels=["endpoint"],
    ),
    MetricDefinition(
        name="akshare_data.service.missing_data_responses_total",
        metric_type=MetricType.COUNTER,
        description="Missing data responses",
        labels=["endpoint", "dataset", "policy"],
    ),
]

FRESHNESS_METRICS: list[MetricDefinition] = [
    MetricDefinition(
        name="akshare_data.freshness.data_lag_seconds",
        metric_type=MetricType.GAUGE,
        description="Data lag in seconds",
        labels=["dataset", "layer"],
    ),
    MetricDefinition(
        name="akshare_data.freshness.missing_partitions_total",
        metric_type=MetricType.COUNTER,
        description="Missing partitions",
        labels=["dataset", "batch_id"],
    ),
    MetricDefinition(
        name="akshare_data.freshness.last_successful_ingest_timestamp",
        metric_type=MetricType.GAUGE,
        description="Last successful ingest timestamp",
        labels=["dataset", "source_name"],
    ),
    MetricDefinition(
        name="akshare_data.freshness.staleness_alerts_total",
        metric_type=MetricType.COUNTER,
        description="Staleness alerts",
        labels=["dataset"],
    ),
]

STORAGE_METRICS: list[MetricDefinition] = [
    MetricDefinition(
        name="akshare_data.storage.parquet_files_total",
        metric_type=MetricType.GAUGE,
        description="Total parquet files",
        labels=["dataset", "layer"],
    ),
    MetricDefinition(
        name="akshare_data.storage.storage_bytes",
        metric_type=MetricType.GAUGE,
        description="Storage usage in bytes",
        labels=["dataset", "layer"],
    ),
    MetricDefinition(
        name="akshare_data.storage.small_files_total",
        metric_type=MetricType.GAUGE,
        description="Small files (<1MB)",
        labels=["dataset", "layer"],
    ),
    MetricDefinition(
        name="akshare_data.storage.compaction_duration_seconds",
        metric_type=MetricType.HISTOGRAM,
        description="Compaction duration",
        labels=["dataset"],
    ),
]

ALL_METRIC_DEFINITIONS: list[MetricDefinition] = (
    INGESTION_METRICS
    + QUALITY_METRICS
    + SERVED_METRICS
    + SERVICE_METRICS
    + FRESHNESS_METRICS
    + STORAGE_METRICS
)


def register_all_metrics() -> None:
    """Register all predefined metric definitions into the global registry."""
    registry = get_registry()
    for definition in ALL_METRIC_DEFINITIONS:
        registry.register(definition)


register_all_metrics()
