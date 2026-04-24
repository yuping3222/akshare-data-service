"""Unit tests for EventBus pipeline lifecycle events."""

from __future__ import annotations

import pytest

from akshare_data.common.events import (
    EventBus,
    PipelineEvent,
    PipelineEventType,
    get_event_bus,
)


def test_event_bus_publish_and_retrieve():
    bus = EventBus()
    bus.publish_batch_started("test-batch-001", "market_quote_daily")
    bus.publish_raw_written("test-batch-001", "market_quote_daily", raw_path="/tmp/raw")
    events = bus.get_events_for_batch("test-batch-001")
    assert len(events) == 2
    assert events[0].event_type == PipelineEventType.BATCH_STARTED


def test_event_bus_format_summary():
    bus = EventBus()
    bus.publish_batch_started("batch-002", "stock_daily")
    summary = bus.format_batch_summary("batch-002")
    assert "batch-002" in summary
    assert "batch_started" in summary


def test_event_bus_file_output(tmp_path):
    bus = EventBus(output_dir=str(tmp_path))
    bus.publish_released(
        "batch-003", "market_quote_daily", release_version="v20260424-001"
    )
    jsonl_files = list(tmp_path.glob("*.jsonl"))
    assert len(jsonl_files) == 1
    content = jsonl_files[0].read_text()
    assert "batch-003" in content


def test_get_event_bus_singleton():
    bus1 = get_event_bus()
    bus2 = get_event_bus()
    assert bus1 is bus2


def test_event_bus_empty_batch_returns_message():
    bus = EventBus()
    summary = bus.format_batch_summary("nonexistent-batch")
    assert "No events found" in summary


def test_event_bus_subscribe_handler():
    received: list[PipelineEvent] = []
    bus = EventBus()
    bus.subscribe(lambda e: received.append(e))
    bus.publish_batch_started("batch-sub-001", "test_dataset")
    assert len(received) == 1
    assert received[0].event_type == PipelineEventType.BATCH_STARTED


def test_pipeline_event_isolation_across_batches():
    bus = EventBus()
    bus.publish_batch_started("batch-A", "ds_a")
    bus.publish_batch_started("batch-B", "ds_b")
    bus.publish_raw_written("batch-A", "ds_a", raw_path="/tmp/a")
    assert len(bus.get_events_for_batch("batch-A")) == 2
    assert len(bus.get_events_for_batch("batch-B")) == 1


def test_pipeline_event_to_dict():
    bus = EventBus()
    bus.publish_quality_evaluated(
        "batch-q", "market_daily", passed_count=99, failed_count=1
    )
    events = bus.get_events_for_batch("batch-q")
    assert len(events) == 1
    d = events[0].to_dict()
    assert d["event_type"] == "quality_evaluated"
    assert d["details"]["passed_count"] == 99
    assert d["details"]["failed_count"] == 1


def test_file_output_jsonl_format(tmp_path):
    import json

    bus = EventBus(output_dir=str(tmp_path))
    bus.publish_gate_decided("batch-gd", "stock_daily", decision="pass")
    jsonl_files = list(tmp_path.glob("*.jsonl"))
    assert len(jsonl_files) == 1
    lines = jsonl_files[0].read_text().strip().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["event_type"] == "gate_decided"
    assert parsed["batch_id"] == "batch-gd"


@pytest.mark.parametrize("marker", ["unit"])
def test_markers(marker):
    """Ensure test is discoverable under the expected marker set."""
    assert marker == "unit"
