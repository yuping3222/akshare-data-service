"""Tests for the Served (L2) layer: publisher, reader, rollback, manifest."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd
import pytest

from akshare_data.served.manifest import (
    ReleaseManifest,
    ReleaseStatus,
    SourceBatch,
)
from akshare_data.served.publisher import PublishError, Publisher
from akshare_data.served.reader import ReadError, Reader
from akshare_data.served.rollback import RollbackError, RollbackManager


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FakeGateDecision:
    """A minimal GateDecision implementation for testing."""

    _dataset: str
    _batch_id: str
    _passed: bool = True
    _failed_rules: List[str] = None
    _warnings: List[str] = None

    @property
    def dataset(self) -> str:
        return self._dataset

    @property
    def batch_id(self) -> str:
        return self._batch_id

    @property
    def gate_passed(self) -> bool:
        return self._passed

    @property
    def evaluated_at(self) -> datetime:
        return datetime.now(timezone.utc)

    @property
    def failed_rules(self) -> List[str]:
        return self._failed_rules or []

    @property
    def warnings(self) -> List[str]:
        return self._warnings or []


def _make_standardized_df(n: int = 5) -> pd.DataFrame:
    """Create a minimal standardized DataFrame."""
    return pd.DataFrame(
        {
            "security_id": ["sh600000"] * n,
            "trade_date": pd.date_range("2024-01-02", periods=n, freq="B"),
            "adjust_type": ["none"] * n,
            "open_price": [10.0 + i for i in range(n)],
            "high_price": [11.0 + i for i in range(n)],
            "low_price": [9.0 + i for i in range(n)],
            "close_price": [10.5 + i for i in range(n)],
            "volume": [100_000.0] * n,
            "turnover_amount": [1_000_000.0] * n,
            "source_name": ["lixinger"] * n,
            "interface_name": ["test_iface"] * n,
            "batch_id": ["batch_001"] * n,
            "ingest_time": [datetime.now(timezone.utc)] * n,
            "normalize_version": ["v1"] * n,
            "schema_version": ["v1"] * n,
            "quality_status": ["passed"] * n,
        }
    )


# ---------------------------------------------------------------------------
# Manifest tests
# ---------------------------------------------------------------------------


class TestSourceBatch:
    def test_to_dict_and_back(self):
        sb = SourceBatch(
            batch_id="b1",
            source_name="lixinger",
            interface_name="iface1",
            record_count=100,
            partition_values=["2024-01-02"],
        )
        d = sb.to_dict()
        restored = SourceBatch.from_dict(d)
        assert restored == sb


class TestReleaseManifest:
    def test_create_with_gate_decision(self):
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="b1",
            _passed=True,
        )
        manifest = ReleaseManifest.create(
            dataset="market_quote_daily",
            release_version="rv_20240102_abc123",
            source_batches=[
                SourceBatch("b1", "lixinger", "iface1", 100, ["2024-01-02"])
            ],
            partitions_covered=["2024-01-02"],
            total_record_count=100,
            gate_decision=gate,
        )
        assert manifest.gate_passed is True
        assert manifest.dataset == "market_quote_daily"
        assert manifest.release_version == "rv_20240102_abc123"
        assert manifest.total_record_count == 100

    def test_create_without_gate_decision(self):
        manifest = ReleaseManifest.create(
            dataset="market_quote_daily",
            release_version="rv_20240102_abc123",
            source_batches=[],
            partitions_covered=[],
            total_record_count=0,
        )
        assert manifest.gate_passed is True

    def test_serialization_roundtrip(self):
        manifest = ReleaseManifest.create(
            dataset="market_quote_daily",
            release_version="rv_20240102_abc123",
            source_batches=[
                SourceBatch("b1", "lixinger", "iface1", 100, ["2024-01-02"])
            ],
            partitions_covered=["2024-01-02"],
            total_record_count=100,
        )
        text = manifest.to_json()
        restored = ReleaseManifest.from_json(text)
        assert restored.release_version == manifest.release_version
        assert restored.total_record_count == manifest.total_record_count

    def test_save_and_load(self, tmp_path: Path):
        manifest = ReleaseManifest.create(
            dataset="market_quote_daily",
            release_version="rv_20240102_abc123",
            source_batches=[],
            partitions_covered=[],
            total_record_count=0,
        )
        path = tmp_path / "manifest.json"
        manifest.save(path)
        loaded = ReleaseManifest.load(path)
        assert loaded.release_version == manifest.release_version

    def test_mark_rolled_back(self):
        manifest = ReleaseManifest.create(
            dataset="market_quote_daily",
            release_version="rv_20240102_abc123",
            source_batches=[],
            partitions_covered=[],
            total_record_count=0,
        )
        rolled = manifest.mark_rolled_back(reason="bad data")
        assert rolled.status == ReleaseStatus.ROLLED_BACK.value
        assert (
            rolled.rollback_reason == "bad" in rolled.rollback_reason
            or rolled.rollback_reason == "bad data"
        )
        assert rolled.rolled_back_at is not None


# ---------------------------------------------------------------------------
# Publisher tests
# ---------------------------------------------------------------------------


class TestPublisher:
    def test_publish_creates_release(self, tmp_path: Path):
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="batch_001",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        manifest = publisher.publish(
            dataset="market_quote_daily",
            df=df,
            gate_decision=gate,
            partition_col="trade_date",
        )
        assert manifest.dataset == "market_quote_daily"
        assert manifest.total_record_count == len(df)
        assert manifest.status == ReleaseStatus.PUBLISHED.value

        release_dir = (
            tmp_path / "market_quote_daily" / "releases" / manifest.release_version
        )
        assert (release_dir / "manifest.json").exists()
        assert (release_dir / "data").exists()

        latest = tmp_path / "market_quote_daily" / "releases" / "latest"
        assert latest.exists()
        assert latest.read_text().strip() == manifest.release_version

    def test_publish_rejects_failed_gate(self, tmp_path: Path):
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="batch_001",
            _passed=False,
            _failed_rules=["mq_daily_pk_unique"],
        )
        publisher = Publisher(served_dir=tmp_path)
        with pytest.raises(PublishError, match="did not pass"):
            publisher.publish(
                dataset="market_quote_daily",
                df=df,
                gate_decision=gate,
            )

    def test_publish_rejects_dataset_mismatch(self, tmp_path: Path):
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="financial_indicator",
            _batch_id="batch_001",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        with pytest.raises(PublishError, match="does not match"):
            publisher.publish(
                dataset="market_quote_daily",
                df=df,
                gate_decision=gate,
            )

    def test_publish_rejects_empty_df(self, tmp_path: Path):
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="batch_001",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        with pytest.raises(PublishError, match="empty"):
            publisher.publish(
                dataset="market_quote_daily",
                df=pd.DataFrame(),
                gate_decision=gate,
            )

    def test_publish_without_partition(self, tmp_path: Path):
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="batch_001",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        manifest = publisher.publish(
            dataset="market_quote_daily",
            df=df,
            gate_decision=gate,
        )
        release_dir = (
            tmp_path / "market_quote_daily" / "releases" / manifest.release_version
        )
        data_files = list((release_dir / "data").glob("*.parquet"))
        assert len(data_files) == 1


# ---------------------------------------------------------------------------
# Reader tests
# ---------------------------------------------------------------------------


class TestReader:
    def _publish_first(self, tmp_path: Path, version_hint: str = "1") -> str:
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id=f"batch_{version_hint}",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        manifest = publisher.publish(
            dataset="market_quote_daily",
            df=df,
            gate_decision=gate,
        )
        return manifest.release_version

    def test_read_latest(self, tmp_path: Path):
        version = self._publish_first(tmp_path)
        reader = Reader(served_dir=tmp_path)
        df, manifest = reader.read("market_quote_daily")
        assert len(df) == 5
        assert manifest.release_version == version

    def test_read_specific_version(self, tmp_path: Path):
        version = self._publish_first(tmp_path)
        reader = Reader(served_dir=tmp_path)
        df, manifest = reader.read("market_quote_daily", version=version)
        assert manifest.release_version == version

    def test_list_versions(self, tmp_path: Path):
        v1 = self._publish_first(tmp_path, "1")
        v2 = self._publish_first(tmp_path, "2")
        reader = Reader(served_dir=tmp_path)
        versions = reader.list_versions("market_quote_daily")
        assert len(versions) == 2
        assert v2 in versions
        assert v1 in versions

    def test_get_latest_version(self, tmp_path: Path):
        self._publish_first(tmp_path, "1")
        v2 = self._publish_first(tmp_path, "2")
        reader = Reader(served_dir=tmp_path)
        latest = reader.get_latest_version("market_quote_daily")
        assert latest == v2

    def test_read_nonexistent_dataset_raises(self, tmp_path: Path):
        reader = Reader(served_dir=tmp_path)
        with pytest.raises(ReadError):
            reader.read("nonexistent")

    def test_get_manifest(self, tmp_path: Path):
        version = self._publish_first(tmp_path)
        reader = Reader(served_dir=tmp_path)
        manifest = reader.get_manifest("market_quote_daily")
        assert manifest.release_version == version

    def test_list_versions_empty(self, tmp_path: Path):
        reader = Reader(served_dir=tmp_path)
        assert reader.list_versions("nonexistent") == []


# ---------------------------------------------------------------------------
# Rollback tests
# ---------------------------------------------------------------------------


class TestRollback:
    def _publish(self, tmp_path: Path, hint: str = "1") -> str:
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id=f"batch_{hint}",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        manifest = publisher.publish(
            dataset="market_quote_daily",
            df=df,
            gate_decision=gate,
        )
        return manifest.release_version

    def test_rollback_to_previous(self, tmp_path: Path):
        v1 = self._publish(tmp_path, "1")
        v2 = self._publish(tmp_path, "2")

        rb = RollbackManager(served_dir=tmp_path)
        new_latest = rb.rollback("market_quote_daily", reason="test rollback")

        assert new_latest == v1
        reader = Reader(served_dir=tmp_path)
        latest = reader.get_latest_version("market_quote_daily")
        assert latest == v1

        v2_manifest = reader.get_manifest("market_quote_daily", version=v2)
        assert v2_manifest.status == ReleaseStatus.ROLLED_BACK.value

    def test_rollback_no_previous_raises(self, tmp_path: Path):
        self._publish(tmp_path, "1")
        rb = RollbackManager(served_dir=tmp_path)
        with pytest.raises(RollbackError, match="No eligible previous version"):
            rb.rollback("market_quote_daily")

    def test_rollback_to_specific_version(self, tmp_path: Path):
        v1 = self._publish(tmp_path, "1")
        self._publish(tmp_path, "2")
        self._publish(tmp_path, "3")

        rb = RollbackManager(served_dir=tmp_path)
        new_latest = rb.rollback("market_quote_daily", target_version=v1)
        assert new_latest == v1

    def test_double_rollback_skips_rolled_back_version(self, tmp_path: Path):
        v1 = self._publish(tmp_path, "1")
        v2 = self._publish(tmp_path, "2")
        self._publish(tmp_path, "3")

        rb = RollbackManager(served_dir=tmp_path)
        rb.rollback("market_quote_daily")
        assert (
            Reader(served_dir=tmp_path).get_latest_version("market_quote_daily") == v2
        )

        rb.rollback("market_quote_daily")
        assert (
            Reader(served_dir=tmp_path).get_latest_version("market_quote_daily") == v1
        )


# ---------------------------------------------------------------------------
# Integration: publish -> read -> rollback -> read
# ---------------------------------------------------------------------------


class TestPublishReadRollbackIntegration:
    def test_full_lifecycle(self, tmp_path: Path):
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="batch_001",
            _passed=True,
        )

        publisher = Publisher(served_dir=tmp_path)
        manifest = publisher.publish(
            dataset="market_quote_daily",
            df=df,
            gate_decision=gate,
        )
        v1 = manifest.release_version

        reader = Reader(served_dir=tmp_path)
        read_df, read_manifest = reader.read("market_quote_daily")
        assert len(read_df) == 5
        assert read_manifest.release_version == v1

        df2 = _make_standardized_df(n=3)
        gate2 = FakeGateDecision(
            _dataset="market_quote_daily",
            _batch_id="batch_002",
            _passed=True,
        )
        manifest2 = publisher.publish(
            dataset="market_quote_daily",
            df=df2,
            gate_decision=gate2,
        )
        v2 = manifest2.release_version

        assert reader.get_latest_version("market_quote_daily") == v2

        rb = RollbackManager(served_dir=tmp_path)
        rb.rollback("market_quote_daily", reason="integration test")

        assert reader.get_latest_version("market_quote_daily") == v1

        final_df, final_manifest = reader.read("market_quote_daily")
        assert len(final_df) == 5
        assert final_manifest.release_version == v1
