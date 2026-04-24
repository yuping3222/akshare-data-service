"""Tests for the Served layer: reader, versioning, concurrency scenarios."""

from __future__ import annotations

import threading
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
from akshare_data.served.versioning import (
    ReleaseVersion,
    ReleaseVersionError,
    next_release_version,
    list_release_versions,
)


@dataclass(frozen=True)
class FakeGateDecision:
    """Minimal GateDecision implementation for testing."""

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


def _make_standardized_df(n: int = 5, start_date: str = "2024-01-02") -> pd.DataFrame:
    """Create a minimal standardized DataFrame."""
    return pd.DataFrame(
        {
            "security_id": ["sh600000"] * n,
            "trade_date": pd.date_range(start_date, periods=n, freq="B"),
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


class TestServedReaderRead:
    def _publish(self, tmp_path: Path, hint: str = "1", partition: bool = False) -> str:
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
            partition_col="trade_date" if partition else None,
        )
        return manifest.release_version

    def test_read_basic(self, tmp_path: Path):
        version = self._publish(tmp_path)
        reader = Reader(served_dir=tmp_path)
        df, manifest = reader.read("market_quote_daily")
        assert len(df) == 5
        assert manifest.release_version == version
        assert manifest.dataset == "market_quote_daily"

    def test_read_without_partition(self, tmp_path: Path):
        version = self._publish(tmp_path, partition=False)
        reader = Reader(served_dir=tmp_path)
        df, manifest = reader.read("market_quote_daily")
        assert len(df) == 5
        assert manifest.release_version == version

    def test_read_with_version(self, tmp_path: Path):
        version = self._publish(tmp_path)
        reader = Reader(served_dir=tmp_path)
        df, manifest = reader.read("market_quote_daily", version=version)
        assert len(df) == 5
        assert manifest.release_version == version

    def test_read_empty_result_with_partitions(self, tmp_path: Path):
        self._publish(tmp_path, partition=True)
        reader = Reader(served_dir=tmp_path)
        df, _ = reader.read(
            "market_quote_daily",
            partitions={"trade_date": ["2099-01-01"]},
        )
        assert df.empty

    def test_read_nonexistent_dataset_raises(self, tmp_path: Path):
        reader = Reader(served_dir=tmp_path)
        with pytest.raises(ReadError):
            reader.read("nonexistent_dataset")

    def test_read_nonexistent_version_raises(self, tmp_path: Path):
        self._publish(tmp_path)
        reader = Reader(served_dir=tmp_path)
        with pytest.raises(ReadError):
            reader.read("market_quote_daily", version="nonexistent_version")


class TestServedReaderMetadata:
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

    def test_get_table_info(self, tmp_path: Path):
        version = self._publish(tmp_path)
        reader = Reader(served_dir=tmp_path)
        manifest = reader.get_manifest("market_quote_daily")
        assert manifest.dataset == "market_quote_daily"
        assert manifest.release_version == version
        assert manifest.total_record_count == 5

    def test_get_table_info_with_version(self, tmp_path: Path):
        v1 = self._publish(tmp_path, "1")
        v2 = self._publish(tmp_path, "2")
        reader = Reader(served_dir=tmp_path)
        manifest = reader.get_manifest("market_quote_daily", version=v1)
        assert manifest.release_version == v1

    def test_list_tables(self, tmp_path: Path):
        self._publish(tmp_path)
        reader = Reader(served_dir=tmp_path)
        versions = reader.list_versions("market_quote_daily")
        assert len(versions) == 1

    def test_list_tables_multiple_versions(self, tmp_path: Path):
        self._publish(tmp_path, "1")
        self._publish(tmp_path, "2")
        reader = Reader(served_dir=tmp_path)
        versions = reader.list_versions("market_quote_daily")
        assert len(versions) == 2

    def test_list_tables_empty(self, tmp_path: Path):
        reader = Reader(served_dir=tmp_path)
        versions = reader.list_versions("nonexistent")
        assert versions == []

    def test_exists_dataset(self, tmp_path: Path):
        self._publish(tmp_path)
        reader = Reader(served_dir=tmp_path)
        latest = reader.get_latest_version("market_quote_daily")
        assert latest is not None

    def test_exists_no_dataset_raises(self, tmp_path: Path):
        reader = Reader(served_dir=tmp_path)
        with pytest.raises(ReadError):
            reader.get_latest_version("nonexistent")


class TestServedReaderHasDateRange:
    def _publish_with_dates(
        self, tmp_path: Path, start_date: str, n: int, partition: bool = False
    ) -> str:
        df = _make_standardized_df(n=n, start_date=start_date)
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
            partition_col="trade_date" if partition else None,
        )
        return manifest.release_version

    def test_has_range_read_all(self, tmp_path: Path):
        self._publish_with_dates(tmp_path, "2024-01-02", 5, partition=False)
        reader = Reader(served_dir=tmp_path)
        df, _ = reader.read("market_quote_daily")
        assert len(df) == 5
        assert df["trade_date"].min().date() == pd.Timestamp("2024-01-02").date()

    def test_has_range_no_data(self, tmp_path: Path):
        self._publish_with_dates(tmp_path, "2024-01-02", 5, partition=True)
        reader = Reader(served_dir=tmp_path)
        df, _ = reader.read(
            "market_quote_daily",
            partitions={"trade_date": ["2099-01-01"]},
        )
        assert df.empty


class TestVersionSelectorResolve:
    def test_resolve_default_version(self):
        from akshare_data.service.version_selector import VersionSelector, VersionInfo

        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="active"))

        resolved = selector.resolve()
        assert resolved == "v2"

    def test_resolve_explicit_version(self):
        from akshare_data.service.version_selector import VersionSelector, VersionInfo

        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="active"))

        resolved = selector.resolve("v1")
        assert resolved == "v1"

    def test_resolve_nonexistent_fallback(self):
        from akshare_data.service.version_selector import VersionSelector, VersionInfo

        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))

        resolved = selector.resolve("nonexistent")
        assert resolved == "v1"

    def test_resolve_inactive_fallback(self):
        from akshare_data.service.version_selector import VersionSelector, VersionInfo

        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="inactive"))

        resolved = selector.resolve("v2")
        assert resolved == "v1"

    def test_resolve_latest_keyword(self):
        from akshare_data.service.version_selector import VersionSelector, VersionInfo

        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))

        resolved = selector.resolve("latest")
        assert resolved == "v1"

    def test_resolve_no_active_versions(self):
        from akshare_data.service.version_selector import VersionSelector

        selector = VersionSelector()
        resolved = selector.resolve()
        assert resolved == "latest"

    def test_version_priority(self):
        from akshare_data.service.version_selector import VersionSelector, VersionInfo

        selector = VersionSelector()
        selector.register_version("v_20240101_01", VersionInfo(version="v_20240101_01", status="active"))
        selector.register_version("v_20240102_01", VersionInfo(version="v_20240102_01", status="active"))
        selector.register_version("v_20240103_01", VersionInfo(version="v_20240103_01", status="active"))

        resolved = selector.resolve()
        assert resolved == "v_20240103_01"


class TestVersioningModel:
    def test_parse_valid_version(self):
        rv = ReleaseVersion.parse("market_quote_daily-r202401021200-01")
        assert rv.dataset == "market_quote_daily"
        assert rv.sequence == 1

    def test_parse_invalid_format_raises(self):
        with pytest.raises(ReleaseVersionError):
            ReleaseVersion.parse("invalid_format")

    def test_parse_invalid_sequence_raises(self):
        with pytest.raises(ReleaseVersionError):
            ReleaseVersion.parse("dataset-r202401021200-00")

    def test_to_string(self):
        rv = ReleaseVersion.parse("test-r202401021200-01")
        assert rv.to_string() == "test-r202401021200-01"

    def test_next_release_version_first(self):
        version = next_release_version("test_dataset")
        assert version.startswith("test_dataset-r")

    def test_next_release_version_increment(self):
        versions = ["test-r202401021200-01"]
        now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
        next_v = next_release_version("test", now=now, existing_versions=versions)
        assert next_v == "test-r202401021200-02"

    def test_next_release_version_different_minute(self):
        versions = ["test-r202401021200-01"]
        now = datetime(2024, 1, 3, 12, 30, tzinfo=timezone.utc)
        next_v = next_release_version("test", now=now, existing_versions=versions)
        assert "202401031230" in next_v
        assert next_v.endswith("-01")

    def test_list_release_versions(self, tmp_path: Path):
        releases_dir = tmp_path / "dataset" / "releases"
        releases_dir.mkdir(parents=True)
        (releases_dir / "dataset-r202401021200-01").mkdir()
        (releases_dir / "dataset-r202401031200-02").mkdir()

        versions = list_release_versions(releases_dir)
        assert len(versions) == 2
        assert "dataset-r202401021200-01" in versions

    def test_list_release_versions_empty(self, tmp_path: Path):
        versions = list_release_versions(tmp_path / "nonexistent")
        assert versions == []


class TestManifestConcurrency:
    def test_concurrent_read_write(self, tmp_path: Path):
        df = _make_standardized_df()
        gate = FakeGateDecision(
            _dataset="rw_test",
            _batch_id="batch_001",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        publisher.publish(dataset="rw_test", df=df, gate_decision=gate)

        errors = []
        results = []
        lock = threading.Lock()

        def read_worker():
            try:
                reader = Reader(served_dir=tmp_path)
                df, manifest = reader.read("rw_test")
                with lock:
                    results.append(len(df))
            except Exception as e:
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=read_worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(r == 5 for r in results)

    def test_sequential_publish_multiple_versions(self, tmp_path: Path):
        versions = []
        for i in range(3):
            df = _make_standardized_df(n=3)
            gate = FakeGateDecision(
                _dataset="seq_test",
                _batch_id=f"batch_{i}",
                _passed=True,
            )
            publisher = Publisher(served_dir=tmp_path)
            manifest = publisher.publish(
                dataset="seq_test",
                df=df,
                gate_decision=gate,
            )
            versions.append(manifest.release_version)

        assert len(versions) == 3
        assert len(set(versions)) == 3

        reader = Reader(served_dir=tmp_path)
        all_versions = reader.list_versions("seq_test")
        assert len(all_versions) == 3


class TestPublisherLargeData:
    def test_large_data(self, tmp_path: Path):
        df = pd.DataFrame(
            {
                "security_id": ["sh600000"] * 1000,
                "trade_date": pd.date_range("2024-01-02", periods=1000, freq="B"),
                "adjust_type": ["none"] * 1000,
                "open_price": [10.0 + i * 0.01 for i in range(1000)],
                "high_price": [11.0 + i * 0.01 for i in range(1000)],
                "low_price": [9.0 + i * 0.01 for i in range(1000)],
                "close_price": [10.5 + i * 0.01 for i in range(1000)],
                "volume": [100_000.0] * 1000,
                "turnover_amount": [1_000_000.0] * 1000,
                "source_name": ["lixinger"] * 1000,
                "interface_name": ["test_iface"] * 1000,
                "batch_id": ["batch_large"] * 1000,
                "ingest_time": [datetime.now(timezone.utc)] * 1000,
                "normalize_version": ["v1"] * 1000,
                "schema_version": ["v1"] * 1000,
                "quality_status": ["passed"] * 1000,
            }
        )

        gate = FakeGateDecision(
            _dataset="large_test",
            _batch_id="batch_large",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        manifest = publisher.publish(
            dataset="large_test",
            df=df,
            gate_decision=gate,
        )

        assert manifest.total_record_count == 1000

        reader = Reader(served_dir=tmp_path)
        read_df, read_manifest = reader.read("large_test")
        assert len(read_df) == 1000
        assert read_manifest.release_version == manifest.release_version

    def test_multiple_publishes(self, tmp_path: Path):
        df = pd.DataFrame(
            {
                "security_id": ["sh600000"] * 5,
                "trade_date": pd.date_range("2024-01-02", periods=5, freq="B"),
                "adjust_type": ["none"] * 5,
                "open_price": [10.0] * 5,
                "high_price": [11.0] * 5,
                "low_price": [9.0] * 5,
                "close_price": [10.5] * 5,
                "volume": [100_000.0] * 5,
                "turnover_amount": [1_000_000.0] * 5,
                "source_name": ["lixinger"] * 5,
                "interface_name": ["test_iface"] * 5,
                "batch_id": ["batch_1"] * 5,
                "ingest_time": [datetime.now(timezone.utc)] * 5,
                "normalize_version": ["v1"] * 5,
                "schema_version": ["v1"] * 5,
                "quality_status": ["passed"] * 5,
            }
        )

        gate = FakeGateDecision(
            _dataset="multi_publish_test",
            _batch_id="batch_1",
            _passed=True,
        )
        publisher = Publisher(served_dir=tmp_path)
        manifest1 = publisher.publish(
            dataset="multi_publish_test",
            df=df,
            gate_decision=gate,
        )

        gate2 = FakeGateDecision(
            _dataset="multi_publish_test",
            _batch_id="batch_2",
            _passed=True,
        )
        manifest2 = publisher.publish(
            dataset="multi_publish_test",
            df=df,
            gate_decision=gate2,
        )

        assert manifest1.release_version != manifest2.release_version

        reader = Reader(served_dir=tmp_path)
        versions = reader.list_versions("multi_publish_test")
        assert len(versions) == 2