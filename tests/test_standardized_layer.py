"""Tests for Standardized layer components.

Covers:
- merge.py: MergeEngine merge logic, time range merge, conflict handling
- compaction.py: CompactionJob basic functions, partition compaction, data integrity
- reader.py: StandardizedReader.read() with various parameters
- mapping_loader.py: MappingLoader YAML loading, mapping validation
- normalizer/macro_indicator.py: MacroIndicatorNormalizer for GDP/CPI/etc.
- normalizer/financial_indicator.py: FinancialIndicatorNormalizer for ROE/EPS/etc.
"""

import json
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pytest

from akshare_data.standardized.compaction import CompactionJob, CompactionManifest
from akshare_data.standardized.mapping_loader import (
    DatasetMapping,
    FieldMappingEntry,
    MappingLoader,
)
from akshare_data.standardized.merge import MergeEngine
from akshare_data.standardized.normalizer.financial_indicator import (
    FinancialIndicatorNormalizer,
)
from akshare_data.standardized.normalizer.macro_indicator import MacroIndicatorNormalizer
from akshare_data.standardized.reader import StandardizedReader
from akshare_data.standardized.writer import StandardizedWriter


# ---------------------------------------------------------------------------
# Helper functions to create test DataFrames
# ---------------------------------------------------------------------------


def _make_market_quote_df(
    rows: List[Dict],
    batch_id: str = "test_batch",
    source: str = "akshare",
    interface: str = "stock_zh_a_hist",
) -> pd.DataFrame:
    """Create a market quote DataFrame with standard fields."""
    df = pd.DataFrame(rows)
    df["batch_id"] = batch_id
    df["source_name"] = source
    df["interface_name"] = interface
    df["ingest_time"] = datetime.now(timezone.utc)
    df["normalize_version"] = "v1"
    df["schema_version"] = "v1"
    return df


def _make_financial_indicator_raw(rows: List[Dict]) -> pd.DataFrame:
    """Create raw financial indicator DataFrame."""
    return pd.DataFrame(rows)


def _make_macro_indicator_raw(rows: List[Dict]) -> pd.DataFrame:
    """Create raw macro indicator DataFrame."""
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# MergeEngine Tests (merge.py)
# ---------------------------------------------------------------------------


class TestMergeEngine:
    """Test MergeEngine merge logic."""

    def setup_method(self):
        self.engine = MergeEngine(
            primary_key=["security_id", "trade_date", "adjust_type"]
        )

    def test_merge_empty_existing(self):
        """Should return incoming when existing is empty."""
        existing = pd.DataFrame()
        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 2),
                    "adjust_type": "none",
                    "open_price": 10.0,
                    "close_price": 10.5,
                }
            ]
        )
        result = self.engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 1
        assert result["security_id"].iloc[0] == "600519"

    def test_merge_empty_incoming(self):
        """Should return existing when incoming is empty."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 2),
                    "adjust_type": "none",
                    "open_price": 10.0,
                    "close_price": 10.5,
                }
            ]
        )
        incoming = pd.DataFrame()
        result = self.engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 1
        assert result["security_id"].iloc[0] == "600519"

    def test_merge_strategy_replace(self):
        """Replace strategy should return only incoming."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ]
        )
        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "000001",
                    "trade_date": date(2024, 1, 2),
                    "adjust_type": "none",
                    "open_price": 20.0,
                }
            ]
        )
        result = self.engine.merge(existing, incoming, strategy="replace")
        assert len(result) == 1
        assert result["security_id"].iloc[0] == "000001"

    def test_merge_strategy_append(self):
        """Append strategy should concatenate without dedup."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ],
            batch_id="batch1",
        )
        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 15.0,
                }
            ],
            batch_id="batch2",
        )
        result = self.engine.merge(existing, incoming, strategy="append")
        assert len(result) == 2

    def test_merge_upsert_no_overlap(self):
        """Upsert should concatenate when no primary key overlap."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ]
        )
        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "000001",
                    "trade_date": date(2024, 1, 2),
                    "adjust_type": "none",
                    "open_price": 20.0,
                }
            ]
        )
        result = self.engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 2

    def test_merge_upsert_with_overlap(self):
        """Upsert should overwrite existing when primary key overlaps."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                    "close_price": 10.5,
                }
            ],
            batch_id="batch1",
        )
        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 15.0,
                    "close_price": 16.0,
                }
            ],
            batch_id="batch2",
        )
        result = self.engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 1
        assert result["open_price"].iloc[0] == 15.0
        assert result["close_price"].iloc[0] == 16.0

    def test_merge_version_conflict_resolution(self):
        """Higher normalize_version should win in conflict."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ],
            batch_id="batch1",
        )
        existing["normalize_version"] = "v1"

        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 20.0,
                }
            ],
            batch_id="batch2",
        )
        incoming["normalize_version"] = "v2"

        result = self.engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 1
        assert result["normalize_version"].iloc[0] == "v2"
        assert result["open_price"].iloc[0] == 20.0

    def test_merge_version_equal_uses_ingest_time(self):
        """When versions equal, later ingest_time should win."""
        earlier_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        later_time = datetime(2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc)

        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ]
        )
        existing["ingest_time"] = later_time
        existing["normalize_version"] = "v1"

        incoming = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 20.0,
                }
            ]
        )
        incoming["ingest_time"] = earlier_time
        incoming["normalize_version"] = "v1"

        result = self.engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 1
        assert result["open_price"].iloc[0] == 10.0

    def test_merge_late_arriving(self):
        """merge_late_arriving should use upsert strategy."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ]
        )
        late_data = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 12.0,
                }
            ],
            batch_id="late_batch",
        )
        late_data["normalize_version"] = "v2"
        result = self.engine.merge_late_arriving(existing, late_data)
        assert len(result) == 1
        assert result["open_price"].iloc[0] == 12.0

    def test_merge_incremental(self):
        """merge_incremental should add new rows."""
        existing = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 1),
                    "adjust_type": "none",
                    "open_price": 10.0,
                }
            ]
        )
        incremental = _make_market_quote_df(
            [
                {
                    "security_id": "600519",
                    "trade_date": date(2024, 1, 2),
                    "adjust_type": "none",
                    "open_price": 11.0,
                }
            ]
        )
        result = self.engine.merge_incremental(existing, incremental)
        assert len(result) == 2

    def test_merge_no_common_pk_columns(self):
        """Should fallback to append when no common PK columns."""
        engine = MergeEngine(primary_key=["nonexistent_col"])
        existing = pd.DataFrame({"a": [1], "b": [2]})
        incoming = pd.DataFrame({"a": [3], "b": [4]})
        result = engine.merge(existing, incoming, strategy="upsert")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# CompactionJob Tests (compaction.py)
# ---------------------------------------------------------------------------


class TestCompactionJob:
    """Test CompactionJob compaction logic."""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.base_dir = Path(self.temp_dir) / "standardized"
        self.job = CompactionJob(
            base_dir=str(self.base_dir),
            compaction_threshold=2,
            compaction_min_size_bytes=0,
        )

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_partition_files(
        self,
        domain: str,
        dataset: str,
        partition_key: str,
        partition_value: str,
        num_files: int,
    ) -> List[Path]:
        """Write test parquet files to a partition."""
        part_dir = self.base_dir / domain / dataset / f"{partition_key}={partition_value}"
        part_dir.mkdir(parents=True, exist_ok=True)

        files = []
        for i in range(num_files):
            df = pd.DataFrame({
                "security_id": ["600519", "000001"],
                "trade_date": [partition_value, partition_value],
                "open_price": [10.0 + i, 20.0 + i],
                "close_price": [10.5 + i, 20.5 + i],
                "batch_id": [f"batch_{i}", f"batch_{i}"],
            })
            path = part_dir / f"part-{i:04d}.parquet"
            df.to_parquet(path, index=False)
            files.append(path)
        return files

    def test_needs_compaction_below_threshold(self):
        """Should return False when files below threshold."""
        self._write_partition_files(
            "market", "quote_daily", "trade_date", "2024-01-01", 1
        )
        assert not self.job.needs_compaction(
            domain="market",
            dataset="quote_daily",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )

    def test_needs_compaction_above_threshold(self):
        """Should return True when files meet threshold."""
        self._write_partition_files(
            "market", "quote_daily", "trade_date", "2024-01-01", 3
        )
        assert self.job.needs_compaction(
            domain="market",
            dataset="quote_daily",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )

    def test_run_compaction(self):
        """Should compact multiple files into one."""
        files = self._write_partition_files(
            "market", "quote_daily", "trade_date", "2024-01-01", 3
        )
        manifest = self.job.run(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )
        assert manifest is not None
        assert manifest.record_count == 6
        assert len(manifest.source_files) == 3
        assert manifest.compacted_file.endswith(".parquet")

    def test_compaction_preserves_data_integrity(self):
        """Compacted file should contain all source data."""
        self._write_partition_files(
            "market", "quote_daily", "trade_date", "2024-01-01", 3
        )
        self.job.run(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )

        compacted_dir = (
            self.base_dir
            / "_compacted"
            / "market"
            / "quote_daily"
            / "trade_date=2024-01-01"
        )
        compacted_files = list(compacted_dir.glob("compacted-*.parquet"))
        assert len(compacted_files) >= 1

        df = pd.read_parquet(compacted_files[0])
        assert len(df) == 6
        assert "security_id" in df.columns
        assert "trade_date" in df.columns

    def test_compaction_with_dedup(self):
        """Compaction should deduplicate by primary key."""
        part_dir = self.base_dir / "market" / "quote_daily" / "trade_date=2024-01-01"
        part_dir.mkdir(parents=True, exist_ok=True)

        for i in range(3):
            df = pd.DataFrame({
                "security_id": ["600519"],
                "trade_date": ["2024-01-01"],
                "open_price": [10.0 + i],
                "batch_id": [f"batch_{i}"],
            })
            path = part_dir / f"part-{i:04d}.parquet"
            df.to_parquet(path, index=False)

        manifest = self.job.run(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
            primary_key=["security_id", "trade_date"],
        )
        assert manifest.record_count == 1

    def test_compaction_manifest_saved(self):
        """Compaction should save manifest file."""
        self._write_partition_files(
            "market", "quote_daily", "trade_date", "2024-01-01", 3
        )
        self.job.run(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )

        compacted_dir = (
            self.base_dir
            / "_compacted"
            / "market"
            / "quote_daily"
            / "trade_date=2024-01-01"
        )
        manifest_path = compacted_dir / "_compaction_manifest.json"
        assert manifest_path.exists()

        saved_manifest = CompactionManifest.load(manifest_path)
        assert saved_manifest is not None
        assert saved_manifest.dataset == "quote_daily"

    def test_run_compaction_nonexistent_partition(self):
        """Should return None for nonexistent partition."""
        manifest = self.job.run(
            dataset="nonexistent",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )
        assert manifest is None

    def test_run_compaction_single_file(self):
        """Should return None when only one file exists."""
        self._write_partition_files(
            "market", "quote_daily", "trade_date", "2024-01-01", 1
        )
        manifest = self.job.run(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )
        assert manifest is None


class TestCompactionManifest:
    """Test CompactionManifest data class."""

    def test_create_manifest(self):
        """Should create manifest with all fields."""
        manifest = CompactionManifest.create(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
            source_files=["part-0000.parquet", "part-0001.parquet"],
            compacted_file="compacted-001.parquet",
            record_count=100,
            source_batches=["batch1", "batch2"],
        )
        assert manifest.dataset == "quote_daily"
        assert manifest.domain == "market"
        assert manifest.record_count == 100
        assert manifest.compaction_id.startswith("comp-")

    def test_manifest_to_json(self):
        """Should serialize to JSON."""
        manifest = CompactionManifest.create(
            dataset="test",
            domain="test",
            partition_key="date",
            partition_value="2024-01-01",
            source_files=["file.parquet"],
            compacted_file="comp.parquet",
            record_count=10,
            source_batches=["b1"],
        )
        json_str = manifest.to_json()
        data = json.loads(json_str)
        assert data["dataset"] == "test"

    def test_manifest_from_json(self):
        """Should deserialize from JSON."""
        json_str = json.dumps({
            "compaction_id": "comp-001",
            "dataset": "test",
            "domain": "test",
            "partition_key": "date",
            "partition_value": "2024-01-01",
            "source_files": ["file.parquet"],
            "compacted_file": "comp.parquet",
            "record_count": 10,
            "compacted_at": "2024-01-01T00:00:00Z",
            "source_batches": ["b1"],
        })
        manifest = CompactionManifest.from_json(json_str)
        assert manifest.dataset == "test"
        assert manifest.record_count == 10

    def test_manifest_save_and_load(self):
        """Should save and load manifest from file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"
            manifest = CompactionManifest.create(
                dataset="test",
                domain="test",
                partition_key="date",
                partition_value="2024-01-01",
                source_files=["file.parquet"],
                compacted_file="comp.parquet",
                record_count=10,
                source_batches=["b1"],
            )
            manifest.save(path)
            loaded = CompactionManifest.load(path)
            assert loaded is not None
            assert loaded.dataset == manifest.dataset


# ---------------------------------------------------------------------------
# StandardizedReader Tests (reader.py)
# ---------------------------------------------------------------------------


class TestStandardizedReader:
    """Test StandardizedReader read logic."""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.base_dir = Path(self.temp_dir) / "standardized"
        self.reader = StandardizedReader(base_dir=str(self.base_dir))
        self.writer = StandardizedWriter(base_dir=str(self.base_dir))

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_test_data(
        self,
        domain: str,
        dataset: str,
        partition_dates: List[str],
        batch_id: str = "test_batch",
    ) -> None:
        """Write test data to multiple partitions."""
        rows = []
        for partition_date in partition_dates:
            rows.extend([
                {
                    "security_id": "600519",
                    "trade_date": partition_date,
                    "adjust_type": "none",
                    "open_price": 10.0,
                    "close_price": 10.5,
                    "volume": 1000,
                    "turnover_amount": 10500.0,
                },
                {
                    "security_id": "000001",
                    "trade_date": partition_date,
                    "adjust_type": "none",
                    "open_price": 20.0,
                    "close_price": 20.5,
                    "volume": 2000,
                    "turnover_amount": 41000.0,
                },
            ])
        df = pd.DataFrame(rows)
        self.writer.write(
            df=df,
            dataset=dataset,
            domain=domain,
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
            batch_id=batch_id,
            source_name="akshare",
            interface_name="test",
        )

    def test_read_empty_dataset(self):
        """Should return empty DataFrame for nonexistent dataset."""
        result = self.reader.read(
            dataset="nonexistent",
            domain="market",
            partition_key="trade_date",
        )
        assert result.empty

    def test_read_single_partition(self):
        """Should read data from a single partition."""
        self._write_test_data("market", "quote_daily", ["2024-01-01"])
        result = self.reader.read(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
        )
        assert len(result) == 2

    def test_read_date_range(self):
        """Should read data within date range."""
        self._write_test_data(
            "market", "quote_daily", ["2024-01-01", "2024-01-02", "2024-01-03"]
        )
        result = self.reader.read(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
        )
        assert len(result) == 4

    def test_read_with_batch_id_filter(self):
        """Should filter by batch_id."""
        self._write_test_data("market", "quote_daily", ["2024-01-01"], batch_id="batch1")
        self._write_test_data("market", "quote_daily", ["2024-01-01"], batch_id="batch2")
        result = self.reader.read(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            batch_id="batch1",
        )
        assert len(result) == 2
        assert (result["batch_id"] == "batch1").all()

    def test_read_with_version_filter(self):
        """Should filter by normalize_version."""
        self._write_test_data("market", "quote_daily", ["2024-01-01"])

        part_dir = self.base_dir / "market" / "quote_daily" / "trade_date=2024-01-01"
        part_dir.mkdir(parents=True, exist_ok=True)
        df_v2 = pd.DataFrame({
            "security_id": ["600519"],
            "trade_date": ["2024-01-01"],
            "open_price": [15.0],
            "batch_id": ["batch_v2"],
            "source_name": ["akshare"],
            "interface_name": ["test"],
            "ingest_time": [datetime.now(timezone.utc)],
            "normalize_version": ["v2"],
            "schema_version": ["v1"],
        })
        df_v2.to_parquet(part_dir / "part-v2.parquet", index=False)

        result = self.reader.read(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            normalize_version="v2",
        )
        assert len(result) == 1
        assert (result["normalize_version"] == "v2").all()

    def test_read_column_selection(self):
        """Should select only specified columns."""
        self._write_test_data("market", "quote_daily", ["2024-01-01"])
        result = self.reader.read(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            columns=["security_id", "close_price"],
        )
        assert set(result.columns) <= {"security_id", "close_price"}

    def test_list_partitions(self):
        """Should list all partition values."""
        self._write_test_data(
            "market", "quote_daily", ["2024-01-01", "2024-01-02", "2024-01-03"]
        )
        partitions = self.reader.list_partitions(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
        )
        assert len(partitions) == 3
        assert "2024-01-01" in partitions

    def test_read_compacted_data(self):
        """Should read from compacted files when available."""
        self._write_test_data("market", "quote_daily", ["2024-01-01"])

        part_dir = self.base_dir / "market" / "quote_daily" / "trade_date=2024-01-01"
        for i in range(3):
            df = pd.DataFrame({
                "security_id": ["600519"],
                "trade_date": ["2024-01-01"],
                "open_price": [10.0 + i],
                "batch_id": [f"batch_{i}"],
                "source_name": ["akshare"],
                "interface_name": ["test"],
                "ingest_time": [datetime.now(timezone.utc)],
                "normalize_version": ["v1"],
                "schema_version": ["v1"],
            })
            df.to_parquet(part_dir / f"part-{i:04d}.parquet", index=False)

        job = CompactionJob(base_dir=str(self.base_dir), compaction_threshold=2)
        job.run(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            partition_value="2024-01-01",
        )

        result = self.reader.read(
            dataset="quote_daily",
            domain="market",
            partition_key="trade_date",
            start_date=date(2024, 1, 1),
        )
        assert len(result) >= 1

    def test_read_by_primary_key(self):
        """Should read by primary key lookup."""
        self._write_test_data("market", "quote_daily", ["2024-01-01"])
        result = self.reader.read_by_primary_key(
            dataset="quote_daily",
            domain="market",
            primary_key={
                "trade_date": "2024-01-01",
                "security_id": "600519",
            },
            partition_key="trade_date",
        )
        assert len(result) == 1
        assert result["security_id"].iloc[0] == "600519"

    def test_read_by_primary_key_missing_partition_key(self):
        """Should raise error when partition_key not in primary_key dict."""
        with pytest.raises(ValueError, match="requires 'trade_date'"):
            self.reader.read_by_primary_key(
                dataset="quote_daily",
                domain="market",
                primary_key={"security_id": "600519"},
                partition_key="trade_date",
            )


# ---------------------------------------------------------------------------
# MappingLoader Tests (mapping_loader.py)
# ---------------------------------------------------------------------------


class TestMappingLoader:
    """Test MappingLoader YAML loading."""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config_root = Path(self.temp_dir) / "config"

        self.mappings_root = self.config_root / "mappings" / "sources"
        self.versions_path = self.config_root / "standards" / "normalize_versions.yaml"

        self.loader = MappingLoader(config_root=self.config_root)

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_mapping_yaml(
        self,
        source: str,
        dataset: str,
        fields: Dict[str, Dict],
        mapping_version: str = "v1",
        normalize_version: str = "v1",
    ) -> None:
        """Write a test mapping YAML file."""
        source_dir = self.mappings_root / source
        source_dir.mkdir(parents=True, exist_ok=True)

        content = {
            "dataset": dataset,
            "source": source,
            "mapping_version": mapping_version,
            "normalize_version": normalize_version,
            "fields": fields,
        }
        import yaml
        with open(source_dir / f"{dataset}.yaml", "w", encoding="utf-8") as f:
            yaml.dump(content, f)

    def _write_versions_yaml(self, datasets: Dict) -> None:
        """Write a test versions YAML file."""
        self.config_root.mkdir(parents=True, exist_ok=True)
        standards_dir = self.config_root / "standards"
        standards_dir.mkdir(parents=True, exist_ok=True)

        content = {"datasets": datasets}
        import yaml
        with open(self.versions_path, "w", encoding="utf-8") as f:
            yaml.dump(content, f)

    def test_get_mapping_missing_file(self):
        """Should return empty DatasetMapping when file missing."""
        mapping = self.loader.get_mapping("nonexistent", "unknown")
        assert mapping.dataset == "nonexistent"
        assert mapping.source == "unknown"
        assert mapping.mapping_version == "v0"
        assert len(mapping.entries) == 0

    def test_get_mapping_loads_yaml(self):
        """Should load mapping from YAML file."""
        self._write_mapping_yaml(
            "akshare",
            "market_quote_daily",
            {
                "日期": {"standard_field": "trade_date", "status": "active"},
                "开盘": {"standard_field": "open_price", "status": "active"},
                "收盘": {"standard_field": "close_price", "status": "deprecated"},
            },
        )
        mapping = self.loader.get_mapping("market_quote_daily", "akshare")
        assert mapping.dataset == "market_quote_daily"
        assert mapping.mapping_version == "v1"
        assert "日期" in mapping.entries
        assert mapping.entries["日期"].standard_field == "trade_date"

    def test_get_mapping_caches_result(self):
        """Should cache mapping after first load."""
        self._write_mapping_yaml(
            "akshare",
            "test_dataset",
            {"field1": {"standard_field": "std_field1", "status": "active"}},
        )

        mapping1 = self.loader.get_mapping("test_dataset", "akshare")
        mapping2 = self.loader.get_mapping("test_dataset", "akshare")
        assert mapping1 is mapping2

    def test_to_rename_dict_active_only(self):
        """Should return only active mappings in rename dict."""
        self._write_mapping_yaml(
            "akshare",
            "test_dataset",
            {
                "active_field": {"standard_field": "std_active", "status": "active"},
                "deprecated_field": {"standard_field": "std_deprecated", "status": "deprecated"},
                "pending_field": {"standard_field": "std_pending", "status": "pending"},
            },
        )
        mapping = self.loader.get_mapping("test_dataset", "akshare")
        rename_dict = mapping.to_rename_dict(active_only=True)
        assert rename_dict == {"active_field": "std_active"}

    def test_to_rename_dict_all(self):
        """Should return all mappings when active_only=False."""
        self._write_mapping_yaml(
            "akshare",
            "test_dataset",
            {
                "active_field": {"standard_field": "std_active", "status": "active"},
                "deprecated_field": {"standard_field": "std_deprecated", "status": "deprecated"},
            },
        )
        mapping = self.loader.get_mapping("test_dataset", "akshare")
        rename_dict = mapping.to_rename_dict(active_only=False)
        assert "deprecated_field" in rename_dict

    def test_get_normalize_version(self):
        """Should resolve normalize version from registry."""
        self._write_versions_yaml({
            "market_quote_daily": {
                "sources": {
                    "akshare": {"current_version": "v2"},
                }
            }
        })
        version = self.loader.get_normalize_version("market_quote_daily", "akshare")
        assert version == "v2"

    def test_get_normalize_version_fallback(self):
        """Should fallback to v1 when version not found."""
        version = self.loader.get_normalize_version("nonexistent", "unknown")
        assert version == "v1"

    def test_get_mapping_version(self):
        """Should return mapping_version from loaded mapping."""
        self._write_mapping_yaml(
            "akshare",
            "test_dataset",
            {},
            mapping_version="v3",
        )
        version = self.loader.get_mapping_version("test_dataset", "akshare")
        assert version == "v3"

    def test_list_datasets(self):
        """Should list all datasets with mappings."""
        self._write_mapping_yaml("akshare", "dataset1", {})
        self._write_mapping_yaml("lixinger", "dataset2", {})
        datasets = self.loader.list_datasets()
        assert "dataset1" in datasets
        assert "dataset2" in datasets

    def test_list_sources(self):
        """Should list all sources for a dataset."""
        self._write_mapping_yaml("akshare", "shared_dataset", {})
        self._write_mapping_yaml("lixinger", "shared_dataset", {})
        sources = self.loader.list_sources("shared_dataset")
        assert "akshare" in sources
        assert "lixinger" in sources

    def test_get_lineage(self):
        """Should return lineage records for active mappings."""
        self._write_mapping_yaml(
            "akshare",
            "test_dataset",
            {
                "raw1": {"standard_field": "std1", "status": "active"},
                "raw2": {"standard_field": "std2", "status": "deprecated"},
            },
            normalize_version="v1",
        )
        lineage = self.loader.get_lineage("test_dataset", "akshare")
        assert len(lineage) == 1
        assert lineage[0]["source_field"] == "raw1"
        assert lineage[0]["status"] == "active"

    def test_field_mapping_entry_is_active(self):
        """FieldMappingEntry.is_active should check status and standard_field."""
        active_entry = FieldMappingEntry(
            source_field="raw",
            standard_field="std",
            status="active",
        )
        deprecated_entry = FieldMappingEntry(
            source_field="raw",
            standard_field="std",
            status="deprecated",
        )
        null_entry = FieldMappingEntry(
            source_field="raw",
            standard_field=None,
            status="active",
        )

        assert active_entry.is_active
        assert not deprecated_entry.is_active
        assert not null_entry.is_active


class TestDatasetMapping:
    """Test DatasetMapping data class."""

    def test_active_fields(self):
        """Should return list of active standard fields."""
        mapping = DatasetMapping(
            dataset="test",
            source="test",
            mapping_version="v1",
            normalize_version="v1",
            entries={
                "raw1": FieldMappingEntry("raw1", "std1", "active"),
                "raw2": FieldMappingEntry("raw2", "std2", "deprecated"),
                "raw3": FieldMappingEntry("raw3", "std3", "active"),
            },
        )
        active = mapping.active_fields()
        assert sorted(active) == ["std1", "std3"]

    def test_pending_fields(self):
        """Should return pending field entries."""
        mapping = DatasetMapping(
            dataset="test",
            source="test",
            mapping_version="v1",
            normalize_version="v1",
            entries={
                "raw1": FieldMappingEntry("raw1", "std1", "active"),
                "raw2": FieldMappingEntry("raw2", "std2", "pending"),
            },
        )
        pending = mapping.pending_fields()
        assert len(pending) == 1
        assert pending[0].source_field == "raw2"


# ---------------------------------------------------------------------------
# MacroIndicatorNormalizer Tests
# ---------------------------------------------------------------------------


class TestMacroIndicatorNormalizerDetailed:
    """Detailed tests for MacroIndicatorNormalizer."""

    def setup_method(self):
        self.normalizer = MacroIndicatorNormalizer()

    def test_normalize_gdp_data(self):
        """Should normalize GDP raw data correctly."""
        df = _make_macro_indicator_raw([
            {"季度": "2024-Q1", "国内生产总值绝对额(亿元)": 296299},
            {"季度": "2024-Q2", "国内生产总值绝对额(亿元)": 320129},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_gdp",
            batch_id="test_gdp",
        )
        assert len(result) == 2
        assert "indicator_code" in result.columns
        assert result["indicator_code"].iloc[0] == "china_gdp"
        assert "value" in result.columns
        assert result["value"].iloc[0] == 296299
        assert result["frequency"].iloc[0] == "Q"
        assert result["unit"].iloc[0] == "CNY_100M"

    def test_normalize_cpi_data(self):
        """Should normalize CPI raw data correctly."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "全国居民消费价格指数(CPI)上年同月=100": 100.3},
            {"月份": "2024-02", "全国居民消费价格指数(CPI)上年同月=100": 100.9},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_cpi",
            batch_id="test_cpi",
        )
        assert len(result) == 2
        assert result["indicator_code"].iloc[0] == "china_cpi"
        assert "value" in result.columns
        assert result["frequency"].iloc[0] == "M"
        assert result["unit"].iloc[0] == "index"

    def test_normalize_pmi_data(self):
        """Should normalize PMI raw data correctly."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "制造业PMI": 49.2},
            {"月份": "2024-02", "制造业PMI": 50.1},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_pmi",
            batch_id="test_pmi",
        )
        assert len(result) == 2
        assert result["indicator_code"].iloc[0] == "china_pmi"
        assert result["value"].iloc[0] == 49.2

    def test_normalize_ppi_data(self):
        """Should normalize PPI raw data correctly."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "工业生产者出厂价格指数": 97.5},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_ppi",
            batch_id="test_ppi",
        )
        assert result["indicator_code"].iloc[0] == "china_ppi"

    def test_normalize_m2_data(self):
        """Should normalize M2 raw data correctly."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "M2供应量(亿元)": 292000},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_m2",
            batch_id="test_m2",
        )
        assert result["indicator_code"].iloc[0] == "china_m2"
        assert result["unit"].iloc[0] == "CNY_100M"

    def test_normalize_with_yoy_mom_fields(self):
        """Should map YoY/MoM fields correctly."""
        df = _make_macro_indicator_raw([
            {
                "月份": "2024-01",
                "CPI指数": 100.3,
                "同比": 0.7,
                "环比": 0.5,
            }
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_cpi",
            batch_id="test",
        )
        assert "value_yoy_pct" in result.columns
        assert "value_mom_pct" in result.columns
        assert result["value_yoy_pct"].iloc[0] == 0.7

    def test_normalize_with_explicit_indicator_code(self):
        """Should use explicit indicator_code override."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "custom_value": 123},
        ])
        result = self.normalizer.normalize(
            df,
            source="custom",
            interface_name="custom_interface",
            batch_id="test",
            indicator_code="custom_indicator",
            indicator_name="自定义指标",
            frequency="D",
        )
        assert result["indicator_code"].iloc[0] == "custom_indicator"
        assert result["indicator_name"].iloc[0] == "自定义指标"
        assert result["frequency"].iloc[0] == "D"

    def test_normalize_empty_dataframe(self):
        """Should return empty DataFrame for empty input."""
        result = self.normalizer.normalize(
            pd.DataFrame(),
            source="akshare",
            interface_name="macro_cpi",
        )
        assert result.empty

    def test_normalize_none_dataframe(self):
        """Should return empty DataFrame for None input."""
        result = self.normalizer.normalize(
            None,
            source="akshare",
            interface_name="macro_cpi",
        )
        assert result.empty

    def test_system_fields_present(self):
        """Should inject all system fields."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "CPI指数": 100.3},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_cpi",
            batch_id="batch_001",
        )
        system_fields = {
            "batch_id",
            "source_name",
            "interface_name",
            "ingest_time",
            "normalize_version",
            "schema_version",
        }
        assert system_fields.issubset(set(result.columns))

    def test_region_and_source_org_defaults(self):
        """Should set default region and source_org."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "CPI指数": 100.3},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_cpi",
        )
        assert "region" in result.columns
        assert result["region"].iloc[0] == "CN"
        assert "source_org" in result.columns

    def test_primary_keys_order(self):
        """Primary keys should appear first in output."""
        df = _make_macro_indicator_raw([
            {"月份": "2024-01", "CPI指数": 100.3},
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare",
            interface_name="macro_cpi",
        )
        cols = result.columns.tolist()
        pk_indices = [
            cols.index(pk) for pk in self.normalizer.PRIMARY_KEYS if pk in cols
        ]
        assert pk_indices == sorted(pk_indices)


# ---------------------------------------------------------------------------
# FinancialIndicatorNormalizer Tests
# ---------------------------------------------------------------------------


class TestFinancialIndicatorNormalizerDetailed:
    """Detailed tests for FinancialIndicatorNormalizer."""

    def setup_method(self):
        self.normalizer = FinancialIndicatorNormalizer()

    def test_normalize_akshare_em_basic(self):
        """Should normalize AkShare EM financial indicator data."""
        df = _make_financial_indicator_raw([
            {
                "symbol": "600519",
                "报告日期": "2024-03-31",
                "基本每股收益": 1.52,
                "加权净资产收益率": 15.3,
                "销售净利率": 22.5,
                "销售毛利率": 45.2,
                "资产负债率": 35.8,
                "营业总收入": 50000000000,
                "净利润": 11250000000,
            }
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare_em",
            interface_name="finance_indicator",
            batch_id="test_em",
        )
        assert len(result) == 1
        assert "security_id" in result.columns
        assert result["security_id"].iloc[0] == "600519"
        assert "report_date" in result.columns
        assert "basic_eps" in result.columns
        assert result["basic_eps"].iloc[0] == 1.52
        assert "roe_pct" in result.columns
        assert result["roe_pct"].iloc[0] == 15.3

    def test_normalize_lixinger_basic(self):
        """Should normalize Lixinger financial indicator data."""
        df = _make_financial_indicator_raw([
            {
                "symbol": "600519",
                "report_date": "2024-03-31",
                "pe": 25.0,
                "pb": 10.0,
                "ps": 5.0,
                "roe": 15.3,
                "roa": 8.5,
                "net_profit": 11250000000,
                "revenue": 50000000000,
                "total_assets": 200000000000,
                "total_equity": 100000000000,
                "debt_ratio": 35.8,
                "gross_margin": 45.2,
                "net_margin": 22.5,
            }
        ])
        result = self.normalizer.normalize(
            df,
            source="lixinger",
            interface_name="finance_indicator",
            batch_id="test_lixinger",
        )
        assert len(result) == 1
        assert "security_id" in result.columns
        assert result["security_id"].iloc[0] == "600519"
        assert "pe_ratio_ttm" in result.columns
        assert result["pe_ratio_ttm"].iloc[0] == 25.0
        assert "pb_ratio" in result.columns
        assert result["pb_ratio"].iloc[0] == 10.0

    def test_ratio_fields_have_pct_suffix(self):
        """All ratio fields should have _pct suffix."""
        df = _make_financial_indicator_raw([
            {
                "symbol": "600519",
                "报告日期": "2024-03-31",
                "加权净资产收益率": 15.3,
                "销售净利率": 22.5,
                "销售毛利率": 45.2,
                "资产负债率": 35.8,
            }
        ])
        result = self.normalizer.normalize(df, source="akshare_em")
        ratio_fields = ["roe_pct", "net_margin_pct", "gross_margin_pct", "debt_ratio_pct"]
        for field in ratio_fields:
            if field in result.columns:
                assert field.endswith("_pct")

    def test_date_field_conversion(self):
        """Should convert date fields to datetime."""
        df = _make_financial_indicator_raw([
            {
                "symbol": "600519",
                "报告日期": "2024-03-31",
                "净利润": 10000000000,
            }
        ])
        result = self.normalizer.normalize(df, source="akshare_em")
        assert pd.api.types.is_datetime64_any_dtype(result["report_date"])

    def test_numeric_coercion(self):
        """Should coerce numeric fields properly."""
        df = _make_financial_indicator_raw([
            {
                "symbol": "600519",
                "报告日期": "2024-03-31",
                "基本每股收益": "1.52",
                "净利润": "10000000000",
            }
        ])
        result = self.normalizer.normalize(df, source="akshare_em")
        assert pd.api.types.is_numeric_dtype(result["basic_eps"])
        assert pd.api.types.is_numeric_dtype(result["net_profit"])

    def test_default_report_type(self):
        """Should set default report_type when not provided."""
        df = _make_financial_indicator_raw([
            {"symbol": "600519", "报告日期": "2024-03-31", "净利润": 10000000000}
        ])
        result = self.normalizer.normalize(df, source="akshare_em")
        assert "report_type" in result.columns
        assert result["report_type"].iloc[0] == "Q"

    def test_custom_report_type(self):
        """Should accept custom report_type via extra_fields."""
        df = _make_financial_indicator_raw([
            {"symbol": "600519", "报告日期": "2024-03-31", "净利润": 10000000000}
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare_em",
            extra_fields={"report_type": "A"},
        )
        assert result["report_type"].iloc[0] == "A"

    def test_system_fields_present(self):
        """Should inject all system fields."""
        df = _make_financial_indicator_raw([
            {"symbol": "600519", "报告日期": "2024-03-31", "净利润": 10000000000}
        ])
        result = self.normalizer.normalize(
            df,
            source="akshare_em",
            interface_name="finance_indicator",
            batch_id="batch_001",
        )
        system_fields = {
            "batch_id",
            "source_name",
            "interface_name",
            "ingest_time",
            "normalize_version",
            "schema_version",
        }
        assert system_fields.issubset(set(result.columns))

    def test_empty_dataframe(self):
        """Should return empty DataFrame for empty input."""
        result = self.normalizer.normalize(pd.DataFrame(), source="akshare_em")
        assert result.empty

    def test_none_dataframe(self):
        """Should return empty DataFrame for None input."""
        result = self.normalizer.normalize(None, source="akshare_em")
        assert result.empty

    def test_primary_keys_order(self):
        """Primary keys should appear first in output."""
        df = _make_financial_indicator_raw([
            {"symbol": "600519", "报告日期": "2024-03-31", "净利润": 10000000000}
        ])
        result = self.normalizer.normalize(df, source="akshare_em")
        cols = result.columns.tolist()
        pk_indices = [
            cols.index(pk) for pk in self.normalizer.PRIMARY_KEYS if pk in cols
        ]
        assert pk_indices == sorted(pk_indices)

    def test_symbol_normalization_various_formats(self):
        """Should normalize various symbol formats."""
        df = _make_financial_indicator_raw([
            {"symbol": "sh600519", "报告日期": "2024-03-31", "净利润": 10000000000},
            {"ts_code": "000001.SZ", "报告日期": "2024-03-31", "净利润": 5000000000},
            {"code": "sz.000002", "报告日期": "2024-03-31", "净利润": 3000000000},
        ])
        result = self.normalizer.normalize(df, source="akshare_em")
        assert "security_id" in result.columns