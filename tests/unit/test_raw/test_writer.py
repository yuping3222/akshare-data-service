"""Tests for RawWriter."""

import json
from datetime import date

import pandas as pd
import pytest

from akshare_data.raw.writer import RawWriter
from akshare_data.raw.system_fields import SYSTEM_FIELD_NAMES
from akshare_data.raw.manifest import Manifest, MANIFEST_FILENAME, SCHEMA_FILENAME
from akshare_data.ingestion.models.task import ExtractTask
from akshare_data.ingestion.models.batch import BatchContext


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-04-01", periods=5, freq="B"),
            "open": [10.0, 10.1, 10.2, 10.3, 10.4],
            "high": [11.0, 11.1, 11.2, 11.3, 11.4],
            "low": [9.0, 9.1, 9.2, 9.3, 9.4],
            "close": [10.5, 10.6, 10.7, 10.8, 10.9],
            "volume": [100000, 110000, 120000, 130000, 140000],
        }
    )


@pytest.fixture
def sample_task():
    return ExtractTask(
        domain="cn",
        dataset="market_quote_daily",
        source_name="akshare",
        interface_name="stock_zh_a_hist",
        params={
            "symbol": "600519",
            "start_date": "2026-04-01",
            "end_date": "2026-04-22",
        },
        extract_date=date(2026, 4, 22),
        batch_id="20260422_001",
    )


@pytest.fixture
def sample_batch_ctx():
    return BatchContext(
        batch_id="20260422_001",
        domain="cn",
    )


class TestRawWriterWrite:
    def test_write_creates_batch_directory(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        assert batch_dir.exists()
        assert batch_dir.is_dir()

    def test_write_path_structure(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        expected = (
            tmp_path
            / "cn"
            / "market_quote_daily"
            / "extract_date=2026-04-22"
            / f"batch_id={sample_batch_ctx.batch_id}"
        )
        assert batch_dir == expected

    def test_write_creates_parquet_file(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        parquet_files = list(batch_dir.glob("part-*.parquet"))
        assert len(parquet_files) == 1

    def test_write_creates_manifest(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        manifest_path = batch_dir / MANIFEST_FILENAME
        assert manifest_path.exists()

    def test_write_creates_schema_snapshot(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        schema_path = batch_dir / SCHEMA_FILENAME
        assert schema_path.exists()

    def test_manifest_content(self, tmp_path, sample_df, sample_task, sample_batch_ctx):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        manifest = Manifest.load(batch_dir / MANIFEST_FILENAME)
        assert manifest.dataset == "market_quote_daily"
        assert manifest.domain == "cn"
        assert manifest.batch_id == sample_batch_ctx.batch_id
        assert manifest.source_name == "akshare"
        assert manifest.interface_name == "stock_zh_a_hist"
        assert manifest.record_count == 5
        assert manifest.status == "success"

    def test_parquet_contains_system_fields(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        parquet_file = list(batch_dir.glob("part-*.parquet"))[0]
        df = pd.read_parquet(parquet_file)
        for field in SYSTEM_FIELD_NAMES:
            assert field in df.columns, f"Missing system field: {field}"

    def test_parquet_preserves_original_columns(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        parquet_file = list(batch_dir.glob("part-*.parquet"))[0]
        df = pd.read_parquet(parquet_file)
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

    def test_system_field_values(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        parquet_file = list(batch_dir.glob("part-*.parquet"))[0]
        df = pd.read_parquet(parquet_file)
        assert (df["batch_id"] == sample_batch_ctx.batch_id).all()
        assert (df["source_name"] == "akshare").all()
        assert (df["interface_name"] == "stock_zh_a_hist").all()
        assert (df["extract_version"] == "v1.0").all()

    def test_request_params_json(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        parquet_file = list(batch_dir.glob("part-*.parquet"))[0]
        df = pd.read_parquet(parquet_file)
        params = json.loads(df["request_params_json"].iloc[0])
        assert params["symbol"] == "600519"

    def test_raw_record_hash_present(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        parquet_file = list(batch_dir.glob("part-*.parquet"))[0]
        df = pd.read_parquet(parquet_file)
        assert (df["raw_record_hash"].str.startswith("sha256:")).all()

    def test_schema_fingerprint_in_manifest(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        manifest = Manifest.load(batch_dir / MANIFEST_FILENAME)
        assert manifest.schema_fingerprint.startswith("sha256:")

    def test_custom_part_index(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx, part_index=5)
        parquet_files = list(batch_dir.glob("part-*.parquet"))
        assert len(parquet_files) == 1
        assert "part-005.parquet" in parquet_files[0].name


class TestRawWriterWriteBatch:
    def test_write_multiple_partitions(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        df1 = sample_df.iloc[:2].copy()
        df2 = sample_df.iloc[2:].copy()
        partitions = [{"df": df1}, {"df": df2}]
        batch_dir = writer.write_batch(partitions, sample_task, sample_batch_ctx)
        parquet_files = list(batch_dir.glob("part-*.parquet"))
        assert len(parquet_files) == 2

    def test_write_batch_manifest_record_count(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        df1 = sample_df.iloc[:2].copy()
        df2 = sample_df.iloc[2:].copy()
        partitions = [{"df": df1}, {"df": df2}]
        batch_dir = writer.write_batch(partitions, sample_task, sample_batch_ctx)
        manifest = Manifest.load(batch_dir / MANIFEST_FILENAME)
        assert manifest.record_count == 5
        assert manifest.file_count == 2

    def test_write_batch_skips_empty_partitions(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        df1 = sample_df.iloc[:2].copy()
        df_empty = pd.DataFrame()
        df2 = sample_df.iloc[2:].copy()
        partitions = [{"df": df1}, {"df": df_empty}, {"df": df2}]
        batch_dir = writer.write_batch(partitions, sample_task, sample_batch_ctx)
        parquet_files = list(batch_dir.glob("part-*.parquet"))
        assert len(parquet_files) == 2


class TestRawWriterAtomicWrite:
    def test_no_tmp_files_on_success(
        self, tmp_path, sample_df, sample_task, sample_batch_ctx
    ):
        writer = RawWriter(base_dir=str(tmp_path))
        batch_dir = writer.write(sample_df, sample_task, sample_batch_ctx)
        tmp_files = list(batch_dir.glob("*.tmp"))
        assert len(tmp_files) == 0

    def test_no_tmp_files_on_failure(self, tmp_path, sample_task, sample_batch_ctx):
        writer = RawWriter(base_dir=str(tmp_path))

        class Unserializable:
            pass

        invalid_df = pd.DataFrame({"a": [Unserializable()]})
        with pytest.raises(Exception):
            writer.write(invalid_df, sample_task, sample_batch_ctx)
        batch_dir = (
            tmp_path
            / "cn"
            / "market_quote_daily"
            / "extract_date=2026-04-22"
            / f"batch_id={sample_batch_ctx.batch_id}"
        )
        if batch_dir.exists():
            tmp_files = list(batch_dir.glob("*.tmp"))
            assert len(tmp_files) == 0
