"""Unit tests for Raw (L0) reader.

Verifies that RawReader can:
- Read by batch_id, dataset, extract_date, extract_date range, source
- Discover batch directories
- Load manifests and schema snapshots
- Handle missing data gracefully
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from akshare_data.raw.manifest import MANIFEST_FILENAME, Manifest, save_schema_snapshot
from akshare_data.raw.reader import RawReader
from akshare_data.raw.system_fields import SYSTEM_FIELD_NAMES


def _make_business_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "日期": ["2026-04-01", "2026-04-02", "2026-04-03"],
            "开盘": [10.5, 10.6, 10.7],
            "收盘": [10.8, 10.9, 11.0],
            "成交量": [100000, 110000, 120000],
        }
    )


def _write_batch(
    base: Path,
    domain: str,
    dataset: str,
    extract_date: str,
    batch_id: str,
    source_name: str = "akshare",
    interface_name: str = "stock_zh_a_hist",
    df: pd.DataFrame | None = None,
) -> Path:
    """Write a complete batch directory with parquet, manifest, and schema."""
    batch_dir = base / domain / dataset / f"extract_date={extract_date}" / f"batch_id={batch_id}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    if df is None:
        df = _make_business_df()

    df_with_sys = df.copy()
    for col in SYSTEM_FIELD_NAMES:
        if col == "batch_id":
            df_with_sys[col] = batch_id
        elif col == "source_name":
            df_with_sys[col] = source_name
        elif col == "interface_name":
            df_with_sys[col] = interface_name
        elif col == "extract_date":
            df_with_sys[col] = extract_date
        else:
            df_with_sys[col] = "test"

    parquet_path = batch_dir / "part-000.parquet"
    df_with_sys.to_parquet(parquet_path, engine="pyarrow", index=False)

    manifest = Manifest.create(
        dataset=dataset,
        domain=domain,
        batch_id=batch_id,
        extract_date=date.fromisoformat(extract_date),
        source_name=source_name,
        interface_name=interface_name,
        request_params={"symbol": "600519"},
        record_count=len(df),
        file_count=1,
        schema_fingerprint="sha256:test",
    )
    manifest.save(batch_dir / MANIFEST_FILENAME)

    schema = [{"name": c, "dtype": str(df_with_sys[c].dtype)} for c in df_with_sys.columns]
    save_schema_snapshot(batch_dir, schema)

    return batch_dir


@pytest.fixture
def raw_tree(tmp_path: Path) -> Path:
    """Create a multi-batch raw tree for testing."""
    _write_batch(tmp_path, "cn", "market_quote_daily", "2026-04-20", "20260420_001",
                 interface_name="stock_zh_a_hist")
    _write_batch(tmp_path, "cn", "market_quote_daily", "2026-04-21", "20260421_001",
                 interface_name="stock_zh_a_hist")
    _write_batch(tmp_path, "cn", "market_quote_daily", "2026-04-22", "20260422_001",
                 interface_name="stock_zh_a_hist")
    _write_batch(tmp_path, "cn", "financial_indicator", "2026-04-22", "20260422_001",
                 interface_name="stock_financial_analysis_indicator")
    return tmp_path


@pytest.mark.unit
class TestRawReader:
    def test_read_by_batch_id(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_batch_id("20260422_001", domain="cn", dataset="market_quote_daily")
        assert len(df) == 3
        assert "batch_id" in df.columns
        assert df["batch_id"].iloc[0] == "20260422_001"

    def test_read_by_batch_id_not_found(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        with pytest.raises(FileNotFoundError, match="No batch directory found"):
            reader.read_by_batch_id("nonexistent")

    def test_read_by_dataset(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_dataset("cn", "market_quote_daily")
        assert len(df) == 9

    def test_read_by_dataset_with_extract_date(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_dataset(
            "cn", "market_quote_daily", extract_date=date(2026, 4, 21),
        )
        assert len(df) == 3

    def test_read_by_extract_date(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_extract_date(
            "cn", "market_quote_daily", date(2026, 4, 22),
        )
        assert len(df) == 3

    def test_read_by_extract_date_range(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_extract_date_range(
            "cn", "market_quote_daily",
            start=date(2026, 4, 20),
            end=date(2026, 4, 21),
        )
        assert len(df) == 6

    def test_read_by_source(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_source("akshare")
        assert len(df) == 12

    def test_read_by_source_with_interface(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_source("akshare", interface_name="stock_zh_a_hist")
        assert len(df) == 9

    def test_read_batch_direct(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        batch_dir = raw_tree / "cn" / "market_quote_daily" / "extract_date=2026-04-22" / "batch_id=20260422_001"
        df, manifest = reader.read_batch(batch_dir)
        assert len(df) == 3
        assert manifest.batch_id == "20260422_001"

    def test_read_batch_missing_manifest(self, tmp_path: Path):
        reader = RawReader(base_dir=str(tmp_path))
        batch_dir = tmp_path / "cn" / "test" / "extract_date=2026-04-22" / "batch_id=b1"
        batch_dir.mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="Manifest not found"):
            reader.read_batch(batch_dir)

    def test_read_batch_with_partition_filter(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        batch_dir = raw_tree / "cn" / "market_quote_daily" / "extract_date=2026-04-22" / "batch_id=20260422_001"
        df, manifest = reader.read_batch(batch_dir, partitions=["part-000.parquet"])
        assert len(df) == 3

    def test_find_batch_dirs(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        dirs = list(reader.find_batch_dirs(batch_id="20260422_001"))
        assert len(dirs) == 2

    def test_find_batch_dirs_by_domain(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        dirs = list(reader.find_batch_dirs(domain="cn"))
        assert len(dirs) == 4

    def test_iter_batches(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        batches = list(reader.iter_batches(domain="cn", dataset="market_quote_daily"))
        assert len(batches) == 3
        for batch_dir, manifest in batches:
            assert manifest.domain == "cn"
            assert manifest.dataset == "market_quote_daily"

    def test_iter_batches_with_extract_date_filter(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        batches = list(
            reader.iter_batches(
                domain="cn",
                dataset="market_quote_daily",
                extract_date=date(2026, 4, 22),
            )
        )
        assert len(batches) == 1

    def test_iter_all_batches(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        batches = list(reader.iter_all_batches())
        assert len(batches) == 4

    def test_list_datasets(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        datasets = reader.list_datasets(domain="cn")
        assert "market_quote_daily" in datasets
        assert "financial_indicator" in datasets

    def test_list_domains(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        domains = reader.list_domains()
        assert "cn" in domains

    def test_get_schema_snapshot(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        batch_dir = raw_tree / "cn" / "market_quote_daily" / "extract_date=2026-04-22" / "batch_id=20260422_001"
        schema = reader.get_schema_snapshot(batch_dir)
        assert isinstance(schema, list)
        assert len(schema) > 0
        assert schema[0]["name"] is not None

    def test_empty_base_dir(self, tmp_path: Path):
        reader = RawReader(base_dir=str(tmp_path / "nonexistent"))
        assert reader.list_domains() == []

    def test_read_empty_dataset(self, raw_tree: Path):
        reader = RawReader(base_dir=str(raw_tree))
        df = reader.read_by_dataset("cn", "nonexistent_dataset")
        assert df.empty
