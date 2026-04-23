"""Unit tests for Raw (L0) replay engine.

Verifies that ReplayEngine can:
- Replay by batch_id
- Replay by extract_date range
- Replay by source + interface
- Apply custom processors
- Detect schema drift
- Generate replay reports
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from akshare_data.raw.manifest import MANIFEST_FILENAME, Manifest, save_schema_snapshot
from akshare_data.raw.replay import ReplayEngine
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
    schema_fingerprint: str = "sha256:test",
) -> Path:
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
        schema_fingerprint=schema_fingerprint,
    )
    manifest.save(batch_dir / MANIFEST_FILENAME)

    schema = [{"name": c, "dtype": str(df_with_sys[c].dtype)} for c in df_with_sys.columns]
    save_schema_snapshot(batch_dir, schema)

    return batch_dir


@pytest.fixture
def raw_tree(tmp_path: Path) -> Path:
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
class TestReplayEngine:
    def test_replay_by_batch_id(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        result = engine.replay_by_batch_id(
            "20260422_001", domain="cn", dataset="market_quote_daily",
        )
        assert result.batch_id == "20260422_001"
        assert result.record_count == 3
        assert result.schema_compatible
        assert result.errors == []
        assert "batch_id" not in result.df.columns
        assert "日期" in result.df.columns

    def test_replay_by_batch_id_not_found(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        with pytest.raises(FileNotFoundError):
            engine.replay_by_batch_id("nonexistent")

    def test_replay_by_batch_id_keep_system_columns(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
            strip_system_columns=False,
        )
        assert "batch_id" in result.df.columns
        assert "source_name" in result.df.columns

    def test_replay_by_date_range(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        report = engine.replay_by_date_range(
            domain="cn",
            dataset="market_quote_daily",
            start=date(2026, 4, 20),
            end=date(2026, 4, 21),
        )
        assert report.mode == "date_range"
        assert report.total_batches == 2
        assert report.successful_batches == 2
        assert report.failed_batches == 0
        assert report.total_records == 6

    def test_replay_by_date_range_no_matches(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        report = engine.replay_by_date_range(
            domain="cn",
            dataset="market_quote_daily",
            start=date(2025, 1, 1),
            end=date(2025, 1, 31),
        )
        assert report.total_batches == 0

    def test_replay_by_source_interface(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        report = engine.replay_by_source_interface(
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert report.mode == "source_interface"
        assert report.total_batches == 3
        assert report.total_records == 9

    def test_replay_by_source_interface_no_interface_filter(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        report = engine.replay_by_source_interface(source_name="akshare")
        assert report.total_batches == 4

    def test_replay_with_processor(self, raw_tree: Path):
        def add_column(df: pd.DataFrame, manifest: Manifest) -> pd.DataFrame:
            df = df.copy()
            df["replayed"] = True
            return df

        engine = ReplayEngine(base_dir=str(raw_tree))
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
            processor=add_column,
        )
        assert "replayed" in result.df.columns
        assert result.df["replayed"].all()

    def test_replay_with_processor_error(self, raw_tree: Path):
        def bad_processor(df: pd.DataFrame, manifest: Manifest) -> pd.DataFrame:
            raise ValueError("processor failed")

        engine = ReplayEngine(base_dir=str(raw_tree))
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
            processor=bad_processor,
        )
        assert len(result.errors) == 1
        assert "Processor error" in result.errors[0]

    def test_replay_schema_drift_detection(self, raw_tree: Path):
        engine = ReplayEngine(
            base_dir=str(raw_tree),
            reference_schemas={"market_quote_daily": "sha256:different"},
        )
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
        )
        assert not result.schema_compatible
        assert len(result.errors) == 1
        assert "Schema drift" in result.errors[0]

    def test_replay_schema_compatible(self, raw_tree: Path):
        engine = ReplayEngine(
            base_dir=str(raw_tree),
            reference_schemas={"market_quote_daily": "sha256:test"},
        )
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
        )
        assert result.schema_compatible

    def test_iter_replay_by_batch_id(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        results = list(
            engine.iter_replay(
                batch_id="20260422_001",
                domain="cn",
                dataset="market_quote_daily",
            )
        )
        assert len(results) == 1
        assert results[0].batch_id == "20260422_001"

    def test_iter_replay_by_date_range(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        results = list(
            engine.iter_replay(
                domain="cn",
                dataset="market_quote_daily",
                start=date(2026, 4, 20),
                end=date(2026, 4, 22),
            )
        )
        assert len(results) == 3

    def test_iter_replay_empty(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        results = list(
            engine.iter_replay(
                domain="cn",
                dataset="nonexistent",
            )
        )
        assert len(results) == 0

    def test_replay_report_success_rate(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        report = engine.replay_by_date_range(
            domain="cn",
            dataset="market_quote_daily",
            start=date(2026, 4, 20),
            end=date(2026, 4, 22),
        )
        assert report.success_rate == 1.0

    def test_replay_report_has_timestamps(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        report = engine.replay_by_date_range(
            domain="cn",
            dataset="market_quote_daily",
            start=date(2026, 4, 20),
            end=date(2026, 4, 22),
        )
        assert report.started_at is not None
        assert report.finished_at is not None

    def test_replay_result_df_full_includes_system_columns(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
        )
        assert "batch_id" in result.df_full.columns
        assert "source_name" in result.df_full.columns
        assert "batch_id" not in result.df.columns

    def test_replay_result_manifest_preserved(self, raw_tree: Path):
        engine = ReplayEngine(base_dir=str(raw_tree))
        result = engine.replay_by_batch_id(
            "20260422_001",
            domain="cn",
            dataset="market_quote_daily",
        )
        assert result.manifest.source_name == "akshare"
        assert result.manifest.interface_name == "stock_zh_a_hist"
        assert result.manifest.batch_id == "20260422_001"
