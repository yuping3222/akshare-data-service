"""Contract tests for Raw (L0) replay functionality.

Verifies that:
- Raw manifest structure conforms to 20-raw-spec.md
- Raw system fields are complete and correct
- Schema fingerprint is computable and stable
- Batch directory structure follows extract_date/batch_id convention
- Replay can reconstruct original records from a batch
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from akshare_data.raw.manifest import (
    Manifest,
    MANIFEST_FILENAME,
    SCHEMA_FILENAME,
    MANIFEST_VERSION,
    save_schema_snapshot,
    load_schema_snapshot,
)
from akshare_data.raw.system_fields import (
    SYSTEM_FIELDS,
    get_system_field_names,
    get_system_field_types,
    is_system_field,
)
from akshare_data.raw.schema_fingerprint import (
    compute_schema_fingerprint,
    describe_schema,
    schemas_match,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_manifest_data() -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "dataset": "market_quote_daily",
        "domain": "cn",
        "batch_id": "20260422_001",
        "extract_date": "2026-04-22",
        "source_name": "akshare",
        "interface_name": "stock_zh_a_hist",
        "request_params": {
            "symbol": "600519",
            "start_date": "2026-04-01",
            "end_date": "2026-04-22",
        },
        "record_count": 15,
        "file_count": 1,
        "schema_fingerprint": "sha256:abc123",
        "extract_version": "v1.0",
        "status": "success",
    }


@pytest.fixture
def sample_business_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "日期": ["2026-04-01", "2026-04-02", "2026-04-03"],
            "开盘": [10.5, 10.6, 10.7],
            "最高": [11.0, 11.1, 11.2],
            "最低": [10.0, 10.1, 10.2],
            "收盘": [10.8, 10.9, 11.0],
            "成交量": [100000, 110000, 120000],
            "成交额": [1080000.0, 1199000.0, 1320000.0],
        }
    )


# ---------------------------------------------------------------------------
# Manifest contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.replay
class TestManifestContract:
    """Manifest structure must conform to 20-raw-spec.md §6."""

    REQUIRED_KEYS = {
        "manifest_version", "dataset", "domain", "batch_id", "extract_date",
        "source_name", "interface_name", "request_params", "record_count",
        "file_count", "schema_fingerprint", "extract_version", "status",
    }

    def test_manifest_create_roundtrip(self, sample_manifest_data: dict):
        m = Manifest.from_dict(sample_manifest_data)
        restored = m.to_dict()
        for key in self.REQUIRED_KEYS:
            assert key in restored
            assert restored[key] == sample_manifest_data[key]

    def test_manifest_create_factory(self):
        m = Manifest.create(
            dataset="market_quote_daily",
            domain="cn",
            batch_id="20260422_001",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            request_params={"symbol": "600519"},
            record_count=10,
            file_count=1,
            schema_fingerprint="sha256:test",
        )
        assert m.manifest_version == MANIFEST_VERSION
        assert m.dataset == "market_quote_daily"
        assert m.domain == "cn"
        assert m.batch_id == "20260422_001"
        assert m.extract_date == "2026-04-22"
        assert m.status == "success"

    def test_manifest_to_json_roundtrip(self, sample_manifest_data: dict):
        m = Manifest.from_dict(sample_manifest_data)
        json_str = m.to_json()
        m2 = Manifest.from_json(json_str)
        assert m2.dataset == m.dataset
        assert m2.batch_id == m.batch_id
        assert m2.record_count == m.record_count

    def test_manifest_save_load(self, sample_manifest_data: dict, tmp_path: Path):
        m = Manifest.from_dict(sample_manifest_data)
        manifest_path = tmp_path / MANIFEST_FILENAME
        m.save(manifest_path)
        assert manifest_path.exists()
        m2 = Manifest.load(manifest_path)
        assert m2.dataset == m.dataset
        assert m2.batch_id == m.batch_id

    def test_manifest_version_is_1_0(self):
        assert MANIFEST_VERSION == "1.0"

    def test_manifest_has_all_required_keys(self, sample_manifest_data: dict):
        actual = set(sample_manifest_data.keys())
        assert self.REQUIRED_KEYS <= actual


# ---------------------------------------------------------------------------
# System fields contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.replay
class TestRawSystemFields:
    """Raw system fields must conform to 20-raw-spec.md §4."""

    EXPECTED_RAW_SYSTEM_FIELDS = {
        "batch_id", "source_name", "interface_name", "request_params_json",
        "request_time", "ingest_time", "extract_date", "extract_version",
        "source_schema_fingerprint", "raw_record_hash",
    }

    def test_system_field_names_complete(self):
        actual = set(get_system_field_names())
        assert actual == self.EXPECTED_RAW_SYSTEM_FIELDS

    def test_system_field_types_defined(self):
        types = get_system_field_types()
        for field in self.EXPECTED_RAW_SYSTEM_FIELDS:
            assert field in types, f"Missing type for: {field}"

    def test_system_field_types_match_spec(self):
        expected_types = {
            "batch_id": "string",
            "source_name": "string",
            "interface_name": "string",
            "request_params_json": "string",
            "request_time": "timestamp",
            "ingest_time": "timestamp",
            "extract_date": "date",
            "extract_version": "string",
            "source_schema_fingerprint": "string",
            "raw_record_hash": "string",
        }
        actual = get_system_field_types()
        for field, expected_type in expected_types.items():
            assert actual[field] == expected_type, (
                f"{field} type mismatch: got {actual[field]}, expected {expected_type}"
            )

    @pytest.mark.parametrize("field", [
        "batch_id", "source_name", "ingest_time", "raw_record_hash",
    ])
    def test_system_field_is_recognized(self, field: str):
        assert is_system_field(field)

    def test_business_field_not_recognized(self):
        assert not is_system_field("close_price")
        assert not is_system_field("security_id")

    def test_system_fields_list_complete(self):
        assert len(SYSTEM_FIELDS) == len(self.EXPECTED_RAW_SYSTEM_FIELDS)
        for sf in SYSTEM_FIELDS:
            assert sf.name in self.EXPECTED_RAW_SYSTEM_FIELDS
            assert sf.required is True


# ---------------------------------------------------------------------------
# Schema fingerprint contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.replay
class TestSchemaFingerprint:
    """Schema fingerprint must be deterministic and stable."""

    def test_fingerprint_is_deterministic(self, sample_business_df: pd.DataFrame):
        fp1 = compute_schema_fingerprint(sample_business_df)
        fp2 = compute_schema_fingerprint(sample_business_df)
        assert fp1 == fp2

    def test_fingerprint_format(self, sample_business_df: pd.DataFrame):
        fp = compute_schema_fingerprint(sample_business_df)
        assert fp.startswith("sha256:")
        assert len(fp) == len("sha256:") + 64

    def test_fingerprint_excludes_system_columns(self, sample_business_df: pd.DataFrame):
        df_with_sys = sample_business_df.copy()
        df_with_sys["batch_id"] = "b1"
        df_with_sys["ingest_time"] = datetime.now(timezone.utc)

        fp_without_sys = compute_schema_fingerprint(
            df_with_sys, exclude_columns=["batch_id", "ingest_time"]
        )
        fp_without_sys2 = compute_schema_fingerprint(
            sample_business_df
        )
        assert fp_without_sys == fp_without_sys2

    def test_fingerprint_changes_on_column_add(self, sample_business_df: pd.DataFrame):
        fp1 = compute_schema_fingerprint(sample_business_df)
        df2 = sample_business_df.copy()
        df2["new_col"] = 0
        fp2 = compute_schema_fingerprint(df2)
        assert fp1 != fp2

    def test_fingerprint_changes_on_type_change(self, sample_business_df: pd.DataFrame):
        fp1 = compute_schema_fingerprint(sample_business_df)
        df2 = sample_business_df.copy()
        df2["成交量"] = df2["成交量"].astype(float)
        fp2 = compute_schema_fingerprint(df2)
        if sample_business_df["成交量"].dtype != float:
            assert fp1 != fp2

    def test_schemas_match_helper(self):
        assert schemas_match("sha256:abc", "sha256:abc")
        assert not schemas_match("sha256:abc", "sha256:def")

    def test_describe_schema(self, sample_business_df: pd.DataFrame):
        desc = describe_schema(sample_business_df)
        assert isinstance(desc, list)
        assert len(desc) == len(sample_business_df.columns)
        for entry in desc:
            assert "name" in entry
            assert "dtype" in entry


# ---------------------------------------------------------------------------
# Schema snapshot contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.replay
class TestSchemaSnapshot:
    """_schema.json must be readable and well-formed."""

    def test_save_and_load_schema_snapshot(self, sample_business_df: pd.DataFrame, tmp_path: Path):
        schema = describe_schema(sample_business_df)
        save_schema_snapshot(tmp_path, schema)
        schema_file = tmp_path / SCHEMA_FILENAME
        assert schema_file.exists()
        loaded = load_schema_snapshot(tmp_path)
        assert len(loaded) == len(schema)
        assert loaded[0]["name"] == schema[0]["name"]

    def test_load_missing_schema_returns_empty(self, tmp_path: Path):
        result = load_schema_snapshot(tmp_path)
        assert result == []


# ---------------------------------------------------------------------------
# Batch directory structure contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.replay
class TestBatchDirectoryStructure:
    """Raw batch directories must follow extract_date/batch_id convention."""

    def test_expected_path_pattern(self):
        """
        Expected: data/raw/<domain>/<dataset>/extract_date=<YYYY-MM-DD>/batch_id=<batch_id>/
        """
        domain = "cn"
        dataset = "market_quote_daily"
        extract_date = "2026-04-22"
        batch_id = "20260422_001"

        expected_parts = [
            "data", "raw", domain, dataset,
            f"extract_date={extract_date}",
            f"batch_id={batch_id}",
        ]
        path = Path(*expected_parts)
        assert "extract_date=" in str(path)
        assert "batch_id=" in str(path)

    def test_batch_dir_contains_manifest(self, sample_manifest_data: dict, tmp_path: Path):
        batch_dir = tmp_path / "cn" / "market_quote_daily" / "extract_date=2026-04-22" / "batch_id=20260422_001"
        batch_dir.mkdir(parents=True)

        m = Manifest.from_dict(sample_manifest_data)
        m.save(batch_dir / MANIFEST_FILENAME)

        assert (batch_dir / MANIFEST_FILENAME).exists()
        loaded = Manifest.load(batch_dir / MANIFEST_FILENAME)
        assert loaded.dataset == "market_quote_daily"
        assert loaded.domain == "cn"

    def test_batch_dir_contains_schema_snapshot(self, sample_business_df: pd.DataFrame, tmp_path: Path):
        batch_dir = tmp_path / "cn" / "market_quote_daily" / "extract_date=2026-04-22" / "batch_id=20260422_001"
        batch_dir.mkdir(parents=True)

        schema = describe_schema(sample_business_df)
        save_schema_snapshot(batch_dir, schema)

        assert (batch_dir / SCHEMA_FILENAME).exists()
        loaded = load_schema_snapshot(batch_dir)
        assert len(loaded) > 0


# ---------------------------------------------------------------------------
# Replay semantics contract
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.replay
class TestReplaySemantics:
    """Replay must work by batch_id or extract_date, not by trade_date."""

    def test_manifest_identifies_batch(self, sample_manifest_data: dict):
        m = Manifest.from_dict(sample_manifest_data)
        assert m.batch_id
        assert m.extract_date
        assert m.dataset

    def test_manifest_is_extraction_event_not_business_snapshot(self, sample_manifest_data: dict):
        """Manifest records the extraction event, not a business-date snapshot."""
        m = Manifest.from_dict(sample_manifest_data)
        assert m.extract_date
        assert m.request_params
        assert m.source_name
        assert m.interface_name

    def test_replay_by_batch_id(self, sample_manifest_data: dict, tmp_path: Path):
        batch_dir = tmp_path / "cn" / "market_quote_daily" / "extract_date=2026-04-22" / "batch_id=20260422_001"
        batch_dir.mkdir(parents=True)

        m = Manifest.from_dict(sample_manifest_data)
        m.save(batch_dir / MANIFEST_FILENAME)

        loaded = Manifest.load(batch_dir / MANIFEST_FILENAME)
        assert loaded.batch_id == "20260422_001"
        assert loaded.dataset == "market_quote_daily"

    def test_replay_by_extract_date_range(self, tmp_path: Path):
        base = tmp_path / "cn" / "market_quote_daily"
        for day in ["2026-04-20", "2026-04-21", "2026-04-22"]:
            d = base / f"extract_date={day}" / f"batch_id={day.replace('-', '')}_001"
            d.mkdir(parents=True)
            m = Manifest.create(
                dataset="market_quote_daily",
                domain="cn",
                batch_id=f"{day.replace('-', '')}_001",
                extract_date=date.fromisoformat(day),
                source_name="akshare",
                interface_name="stock_zh_a_hist",
                request_params={},
                record_count=5,
                file_count=1,
                schema_fingerprint="sha256:test",
            )
            m.save(d / MANIFEST_FILENAME)

        manifests = list(base.rglob(MANIFEST_FILENAME))
        assert len(manifests) == 3
