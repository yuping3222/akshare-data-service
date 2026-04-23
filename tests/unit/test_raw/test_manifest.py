"""Tests for manifest module."""

import tempfile
from datetime import date
from pathlib import Path


from akshare_data.raw.manifest import (
    Manifest,
    MANIFEST_VERSION,
    MANIFEST_FILENAME,
    save_schema_snapshot,
    load_schema_snapshot,
)


class TestManifestCreate:
    def test_minimal_creation(self):
        m = Manifest.create(
            dataset="market_quote_daily",
            domain="cn",
            batch_id="20260422_001",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            request_params={"symbol": "600519"},
            record_count=15,
            file_count=1,
            schema_fingerprint="sha256:abc123",
        )
        assert m.manifest_version == MANIFEST_VERSION
        assert m.dataset == "market_quote_daily"
        assert m.domain == "cn"
        assert m.batch_id == "20260422_001"
        assert m.extract_date == "2026-04-22"
        assert m.record_count == 15
        assert m.status == "success"

    def test_custom_extract_version(self):
        m = Manifest.create(
            dataset="test",
            domain="cn",
            batch_id="b1",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="test_fn",
            request_params={},
            record_count=0,
            file_count=0,
            schema_fingerprint="sha256:x",
            extract_version="v2.0",
        )
        assert m.extract_version == "v2.0"

    def test_custom_status(self):
        m = Manifest.create(
            dataset="test",
            domain="cn",
            batch_id="b1",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="test_fn",
            request_params={},
            record_count=0,
            file_count=0,
            schema_fingerprint="sha256:x",
            status="failed",
        )
        assert m.status == "failed"


class TestManifestSerialization:
    def test_to_dict(self):
        m = Manifest.create(
            dataset="test",
            domain="cn",
            batch_id="b1",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="test_fn",
            request_params={"key": "value"},
            record_count=5,
            file_count=1,
            schema_fingerprint="sha256:abc",
        )
        d = m.to_dict()
        assert d["manifest_version"] == MANIFEST_VERSION
        assert d["request_params"] == {"key": "value"}

    def test_to_json_roundtrip(self):
        m = Manifest.create(
            dataset="test",
            domain="cn",
            batch_id="b1",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="test_fn",
            request_params={"a": 1},
            record_count=10,
            file_count=2,
            schema_fingerprint="sha256:xyz",
            files=["part-000.parquet", "part-001.parquet"],
        )
        json_str = m.to_json()
        m2 = Manifest.from_json(json_str)
        assert m2.batch_id == m.batch_id
        assert m2.record_count == m.record_count
        assert m2.files == m.files

    def test_from_dict(self):
        data = {
            "manifest_version": "1.0",
            "dataset": "test",
            "domain": "cn",
            "batch_id": "b1",
            "extract_date": "2026-04-22",
            "source_name": "akshare",
            "interface_name": "test_fn",
            "request_params": {},
            "record_count": 0,
            "file_count": 0,
            "schema_fingerprint": "sha256:x",
            "extract_version": "v1.0",
            "status": "success",
        }
        m = Manifest.from_dict(data)
        assert m.dataset == "test"


class TestManifestFileIO:
    def test_save_and_load(self):
        m = Manifest.create(
            dataset="market_quote_daily",
            domain="cn",
            batch_id="20260422_001",
            extract_date=date(2026, 4, 22),
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            request_params={"symbol": "600519"},
            record_count=15,
            file_count=1,
            schema_fingerprint="sha256:abc",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / MANIFEST_FILENAME
            m.save(path)
            assert path.exists()
            m2 = Manifest.load(path)
            assert m2.batch_id == m.batch_id
            assert m2.record_count == m.record_count


class TestSchemaSnapshot:
    def test_save_and_load(self):
        schema = [
            {"name": "date", "dtype": "datetime64[ns]"},
            {"name": "close", "dtype": "float64"},
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            save_schema_snapshot(Path(tmpdir), schema)
            loaded = load_schema_snapshot(Path(tmpdir))
            assert loaded == schema

    def test_load_missing_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            loaded = load_schema_snapshot(Path(tmpdir))
            assert loaded == []
