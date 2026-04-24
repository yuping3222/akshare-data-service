"""Integration tests for per-layer schema strict level control.

Verifies:
- AtomicWriter(strict_level="error"): raises SchemaValidationError on violation
- AtomicWriter(strict_level="warn"): warns + quarantines, does not raise
- AtomicWriter(strict_level="none"): silently accepts all data
- Backward compat: strict_schema bool param emits DeprecationWarning
- CacheManager.write(storage_layer=...): routes to correct strict_level per layer
"""
from __future__ import annotations

import json

import pandas as pd
import pytest

from akshare_data.core.config import CacheConfig
from akshare_data.store.manager import CacheManager, reset_cache_manager
from akshare_data.store.parquet import AtomicWriter
from akshare_data.store.validator import SchemaValidationError

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

TABLE = "test_strict_levels"
SCHEMA: dict[str, str] = {"id": "string", "value": "float64"}
PK: list[str] = ["id"]


@pytest.fixture()
def bad_data() -> pd.DataFrame:
    """DataFrame missing the 'value' column — triggers SchemaValidationError."""
    return pd.DataFrame({"id": ["row_a", "row_b"]})


# ---------------------------------------------------------------------------
# AtomicWriter strict_level="error"
# ---------------------------------------------------------------------------


class TestAtomicWriterErrorLevel:
    def test_raises_on_schema_violation(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="error")
        with pytest.raises(SchemaValidationError):
            writer.write(TABLE, "daily", bad_data, schema=SCHEMA, primary_key=PK)

    def test_no_parquet_file_written_on_failure(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="error")
        with pytest.raises(SchemaValidationError):
            writer.write(TABLE, "daily", bad_data, schema=SCHEMA, primary_key=PK)
        daily_dir = tmp_path / "daily" / TABLE
        assert not daily_dir.exists() or not list(daily_dir.rglob("*.parquet"))


# ---------------------------------------------------------------------------
# AtomicWriter strict_level="warn"
# ---------------------------------------------------------------------------


class TestAtomicWriterWarnLevel:
    def test_does_not_raise_on_schema_violation(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="warn")
        path = writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        assert path.exists()

    def test_quarantine_directory_created(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="warn")
        writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        assert (tmp_path / "quarantine" / TABLE).exists()

    def test_quarantine_json_has_correct_metadata(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="warn")
        writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        json_files = list((tmp_path / "quarantine" / TABLE).rglob("quarantine.json"))
        assert len(json_files) >= 1
        with open(json_files[0]) as f:
            q = json.load(f)
        assert q["dataset"] == TABLE
        assert q["layer"] == "raw"
        assert len(q["records"]) == len(bad_data)
        assert q["records"][0]["rule_id"] == "schema_validation_failed"

    def test_warning_is_logged(self, tmp_path, bad_data, caplog):
        writer = AtomicWriter(tmp_path, strict_level="warn")
        with caplog.at_level("WARNING"):
            writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        assert "Schema validation failed" in caplog.text


# ---------------------------------------------------------------------------
# AtomicWriter strict_level="none"
# ---------------------------------------------------------------------------


class TestAtomicWriterNoneLevel:
    def test_does_not_raise_on_schema_violation(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="none")
        path = writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        assert path.exists()

    def test_no_quarantine_created(self, tmp_path, bad_data):
        writer = AtomicWriter(tmp_path, strict_level="none")
        writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        assert not (tmp_path / "quarantine").exists()


# ---------------------------------------------------------------------------
# Backward compatibility: strict_schema bool param
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    def test_strict_schema_true_maps_to_error(self, tmp_path):
        with pytest.warns(DeprecationWarning, match="strict_schema bool is deprecated"):
            writer = AtomicWriter(tmp_path, strict_schema=True)
        assert writer.strict_level == "error"

    def test_strict_schema_false_maps_to_warn(self, tmp_path):
        with pytest.warns(DeprecationWarning, match="strict_schema bool is deprecated"):
            writer = AtomicWriter(tmp_path, strict_schema=False)
        assert writer.strict_level == "warn"

    def test_strict_schema_true_raises_on_violation(self, tmp_path, bad_data):
        with pytest.warns(DeprecationWarning):
            writer = AtomicWriter(tmp_path, strict_schema=True)
        with pytest.raises(SchemaValidationError):
            writer.write(TABLE, "daily", bad_data, schema=SCHEMA, primary_key=PK)

    def test_strict_schema_false_does_not_raise_on_violation(self, tmp_path, bad_data):
        with pytest.warns(DeprecationWarning):
            writer = AtomicWriter(tmp_path, strict_schema=False)
        path = writer.write(TABLE, "raw", bad_data, schema=SCHEMA, primary_key=PK)
        assert path.exists()


# ---------------------------------------------------------------------------
# CacheManager per-layer routing
# ---------------------------------------------------------------------------


class TestCacheManagerLayerRouting:
    @pytest.fixture(autouse=True)
    def _reset(self):
        reset_cache_manager()
        yield
        reset_cache_manager()

    def test_get_strict_level_for_layer_mapping(self, tmp_path):
        cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
        assert cache._get_strict_level_for_layer("raw") == "warn"
        assert cache._get_strict_level_for_layer("standardized") == "error"
        assert cache._get_strict_level_for_layer("served") == "error"
        assert cache._get_strict_level_for_layer("unknown_layer") == "error"
        assert cache._get_strict_level_for_layer(None) == "error"

    def test_raw_layer_does_not_raise(self, tmp_path, bad_data):
        cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
        cache.write(TABLE, bad_data, storage_layer="raw", schema=SCHEMA, primary_key=PK)

    def test_raw_layer_quarantines(self, tmp_path, bad_data):
        cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
        cache.write(TABLE, bad_data, storage_layer="raw", schema=SCHEMA, primary_key=PK)
        json_files = list((tmp_path / "quarantine" / TABLE).rglob("quarantine.json"))
        assert len(json_files) >= 1

    def test_standardized_layer_raises(self, tmp_path, bad_data):
        cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
        with pytest.raises(SchemaValidationError):
            cache.write(
                TABLE,
                bad_data,
                storage_layer="standardized",
                schema=SCHEMA,
                primary_key=PK,
            )

    def test_served_layer_raises(self, tmp_path, bad_data):
        cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
        with pytest.raises(SchemaValidationError):
            cache.write(
                TABLE,
                bad_data,
                storage_layer="served",
                schema=SCHEMA,
                primary_key=PK,
            )

    def test_served_layer_no_quarantine_on_raise(self, tmp_path, bad_data):
        """Served layer raises immediately — no quarantine records written."""
        cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
        with pytest.raises(SchemaValidationError):
            cache.write(
                TABLE,
                bad_data,
                storage_layer="served",
                schema=SCHEMA,
                primary_key=PK,
            )
        assert not (tmp_path / "quarantine").exists()
