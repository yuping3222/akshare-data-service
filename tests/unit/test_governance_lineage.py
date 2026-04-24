"""Unit tests for governance lineage utilities."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

def _load_lineage_tracker_class() -> type:
    lineage_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akshare_data"
        / "governance"
        / "lineage.py"
    )
    module_name = "akshare_data_governance_lineage_test_module"
    spec = importlib.util.spec_from_file_location(module_name, lineage_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load lineage module spec for tests")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module.LineageTracker


def _write_mapping_file(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "mapping.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def test_load_mapping_config_records_batch(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    LineageTracker = _load_lineage_tracker_class()
    tracker = LineageTracker()
    config = _write_mapping_file(
        tmp_path,
        """
dataset: market_quote_daily
batch_id: b_001
schema_version: s_1
normalize_version: n_1
release_version: r_1
mappings:
  - standard_field: close_price
    source_name: akshare
    source_field: close
    interface_name: stock_zh_a_daily
    transform: rename
""".strip(),
    )

    loaded = tracker.load_mapping_config(str(config))

    assert loaded == 1
    sources = tracker.get_field_sources("market_quote_daily", "close_price")
    assert len(sources) == 1
    assert sources[0].source_name == "akshare"
    assert sources[0].release_version == "r_1"


def test_load_mapping_config_rejects_invalid_mapping(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    LineageTracker = _load_lineage_tracker_class()
    tracker = LineageTracker()
    config = _write_mapping_file(
        tmp_path,
        """
dataset: market_quote_daily
mappings:
  - source_name: akshare
    source_field: close
""".strip(),
    )

    with pytest.raises(
        ValueError, match=r"requires `standard_field` and `source_field`"
    ):
        tracker.load_mapping_config(str(config))


def test_build_release_manifest_with_release_validation() -> None:
    LineageTracker = _load_lineage_tracker_class()
    tracker = LineageTracker()
    tracker.record(
        dataset="market_quote_daily",
        standard_field="close_price",
        source_name="akshare",
        source_field="close",
        interface_name="stock_zh_a_daily",
        batch_id="b_001",
        schema_version="s_1",
        normalize_version="n_1",
        release_version="r_1",
    )

    manifest = tracker.build_release_manifest(
        dataset="market_quote_daily",
        release_version="r_1",
        batch_id="b_001",
    )

    assert manifest["dataset"] == "market_quote_daily"
    assert manifest["field_count"] == 1
    assert manifest["known_release_versions"] == ["r_1"]

    with pytest.raises(ValueError, match="Requested release_version does not match"):
        tracker.build_release_manifest(
            dataset="market_quote_daily",
            release_version="r_2",
            batch_id="b_001",
        )
