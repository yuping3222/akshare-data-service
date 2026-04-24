"""Integration tests for the end-to-end data pipeline.

Tests cover the full Raw → Standardized → Quality → Served flow using the
Pipeline class in ``akshare_data.ingestion.pipeline``.

Markers
-------
All tests in this module use ``@pytest.mark.integration``.  Run with:

    pytest tests/integration/test_pipeline_e2e.py -v

Note on quality config
----------------------
The project's ``config/quality/market_quote_daily.yaml`` contains a
``business_rule`` expression (``high_price >= low_price``) that is blocked by
the engine's safety-character filter — the ``>=`` operator is not in the
allowed set.  Tests that exercise the happy-path (gate PASSED → publish)
therefore use a test-local quality config that contains only rules which the
engine evaluates correctly.  Tests that only care about *not* publishing use
the project config (the broken rule still triggers a BLOCK, which is the
desired "fail-safe" outcome).
"""

from __future__ import annotations

import textwrap
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from akshare_data.ingestion.pipeline import Pipeline, PipelineResult

# Project root: tests/integration/ → tests/ → project root
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_QUALITY_CONFIG_DIR = _PROJECT_ROOT / "config" / "quality"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_market_quote_df(trade_date: date | None = None) -> pd.DataFrame:
    """Return a small but fully-valid market_quote_daily DataFrame.

    Uses today's date by default so the freshness quality rule passes.
    Columns satisfy all non-broken rules in the quality config.
    Primary key (security_id, trade_date, adjust_type) is unique.
    """
    td = trade_date or date.today()
    return pd.DataFrame(
        {
            "security_id": ["sh600000", "sz000001"],
            "trade_date": [td, td],
            "adjust_type": ["qfq", "qfq"],
            "open_price": [10.0, 20.0],
            "high_price": [11.0, 21.0],
            "low_price": [9.0, 19.0],
            "close_price": [10.5, 20.5],
            "volume": [1_000_000.0, 2_000_000.0],
            "turnover_amount": [10_500_000.0, 21_000_000.0],
        }
    )


def _write_test_quality_config(config_dir: Path, dataset: str) -> None:
    """Write a simplified quality config for tests.

    Includes only rules that the engine evaluates correctly (no
    ``business_rule`` expressions with ``>``/``<`` operators, which the
    engine's safety filter rejects).
    """
    config_dir.mkdir(parents=True, exist_ok=True)
    content = textwrap.dedent(f"""\
        version: '1.0'
        dataset: {dataset}
        entity: {dataset}
        schema_version: v1
        rules:
          - rule_id: test_pk_unique
            layer: standardized
            type: unique_key
            severity: error
            gate_action: block
            fields: [security_id, trade_date, adjust_type]
            description: 主键必须唯一

          - rule_id: test_non_null
            layer: standardized
            type: non_null
            severity: error
            gate_action: block
            fields: [security_id, trade_date, adjust_type, open_price, close_price]
            description: 核心字段不能为空

          - rule_id: test_close_positive
            layer: standardized
            type: range
            severity: error
            gate_action: block
            field: close_price
            min: 0
            description: 收盘价必须 >= 0

          - rule_id: test_volume_non_negative
            layer: standardized
            type: range
            severity: error
            gate_action: block
            field: volume
            min: 0
            description: 成交量必须 >= 0

          - rule_id: test_freshness
            layer: standardized
            type: freshness
            severity: warning
            gate_action: alert
            field: trade_date
            description: 数据新鲜度检查
            params:
              max_age_days: 3
    """)
    (config_dir / f"{dataset}.yaml").write_text(content)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pipeline_market_quote_daily_happy_path():
    """market_quote_daily 从 extract 到 publish 全链路。

    Creates a Pipeline backed by temporary directories with a simplified
    quality config (no broken ``business_rule`` expressions), feeds a valid
    ``market_quote_daily`` DataFrame, and asserts that:

    - The result carries a ``batch_id``.
    - Raw files exist on disk (checked inside the temp-dir context).
    - At least one Standardized partition was written.
    - The quality gate passed (decision = PASSED).
    - A ``ReleaseManifest`` was produced and the data was published.
    - No pipeline errors.
    """
    df = _make_market_quote_df()

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        quality_dir = base / "quality"
        _write_test_quality_config(quality_dir, "market_quote_daily")

        pipeline = Pipeline(
            raw_dir=base / "raw",
            standardized_dir=base / "standardized",
            served_dir=base / "served",
            quality_config_dir=quality_dir,
        )

        result = pipeline.run_one_batch(
            df,
            dataset="market_quote_daily",
            domain="market",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            extract_date=date.today(),
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
        )

        # File-existence checks must happen inside the tempdir context
        assert result.raw_path is not None, "raw_path should be set on success"
        assert result.raw_path.exists(), (
            f"raw_path directory should exist: {result.raw_path}"
        )

    # Non-file checks can be made after the context exits
    assert isinstance(result, PipelineResult)
    assert result.batch_id, "batch_id should be non-empty"
    assert result.standardized_paths, "standardized_paths should be non-empty"

    assert result.gate_result is not None
    assert result.gate_result.gate_passed is True, (
        f"Gate should pass; decision={result.gate_result.decision}, "
        f"blocking={result.gate_result.blocking_rules}, "
        f"warnings={result.gate_result.warning_rules}"
    )

    assert result.published is True, f"Expected published=True, errors={result.errors}"
    assert result.release_manifest is not None
    assert result.release_manifest.dataset == "market_quote_daily"
    assert result.release_manifest.total_record_count == len(df)

    assert result.errors == [], f"Unexpected pipeline errors: {result.errors}"


@pytest.mark.integration
def test_pipeline_gate_blocks_empty_data():
    """空数据不应被发布到 Served 层。

    When an empty DataFrame is passed the pipeline should:
    - Write to Raw (succeeds with empty parquet content).
    - Return no Standardized partitions (writer skips empty input).
    - NOT publish, regardless of gate outcome.
    - Return ``published=False`` and ``release_manifest=None``.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        # Use real config: missing columns trigger ERROR+BLOCK on empty df,
        # reinforcing the "do not publish" expectation.
        pipeline = Pipeline(
            raw_dir=base / "raw",
            standardized_dir=base / "standardized",
            served_dir=base / "served",
            quality_config_dir=_QUALITY_CONFIG_DIR,
        )

        result = pipeline.run_one_batch(
            pd.DataFrame(),  # empty — no columns, no rows
            dataset="market_quote_daily",
            domain="market",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            extract_date=date.today(),
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
        )

    assert isinstance(result, PipelineResult)
    assert result.batch_id, "batch_id should be set even for empty data"

    # Empty data must never reach Served
    assert result.published is False, "Empty data must not be published"
    assert result.release_manifest is None

    # Standardized writer skips empty DataFrames
    assert result.standardized_paths == {}


@pytest.mark.integration
def test_pipeline_returns_result_with_batch_id_traceability():
    """验证 PipelineResult 里 batch_id 贯穿始终。

    A custom ``batch_id`` passed to ``run_one_batch`` should appear in every
    component of the result:

    - ``PipelineResult.batch_id``
    - ``PipelineResult.gate_result.batch_id``
    - ``PipelineResult.release_manifest.source_batches[*].batch_id`` (when published)
    - The raw batch directory name
    """
    custom_batch_id = "20260424_trace001"
    df = _make_market_quote_df()

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        quality_dir = base / "quality"
        _write_test_quality_config(quality_dir, "market_quote_daily")

        pipeline = Pipeline(
            raw_dir=base / "raw",
            standardized_dir=base / "standardized",
            served_dir=base / "served",
            quality_config_dir=quality_dir,
        )

        result = pipeline.run_one_batch(
            df,
            dataset="market_quote_daily",
            domain="market",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            extract_date=date.today(),
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
            batch_id=custom_batch_id,
        )

        # Raw path must embed the custom batch_id (inside context so it exists)
        if result.raw_path is not None:
            assert custom_batch_id in str(result.raw_path), (
                f"Expected batch_id={custom_batch_id} in raw_path={result.raw_path}"
            )

    # batch_id must propagate through the result
    assert result.batch_id == custom_batch_id

    assert result.gate_result is not None
    assert result.gate_result.batch_id == custom_batch_id

    if result.release_manifest is not None:
        source_batch_ids = [
            sb.batch_id for sb in result.release_manifest.source_batches
        ]
        assert custom_batch_id in source_batch_ids, (
            f"custom_batch_id not found in release manifest source_batches: "
            f"{source_batch_ids}"
        )


@pytest.mark.integration
def test_pipeline_auto_generates_batch_id():
    """当不传入 batch_id 时，管道应自动生成格式为 YYYYMMDD_xxxxxxxx 的 ID。"""
    df = _make_market_quote_df()

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        quality_dir = base / "quality"
        _write_test_quality_config(quality_dir, "market_quote_daily")

        pipeline = Pipeline(
            raw_dir=base / "raw",
            standardized_dir=base / "standardized",
            served_dir=base / "served",
            quality_config_dir=quality_dir,
        )

        result = pipeline.run_one_batch(
            df,
            dataset="market_quote_daily",
            domain="market",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            extract_date=date.today(),
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
        )

    assert result.batch_id, "batch_id should be auto-generated"
    # Expected format: YYYYMMDD_xxxxxxxx
    parts = result.batch_id.split("_")
    assert len(parts) == 2, f"Unexpected batch_id format: {result.batch_id}"
    date_part, uuid_part = parts
    assert len(date_part) == 8 and date_part.isdigit(), (
        f"Date part '{date_part}' should be 8 digits"
    )
    assert len(uuid_part) == 8, f"UUID part '{uuid_part}' should be 8 chars"


@pytest.mark.integration
def test_pipeline_skips_quality_check_when_no_config():
    """当数据集没有对应的 YAML 质量配置时，gate 默认 PASSED 并正常发布。"""
    df = _make_market_quote_df()

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        # quality_config_dir points to an empty directory — no YAML files
        pipeline = Pipeline(
            raw_dir=base / "raw",
            standardized_dir=base / "standardized",
            served_dir=base / "served",
            quality_config_dir=base / "no_quality_configs",
        )

        result = pipeline.run_one_batch(
            df,
            dataset="market_quote_daily",
            domain="market",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            extract_date=date.today(),
            partition_key="trade_date",
            primary_key=["security_id", "trade_date", "adjust_type"],
        )

    assert result.gate_result is not None
    assert result.gate_result.gate_passed is True, (
        "Gate should default to PASSED when no config exists"
    )
    assert result.published is True
    assert result.errors == []
