"""End-to-end data pipeline: Raw → Standardized → Quality → Served.

Wires the five data layers into a single callable entry point so that a
single ``run_one_batch()`` call takes a raw DataFrame all the way through
to the Served release.

Usage::

    from datetime import date
    from akshare_data.ingestion.pipeline import Pipeline

    pipeline = Pipeline()
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
    print(result.published, result.raw_path)
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.ingestion.models.batch import BatchContext
from akshare_data.ingestion.models.task import ExtractTask
from akshare_data.quality.engine import Layer, QualityEngine
from akshare_data.quality.gate import GateDecision, GateResult, QualityGate
from akshare_data.quality.quarantine import QuarantineStore
from akshare_data.raw.writer import RawWriter
from akshare_data.served.manifest import ReleaseManifest
from akshare_data.served.publisher import Publisher
from akshare_data.standardized.writer import StandardizedWriter

logger = logging.getLogger("akshare_data.pipeline")


# ---------------------------------------------------------------------------
# GateDecision adapter
# ---------------------------------------------------------------------------


class _GateDecisionBridge:
    """Bridges QualityGate.GateResult to Publisher's GateDecision Protocol.

    ``QualityGate.GateResult`` uses ``blocking_rules`` / ``warning_rules``,
    whereas the ``served.manifest.GateDecision`` Protocol expects
    ``failed_rules`` / ``warnings``.  This adapter bridges the gap.
    """

    def __init__(self, gate_result: GateResult) -> None:
        self._result = gate_result
        self._evaluated_at = datetime.now(timezone.utc)

    @property
    def dataset(self) -> str:
        return self._result.dataset

    @property
    def batch_id(self) -> str:
        return self._result.batch_id

    @property
    def gate_passed(self) -> bool:
        return self._result.gate_passed

    @property
    def evaluated_at(self) -> datetime:
        return self._evaluated_at

    @property
    def failed_rules(self) -> List[str]:
        """Protocol field name; maps to GateResult.blocking_rules."""
        return self._result.blocking_rules

    @property
    def warnings(self) -> List[str]:
        """Protocol field name; maps to GateResult.warning_rules."""
        return self._result.warning_rules


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Outcome of a single ``Pipeline.run_one_batch()`` call.

    Attributes
    ----------
    batch_id:
        Unique batch identifier threaded through all layers.
    dataset:
        Canonical dataset name.
    source_name:
        Data source adapter name.
    raw_path:
        Directory where Raw (L0) parquet files were written;
        ``None`` if the write step failed.
    standardized_paths:
        Mapping of partition_value → directory path for Standardized (L1).
    gate_result:
        Quality gate evaluation outcome; ``None`` if quality check errored.
    release_manifest:
        Served (L2) release manifest; ``None`` if gate blocked or publish failed.
    published:
        ``True`` when data was successfully published to Served.
    pipeline_duration_ms:
        Wall-clock time for the entire pipeline run.
    errors:
        Non-fatal error messages collected from individual pipeline steps.
    """

    batch_id: str
    dataset: str
    source_name: str
    raw_path: Optional[Path]
    standardized_paths: Dict[str, Path]
    gate_result: Optional[GateResult]
    release_manifest: Optional[ReleaseManifest]
    published: bool
    pipeline_duration_ms: float
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline:
    """Orchestrates the Raw → Standardized → Quality → Served data flow.

    Parameters
    ----------
    raw_dir:
        Root directory for Raw (L0) storage.
    standardized_dir:
        Root directory for Standardized (L1) storage.
    served_dir:
        Root directory for Served (L2) storage.
    quality_config_dir:
        Directory that contains per-dataset YAML quality configs
        (e.g. ``config/quality/market_quote_daily.yaml``).
        If the file for a given dataset does not exist, quality checks are
        skipped and the gate defaults to PASSED.
    """

    def __init__(
        self,
        raw_dir: str | Path = "data/raw",
        standardized_dir: str | Path = "data/standardized",
        served_dir: str | Path = "data/served",
        quality_config_dir: str | Path = "config/quality",
    ) -> None:
        raw_path = Path(raw_dir)
        self._raw_writer = RawWriter(base_dir=str(raw_path))
        self._std_writer = StandardizedWriter(base_dir=str(standardized_dir))
        self._publisher = Publisher(served_dir=Path(served_dir))
        self._quarantine = QuarantineStore(
            quarantine_dir=str(raw_path.parent / "quarantine")
        )
        self._quality_config_dir = Path(quality_config_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_one_batch(
        self,
        df: pd.DataFrame,
        *,
        dataset: str,
        domain: str,
        source_name: str,
        interface_name: str,
        extract_date: date,
        partition_key: str = "trade_date",
        primary_key: List[str],
        schema: Optional[Dict[str, Any]] = None,
        schema_version: str = "v1",
        normalize_version: str = "v1",
        batch_id: Optional[str] = None,
    ) -> PipelineResult:
        """Run the full pipeline for a single batch of data.

        Execution order
        ---------------
        1. Generate ``batch_id`` (if not supplied).
        2. Build ``ExtractTask`` + ``BatchContext`` for traceability.
        3. **Raw write** — ``RawWriter.write()`` → ``raw_path``.
        4. **Standardized write** — ``StandardizedWriter.write()`` → ``standardized_paths``.
        5. **Quality check** — ``QualityEngine`` + ``QualityGate`` → ``GateResult``.
           Skipped (gate = PASSED) when no ``config/quality/{dataset}.yaml`` exists.
        6. **Publish** (only when gate passed and data is non-empty) —
           ``Publisher.publish()`` → ``ReleaseManifest``.
        7. **Quarantine** (only when gate blocked) — ``QuarantineStore.store()``.
        8. Return ``PipelineResult``.

        Non-critical step failures are captured in ``PipelineResult.errors``
        rather than propagating as exceptions.

        Parameters
        ----------
        df:
            Source DataFrame to process through all layers.
        dataset:
            Canonical dataset name, e.g. ``market_quote_daily``.
        domain:
            Logical domain, e.g. ``market``, ``macro``.
        source_name:
            Source adapter name, e.g. ``akshare``, ``lixinger``.
        interface_name:
            Source-specific function name, e.g. ``stock_zh_a_hist``.
        extract_date:
            Planned extraction date (used as the Raw physical partition).
        partition_key:
            Business time column used to partition Standardized data.
        primary_key:
            Columns that form the composite primary key.
        schema:
            Optional entity schema dict for Standardized validation.
        schema_version:
            Entity schema version tag (default ``"v1"``).
        normalize_version:
            Normalization rule version tag (default ``"v1"``).
        batch_id:
            Batch identifier; auto-generated as ``YYYYMMDD_<8-char uuid>``
            when ``None``.

        Returns
        -------
        PipelineResult
        """
        t_start = time.monotonic()

        # Step 1: resolve batch_id
        if not batch_id:
            batch_id = (
                datetime.now(timezone.utc).strftime("%Y%m%d")
                + f"_{uuid.uuid4().hex[:8]}"
            )

        errors: List[str] = []
        raw_path: Optional[Path] = None
        standardized_paths: Dict[str, Path] = {}
        gate_result: Optional[GateResult] = None
        release_manifest: Optional[ReleaseManifest] = None
        published = False

        logger.info(
            "Pipeline start: dataset=%s batch_id=%s source=%s extract_date=%s",
            dataset,
            batch_id,
            source_name,
            extract_date,
        )

        # Step 2: build task / batch context
        task = ExtractTask.new(
            batch_id=batch_id,
            dataset=dataset,
            domain=domain,
            source_name=source_name,
            interface_name=interface_name,
            params={},
            extract_date=extract_date,
        )
        batch_ctx = BatchContext(
            batch_id=batch_id,
            tasks=[task],
            domain=domain,
        )

        # Step 3: write to Raw layer
        try:
            raw_path = self._raw_writer.write(df, task, batch_ctx)
            logger.info("Raw write OK → %s", raw_path)
        except Exception as exc:
            msg = f"RawWriter.write failed: {exc}"
            logger.error(msg)
            errors.append(msg)

        # Step 4: write to Standardized layer
        try:
            standardized_paths = self._std_writer.write(
                df,
                dataset=dataset,
                domain=domain,
                partition_key=partition_key,
                primary_key=primary_key,
                schema=schema,
                batch_id=batch_id,
                source_name=source_name,
                interface_name=interface_name,
                normalize_version=normalize_version,
                schema_version=schema_version,
            )
            logger.info(
                "Standardized write OK → %d partition(s)", len(standardized_paths)
            )
        except Exception as exc:
            msg = f"StandardizedWriter.write failed: {exc}"
            logger.error(msg)
            errors.append(msg)

        # Step 5–6: quality check + gate evaluation
        try:
            gate_result = self._run_quality(df, dataset=dataset, batch_id=batch_id)
            logger.info(
                "Gate decision=%s blocking=%s",
                gate_result.decision.value,
                gate_result.blocking_rules,
            )
        except Exception as exc:
            msg = f"Quality check failed: {exc}"
            logger.error(msg)
            errors.append(msg)
            # Default to PASSED so the rest of the pipeline can still proceed
            gate_result = GateResult(
                decision=GateDecision.PASSED,
                dataset=dataset,
                batch_id=batch_id,
                layer="standardized",
            )

        # Step 7: publish when gate passes and there is data to publish
        if gate_result.gate_passed and not df.empty and standardized_paths:
            try:
                bridge = _GateDecisionBridge(gate_result)
                release_manifest = self._publisher.publish(
                    dataset=dataset,
                    df=df,
                    gate_decision=bridge,
                    schema_version=schema_version,
                    normalize_version=normalize_version,
                    partition_col=(
                        partition_key if partition_key in df.columns else None
                    ),
                )
                published = True
                logger.info(
                    "Published → release_version=%s records=%d",
                    release_manifest.release_version,
                    release_manifest.total_record_count,
                )
            except Exception as exc:
                msg = f"Publisher.publish failed: {exc}"
                logger.error(msg)
                errors.append(msg)

        # Step 8: quarantine when gate is blocked
        elif not gate_result.gate_passed:
            try:
                blocking = gate_result.blocking_rules
                rule_id = blocking[0] if blocking else "gate_blocked"
                self._quarantine.store(
                    dataset=dataset,
                    batch_id=batch_id,
                    layer="standardized",
                    failed_df=df if not df.empty else pd.DataFrame({"_empty": [True]}),
                    rule_id=rule_id,
                    reason=f"Gate blocked by rules: {blocking}",
                )
                logger.warning(
                    "Gate BLOCKED dataset=%s batch=%s → quarantined (rules=%s)",
                    dataset,
                    batch_id,
                    blocking,
                )
            except Exception as exc:
                msg = f"QuarantineStore.store failed: {exc}"
                logger.error(msg)
                errors.append(msg)

        duration_ms = (time.monotonic() - t_start) * 1000
        logger.info(
            "Pipeline done: dataset=%s batch_id=%s published=%s errors=%d duration_ms=%.1f",
            dataset,
            batch_id,
            published,
            len(errors),
            duration_ms,
        )

        return PipelineResult(
            batch_id=batch_id,
            dataset=dataset,
            source_name=source_name,
            raw_path=raw_path,
            standardized_paths=standardized_paths,
            gate_result=gate_result,
            release_manifest=release_manifest,
            published=published,
            pipeline_duration_ms=duration_ms,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_quality(
        self,
        df: pd.DataFrame,
        *,
        dataset: str,
        batch_id: str,
    ) -> GateResult:
        """Execute quality rules for the Standardized layer.

        Returns a default PASSED ``GateResult`` when no config file exists for
        the given dataset.
        """
        config_path = self._quality_config_dir / f"{dataset}.yaml"

        if not config_path.exists():
            logger.info(
                "No quality config at '%s' — gate defaults to PASSED", config_path
            )
            return GateResult(
                decision=GateDecision.PASSED,
                dataset=dataset,
                batch_id=batch_id,
                layer="standardized",
            )

        engine = QualityEngine()
        engine.load_config(config_path)
        results = engine.run(df, layer=Layer.STANDARDIZED)

        gate = QualityGate(engine=engine)
        return gate.evaluate(
            results,
            dataset=dataset,
            batch_id=batch_id,
            layer="standardized",
        )
