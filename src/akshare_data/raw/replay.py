"""Raw (L0) replay engine.

Replays Raw evidence through downstream pipelines without re-fetching
from source stations. Input is Raw batch data, not live API calls.

Supported replay modes:
- By batch_id
- By extract_date range
- By dataset + source_name + interface_name

Spec: docs/design/20-raw-spec.md §8
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional

import pandas as pd

from akshare_data.raw.manifest import Manifest
from akshare_data.raw.reader import RawReader
from akshare_data.raw.schema_fingerprint import schemas_match

logger = logging.getLogger(__name__)


@dataclass
class ReplayResult:
    """Result of a single batch replay.

    Attributes:
        batch_id: The replayed batch identifier.
        manifest: The loaded manifest for this batch.
        df: The replayed DataFrame (business columns only).
        df_full: The full DataFrame including system columns.
        record_count: Number of records replayed.
        schema_compatible: Whether the schema matches the reference.
        errors: List of error messages, if any.
    """

    batch_id: str
    manifest: Manifest
    df: pd.DataFrame
    df_full: pd.DataFrame
    record_count: int = 0
    schema_compatible: bool = True
    errors: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.record_count:
            self.record_count = len(self.df)


@dataclass
class ReplayReport:
    """Aggregate report for a replay session.

    Attributes:
        mode: Replay mode used (batch_id, date_range, source_interface).
        total_batches: Total number of batches processed.
        successful_batches: Batches replayed without errors.
        failed_batches: Batches that had errors.
        total_records: Total records replayed across all batches.
        results: Per-batch replay results.
        started_at: Replay start timestamp (ISO format).
        finished_at: Replay end timestamp (ISO format).
    """

    mode: str
    total_batches: int = 0
    successful_batches: int = 0
    failed_batches: int = 0
    total_records: int = 0
    results: List[ReplayResult] = field(default_factory=list)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    @property
    def success_rate(self) -> float:
        if self.total_batches == 0:
            return 1.0
        return self.successful_batches / self.total_batches


class ReplayEngine:
    """Replays Raw evidence through downstream pipelines.

    The replay engine reads stored Raw batches and emits DataFrames
    for downstream processing (normalization, validation, etc.).
    It does NOT re-fetch from source stations.

    Usage::

        engine = ReplayEngine(base_dir="data/raw")

        # Replay a single batch
        result = engine.replay_by_batch_id("20260422_001")

        # Replay with a custom processor
        def my_processor(df: pd.DataFrame, manifest: Manifest) -> pd.DataFrame:
            # transform df
            return df

        result = engine.replay_by_batch_id(
            "20260422_001",
            processor=my_processor,
        )

        # Replay a date range
        report = engine.replay_by_date_range(
            domain="cn",
            dataset="market_quote_daily",
            start=date(2026, 4, 1),
            end=date(2026, 4, 30),
        )
    """

    def __init__(
        self,
        base_dir: str = "data/raw",
        reference_schemas: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize the replay engine.

        Args:
            base_dir: Root directory of Raw data.
            reference_schemas: Optional mapping of dataset -> schema fingerprint
                for compatibility checking during replay.
        """
        self._reader = RawReader(base_dir)
        self._reference_schemas = reference_schemas or {}

    @property
    def reader(self) -> RawReader:
        return self._reader

    # ------------------------------------------------------------------
    # Replay by batch_id
    # ------------------------------------------------------------------

    def replay_by_batch_id(
        self,
        batch_id: str,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
        processor: Optional[Callable[[pd.DataFrame, Manifest], pd.DataFrame]] = None,
        strip_system_columns: bool = True,
    ) -> ReplayResult:
        """Replay a single batch by batch_id.

        Args:
            batch_id: Batch identifier to replay.
            domain: Optional domain filter for batch lookup.
            dataset: Optional dataset filter for batch lookup.
            processor: Optional callable to transform the DataFrame.
                Signature: (df, manifest) -> transformed_df.
            strip_system_columns: If True, remove system columns from
                the returned df (they remain in df_full).

        Returns:
            ReplayResult for the batch.

        Raises:
            FileNotFoundError: If no batch is found for the batch_id.
        """
        from akshare_data.raw.system_fields import SYSTEM_FIELD_NAMES

        batch_dirs = list(
            self._reader.find_batch_dirs(
                batch_id=batch_id,
                domain=domain,
                dataset=dataset,
            )
        )
        if not batch_dirs:
            raise FileNotFoundError(f"No batch directory found for batch_id={batch_id}")

        batch_dir = batch_dirs[0]
        df_full, manifest = self._reader.read_batch(batch_dir)

        errors: List[str] = []
        schema_compatible = True

        if self._reference_schemas and manifest.dataset in self._reference_schemas:
            ref_fp = self._reference_schemas[manifest.dataset]
            schema_compatible = schemas_match(
                manifest.schema_fingerprint,
                ref_fp,
            )
            if not schema_compatible:
                errors.append(
                    f"Schema drift detected for {manifest.dataset}: "
                    f"batch={manifest.schema_fingerprint}, "
                    f"reference={ref_fp}"
                )

        df = df_full.copy()
        if processor is not None:
            try:
                df = processor(df, manifest)
            except Exception as e:
                errors.append(f"Processor error: {e}")

        if strip_system_columns:
            sys_cols = [c for c in SYSTEM_FIELD_NAMES if c in df.columns]
            df = df.drop(columns=sys_cols, errors="ignore")

        return ReplayResult(
            batch_id=batch_id,
            manifest=manifest,
            df=df,
            df_full=df_full,
            schema_compatible=schema_compatible,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Replay by extract_date range
    # ------------------------------------------------------------------

    def replay_by_date_range(
        self,
        domain: str,
        dataset: str,
        start: date,
        end: date,
        processor: Optional[Callable[[pd.DataFrame, Manifest], pd.DataFrame]] = None,
        strip_system_columns: bool = True,
    ) -> ReplayReport:
        """Replay all batches within an extract_date range.

        Args:
            domain: Data domain.
            dataset: Canonical dataset name.
            start: Start date (inclusive).
            end: End date (inclusive).
            processor: Optional callable to transform each DataFrame.
            strip_system_columns: If True, remove system columns from returned df.

        Returns:
            ReplayReport with aggregate results.
        """
        from datetime import datetime, timezone

        report = ReplayReport(mode="date_range")
        report.started_at = datetime.now(timezone.utc).isoformat()

        for batch_dir, manifest in self._reader.iter_batches(
            domain=domain,
            dataset=dataset,
        ):
            ed = manifest.extract_date_parsed
            if ed is None or ed < start or ed > end:
                continue

            result = self._replay_single_batch(
                batch_dir,
                manifest,
                processor,
                strip_system_columns,
            )
            report.results.append(result)
            report.total_batches += 1
            report.total_records += result.record_count

            if result.errors:
                report.failed_batches += 1
            else:
                report.successful_batches += 1

        report.finished_at = datetime.now(timezone.utc).isoformat()
        return report

    # ------------------------------------------------------------------
    # Replay by source + interface
    # ------------------------------------------------------------------

    def replay_by_source_interface(
        self,
        source_name: str,
        interface_name: Optional[str] = None,
        processor: Optional[Callable[[pd.DataFrame, Manifest], pd.DataFrame]] = None,
        strip_system_columns: bool = True,
    ) -> ReplayReport:
        """Replay all batches from a specific source/interface.

        Args:
            source_name: Source adapter name.
            interface_name: Optional interface name filter.
            processor: Optional callable to transform each DataFrame.
            strip_system_columns: If True, remove system columns from returned df.

        Returns:
            ReplayReport with aggregate results.
        """
        from datetime import datetime, timezone

        report = ReplayReport(mode="source_interface")
        report.started_at = datetime.now(timezone.utc).isoformat()

        for batch_dir, manifest in self._reader.iter_all_batches():
            if manifest.source_name != source_name:
                continue
            if interface_name and manifest.interface_name != interface_name:
                continue

            result = self._replay_single_batch(
                batch_dir,
                manifest,
                processor,
                strip_system_columns,
            )
            report.results.append(result)
            report.total_batches += 1
            report.total_records += result.record_count

            if result.errors:
                report.failed_batches += 1
            else:
                report.successful_batches += 1

        report.finished_at = datetime.now(timezone.utc).isoformat()
        return report

    # ------------------------------------------------------------------
    # Iterator-style replay
    # ------------------------------------------------------------------

    def iter_replay(
        self,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
        batch_id: Optional[str] = None,
        start: Optional[date] = None,
        end: Optional[date] = None,
        processor: Optional[Callable[[pd.DataFrame, Manifest], pd.DataFrame]] = None,
        strip_system_columns: bool = True,
    ) -> Iterator[ReplayResult]:
        """Iterate replay results one batch at a time.

        This is useful for large replays where you want to process
        batches incrementally rather than collecting all results.

        Args:
            domain: Optional domain filter.
            dataset: Optional dataset filter.
            batch_id: Optional batch_id filter.
            start: Optional start date filter.
            end: Optional end date filter.
            processor: Optional callable to transform each DataFrame.
            strip_system_columns: If True, remove system columns.

        Yields:
            ReplayResult for each matching batch.
        """
        if batch_id:
            batch_dirs = list(
                self._reader.find_batch_dirs(
                    batch_id=batch_id,
                    domain=domain,
                    dataset=dataset,
                )
            )
            for batch_dir in batch_dirs:
                _, manifest = self._reader.read_batch(batch_dir)
                yield self._replay_single_batch(
                    batch_dir,
                    manifest,
                    processor,
                    strip_system_columns,
                )
            return

        for batch_dir, manifest in self._reader.iter_batches(
            domain=domain,
            dataset=dataset,
        ):
            if start or end:
                ed = manifest.extract_date_parsed
                if ed is None:
                    continue
                if start and ed < start:
                    continue
                if end and ed > end:
                    continue

            yield self._replay_single_batch(
                batch_dir,
                manifest,
                processor,
                strip_system_columns,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _replay_single_batch(
        self,
        batch_dir: Path,
        manifest: Manifest,
        processor: Optional[Callable[[pd.DataFrame, Manifest], pd.DataFrame]],
        strip_system_columns: bool,
    ) -> ReplayResult:
        """Replay a single batch directory.

        Args:
            batch_dir: Path to the batch directory.
            manifest: Loaded manifest for the batch.
            processor: Optional transformation callable.
            strip_system_columns: Whether to strip system columns.

        Returns:
            ReplayResult for this batch.
        """
        from akshare_data.raw.system_fields import SYSTEM_FIELD_NAMES

        df_full, manifest = self._reader.read_batch(batch_dir)

        errors: List[str] = []
        schema_compatible = True

        if self._reference_schemas and manifest.dataset in self._reference_schemas:
            ref_fp = self._reference_schemas[manifest.dataset]
            schema_compatible = schemas_match(
                manifest.schema_fingerprint,
                ref_fp,
            )
            if not schema_compatible:
                errors.append(
                    f"Schema drift detected for {manifest.dataset}: "
                    f"batch={manifest.schema_fingerprint}, "
                    f"reference={ref_fp}"
                )

        df = df_full.copy()
        if processor is not None:
            try:
                df = processor(df, manifest)
            except Exception as e:
                errors.append(f"Processor error: {e}")

        if strip_system_columns:
            sys_cols = [c for c in SYSTEM_FIELD_NAMES if c in df.columns]
            df = df.drop(columns=sys_cols, errors="ignore")

        return ReplayResult(
            batch_id=manifest.batch_id,
            manifest=manifest,
            df=df,
            df_full=df_full,
            schema_compatible=schema_compatible,
            errors=errors,
        )
