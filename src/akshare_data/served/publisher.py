"""Publisher — move gate-approved standardized data into the Served layer.

The Publisher:
1. Validates the GateDecision (must pass, must match dataset).
2. Generates a deterministic release_version.
3. Writes parquet files atomically under releases/<release_version>/.
4. Produces a ReleaseManifest and persists it alongside the data.

The publisher does NOT re-run quality checks.  It trusts the GateDecision
object passed to it.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .manifest import GateDecision, ReleaseManifest, SourceBatch
from .versioning import list_release_versions, next_release_version

logger = logging.getLogger(__name__)

_DEFAULT_SERVED_DIR = Path("./data/served")


class PublishError(Exception):
    """Raised when a publish operation cannot complete."""


class Publisher:
    """Publish standardized DataFrames to the Served layer."""

    def __init__(
        self,
        served_dir: Optional[Path] = None,
        compression: str = "snappy",
    ) -> None:
        self.served_dir = served_dir or _DEFAULT_SERVED_DIR
        self.compression = compression

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(
        self,
        *,
        dataset: str,
        df: pd.DataFrame,
        gate_decision: GateDecision,
        schema_version: str = "v1",
        normalize_version: str = "v1",
        partition_col: Optional[str] = None,
    ) -> ReleaseManifest:
        """Publish a standardized batch to the served layer.

        Args:
            dataset: Canonical dataset name (e.g. 'market_quote_daily').
            df: Standardized DataFrame ready for serving.
            gate_decision: Quality gate result — must have gate_passed=True.
            schema_version: Entity schema version tag.
            normalize_version: Normalization rule version tag.
            partition_col: Column to partition by (e.g. 'trade_date').

        Returns:
            The persisted ReleaseManifest.

        Raises:
            PublishError: If the gate did not pass, data is empty, or I/O fails.
        """
        self._validate_gate(dataset, gate_decision)

        if df.empty:
            raise PublishError(f"Cannot publish empty DataFrame for dataset={dataset}")

        release_version = self._generate_release_version(dataset)
        release_dir = self._release_dir(dataset, release_version)
        data_dir = release_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        df = df.copy()
        publish_time = datetime.now(timezone.utc)
        df["publish_time"] = publish_time
        df["release_version"] = release_version

        partition_values = self._write_parquet_files(df, data_dir, partition_col)
        files = sorted(
            str(p.relative_to(release_dir)) for p in data_dir.glob("*.parquet")
        )

        source_batch = SourceBatch(
            batch_id=gate_decision.batch_id,
            source_name=df["source_name"].iloc[0]
            if "source_name" in df.columns
            else "",
            interface_name=df["interface_name"].iloc[0]
            if "interface_name" in df.columns
            else "",
            record_count=len(df),
            partition_values=partition_values,
        )

        manifest = ReleaseManifest.create(
            dataset=dataset,
            release_version=release_version,
            source_batches=[source_batch],
            partitions_covered=partition_values,
            total_record_count=len(df),
            schema_version=schema_version,
            normalize_version=normalize_version,
            gate_decision=gate_decision,
            published_at=publish_time,
            files=files,
        )

        manifest.save(release_dir / "manifest.json")
        self._update_latest_link(dataset, release_version)

        logger.info(
            "Published dataset=%s release_version=%s records=%d",
            dataset,
            release_version,
            len(df),
        )
        return manifest

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_gate(dataset: str, gate_decision: GateDecision) -> None:
        if gate_decision.dataset != dataset:
            raise PublishError(
                f"GateDecision dataset='{gate_decision.dataset}' does not match "
                f"publish dataset='{dataset}'"
            )
        if not gate_decision.gate_passed:
            raise PublishError(
                f"GateDecision for dataset='{dataset}' batch='{gate_decision.batch_id}' "
                f"did not pass. Failed rules: {gate_decision.failed_rules}"
            )

    # ------------------------------------------------------------------
    # Release version generation
    # ------------------------------------------------------------------

    def _generate_release_version(self, dataset: str) -> str:
        """Generate release version following the T7-002 model.

        Format: ``{dataset}-r{YYYYMMDDHHMM}-{seq}``.
        """
        releases_dir = self.served_dir / dataset / "releases"
        existing = list_release_versions(releases_dir)
        return next_release_version(dataset, existing_versions=existing)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _release_dir(self, dataset: str, release_version: str) -> Path:
        return self.served_dir / dataset / "releases" / release_version

    def _update_latest_link(self, dataset: str, release_version: str) -> None:
        """Create/update a 'latest' text file pointing to the current release."""
        dataset_dir = self.served_dir / dataset / "releases"
        dataset_dir.mkdir(parents=True, exist_ok=True)
        latest_path = dataset_dir / "latest"
        tmp_path = latest_path.with_suffix(".tmp")
        tmp_path.write_text(release_version, encoding="utf-8")
        os.replace(str(tmp_path), str(latest_path))

    # ------------------------------------------------------------------
    # Parquet writing
    # ------------------------------------------------------------------

    def _write_parquet_files(
        self,
        df: pd.DataFrame,
        data_dir: Path,
        partition_col: Optional[str],
    ) -> List[str]:
        """Write DataFrame to parquet files, optionally partitioned.

        Returns the list of distinct partition values written.
        """
        partition_values: List[str] = []

        if partition_col and partition_col in df.columns:
            for value, group in df.groupby(partition_col, sort=True):
                partition_values.append(str(value))
                part_dir = data_dir / f"{partition_col}={value}"
                part_dir.mkdir(parents=True, exist_ok=True)
                self._write_single_parquet(group, part_dir / "data.parquet")
        else:
            self._write_single_parquet(df, data_dir / "data.parquet")

        return partition_values

    def _write_single_parquet(self, df: pd.DataFrame, target: Path) -> None:
        """Atomically write a single parquet file."""
        tmp = target.with_suffix(".tmp")
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, str(tmp), compression=self.compression)
            os.replace(str(tmp), str(target))
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise
