"""Quarantine store: isolates records that failed quality checks.

Failed records are written to quarantine storage for later inspection,
replay, or manual review. Quarantine data is never published to Served.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field as dc_field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class _QuarantineEncoder(json.JSONEncoder):
    """JSON encoder that handles pandas/numpy types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class QuarantineRecord:
    """A single quarantined record with metadata."""

    dataset: str
    batch_id: str
    layer: str
    rule_id: str
    record_index: int
    record_data: Dict[str, Any]
    reason: str
    quarantined_at: str = ""

    def __post_init__(self) -> None:
        if not self.quarantined_at:
            self.quarantined_at = datetime.now(timezone.utc).isoformat()


@dataclass
class QuarantineBatch:
    """A batch of quarantined records."""

    dataset: str
    batch_id: str
    layer: str
    quarantined_at: str = ""
    total_records: int = 0
    rule_ids: List[str] = dc_field(default_factory=list)
    records: List[QuarantineRecord] = dc_field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.quarantined_at:
            self.quarantined_at = datetime.now(timezone.utc).isoformat()
        if not self.total_records:
            self.total_records = len(self.records)
        if not self.rule_ids:
            self.rule_ids = list({r.rule_id for r in self.records})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "batch_id": self.batch_id,
            "layer": self.layer,
            "quarantined_at": self.quarantined_at,
            "total_records": self.total_records,
            "rule_ids": self.rule_ids,
            "records": [asdict(r) for r in self.records],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False, cls=_QuarantineEncoder)


class QuarantineStore:
    """Stores and retrieves quarantined records.

    Quarantine files are organized as::

        <quarantine_dir>/
            <dataset>/
                <batch_id>/
                    <layer>/
                        quarantine.json
                        failed_records.parquet
    """

    def __init__(self, quarantine_dir: str | Path = "./quarantine") -> None:
        self._base = Path(quarantine_dir)

    @property
    def base_dir(self) -> Path:
        return self._base

    def store(
        self,
        dataset: str,
        batch_id: str,
        layer: str,
        failed_df: pd.DataFrame,
        rule_id: str,
        reason: str,
    ) -> QuarantineBatch:
        """Store failed records in quarantine.

        Args:
            dataset: Standard dataset name.
            batch_id: Batch identifier.
            layer: Data layer (raw / standardized / served).
            failed_df: DataFrame containing only the failed records.
            rule_id: The rule that caused the failure.
            reason: Human-readable reason.

        Returns:
            QuarantineBatch describing what was stored.
        """
        records: List[QuarantineRecord] = []
        for idx, row in failed_df.iterrows():
            records.append(
                QuarantineRecord(
                    dataset=dataset,
                    batch_id=batch_id,
                    layer=layer,
                    rule_id=rule_id,
                    record_index=int(idx) if not isinstance(idx, int) else idx,
                    record_data=row.to_dict(),
                    reason=reason,
                )
            )

        batch = QuarantineBatch(
            dataset=dataset,
            batch_id=batch_id,
            layer=layer,
            records=records,
        )

        self._write_batch(batch)
        logger.info(
            "Quarantined %d records for dataset=%s batch=%s layer=%s rule=%s",
            len(records),
            dataset,
            batch_id,
            layer,
            rule_id,
        )
        return batch

    def store_multi(
        self,
        dataset: str,
        batch_id: str,
        layer: str,
        rule_results_map: Dict[str, pd.DataFrame],
    ) -> List[QuarantineBatch]:
        """Store failed records for multiple rules at once.

        Args:
            dataset: Standard dataset name.
            batch_id: Batch identifier.
            layer: Data layer.
            rule_results_map: Mapping of rule_id -> failed DataFrame.

        Returns:
            List of QuarantineBatch, one per rule.
        """
        batches: List[QuarantineBatch] = []
        for rule_id, failed_df in rule_results_map.items():
            if failed_df.empty:
                continue
            batch = self.store(
                dataset=dataset,
                batch_id=batch_id,
                layer=layer,
                failed_df=failed_df,
                rule_id=rule_id,
                reason=f"Failed rule: {rule_id}",
            )
            batches.append(batch)
        return batches

    def load(
        self,
        dataset: str,
        batch_id: str,
        layer: str,
    ) -> Optional[QuarantineBatch]:
        """Load quarantined records for a given dataset/batch/layer."""
        path = self._batch_dir(dataset, batch_id, layer) / "quarantine.json"
        if not path.exists():
            return None

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        records = []
        for r in data.get("records", []):
            records.append(QuarantineRecord(**r))

        return QuarantineBatch(
            dataset=data["dataset"],
            batch_id=data["batch_id"],
            layer=data["layer"],
            quarantined_at=data.get("quarantined_at", ""),
            records=records,
        )

    def list_batches(self, dataset: str) -> List[Dict[str, str]]:
        """List all quarantined batches for a dataset."""
        ds_dir = self._base / dataset
        if not ds_dir.exists():
            return []

        batches = []
        for batch_dir in sorted(ds_dir.iterdir()):
            if not batch_dir.is_dir():
                continue
            for layer_dir in batch_dir.iterdir():
                if not layer_dir.is_dir():
                    continue
                qfile = layer_dir / "quarantine.json"
                if qfile.exists():
                    batches.append({
                        "dataset": dataset,
                        "batch_id": batch_dir.name,
                        "layer": layer_dir.name,
                        "path": str(qfile),
                    })
        return batches

    def _batch_dir(self, dataset: str, batch_id: str, layer: str) -> Path:
        return self._base / dataset / batch_id / layer

    def _write_batch(self, batch: QuarantineBatch) -> None:
        """Write quarantine batch to disk atomically."""
        dest = self._batch_dir(batch.dataset, batch.batch_id, batch.layer)
        dest.mkdir(parents=True, exist_ok=True)

        # Write quarantine metadata
        meta_path = dest / "quarantine.json"
        tmp_path = dest / "quarantine.json.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(batch.to_json())
        tmp_path.rename(meta_path)

        # Write failed records as Parquet
        if batch.records:
            records_df = pd.DataFrame([r.record_data for r in batch.records])
            records_df["_quarantine_rule_id"] = [r.rule_id for r in batch.records]
            records_df["_quarantine_reason"] = [r.reason for r in batch.records]
            records_df["_quarantined_at"] = [r.quarantined_at for r in batch.records]

            parquet_path = dest / "failed_records.parquet"
            tmp_parquet = dest / "failed_records.parquet.tmp"
            records_df.to_parquet(tmp_parquet, index=False)
            tmp_parquet.rename(parquet_path)
