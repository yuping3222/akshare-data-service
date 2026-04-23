"""Raw (L0) writer for parquet batches.

Writes DataFrames to the Raw layer with:
- System field injection
- Atomic parquet writes (tmp → rename)
- Manifest and schema snapshot output

Spec: docs/design/20-raw-spec.md
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.raw.system_fields import (
    SYSTEM_FIELD_NAMES,
    get_system_field_names,
    get_system_field_types,
)
from akshare_data.raw.schema_fingerprint import (
    compute_schema_fingerprint,
    describe_schema,
)
from akshare_data.raw.manifest import (
    Manifest,
    MANIFEST_FILENAME,
    save_schema_snapshot,
)
from akshare_data.ingestion.models.task import ExtractTask
from akshare_data.ingestion.models.batch import BatchContext


class RawWriter:
    """Writes DataFrames to Raw (L0) storage.

    Usage::

        writer = RawWriter(base_dir="data/raw")
        writer.write(
            df=df,
            task=task,
            batch_ctx=batch_ctx,
        )
    """

    def __init__(
        self,
        base_dir: str = "data/raw",
        compression: str = "snappy",
    ) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._compression = compression

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def _batch_dir(
        self,
        domain: str,
        dataset: str,
        extract_date: date,
        batch_id: str,
    ) -> Path:
        return (
            self._base_dir
            / domain
            / dataset
            / f"extract_date={extract_date.isoformat()}"
            / f"batch_id={batch_id}"
        )

    @staticmethod
    def _compute_record_hash(df_business: pd.DataFrame) -> str:
        """Hash business content only (no system fields).

        Uses sorted column values to produce a deterministic hash.
        """
        if df_business.empty:
            return "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

        sorted_df = df_business.sort_index(axis=1)
        payload = sorted_df.to_json(orient="split", date_format="iso").encode("utf-8")
        h = hashlib.sha256(payload)
        return f"sha256:{h.hexdigest()}"

    def _enrich_with_system_fields(
        self,
        df: pd.DataFrame,
        task: ExtractTask,
        batch_ctx: BatchContext,
        schema_fingerprint: str,
        request_time: datetime,
    ) -> pd.DataFrame:
        """Append system fields to the DataFrame."""
        df = df.copy()
        ingest_time = datetime.now(timezone.utc)

        business_df = df.drop(
            columns=[c for c in get_system_field_names() if c in df.columns],
            errors="ignore",
        )
        record_hash = self._compute_record_hash(business_df)

        for field_name in get_system_field_names():
            if field_name == "batch_id":
                df[field_name] = batch_ctx.batch_id
            elif field_name == "source_name":
                df[field_name] = task.source_name
            elif field_name == "interface_name":
                df[field_name] = task.interface_name
            elif field_name == "request_params_json":
                df[field_name] = task.to_request_params_json()
            elif field_name == "request_time":
                df[field_name] = request_time
            elif field_name == "ingest_time":
                df[field_name] = ingest_time
            elif field_name == "extract_date":
                df[field_name] = task.extract_date
            elif field_name == "extract_version":
                df[field_name] = "v1.0"
            elif field_name == "source_schema_fingerprint":
                df[field_name] = schema_fingerprint
            elif field_name == "raw_record_hash":
                df[field_name] = record_hash

        type_map = get_system_field_types()
        for field_name, dtype in type_map.items():
            if field_name in df.columns:
                if dtype == "timestamp":
                    df[field_name] = pd.to_datetime(df[field_name], utc=True)
                elif dtype == "date":
                    df[field_name] = pd.to_datetime(df[field_name]).dt.date

        return df

    @staticmethod
    def _atomic_parquet_write(
        df: pd.DataFrame,
        target: Path,
        compression: str,
    ) -> None:
        """Write parquet atomically via tmp + rename.

        Closes the mkstemp file descriptor to avoid resource leaks.
        """
        tmp_fd, tmp_path_str = tempfile.mkstemp(
            suffix=".parquet.tmp", dir=str(target.parent)
        )
        tmp_path = Path(tmp_path_str)
        try:
            os.close(tmp_fd)
            df.to_parquet(
                tmp_path,
                engine="pyarrow",
                compression=compression,
                index=False,
            )
            tmp_path.rename(target)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def write(
        self,
        df: pd.DataFrame,
        task: ExtractTask,
        batch_ctx: BatchContext,
        request_time: Optional[datetime] = None,
        part_index: int = 0,
    ) -> Path:
        """Write a single partition file plus manifest and schema.

        Args:
            df: Business data (original column names preserved).
            task: Extraction task descriptor.
            batch_ctx: Batch context.
            request_time: Time the request was initiated (defaults to now).
            part_index: Partition file index (0-based).

        Returns:
            Path to the batch directory.
        """
        request_time = request_time or datetime.now(timezone.utc)

        schema_fp = compute_schema_fingerprint(
            df, exclude_columns=list(SYSTEM_FIELD_NAMES)
        )

        enriched = self._enrich_with_system_fields(
            df, task, batch_ctx, schema_fp, request_time
        )

        batch_dir = self._batch_dir(
            domain=task.domain,
            dataset=task.dataset,
            extract_date=task.extract_date,
            batch_id=batch_ctx.batch_id,
        )
        batch_dir.mkdir(parents=True, exist_ok=True)

        parquet_filename = f"part-{part_index:03d}.parquet"
        parquet_path = batch_dir / parquet_filename

        self._atomic_parquet_write(enriched, parquet_path, self._compression)

        manifest = Manifest.create(
            dataset=task.dataset,
            domain=task.domain,
            batch_id=batch_ctx.batch_id,
            extract_date=task.extract_date,
            source_name=task.source_name,
            interface_name=task.interface_name,
            request_params=task.params,
            record_count=len(enriched),
            file_count=1,
            schema_fingerprint=schema_fp,
            extract_version="v1.0",
            status="success",
            files=[parquet_filename],
            schema_snapshot=describe_schema(
                enriched, exclude_columns=list(SYSTEM_FIELD_NAMES)
            ),
        )

        manifest_path = batch_dir / MANIFEST_FILENAME
        manifest.save(manifest_path)

        save_schema_snapshot(
            batch_dir,
            describe_schema(enriched, exclude_columns=list(SYSTEM_FIELD_NAMES)),
        )

        return batch_dir

    def write_batch(
        self,
        partitions: List[Dict[str, Any]],
        task: ExtractTask,
        batch_ctx: BatchContext,
        request_time: Optional[datetime] = None,
    ) -> Path:
        """Write multiple partition files for a single task.

        Args:
            partitions: List of dicts with at least a "df" key.
            task: Extraction task descriptor.
            batch_ctx: Batch context.
            request_time: Time the request was initiated.

        Returns:
            Path to the batch directory.
        """
        request_time = request_time or datetime.now(timezone.utc)

        all_dfs = [p["df"] for p in partitions if isinstance(p, dict) and "df" in p]
        combined = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

        schema_fp = compute_schema_fingerprint(
            combined, exclude_columns=list(SYSTEM_FIELD_NAMES)
        )

        batch_dir = self._batch_dir(
            domain=task.domain,
            dataset=task.dataset,
            extract_date=task.extract_date,
            batch_id=batch_ctx.batch_id,
        )
        batch_dir.mkdir(parents=True, exist_ok=True)

        filenames: List[str] = []
        total_records = 0

        for idx, part in enumerate(partitions):
            if not isinstance(part, dict) or "df" not in part:
                continue
            part_df = part["df"]
            if part_df.empty:
                continue

            enriched = self._enrich_with_system_fields(
                part_df, task, batch_ctx, schema_fp, request_time
            )

            parquet_filename = f"part-{idx:03d}.parquet"
            parquet_path = batch_dir / parquet_filename

            self._atomic_parquet_write(enriched, parquet_path, self._compression)
            filenames.append(parquet_filename)
            total_records += len(enriched)

        manifest = Manifest.create(
            dataset=task.dataset,
            domain=task.domain,
            batch_id=batch_ctx.batch_id,
            extract_date=task.extract_date,
            source_name=task.source_name,
            interface_name=task.interface_name,
            request_params=task.params,
            record_count=total_records,
            file_count=len(filenames),
            schema_fingerprint=schema_fp,
            extract_version="v1.0",
            status="success",
            files=filenames,
            schema_snapshot=describe_schema(
                combined, exclude_columns=list(SYSTEM_FIELD_NAMES)
            ),
        )

        manifest_path = batch_dir / MANIFEST_FILENAME
        manifest.save(manifest_path)

        save_schema_snapshot(
            batch_dir,
            describe_schema(combined, exclude_columns=list(SYSTEM_FIELD_NAMES)),
        )

        return batch_dir
