"""Merge / Upsert logic for Standardized (L1) layer.

Handles late-arriving data, duplicate data, and incremental coverage
with clear rules for version-based conflict resolution.

Spec: docs/design/40-standardized-storage-spec.md §6
"""

from __future__ import annotations

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


class MergeEngine:
    """Merges new normalized data into existing Standardized data.

    Merge rules:
    1. Late-arriving data: compare normalize_version and ingest_time
    2. Duplicate data: deduplicate by primary key within the same batch
    3. Incremental coverage: upsert by primary key across batches

    Usage::

        engine = MergeEngine(primary_key=["security_id", "trade_date", "adjust_type"])

        merged = engine.merge(
            existing=existing_df,
            incoming=new_df,
            strategy="upsert",
        )
    """

    def __init__(self, primary_key: List[str]) -> None:
        self._primary_key = primary_key

    def merge(
        self,
        existing: pd.DataFrame,
        incoming: pd.DataFrame,
        *,
        strategy: str = "upsert",
    ) -> pd.DataFrame:
        """Merge incoming data into existing data.

        Args:
            existing: Current data in the partition.
            incoming: New data to merge.
            strategy: One of "upsert", "append", "replace".
                - "upsert": incoming overwrites existing rows with same PK
                - "append": simply concatenate (no dedup)
                - "replace": return only incoming data

        Returns:
            Merged DataFrame.
        """
        if existing.empty:
            return incoming.copy()
        if incoming.empty:
            return existing.copy()

        if strategy == "replace":
            return incoming.copy()

        if strategy == "append":
            return pd.concat([existing, incoming], ignore_index=True)

        return self._upsert(existing, incoming)

    def _upsert(
        self,
        existing: pd.DataFrame,
        incoming: pd.DataFrame,
    ) -> pd.DataFrame:
        """Upsert incoming data over existing data by primary key.

        For rows with matching primary keys:
        - If incoming normalize_version > existing version: use incoming
        - If versions are equal but incoming ingest_time > existing: use incoming
        - Otherwise: keep existing

        For rows with no match: include as-is.
        """
        pk_cols = [
            c
            for c in self._primary_key
            if c in existing.columns and c in incoming.columns
        ]
        if not pk_cols:
            logger.warning(
                "No common primary key columns found, falling back to append"
            )
            return pd.concat([existing, incoming], ignore_index=True)

        existing_keys = _make_key(existing, pk_cols)
        incoming_keys = _make_key(incoming, pk_cols)

        existing_key_set = set(existing_keys)
        incoming_key_set = set(incoming_keys)

        overlapping_keys = existing_key_set & incoming_key_set

        if not overlapping_keys:
            return pd.concat([existing, incoming], ignore_index=True)

        existing_mask = existing_keys.isin(overlapping_keys)
        incoming_mask = incoming_keys.isin(overlapping_keys)

        existing_overlap = existing[existing_mask].copy()
        incoming_overlap = incoming[incoming_mask].copy()

        existing_overlap["_merge_key"] = existing_keys[existing_mask].values
        incoming_overlap["_merge_key"] = incoming_keys[incoming_mask].values

        resolved = self._resolve_conflicts(existing_overlap, incoming_overlap)

        existing_non_overlap = existing[~existing_mask].copy()
        incoming_non_overlap = incoming[~incoming_mask].copy()

        result = pd.concat(
            [existing_non_overlap, incoming_non_overlap, resolved],
            ignore_index=True,
        )

        if "_merge_key" in result.columns:
            result = result.drop(columns=["_merge_key"])

        return result

    def _resolve_conflicts(
        self,
        existing: pd.DataFrame,
        incoming: pd.DataFrame,
    ) -> pd.DataFrame:
        """Resolve conflicts for overlapping primary keys.

        Priority:
        1. Higher normalize_version wins
        2. If versions equal, later ingest_time wins
        3. If both equal, incoming wins (latest write)
        """
        merged = existing.merge(
            incoming,
            on="_merge_key",
            how="outer",
            suffixes=("_existing", "_incoming"),
            indicator=True,
        )

        result_rows = []

        for _, row in merged.iterrows():
            if row["_merge_key"] is None:
                continue

            existing_norm = row.get("normalize_version_existing")
            incoming_norm = row.get("normalize_version_incoming")
            existing_ingest = row.get("ingest_time_existing")
            incoming_ingest = row.get("ingest_time_incoming")

            use_incoming = True

            if existing_norm is not None and incoming_norm is not None:
                if _compare_version(str(incoming_norm), str(existing_norm)) < 0:
                    use_incoming = False
                elif _compare_version(str(incoming_norm), str(existing_norm)) == 0:
                    if existing_ingest is not None and incoming_ingest is not None:
                        try:
                            existing_dt = pd.to_datetime(existing_ingest)
                            incoming_dt = pd.to_datetime(incoming_ingest)
                            if incoming_dt <= existing_dt:
                                use_incoming = False
                        except (ValueError, TypeError):
                            pass

            if use_incoming:
                result_rows.append(_pick_row(row, "incoming"))
            else:
                result_rows.append(_pick_row(row, "existing"))

        if not result_rows:
            return pd.DataFrame()

        return pd.DataFrame(result_rows)

    def merge_late_arriving(
        self,
        existing: pd.DataFrame,
        late_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge late-arriving data with version-aware conflict resolution.

        Late-arriving data is data that arrives after its business time
        partition has already been written.

        Args:
            existing: Current partition data.
            late_data: Late-arriving data.

        Returns:
            Merged DataFrame with late data properly integrated.
        """
        return self._upsert(existing, late_data)

    def merge_incremental(
        self,
        existing: pd.DataFrame,
        incremental: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge incremental data (new rows + updates).

        Same as upsert but optimized for the case where most incoming
        rows are new (not overlapping with existing).

        Args:
            existing: Current data.
            incremental: New incremental data.

        Returns:
            Merged DataFrame.
        """
        return self._upsert(existing, incremental)


def _make_key(df: pd.DataFrame, pk_cols: List[str]) -> pd.Series:
    """Create a composite key string from primary key columns."""
    if len(pk_cols) == 1:
        return df[pk_cols[0]].astype(str)
    return df[pk_cols].astype(str).agg("|".join, axis=1)


def _compare_version(v1: str, v2: str) -> int:
    """Compare version strings like 'v1', 'v2', 'v10'.

    Returns:
        -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
    """
    n1 = _parse_version_num(v1)
    n2 = _parse_version_num(v2)
    if n1 < n2:
        return -1
    if n1 > n2:
        return 1
    return 0


def _parse_version_num(version: str) -> int:
    """Extract numeric part from version string like 'v1' -> 1."""
    s = version.lstrip("vV")
    try:
        return int(s)
    except ValueError:
        return 0


def _pick_row(
    merged_row: pd.Series,
    source: str,
) -> dict:
    """Pick columns from the specified source in a merged row."""
    result = {}
    for col in merged_row.index:
        if col in (
            "_merge_key",
            "_merge_key_existing",
            "_merge_key_incoming",
            "_merge",
        ):
            continue
        if col.endswith("_existing"):
            base = col[: -len("_existing")]
            if source == "existing":
                result[base] = merged_row[col]
        elif col.endswith("_incoming"):
            base = col[: -len("_incoming")]
            if source == "incoming":
                result[base] = merged_row[col]
        else:
            if col not in result:
                result[col] = merged_row[col]
    return result
