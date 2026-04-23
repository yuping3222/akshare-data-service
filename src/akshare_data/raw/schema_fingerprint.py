"""Schema fingerprinting for Raw (L0) layer.

Computes a deterministic hash over the column names and dtypes of a
DataFrame so that schema drift between batches can be detected.

Spec: docs/design/20-raw-spec.md §7
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional

import pandas as pd


def compute_schema_fingerprint(
    df: pd.DataFrame,
    exclude_columns: Optional[List[str]] = None,
    algorithm: str = "sha256",
) -> str:
    """Compute a deterministic fingerprint of a DataFrame's schema.

    The fingerprint is based on sorted (column_name, dtype_string) pairs
    so that column order does not affect the result.

    Args:
        df: Input DataFrame.
        exclude_columns: Columns to exclude from fingerprint (e.g. system fields).
        algorithm: Hash algorithm name (default: sha256).

    Returns:
        Fingerprint string in the form "sha256:<hex>".
    """
    columns = list(df.columns)
    if exclude_columns:
        columns = [c for c in columns if c not in exclude_columns]

    pairs = sorted((col, str(df[col].dtype)) for col in columns)
    canonical = repr(pairs).encode("utf-8")

    h = hashlib.new(algorithm, canonical)
    return f"{algorithm}:{h.hexdigest()}"


def compute_column_fingerprint(
    df: pd.DataFrame,
    exclude_columns: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Compute per-column type fingerprints.

    Returns a mapping of column_name -> "sha256:<hex>" where the hash
    covers the column's dtype and a sample of its values.

    Args:
        df: Input DataFrame.
        exclude_columns: Columns to exclude.

    Returns:
        Dict mapping column name to fingerprint string.
    """
    result: Dict[str, str] = {}
    columns = list(df.columns)
    if exclude_columns:
        columns = [c for c in columns if c not in exclude_columns]

    for col in columns:
        dtype_str = str(df[col].dtype)
        sample = df[col].head(10).to_list()
        payload = f"{col}:{dtype_str}:{sample!r}".encode("utf-8")
        h = hashlib.sha256(payload)
        result[col] = f"sha256:{h.hexdigest()}"

    return result


def schemas_match(
    fingerprint_a: str,
    fingerprint_b: str,
) -> bool:
    """Check if two schema fingerprints are identical."""
    return fingerprint_a == fingerprint_b


def describe_schema(
    df: pd.DataFrame,
    exclude_columns: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """Return a JSON-serialisable schema description.

    Each entry has keys: "name", "dtype".

    Args:
        df: Input DataFrame.
        exclude_columns: Columns to exclude.

    Returns:
        List of dicts describing each column.
    """
    columns = list(df.columns)
    if exclude_columns:
        columns = [c for c in columns if c not in exclude_columns]

    return [{"name": col, "dtype": str(df[col].dtype)} for col in columns]
