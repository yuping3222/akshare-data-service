"""Idempotency key computation.

Provides a stable, deterministic key derived from task attributes so that
Raw writer, scheduler, and backfill utilities can detect duplicate work
without relying on a generated ``task_id``.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any, Dict


def compute_idempotency_key(
    *,
    dataset: str,
    source_name: str,
    interface_name: str,
    params: Dict[str, Any],
    extract_date: date,
) -> str:
    """Compute a stable idempotency key for a task.

    The key is a SHA-256 hex digest of the canonical JSON representation of:

    - ``dataset``
    - ``source_name``
    - ``interface_name``
    - ``params`` (sorted keys, deterministic serialization)
    - ``extract_date`` (ISO format)

    This key is fully re-computable from the same inputs, making it safe for:
    - Raw writer deduplication
    - Scheduler skip-already-done logic
    - Backfill / replay idempotent runs
    """
    canonical = json.dumps(
        {
            "dataset": dataset,
            "source_name": source_name,
            "interface_name": interface_name,
            "params": _normalize_params(params),
            "extract_date": extract_date.isoformat(),
        },
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize params for deterministic hashing.

    Converts date/datetime values to ISO strings and ensures all nested
    structures are serializable.
    """
    result: Dict[str, Any] = {}
    for key, value in sorted(params.items()):
        if isinstance(value, date):
            result[key] = value.isoformat()
        elif isinstance(value, dict):
            result[key] = _normalize_params(value)
        elif isinstance(value, (list, tuple)):
            result[key] = [
                _normalize_params(v)
                if isinstance(v, dict)
                else (v.isoformat() if isinstance(v, date) else v)
                for v in value
            ]
        else:
            result[key] = value
    return result
