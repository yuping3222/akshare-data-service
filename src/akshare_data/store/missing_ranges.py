"""Shared logic for finding missing date ranges in incremental caching."""

from datetime import datetime, timedelta

import pandas as pd


def find_missing_ranges(
    start: str,
    end: str,
    existing_ranges: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Compute missing date ranges between *start* and *end*, excluding *existing_ranges*."""
    if not existing_ranges:
        return [(start, end)]

    target_start = datetime.strptime(start, "%Y-%m-%d")
    target_end = datetime.strptime(end, "%Y-%m-%d")
    sorted_existing = sorted(existing_ranges, key=lambda x: x[0])

    missing: list[tuple[str, str]] = []
    current_start = target_start

    for ex_start_str, ex_end_str in sorted_existing:
        ex_start = pd.to_datetime(ex_start_str)
        ex_end = pd.to_datetime(ex_end_str)

        if ex_start > target_end:
            break

        if ex_start > current_start:
            missing.append(
                (
                    current_start.strftime("%Y-%m-%d"),
                    (ex_start - timedelta(days=1)).strftime("%Y-%m-%d"),
                )
            )

        current_start = max(current_start, ex_end + timedelta(days=1))
        if current_start >= target_end:
            break

    if current_start < target_end:
        missing.append(
            (
                current_start.strftime("%Y-%m-%d"),
                target_end.strftime("%Y-%m-%d"),
            )
        )

    return missing
