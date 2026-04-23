"""Release version model utilities for the Served layer.

Implements T7-002 release identifier semantics:

- format: ``{dataset}-r{YYYYMMDDHHMM}-{seq}``
- sortable by timestamp + sequence
- deterministic next-version generation against existing release names
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


_VERSION_RE = re.compile(
    r"^(?P<dataset>[a-z0-9_]+)-r(?P<ts>\d{12})-(?P<seq>\d{2})$"
)


class ReleaseVersionError(ValueError):
    """Raised when a release version string is invalid."""


@dataclass(frozen=True, order=True)
class ReleaseVersion:
    """Parsed release version."""

    dataset: str
    timestamp: datetime
    sequence: int

    @classmethod
    def parse(cls, value: str) -> "ReleaseVersion":
        match = _VERSION_RE.match(value)
        if not match:
            raise ReleaseVersionError(f"Invalid release_version format: {value}")

        ts = datetime.strptime(match.group("ts"), "%Y%m%d%H%M").replace(
            tzinfo=timezone.utc
        )
        seq = int(match.group("seq"))
        if seq < 1:
            raise ReleaseVersionError(f"sequence must be >= 1, got {seq}")

        return cls(dataset=match.group("dataset"), timestamp=ts, sequence=seq)

    def to_string(self) -> str:
        return (
            f"{self.dataset}-r{self.timestamp.astimezone(timezone.utc).strftime('%Y%m%d%H%M')}"
            f"-{self.sequence:02d}"
        )


def next_release_version(
    dataset: str,
    *,
    now: datetime | None = None,
    existing_versions: Iterable[str] = (),
) -> str:
    """Generate next release version for a dataset.

    Sequence is incremented only for versions sharing the same minute prefix.
    """
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ts_key = current.strftime("%Y%m%d%H%M")

    max_seq = 0
    for version in existing_versions:
        try:
            parsed = ReleaseVersion.parse(version)
        except ReleaseVersionError:
            continue

        if parsed.dataset != dataset:
            continue
        if parsed.timestamp.strftime("%Y%m%d%H%M") != ts_key:
            continue
        max_seq = max(max_seq, parsed.sequence)

    next_seq = max_seq + 1
    return f"{dataset}-r{ts_key}-{next_seq:02d}"


def list_release_versions(releases_dir: Path) -> list[str]:
    """List release-version directory names under a dataset releases path."""
    if not releases_dir.exists():
        return []
    versions: list[str] = []
    for child in releases_dir.iterdir():
        if child.is_dir():
            versions.append(child.name)
    return sorted(versions)
