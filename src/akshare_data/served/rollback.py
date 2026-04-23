"""Rollback — revert a dataset to its previous release version.

The RollbackManager:
1. Identifies the current latest release.
2. Finds the previous non-rolled-back release.
3. Marks the current release as rolled_back in its manifest.
4. Updates the 'latest' pointer to the previous version.

Rollback does NOT delete data files — it only changes metadata and pointers.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from .manifest import ReleaseManifest, ReleaseStatus
from .reader import Reader, ReadError

logger = logging.getLogger(__name__)

_DEFAULT_SERVED_DIR = Path("./data/served")


class RollbackError(Exception):
    """Raised when a rollback operation cannot complete."""


class RollbackManager:
    """Manage rollback of served releases."""

    def __init__(self, served_dir: Optional[Path] = None) -> None:
        self.served_dir = served_dir or _DEFAULT_SERVED_DIR
        self._reader = Reader(served_dir=self.served_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rollback(
        self,
        dataset: str,
        *,
        reason: str = "",
        target_version: Optional[str] = None,
    ) -> str:
        """Roll back a dataset to a previous release version.

        Args:
            dataset: Canonical dataset name.
            reason: Human-readable rollback reason.
            target_version: Specific version to roll back to.
                If None, rolls back to the previous stable version.

        Returns:
            The new active release version.

        Raises:
            RollbackError: If rollback is not possible.
        """
        current_version = self._reader.get_latest_version(dataset)

        if target_version is not None:
            new_version = target_version
        else:
            new_version = self._find_previous_version(dataset, current_version)

        if new_version == current_version:
            raise RollbackError(f"No previous version available for dataset={dataset}")

        self._validate_target(dataset, new_version)

        self._mark_rolled_back(dataset, current_version, reason)
        self._update_latest(dataset, new_version)

        logger.info(
            "Rolled back dataset=%s from %s to %s (reason: %s)",
            dataset,
            current_version,
            new_version,
            reason or "unspecified",
        )
        return new_version

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_previous_version(
        self,
        dataset: str,
        exclude_version: str,
    ) -> str:
        """Find the most recent non-rolled-back version excluding the given one."""
        versions = self._reader.list_versions(dataset)
        for v in versions:
            if v == exclude_version:
                continue
            manifest = self._reader.get_manifest(dataset, version=v)
            if manifest.status != ReleaseStatus.ROLLED_BACK.value:
                return v
        raise RollbackError(f"No eligible previous version for dataset={dataset}")

    def _validate_target(self, dataset: str, version: str) -> None:
        """Ensure the target version exists and is not already rolled back."""
        try:
            manifest = self._reader.get_manifest(dataset, version=version)
        except ReadError:
            raise RollbackError(
                f"Target version={version} not found for dataset={dataset}"
            )
        if manifest.status == ReleaseStatus.ROLLED_BACK.value:
            raise RollbackError(f"Target version={version} is already rolled back")

    def _mark_rolled_back(
        self,
        dataset: str,
        version: str,
        reason: str,
    ) -> None:
        """Update the manifest of the given version to rolled_back status."""
        release_dir = self.served_dir / dataset / "releases" / version
        manifest_path = release_dir / "manifest.json"
        manifest = ReleaseManifest.load(manifest_path)
        updated = manifest.mark_rolled_back(reason=reason)
        updated.save(manifest_path)

    def _update_latest(self, dataset: str, version: str) -> None:
        """Update the 'latest' pointer file."""
        releases_dir = self.served_dir / dataset / "releases"
        latest_path = releases_dir / "latest"
        tmp_path = latest_path.with_suffix(".tmp")
        tmp_path.write_text(version, encoding="utf-8")
        os.replace(str(tmp_path), str(latest_path))
