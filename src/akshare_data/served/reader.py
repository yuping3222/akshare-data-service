"""Reader — read served data by dataset, defaulting to the latest stable version.

The Reader:
- Resolves the latest stable release via the 'latest' pointer file.
- Reads all parquet files under a release, optionally filtering by partition.
- Returns the manifest alongside data for transparency.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .manifest import ReleaseManifest, ReleaseStatus

logger = logging.getLogger(__name__)

_DEFAULT_SERVED_DIR = Path("./data/served")


class ReadError(Exception):
    """Raised when a read operation cannot complete."""


class Reader:
    """Read served (L2) data for a given dataset."""

    def __init__(self, served_dir: Optional[Path] = None) -> None:
        self.served_dir = served_dir or _DEFAULT_SERVED_DIR

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        dataset: str,
        *,
        version: Optional[str] = None,
        partitions: Optional[Dict[str, List[str]]] = None,
    ) -> Tuple[pd.DataFrame, ReleaseManifest]:
        """Read served data for a dataset.

        Args:
            dataset: Canonical dataset name.
            version: Specific release_version to read.  None = latest stable.
            partitions: Optional {col: [values]} filter.

        Returns:
            (DataFrame, ReleaseManifest) tuple.

        Raises:
            ReadError: If no release is found or data cannot be read.
        """
        if version is None:
            version = self.get_latest_version(dataset)

        release_dir = self._release_dir(dataset, version)
        manifest = self._load_manifest(release_dir)

        if manifest.status == ReleaseStatus.ROLLED_BACK.value:
            logger.warning(
                "Reading rolled-back release: dataset=%s version=%s",
                dataset,
                version,
            )

        data_dir = release_dir / "data"
        if not data_dir.exists():
            raise ReadError(
                f"No data directory for dataset={dataset} version={version}"
            )

        df = self._read_parquet_files(data_dir, partitions)
        return df, manifest

    def get_latest_version(self, dataset: str) -> str:
        """Return the latest stable release version for a dataset."""
        latest_path = self.served_dir / dataset / "releases" / "latest"
        if not latest_path.exists():
            raise ReadError(
                f"No releases found for dataset={dataset} (missing {latest_path})"
            )
        version = latest_path.read_text(encoding="utf-8").strip()
        if not version:
            raise ReadError(f"Empty latest pointer for dataset={dataset}")
        return version

    def list_versions(self, dataset: str) -> List[str]:
        """List all release versions for a dataset, newest first."""
        releases_dir = self.served_dir / dataset / "releases"
        if not releases_dir.exists():
            return []
        version_times: List[Tuple[str, str]] = []
        for entry in releases_dir.iterdir():
            if entry.is_dir() and entry.name != "data":
                manifest_path = entry / "manifest.json"
                if manifest_path.exists():
                    manifest = ReleaseManifest.load(manifest_path)
                    version_times.append((manifest.published_at, entry.name))
        version_times.sort(reverse=True)
        return [v for _, v in version_times]

    def get_manifest(
        self,
        dataset: str,
        *,
        version: Optional[str] = None,
    ) -> ReleaseManifest:
        """Load the manifest for a specific (or latest) release."""
        if version is None:
            version = self.get_latest_version(dataset)
        release_dir = self._release_dir(dataset, version)
        return self._load_manifest(release_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _release_dir(self, dataset: str, version: str) -> Path:
        return self.served_dir / dataset / "releases" / version

    @staticmethod
    def _load_manifest(release_dir: Path) -> ReleaseManifest:
        manifest_path = release_dir / "manifest.json"
        if not manifest_path.exists():
            raise ReadError(f"No manifest at {manifest_path}")
        return ReleaseManifest.load(manifest_path)

    @staticmethod
    def _read_parquet_files(
        data_dir: Path,
        partitions: Optional[Dict[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Read all parquet files under data_dir, optionally filtering."""
        parquet_files = sorted(data_dir.rglob("*.parquet"))
        if not parquet_files:
            return pd.DataFrame()

        if partitions:
            parquet_files = [
                f for f in parquet_files if _matches_partition(f, partitions)
            ]

        if not parquet_files:
            return pd.DataFrame()

        tables = [pq.read_table(str(f)) for f in parquet_files]
        combined = pa.concat_tables(tables)
        return combined.to_pandas()


def _matches_partition(
    file_path: Path,
    partitions: Dict[str, List[str]],
) -> bool:
    """Check if a file path matches the given partition filters."""
    parts = file_path.parts
    for col, values in partitions.items():
        expected_prefix = f"{col}="
        found = False
        for part in parts:
            if part.startswith(expected_prefix):
                part_value = part.split("=", 1)[1]
                if part_value in values:
                    found = True
                    break
        if not found:
            return False
    return True
