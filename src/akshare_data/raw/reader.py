"""Raw (L0) reader for parquet batches.

Supports reading Raw data by:
- batch_id
- dataset
- extract_date (single or range)
- partition file selection

Spec: docs/design/20-raw-spec.md
"""

from __future__ import annotations

import fnmatch
from datetime import date
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import pandas as pd

from akshare_data.raw.manifest import (
    MANIFEST_FILENAME,
    Manifest,
    load_schema_snapshot,
)


class RawReader:
    """Reads Raw (L0) parquet batches from disk.

    Usage::

        reader = RawReader(base_dir="data/raw")

        # By batch_id
        df = reader.read_by_batch_id("20260422_001")

        # By dataset
        df = reader.read_by_dataset("cn", "market_quote_daily")

        # By extract_date
        df = reader.read_by_extract_date("cn", "market_quote_daily", date(2026, 4, 22))

        # By extract_date range
        df = reader.read_by_extract_date_range(
            "cn", "market_quote_daily",
            start=date(2026, 4, 1),
            end=date(2026, 4, 30),
        )

        # Low-level: read a specific batch directory
        df, manifest = reader.read_batch(batch_dir)
    """

    def __init__(self, base_dir: str = "data/raw") -> None:
        self._base_dir = Path(base_dir).resolve()

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    # ------------------------------------------------------------------
    # High-level read APIs
    # ------------------------------------------------------------------

    def read_by_batch_id(
        self,
        batch_id: str,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
        partitions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read all data for a given batch_id.

        If domain/dataset are provided, search is scoped to that path.
        Otherwise, the entire raw tree is searched.

        Args:
            batch_id: Batch identifier (e.g. "20260422_001").
            domain: Optional domain filter (e.g. "cn").
            dataset: Optional dataset filter (e.g. "market_quote_daily").
            partitions: Optional list of partition filenames to read
                (e.g. ["part-000.parquet"]). Defaults to all.

        Returns:
            Combined DataFrame from all matching partition files.

        Raises:
            FileNotFoundError: If no batch directory is found.
        """
        batch_dirs = list(
            self.find_batch_dirs(
                batch_id=batch_id,
                domain=domain,
                dataset=dataset,
            )
        )
        if not batch_dirs:
            raise FileNotFoundError(f"No batch directory found for batch_id={batch_id}")

        frames: List[pd.DataFrame] = []
        for batch_dir in batch_dirs:
            df, _ = self.read_batch(batch_dir, partitions=partitions)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def read_by_dataset(
        self,
        domain: str,
        dataset: str,
        extract_date: Optional[date] = None,
        partitions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read all batches for a dataset, optionally filtered by extract_date.

        Args:
            domain: Data domain (e.g. "cn").
            dataset: Canonical dataset name (e.g. "market_quote_daily").
            extract_date: Optional single date filter.
            partitions: Optional partition file filter.

        Returns:
            Combined DataFrame from all matching batches.
        """
        frames: List[pd.DataFrame] = []
        for batch_dir, manifest in self.iter_batches(
            domain=domain,
            dataset=dataset,
            extract_date=extract_date,
        ):
            df, _ = self.read_batch(batch_dir, partitions=partitions)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def read_by_extract_date(
        self,
        domain: str,
        dataset: str,
        extract_date: date,
        partitions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read all batches for a specific extract_date.

        Args:
            domain: Data domain.
            dataset: Canonical dataset name.
            extract_date: Target extraction date.
            partitions: Optional partition file filter.

        Returns:
            Combined DataFrame from all matching batches.
        """
        return self.read_by_dataset(
            domain,
            dataset,
            extract_date=extract_date,
            partitions=partitions,
        )

    def read_by_extract_date_range(
        self,
        domain: str,
        dataset: str,
        start: date,
        end: date,
        partitions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read all batches within an extract_date range (inclusive).

        Args:
            domain: Data domain.
            dataset: Canonical dataset name.
            start: Start date (inclusive).
            end: End date (inclusive).
            partitions: Optional partition file filter.

        Returns:
            Combined DataFrame from all matching batches.
        """
        frames: List[pd.DataFrame] = []
        for batch_dir, manifest in self.iter_batches(
            domain=domain,
            dataset=dataset,
        ):
            ed = manifest.extract_date_parsed
            if ed and start <= ed <= end:
                df, _ = self.read_batch(batch_dir, partitions=partitions)
                if not df.empty:
                    frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def read_by_source(
        self,
        source_name: str,
        interface_name: Optional[str] = None,
        partitions: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read all batches from a specific source/interface.

        Args:
            source_name: Source adapter name (e.g. "akshare").
            interface_name: Optional interface name filter.
            partitions: Optional partition file filter.

        Returns:
            Combined DataFrame from all matching batches.
        """
        frames: List[pd.DataFrame] = []
        for batch_dir, manifest in self.iter_all_batches():
            if manifest.source_name != source_name:
                continue
            if interface_name and manifest.interface_name != interface_name:
                continue
            df, _ = self.read_batch(batch_dir, partitions=partitions)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------
    # Low-level read APIs
    # ------------------------------------------------------------------

    def read_batch(
        self,
        batch_dir: Path,
        partitions: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, Manifest]:
        """Read a single batch directory.

        Args:
            batch_dir: Path to the batch directory.
            partitions: Optional list of partition filenames to read.
                Defaults to all part-*.parquet files.

        Returns:
            Tuple of (combined DataFrame, loaded Manifest).

        Raises:
            FileNotFoundError: If manifest or parquet files are missing.
        """
        manifest_path = batch_dir / MANIFEST_FILENAME
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        manifest = Manifest.load(manifest_path)

        if partitions:
            parquet_files = [batch_dir / p for p in partitions]
        else:
            parquet_files = sorted(batch_dir.glob("part-*.parquet"))

        if not parquet_files:
            return pd.DataFrame(), manifest

        frames: List[pd.DataFrame] = []
        for pf in parquet_files:
            if pf.exists():
                frames.append(pd.read_parquet(pf))

        if not frames:
            return pd.DataFrame(), manifest

        return pd.concat(frames, ignore_index=True), manifest

    # ------------------------------------------------------------------
    # Discovery APIs
    # ------------------------------------------------------------------

    def find_batch_dirs(
        self,
        batch_id: Optional[str] = None,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> Iterator[Path]:
        """Find batch directories matching the given filters.

        Args:
            batch_id: Optional batch_id filter.
            domain: Optional domain filter.
            dataset: Optional dataset filter.

        Yields:
            Paths to matching batch directories.
        """
        search_root = self._build_search_root(domain, dataset)
        pattern = "**/batch_id=*"

        for batch_dir in sorted(search_root.glob(pattern)):
            if not batch_dir.is_dir():
                continue

            manifest_path = batch_dir / MANIFEST_FILENAME
            if not manifest_path.exists():
                continue

            if batch_id:
                dir_batch_id = batch_dir.name
                if not fnmatch.fnmatch(dir_batch_id, f"batch_id={batch_id}"):
                    continue

            yield batch_dir

    def iter_batches(
        self,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
        extract_date: Optional[date] = None,
    ) -> Iterator[Tuple[Path, Manifest]]:
        """Iterate over all batches, yielding (batch_dir, manifest).

        Args:
            domain: Optional domain filter.
            dataset: Optional dataset filter.
            extract_date: Optional extract_date filter.

        Yields:
            Tuple of (batch directory path, loaded Manifest).
        """
        search_root = self._build_search_root(domain, dataset)

        for batch_dir in sorted(search_root.glob("**/batch_id=*")):
            if not batch_dir.is_dir():
                continue

            manifest_path = batch_dir / MANIFEST_FILENAME
            if not manifest_path.exists():
                continue

            manifest = Manifest.load(manifest_path)

            if extract_date:
                ed = manifest.extract_date_parsed
                if ed is None or ed != extract_date:
                    continue

            yield batch_dir, manifest

    def iter_all_batches(self) -> Iterator[Tuple[Path, Manifest]]:
        """Iterate over every batch in the raw tree.

        Yields:
            Tuple of (batch directory path, loaded Manifest).
        """
        yield from self.iter_batches()

    def list_datasets(self, domain: Optional[str] = None) -> List[str]:
        """List all dataset names under a domain.

        Args:
            domain: Optional domain filter. If None, lists across all domains.

        Returns:
            Sorted list of unique dataset names.
        """
        datasets: set[str] = set()

        if domain:
            domain_dir = self._base_dir / domain
            if not domain_dir.exists():
                return []
            for entry in domain_dir.iterdir():
                if entry.is_dir() and not entry.name.startswith((".", "_")):
                    datasets.add(entry.name)
        else:
            if not self._base_dir.exists():
                return []
            for domain_entry in self._base_dir.iterdir():
                if domain_entry.is_dir() and not domain_entry.name.startswith(
                    (".", "_")
                ):
                    for ds_entry in domain_entry.iterdir():
                        if ds_entry.is_dir() and not ds_entry.name.startswith(
                            (".", "_")
                        ):
                            datasets.add(ds_entry.name)

        return sorted(datasets)

    def list_domains(self) -> List[str]:
        """List all domain names in the raw tree.

        Returns:
            Sorted list of unique domain names.
        """
        if not self._base_dir.exists():
            return []
        return sorted(
            entry.name
            for entry in self._base_dir.iterdir()
            if entry.is_dir() and not entry.name.startswith((".", "_"))
        )

    def get_schema_snapshot(self, batch_dir: Path) -> List[Dict[str, str]]:
        """Load the _schema.json for a batch directory.

        Args:
            batch_dir: Path to the batch directory.

        Returns:
            List of {"name": ..., "dtype": ...} entries.
        """
        return load_schema_snapshot(batch_dir)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_search_root(
        self,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> Path:
        """Build the root path for glob searches.

        Args:
            domain: Optional domain to scope the search.
            dataset: Optional dataset to scope the search.

        Returns:
            Path to use as the glob root.
        """
        if not self._base_dir.exists():
            return self._base_dir

        if domain and dataset:
            return self._base_dir / domain / dataset
        if domain:
            return self._base_dir / domain
        return self._base_dir
