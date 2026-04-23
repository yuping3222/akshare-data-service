"""tests/test_store_parquet.py

Comprehensive tests for PartitionManager and AtomicWriter classes in store/parquet.py
"""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from akshare_data.store.parquet import AtomicWriter, PartitionManager


@pytest.fixture
def temp_base_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", "2024-01-10"),
            "symbol": ["sh600000"] * 10,
            "open": [10.0] * 10,
            "high": [11.0] * 10,
            "low": [9.0] * 10,
            "close": [10.5] * 10,
            "volume": [100000] * 10,
        }
    )


class TestPartitionManagerInit:
    """Tests for PartitionManager initialization."""

    def test_init_with_string_path(self, temp_base_dir):
        """Test initialization with string path."""
        pm = PartitionManager(str(temp_base_dir))
        assert pm.base_dir == temp_base_dir

    def test_init_with_path_object(self, temp_base_dir):
        """Test initialization with Path object."""
        pm = PartitionManager(temp_base_dir)
        assert pm.base_dir == temp_base_dir


class TestPartitionManagerPaths:
    """Tests for PartitionManager path generation methods."""

    def test_raw_partition_path_with_partition(self, temp_base_dir):
        """Test raw_partition_path with partition specified."""
        pm = PartitionManager(temp_base_dir)
        path = pm.raw_partition_path("table", "daily", "date", "2024-01-01")
        expected = temp_base_dir / "daily" / "table" / "date=2024-01-01"
        assert path == expected

    def test_raw_partition_path_without_partition(self, temp_base_dir):
        """Test raw_partition_path without partition returns meta path."""
        pm = PartitionManager(temp_base_dir)
        path = pm.raw_partition_path("table", "daily", None, None)
        expected = temp_base_dir / "meta" / "table"
        assert path == expected

    def test_aggregated_path_with_partition(self, temp_base_dir):
        """Test aggregated_path with partition specified."""
        pm = PartitionManager(temp_base_dir)
        path = pm.aggregated_path("table", "daily", "date", "2024-01-01")
        expected = (
            temp_base_dir / "aggregated" / "daily" / "table" / "date=2024-01-01.parquet"
        )
        assert path == expected

    def test_aggregated_path_without_partition(self, temp_base_dir):
        """Test aggregated_path without partition."""
        pm = PartitionManager(temp_base_dir)
        path = pm.aggregated_path("table", "daily", None, None)
        expected = temp_base_dir / "aggregated" / "meta" / "table.parquet"
        assert path == expected


class TestPartitionManagerFileOperations:
    """Tests for PartitionManager file operations."""

    def test_generate_filename(self, temp_base_dir):
        """Test filename generation includes pid and uuid."""
        pm = PartitionManager(temp_base_dir)
        filename = pm.generate_filename()
        assert filename.startswith("part_")
        assert filename.endswith(".parquet")
        pid_in_filename = f"part_{os.getpid()}_" in filename
        assert pid_in_filename

    def test_generate_filename_with_partition_value(self, temp_base_dir):
        """Test filename generation with partition value."""
        pm = PartitionManager(temp_base_dir)
        filename = pm.generate_filename("2024-01-01")
        assert filename.startswith("part_")
        assert filename.endswith(".parquet")

    def test_list_partition_files_empty(self, temp_base_dir):
        """Test list_partition_files returns empty list for non-existent path."""
        pm = PartitionManager(temp_base_dir)
        files = pm.list_partition_files("table", "daily", "date", "2024-01-01")
        assert files == []

    def test_list_partition_files_with_files(self, temp_base_dir, sample_df):
        """Test list_partition_files returns existing files."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)
        writer.write(
            "table",
            "daily",
            sample_df,
            partition_by="date",
            partition_value="2024-01-01",
        )
        files = pm.list_partition_files("table", "daily", "date", "2024-01-01")
        assert len(files) > 0
        assert all(f.suffix == ".parquet" for f in files)

    def test_list_partition_files_excludes_tmp(self, temp_base_dir, sample_df):
        """Test list_partition_files excludes .tmp files."""
        pm = PartitionManager(temp_base_dir)
        path = pm.raw_partition_path("table", "daily", "date", "2024-01-01")
        path.mkdir(parents=True, exist_ok=True)
        (path / "part_123_abc.tmp").touch()
        (path / "part_123_abc.parquet").touch()
        files = pm.list_partition_files("table", "daily", "date", "2024-01-01")
        assert len(files) == 1
        assert files[0].name == "part_123_abc.parquet"

    def test_list_all_partitions_empty(self, temp_base_dir):
        """Test list_all_partitions returns empty for non-existent table."""
        pm = PartitionManager(temp_base_dir)
        partitions = pm.list_all_partitions("table", "daily", "date")
        assert partitions == []

    def test_list_all_partitions_with_data(self, temp_base_dir, sample_df):
        """Test list_all_partitions returns partition values."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)
        for date in ["2024-01-01", "2024-01-02", "2024-01-03"]:
            writer.write(
                "table",
                "daily",
                sample_df.head(1),
                partition_by="date",
                partition_value=date,
            )
        partitions = pm.list_all_partitions("table", "daily", "date")
        assert len(partitions) == 3
        assert "2024-01-01" in partitions
        assert "2024-01-02" in partitions
        assert "2024-01-03" in partitions

    def test_list_all_partitions_with_none_partition_by(self, temp_base_dir):
        """Test list_all_partitions returns [''] when partition_by is None."""
        pm = PartitionManager(temp_base_dir)
        partitions = pm.list_all_partitions("table", "daily", None)
        assert partitions == [""]

    def test_list_all_glob_paths_raw(self, temp_base_dir):
        """Test list_all_glob_paths for raw layer."""
        pm = PartitionManager(temp_base_dir)
        glob_path = pm.list_all_glob_paths("table", "daily", "date", layer="raw")
        assert "daily" in glob_path
        assert "table" in glob_path

    def test_list_all_glob_paths_aggregated(self, temp_base_dir):
        """Test list_all_glob_paths for aggregated layer."""
        pm = PartitionManager(temp_base_dir)
        glob_path = pm.list_all_glob_paths("table", "daily", "date", layer="aggregated")
        assert "aggregated" in glob_path


class TestPartitionManagerEnsureDir:
    """Tests for PartitionManager.ensure_dir() method."""

    def test_ensure_dir_creates_path(self, temp_base_dir):
        """Test ensure_dir creates directory."""
        pm = PartitionManager(temp_base_dir)
        path = temp_base_dir / "new" / "nested" / "path"
        result = pm.ensure_dir(path)
        assert path.exists()
        assert result == path

    def test_ensure_dir_existing_path(self, temp_base_dir):
        """Test ensure_dir on existing path doesn't fail."""
        pm = PartitionManager(temp_base_dir)
        path = temp_base_dir / "existing"
        path.mkdir()
        pm.ensure_dir(path)
        assert path.exists()


class TestPartitionManagerFileStats:
    """Tests for PartitionManager file statistics methods."""

    def test_file_count_empty(self, temp_base_dir):
        """Test file_count returns 0 for empty directory."""
        pm = PartitionManager(temp_base_dir)
        count = pm.file_count("table", "daily", "date", "2024-01-01")
        assert count == 0

    def test_file_count_with_files(self, temp_base_dir, sample_df):
        """Test file_count returns correct count."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)
        writer.write(
            "table",
            "daily",
            sample_df.head(1),
            partition_by="date",
            partition_value="2024-01-01",
        )
        writer.write(
            "table",
            "daily",
            sample_df.tail(1),
            partition_by="date",
            partition_value="2024-01-02",
        )
        count = pm.file_count("table", "daily", "date", "2024-01-01")
        assert count == 1

    def test_total_size_bytes_empty(self, temp_base_dir):
        """Test total_size_bytes returns 0 for empty directory."""
        pm = PartitionManager(temp_base_dir)
        size = pm.total_size_bytes("table", "daily", "date", "2024-01-01")
        assert size == 0

    def test_total_size_bytes_with_files(self, temp_base_dir, sample_df):
        """Test total_size_bytes returns correct size."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)
        writer.write(
            "table",
            "daily",
            sample_df,
            partition_by="date",
            partition_value="2024-01-01",
        )
        size = pm.total_size_bytes("table", "daily", "date", "2024-01-01")
        assert size > 0


class TestPartitionManagerLockPath:
    """Tests for PartitionManager.lock_path() method."""

    def test_lock_path_format(self, temp_base_dir):
        """Test lock_path returns correct format."""
        pm = PartitionManager(temp_base_dir)
        lock_path = pm.lock_path("test_lock")
        expected = temp_base_dir / "_locks" / "test_lock.lock"
        assert lock_path == expected


class TestPartitionManagerRemove:
    """Tests for PartitionManager remove operations."""

    def test_remove_file_success(self, temp_base_dir):
        """Test remove_file returns True for existing file."""
        pm = PartitionManager(temp_base_dir)
        test_file = temp_base_dir / "test_file.txt"
        test_file.write_text("test content")
        result = pm.remove_file(test_file)
        assert result is True
        assert not test_file.exists()

    def test_remove_file_not_found(self, temp_base_dir):
        """Test remove_file returns False for non-existent file."""
        pm = PartitionManager(temp_base_dir)
        test_file = temp_base_dir / "nonexistent_file.txt"
        result = pm.remove_file(test_file)
        assert result is False

    def test_remove_dir_success(self, temp_base_dir):
        """Test remove_dir removes directory tree."""
        pm = PartitionManager(temp_base_dir)
        test_dir = temp_base_dir / "test_dir" / "nested"
        test_dir.mkdir(parents=True)
        result = pm.remove_dir(test_dir)
        assert result is True
        assert not test_dir.exists()

    def test_remove_dir_not_found(self, temp_base_dir):
        """Test remove_dir returns False for non-existent directory."""
        pm = PartitionManager(temp_base_dir)
        test_dir = temp_base_dir / "nonexistent_dir"
        result = pm.remove_dir(test_dir)
        assert result is False


class TestAtomicWriterInit:
    """Tests for AtomicWriter initialization."""

    def test_init_default_values(self, temp_base_dir):
        """Test AtomicWriter default initialization."""
        writer = AtomicWriter(temp_base_dir)
        assert writer.base_dir == temp_base_dir
        assert writer.compression == "snappy"
        assert writer.row_group_size == 100_000

    def test_init_custom_values(self, temp_base_dir):
        """Test AtomicWriter custom initialization."""
        writer = AtomicWriter(
            temp_base_dir,
            compression="gzip",
            row_group_size=50000,
        )
        assert writer.compression == "gzip"
        assert writer.row_group_size == 50000


class TestAtomicWriterWrite:
    """Tests for AtomicWriter.write() method."""

    def test_write_basic(self, temp_base_dir, sample_df):
        """Test basic write operation."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write("table", "daily", sample_df)
        assert file_path.exists()
        assert file_path.suffix == ".parquet"

    def test_write_creates_directory(self, temp_base_dir, sample_df):
        """Test write creates necessary directories."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write("table", "daily", sample_df)
        assert file_path.parent.exists()

    def test_write_with_partition(self, temp_base_dir, sample_df):
        """Test write with partition specification."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write(
            "table",
            "daily",
            sample_df,
            partition_by="symbol",
            partition_value="sh600000",
        )
        assert file_path.exists()
        assert "symbol=sh600000" in str(file_path)

    def test_write_deduplicates_by_primary_key(self, temp_base_dir):
        """Test write deduplicates by primary key."""
        df = pd.DataFrame(
            {
                "symbol": ["sh600000", "sh600000", "sh600000"],
                "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "close": [10.0, 10.5, 11.0],
            }
        )
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write(
            "table",
            "daily",
            df,
            primary_key=["symbol", "date"],
        )
        table = pq.read_table(file_path)
        result_df = table.to_pandas()
        assert len(result_df) == 2

    def test_write_returns_path_object(self, temp_base_dir, sample_df):
        """Test write returns a Path object."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write("table", "daily", sample_df)
        assert isinstance(file_path, Path)

    def test_written_file_is_valid_parquet(self, temp_base_dir, sample_df):
        """Test written file is a valid parquet file."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write("table", "daily", sample_df)
        table = pq.read_table(file_path)
        result_df = table.to_pandas()
        assert len(result_df) == len(sample_df)


class TestAtomicWriterWriteMeta:
    """Tests for AtomicWriter.write_meta() method."""

    def test_write_meta_basic(self, temp_base_dir, sample_df):
        """Test write_meta creates meta table."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write_meta("meta_table", sample_df)
        assert file_path.exists()
        assert file_path.parent.name == "meta"

    def test_write_meta_with_schema(self, temp_base_dir, sample_df):
        """Test write_meta with schema specification."""
        writer = AtomicWriter(temp_base_dir)
        schema = {"date": "string", "symbol": "string", "close": "float"}
        file_path = writer.write_meta("meta_table", sample_df, schema=schema)
        assert file_path.parent.name == "meta"


class TestAtomicWriterAtomicity:
    """Tests for AtomicWriter atomic write behavior."""

    def test_write_is_atomic(self, temp_base_dir, sample_df):
        """Test that write is atomic (no partial files on failure)."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write("table", "daily", sample_df)
        tmp_files = list(file_path.parent.glob("*.tmp"))
        assert len(tmp_files) == 0


class TestAtomicWriterEdgeCases:
    """Tests for AtomicWriter edge cases."""

    def test_write_empty_dataframe(self, temp_base_dir):
        """Test write with empty DataFrame."""
        writer = AtomicWriter(temp_base_dir)
        empty_df = pd.DataFrame()
        file_path = writer.write("table", "daily", empty_df)
        assert file_path.exists()
        table = pq.read_table(file_path)
        assert table.to_pandas().empty

    def test_write_with_special_characters_in_table_name(
        self, temp_base_dir, sample_df
    ):
        """Test write with special characters in table name."""
        writer = AtomicWriter(temp_base_dir)
        file_path = writer.write("table_with_underscore", "daily", sample_df)
        assert file_path.exists()


class TestPartitionManagerIntegration:
    """Integration tests for PartitionManager with actual file operations."""

    def test_full_partition_lifecycle(self, temp_base_dir, sample_df):
        """Test complete partition creation and listing lifecycle."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)

        for date in ["2024-01-01", "2024-01-02", "2024-01-03"]:
            writer.write(
                "stock_daily",
                "daily",
                sample_df.head(3),
                partition_by="date",
                partition_value=date,
            )

        partitions = pm.list_all_partitions("stock_daily", "daily", "date")
        assert len(partitions) == 3

        for date in ["2024-01-01", "2024-01-02", "2024-01-03"]:
            files = pm.list_partition_files("stock_daily", "daily", "date", date)
            assert len(files) >= 1

    def test_remove_partition_files(self, temp_base_dir, sample_df):
        """Test removing partition files."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)

        writer.write(
            "stock_daily",
            "daily",
            sample_df,
            partition_by="date",
            partition_value="2024-01-01",
        )

        files = pm.list_partition_files("stock_daily", "daily", "date", "2024-01-01")
        assert len(files) == 1

        for f in files:
            pm.remove_file(f)

        remaining_files = pm.list_partition_files(
            "stock_daily", "daily", "date", "2024-01-01"
        )
        assert len(remaining_files) == 0

    def test_partition_metadata_access(self, temp_base_dir, sample_df):
        """Test accessing partition metadata."""
        pm = PartitionManager(temp_base_dir)
        writer = AtomicWriter(temp_base_dir)

        writer.write(
            "stock_daily",
            "daily",
            sample_df,
            partition_by="date",
            partition_value="2024-01-01",
        )

        file_count = pm.file_count("stock_daily", "daily", "date", "2024-01-01")
        assert file_count == 1

        total_size = pm.total_size_bytes("stock_daily", "daily", "date", "2024-01-01")
        assert total_size > 0


class TestPartitionManagerPathEdgeCases:
    """Tests for PartitionManager path edge cases."""

    def test_path_with_multiple_partition_levels(self, temp_base_dir):
        """Test path generation with complex partition structure."""
        pm = PartitionManager(temp_base_dir)
        path = pm.raw_partition_path("table", "daily", "symbol", "sh600000")
        assert "symbol=sh600000" in str(path)

    def test_aggregated_path_with_multiple_partitions(self, temp_base_dir):
        """Test aggregated path for partitioned table."""
        pm = PartitionManager(temp_base_dir)
        path = pm.aggregated_path("table", "daily", "date", "2024-01-01")
        assert path.suffix == ".parquet"
        assert "date=2024-01-01" in str(path)
