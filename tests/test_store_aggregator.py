"""tests/test_store_aggregator.py

Comprehensive tests for Aggregator class in store/aggregator.py
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from akshare_data.store.aggregator import (
    AggregationError,
    Aggregator,
    run_aggregation,
)


@pytest.fixture
def temp_base_dir(tmp_path):
    """Create a temporary base directory."""
    return tmp_path


@pytest.fixture
def mock_table_schema():
    """Create a mock table schema."""
    schema = MagicMock()
    schema.name = "test_table"
    schema.aggregation_enabled = True
    schema.partition_by = "date"
    schema.storage_layer = "raw"
    schema.compaction_threshold = 3
    schema.primary_key = ["id"]
    schema.priority = "high"
    return schema


@pytest.fixture
def sample_df():
    """Create a sample DataFrame."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [10, 20, 30],
        }
    )


class TestAggregatorInit:
    """Tests for Aggregator initialization."""

    def test_init_with_string_path(self, temp_base_dir):
        """Test initialization with string path."""
        aggregator = Aggregator(str(temp_base_dir))
        assert aggregator.base_dir == Path(temp_base_dir)
        assert aggregator.lock_timeout == 0

    def test_init_with_path_object(self, temp_base_dir):
        """Test initialization with Path object."""
        aggregator = Aggregator(temp_base_dir)
        assert aggregator.base_dir == temp_base_dir

    def test_init_with_lock_timeout(self, temp_base_dir):
        """Test initialization with custom lock timeout."""
        aggregator = Aggregator(temp_base_dir, lock_timeout=30)
        assert aggregator.lock_timeout == 30


class TestAggregatorAggregateTable:
    """Tests for aggregate_table method."""

    @patch("akshare_data.store.aggregator.get_table_schema")
    @patch("akshare_data.store.aggregator.FileLock")
    def test_aggregate_table_no_schema(self, mock_lock, mock_get_schema, temp_base_dir):
        """Test aggregate_table returns 0 when table schema is None."""
        mock_get_schema.return_value = None
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.aggregate_table("nonexistent_table")
        assert result == 0

    @patch("akshare_data.store.aggregator.get_table_schema")
    @patch("akshare_data.store.aggregator.FileLock")
    def test_aggregate_table_aggregation_disabled(
        self, mock_lock, mock_get_schema, temp_base_dir
    ):
        """Test aggregate_table returns 0 when aggregation is disabled."""
        schema = MagicMock()
        schema.aggregation_enabled = False
        mock_get_schema.return_value = schema
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.aggregate_table("test_table")
        assert result == 0

    @patch("akshare_data.store.aggregator.get_table_schema")
    @patch("akshare_data.store.aggregator.FileLock")
    def test_aggregate_table_lock_acquisition_failure(
        self, mock_lock, mock_get_schema, temp_base_dir
    ):
        """Test aggregate_table returns 0 when lock cannot be acquired."""
        schema = MagicMock()
        schema.aggregation_enabled = True
        mock_get_schema.return_value = schema
        mock_lock.return_value.acquire.side_effect = Exception("Lock failed")
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.aggregate_table("test_table")
        assert result == 0

    @patch("akshare_data.store.aggregator.get_table_schema")
    @patch("akshare_data.store.aggregator.Aggregator.needs_aggregation")
    @patch("akshare_data.store.aggregator.FileLock")
    def test_aggregate_table_no_pending(
        self, mock_lock, mock_needs_agg, mock_get_schema, temp_base_dir
    ):
        """Test aggregate_table returns 0 when no partitions need aggregation."""
        schema = MagicMock()
        schema.aggregation_enabled = True
        schema.partition_by = "date"
        schema.storage_layer = "raw"
        mock_get_schema.return_value = schema
        mock_needs_agg.return_value = []
        mock_lock_instance = MagicMock()
        mock_lock.return_value = mock_lock_instance

        aggregator = Aggregator(temp_base_dir)
        result = aggregator.aggregate_table("test_table")
        assert result == 0
        mock_lock_instance.release.assert_called_once()


class TestAggregatorAggregateAll:
    """Tests for aggregate_all method."""

    @patch("akshare_data.core.schema.SCHEMA_REGISTRY")
    def test_aggregate_all_empty_registry(self, mock_registry, temp_base_dir):
        """Test aggregate_all with empty registry."""
        mock_registry.tables = {}
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.aggregate_all()
        assert result == {}

    @patch("akshare_data.core.schema.SCHEMA_REGISTRY")
    @patch("akshare_data.store.aggregator.FileLock")
    def test_aggregate_all_with_priority_filter(
        self, mock_lock, mock_registry, temp_base_dir
    ):
        """Test aggregate_all filters by priority."""
        schema = MagicMock()
        schema.aggregation_enabled = True
        schema.partition_by = "date"
        schema.storage_layer = "raw"
        schema.compaction_threshold = 3
        schema.priority = "low"

        mock_table_schema = MagicMock()
        mock_table_schema.name = "test_table"
        mock_table_schema.aggregation_enabled = True
        mock_table_schema.partition_by = "date"
        mock_table_schema.storage_layer = "raw"
        mock_table_schema.compaction_threshold = 3
        mock_table_schema.priority = "high"

        mock_registry.tables = {
            "high_table": mock_table_schema,
            "low_table": schema,
        }

        mock_lock_instance = MagicMock()
        mock_lock.return_value = mock_lock_instance

        aggregator = Aggregator(temp_base_dir)
        with patch.object(aggregator, "needs_aggregation", return_value=[]):
            result = aggregator.aggregate_all(priority="high")
        assert "low_table" not in result


class TestAggregatorCleanup:
    """Tests for cleanup method."""

    @patch("akshare_data.core.schema.SCHEMA_REGISTRY")
    def test_cleanup_empty_registry(self, mock_registry, temp_base_dir):
        """Test cleanup with empty registry returns 0."""
        mock_registry.tables = {}
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.cleanup()
        assert result == 0

    @patch("akshare_data.core.schema.SCHEMA_REGISTRY")
    def test_cleanup_with_retention_hours(self, mock_registry, temp_base_dir):
        """Test cleanup with custom retention hours."""
        mock_registry.tables = {}
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.cleanup(retention_hours=48)
        assert result == 0


class TestAggregatorNeedsAggregation:
    """Tests for needs_aggregation method."""

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_needs_aggregation_no_schema(self, mock_get_schema, temp_base_dir):
        """Test needs_aggregation returns empty list when schema is None."""
        mock_get_schema.return_value = None
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.needs_aggregation("nonexistent")
        assert result == []

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_needs_aggregation_disabled(self, mock_get_schema, temp_base_dir):
        """Test needs_aggregation returns empty list when aggregation disabled."""
        schema = MagicMock()
        schema.aggregation_enabled = False
        mock_get_schema.return_value = schema
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.needs_aggregation("test_table")
        assert result == []

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_needs_aggregation_no_partition_by(self, mock_get_schema, temp_base_dir):
        """Test needs_aggregation returns empty list when no partition_by."""
        schema = MagicMock()
        schema.aggregation_enabled = True
        schema.partition_by = None
        mock_get_schema.return_value = schema
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.needs_aggregation("test_table")
        assert result == []


class TestAggregatorGetAggregationStatus:
    """Tests for get_aggregation_status method."""

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_get_aggregation_status_no_schema(self, mock_get_schema, temp_base_dir):
        """Test get_aggregation_status returns empty dict when schema is None."""
        mock_get_schema.return_value = None
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.get_aggregation_status("nonexistent")
        assert result == {}

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_get_aggregation_status_no_partition_by(
        self, mock_get_schema, temp_base_dir
    ):
        """Test get_aggregation_status with no partition_by."""
        schema = MagicMock()
        schema.partition_by = None
        mock_get_schema.return_value = schema
        aggregator = Aggregator(temp_base_dir)
        result = aggregator.get_aggregation_status("test_table")
        assert result["total_partitions"] == 1


class TestAggregatorAcquireLock:
    """Tests for _acquire_lock method."""

    def test_acquire_lock_success(self, temp_base_dir):
        """Test _acquire_lock returns lock on success."""
        aggregator = Aggregator(temp_base_dir)
        lock = aggregator._acquire_lock("test_table")
        assert lock is not None
        lock.release()

    def test_acquire_lock_creates_parent_dirs(self, temp_base_dir):
        """Test _acquire_lock creates parent directories."""
        aggregator = Aggregator(temp_base_dir)
        lock = aggregator._acquire_lock("test_table")
        assert lock is not None
        lock.release()


class TestAggregatorAggregatePartition:
    """Tests for _aggregate_partition method."""

    @patch("akshare_data.store.aggregator.deduplicate_by_key")
    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_aggregate_partition_empty_data(
        self, mock_get_schema, mock_dedup, temp_base_dir, mock_table_schema
    ):
        """Test _aggregate_partition returns False when data is empty."""
        mock_get_schema.return_value = mock_table_schema
        aggregator = Aggregator(temp_base_dir)
        with patch.object(aggregator, "_read_partition_raw", return_value=None):
            result = aggregator._aggregate_partition(mock_table_schema, "2024-01")
            assert result is False

    @patch("akshare_data.store.aggregator.deduplicate_by_key")
    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_aggregate_partition_with_empty_df(
        self, mock_get_schema, mock_dedup, temp_base_dir, mock_table_schema
    ):
        """Test _aggregate_partition returns False when data DataFrame is empty."""
        mock_get_schema.return_value = mock_table_schema
        aggregator = Aggregator(temp_base_dir)
        with patch.object(
            aggregator, "_read_partition_raw", return_value=pd.DataFrame()
        ):
            result = aggregator._aggregate_partition(mock_table_schema, "2024-01")
            assert result is False

    @patch("akshare_data.store.aggregator.deduplicate_by_key")
    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_aggregate_partition_success(
        self, mock_get_schema, mock_dedup, temp_base_dir, mock_table_schema, sample_df
    ):
        """Test _aggregate_partition returns True on success."""
        mock_get_schema.return_value = mock_table_schema
        aggregator = Aggregator(temp_base_dir)
        with patch.object(aggregator, "_read_partition_raw", return_value=sample_df):
            with patch.object(aggregator, "_write_aggregated"):
                result = aggregator._aggregate_partition(mock_table_schema, "2024-01")
                assert result is True

    @patch("akshare_data.store.aggregator.deduplicate_by_key")
    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_aggregate_partition_no_primary_key(
        self, mock_get_schema, mock_dedup, temp_base_dir, sample_df
    ):
        """Test _aggregate_partition skips deduplication when no primary key."""
        schema = MagicMock()
        schema.name = "test_table"
        schema.aggregation_enabled = True
        schema.partition_by = "date"
        schema.storage_layer = "raw"
        schema.compaction_threshold = 3
        schema.primary_key = None

        mock_get_schema.return_value = schema
        aggregator = Aggregator(temp_base_dir)
        with patch.object(aggregator, "_read_partition_raw", return_value=sample_df):
            with patch.object(aggregator, "_write_aggregated"):
                result = aggregator._aggregate_partition(schema, "2024-01")
                assert result is True
        mock_dedup.assert_not_called()


class TestAggregatorReadPartitionRaw:
    """Tests for _read_partition_raw method."""

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_read_partition_raw_path_not_exists(
        self, mock_get_schema, temp_base_dir, mock_table_schema
    ):
        """Test _read_partition_raw returns None when path doesn't exist."""
        mock_get_schema.return_value = mock_table_schema
        aggregator = Aggregator(temp_base_dir)
        result = aggregator._read_partition_raw(mock_table_schema, "nonexistent")
        assert result is None

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_read_partition_raw_no_parquet_files(
        self, mock_get_schema, temp_base_dir, mock_table_schema
    ):
        """Test _read_partition_raw returns None when no parquet files."""
        mock_get_schema.return_value = mock_table_schema
        raw_path = temp_base_dir / "raw" / "test_table" / "date=2024-01"
        raw_path.mkdir(parents=True)
        aggregator = Aggregator(temp_base_dir)
        result = aggregator._read_partition_raw(mock_table_schema, "2024-01")
        assert result is None


class TestAggregatorWriteAggregated:
    """Tests for _write_aggregated method."""

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_write_aggregated_creates_directory(
        self, mock_get_schema, temp_base_dir, mock_table_schema, sample_df
    ):
        """Test _write_aggregated creates parent directory."""
        mock_get_schema.return_value = mock_table_schema
        (
            temp_base_dir
            / "aggregated"
            / "test_table"
            / "date=2024-01"
            / "data.parquet"
        )
        aggregator = Aggregator(temp_base_dir)
        result = aggregator._write_aggregated(mock_table_schema, "2024-01", sample_df)
        assert result.parent.exists()

    @patch("akshare_data.store.aggregator.get_table_schema")
    def test_write_aggregated_success(
        self, mock_get_schema, temp_base_dir, mock_table_schema, sample_df
    ):
        """Test _write_aggregated writes file successfully."""
        mock_get_schema.return_value = mock_table_schema
        aggregator = Aggregator(temp_base_dir)
        result = aggregator._write_aggregated(mock_table_schema, "2024-01", sample_df)
        assert result.exists()


class TestRunAggregation:
    """Tests for run_aggregation function."""

    @patch("akshare_data.store.aggregator.Aggregator")
    def test_run_aggregation_with_tables_list(
        self, mock_aggregator_class, temp_base_dir
    ):
        """Test run_aggregation with specific tables list."""
        mock_aggregator = MagicMock()
        mock_aggregator.aggregate_table.return_value = 2
        mock_aggregator.cleanup.return_value = 5
        mock_aggregator_class.return_value = mock_aggregator

        result = run_aggregation(
            str(temp_base_dir),
            tables=["table1", "table2"],
            cleanup=True,
            retention_hours=24,
        )
        assert result["aggregated"]["table1"] == 2
        assert result["aggregated"]["table2"] == 2
        assert result["cleaned_files"] == 5

    @patch("akshare_data.store.aggregator.Aggregator")
    def test_run_aggregation_no_cleanup(self, mock_aggregator_class, temp_base_dir):
        """Test run_aggregation without cleanup."""
        mock_aggregator = MagicMock()
        mock_aggregator.aggregate_all.return_value = {"table1": 3}
        mock_aggregator.cleanup.return_value = 0
        mock_aggregator_class.return_value = mock_aggregator

        result = run_aggregation(
            str(temp_base_dir),
            cleanup=False,
            retention_hours=24,
        )
        assert result["aggregated"]["table1"] == 3
        assert result["cleaned_files"] == 0

    @patch("akshare_data.store.aggregator.Aggregator")
    def test_run_aggregation_no_tables_no_priority(
        self, mock_aggregator_class, temp_base_dir
    ):
        """Test run_aggregation with no tables and no priority."""
        mock_aggregator = MagicMock()
        mock_aggregator.aggregate_all.return_value = {}
        mock_aggregator.cleanup.return_value = 0
        mock_aggregator_class.return_value = mock_aggregator

        run_aggregation(str(temp_base_dir))
        mock_aggregator.aggregate_all.assert_called_with(priority=None)


class TestAggregationError:
    """Tests for AggregationError exception."""

    def test_aggregation_error_creation(self):
        """Test AggregationError can be created."""
        error = AggregationError("Test error message")
        assert str(error) == "Test error message"

    def test_aggregation_error_with_format(self):
        """Test AggregationError with format string."""
        error = AggregationError("Error: %s %d")
        assert "Error:" in str(error)
