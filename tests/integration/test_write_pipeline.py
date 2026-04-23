"""Integration tests for the data write pipeline.

Tests cover schema registration, validation, parquet writes (atomic,
partitioned, incremental, overwrite), storage strategies, and the
end-to-end write pipeline (schema -> validate -> parquet -> verify).
"""
# ruff: noqa: E402

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

pytestmark = pytest.mark.integration

from akshare_data.core.schema import (
    SCHEMA_REGISTRY,
    get_table_schema,
    list_tables,
)
from akshare_data.store.validator import SchemaValidationError, SchemaValidator
from akshare_data.store.manager import CacheManager, reset_cache_manager
from akshare_data.store.parquet import AtomicWriter, PartitionManager
from akshare_data.store.strategies import FullCacheStrategy, IncrementalStrategy


# ---------------------------------------------------------------------------
# 1. Schema registration
# ---------------------------------------------------------------------------


class TestSchemaRegistration:
    """Verify that all 64 cache tables are properly registered and queryable."""

    def test_schemas_are_initialized(self):
        """Calling init_schemas should populate the global registry."""
        registry = SCHEMA_REGISTRY
        all_tables = registry.list_all()
        assert len(all_tables) >= 64, (
            f"Expected at least 64 tables, got {len(all_tables)}"
        )

    def test_all_registered_tables_are_queryable(self):
        """Every registered table should be retrievable via get() and get_or_none()."""
        for name in list_tables():
            table = get_table_schema(name)
            assert table is not None, (
                f"Table '{name}' not queryable via get_table_schema"
            )
            assert table.name == name
            assert table.schema
            assert table.primary_key

    def test_get_or_none_for_unknown_table(self):
        """get_or_none should return None for an unregistered table."""
        assert get_table_schema("nonexistent_table_xyz") is None

    def test_get_raises_for_unknown_table(self):
        """get() should raise KeyError for an unregistered table."""
        with pytest.raises(KeyError):
            SCHEMA_REGISTRY.get("nonexistent_table_xyz")

    def test_has_method(self):
        """has() should correctly report table existence."""
        assert SCHEMA_REGISTRY.has("stock_daily") is True
        assert SCHEMA_REGISTRY.has("nonexistent_table_xyz") is False

    def test_list_by_layer(self):
        """list_by_layer should return only tables in the specified layer."""
        daily_tables = SCHEMA_REGISTRY.list_by_layer("daily")
        assert len(daily_tables) > 0
        assert all(t.storage_layer == "daily" for t in daily_tables)

        meta_tables = SCHEMA_REGISTRY.list_by_layer("meta")
        assert len(meta_tables) > 0
        assert all(t.storage_layer == "meta" for t in meta_tables)

        snapshot_tables = SCHEMA_REGISTRY.list_by_layer("snapshot")
        assert len(snapshot_tables) > 0
        assert all(t.storage_layer == "snapshot" for t in snapshot_tables)

    def test_list_by_priority(self):
        """list_by_priority should return only tables in the specified priority."""
        p0_tables = SCHEMA_REGISTRY.list_by_priority("P0")
        assert len(p0_tables) > 0
        assert all(t.priority == "P0" for t in p0_tables)

    def test_all_tables_have_required_fields(self):
        """Every CacheTable should have all required fields populated."""
        for name, table in SCHEMA_REGISTRY.list_all().items():
            assert table.name, f"Table '{name}' has empty name"
            assert table.schema, f"Table '{name}' has empty schema"
            assert table.primary_key, f"Table '{name}' has empty primary_key"
            assert table.storage_layer, f"Table '{name}' has empty storage_layer"
            assert table.priority, f"Table '{name}' has empty priority"


# ---------------------------------------------------------------------------
# 2. Schema validation — success
# ---------------------------------------------------------------------------


class TestSchemaValidationSuccess:
    """Write data matching a registered schema — should succeed."""

    def test_validate_and_cast_stock_daily(self):
        """DataFrame matching stock_daily schema should validate and cast cleanly."""
        schema = get_table_schema("stock_daily")
        assert schema is not None

        df = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                "date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100000.0],
                "amount": [1000000.0],
                "adjust": ["qfq"],
            }
        )

        validator = SchemaValidator("stock_daily", schema.schema)
        result = validator.validate_and_cast(df, primary_key=schema.primary_key)

        # No exception raised — validate errors should be empty
        errors = validator.validate(df)
        assert errors == []
        assert len(result) == 1

    def test_validate_and_cast_securities(self):
        """DataFrame matching securities meta schema should pass validation."""
        schema = get_table_schema("securities")
        assert schema is not None

        df = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                "name": ["浦发银行"],
                "type": ["stock"],
                "list_date": [date(1999, 11, 10)],
                "delist_date": [None],
                "exchange": ["SSE"],
            }
        )

        validator = SchemaValidator("securities", schema.schema)
        errors = validator.validate(df)
        assert errors == []


# ---------------------------------------------------------------------------
# 3. Schema validation failure
# ---------------------------------------------------------------------------


class TestSchemaValidationFailure:
    """Write data with wrong columns — should raise SchemaValidationError."""

    def test_missing_column_raises(self):
        """DataFrame missing a required column should fail validation."""
        schema = get_table_schema("stock_daily")
        assert schema is not None

        df = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                # 'date' is missing
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100000.0],
                "amount": [1000000.0],
                "adjust": ["qfq"],
            }
        )

        validator = SchemaValidator("stock_daily", schema.schema)
        errors = validator.validate(df)
        assert len(errors) == 1
        assert "Missing column: 'date'" in errors[0]

    def test_completely_wrong_columns_raises(self):
        """DataFrame with entirely wrong columns should fail with multiple errors."""
        schema = get_table_schema("trade_calendar")
        assert schema is not None

        df = pd.DataFrame(
            {
                "foo": ["bar"],
                "baz": [123],
            }
        )

        validator = SchemaValidator("trade_calendar", schema.schema)
        errors = validator.validate(df)
        assert len(errors) == len(schema.schema)
        assert all("Missing column" in e for e in errors)

    def test_validate_and_cast_raises_on_missing_columns(self):
        """validate_and_cast should raise SchemaValidationError for missing columns."""
        schema = get_table_schema("stock_daily")
        df = pd.DataFrame({"symbol": ["sh600000"]})

        validator = SchemaValidator("stock_daily", schema.schema)
        with pytest.raises(SchemaValidationError) as exc_info:
            validator.validate_and_cast(df, primary_key=schema.primary_key)

        assert exc_info.value.table == "stock_daily"
        assert len(exc_info.value.errors) > 1

    def test_validate_and_cast_raises_on_null_primary_key(self):
        """validate_and_cast should raise when primary key column contains nulls."""
        schema = get_table_schema("stock_daily")
        df = pd.DataFrame(
            {
                "symbol": ["sh600000", "sh600001"],
                "date": [date(2024, 1, 2), pd.NaT],
                "open": [10.0, 10.0],
                "high": [11.0, 11.0],
                "low": [9.0, 9.0],
                "close": [10.5, 10.5],
                "volume": [100000.0, 100000.0],
                "amount": [1000000.0, 1000000.0],
                "adjust": ["qfq", "qfq"],
            }
        )

        validator = SchemaValidator("stock_daily", schema.schema)
        with pytest.raises(SchemaValidationError) as exc_info:
            validator.validate_and_cast(df, primary_key=["symbol", "date"])
        assert "null" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# 4. Parquet atomic write
# ---------------------------------------------------------------------------


class TestParquetAtomicWrite:
    """Parquet atomic write: write completes -> file exists; interrupt -> no corrupt file."""

    def test_write_completes_file_exists(self, temp_cache_dir):
        """A successful write should produce a valid parquet file."""
        writer = AtomicWriter(temp_cache_dir)
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["sh600000"] * 5,
                "close": [10.0, 10.1, 10.2, 10.3, 10.4],
            }
        )

        file_path = writer.write(
            "stock_daily",
            "daily",
            df,
            partition_by="date",
            partition_value="2024-01-01",
        )
        assert file_path.exists()
        assert file_path.suffix == ".parquet"
        # Read back to verify it's valid
        read_df = pd.read_parquet(file_path)
        assert len(read_df) == 5

    def test_no_corrupt_file_on_interrupt(self, temp_cache_dir):
        """Simulating an interrupt during write should leave no corrupt or .tmp files."""
        writer = AtomicWriter(temp_cache_dir)
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["sh600000"] * 5,
                "close": [10.0, 10.1, 10.2, 10.3, 10.4],
            }
        )

        # Simulate interruption during os.replace by raising OSError
        with patch(
            "akshare_data.store.parquet.os.replace",
            side_effect=OSError("simulated interrupt"),
        ):
            with pytest.raises(IOError, match="simulated interrupt"):
                writer.write(
                    "stock_daily",
                    "daily",
                    df,
                    partition_by="date",
                    partition_value="2024-01-01",
                )

        # No .tmp files should remain
        tmp_files = list(Path(temp_cache_dir).glob("**/*.tmp"))
        assert len(tmp_files) == 0, "Leftover .tmp file found after interrupted write"

    def test_atomic_write_replaces_existing(self, temp_cache_dir):
        """A second write to the same location should produce a valid file (overwrite)."""
        writer = AtomicWriter(temp_cache_dir)

        df1 = pd.DataFrame({"value": [1]})
        writer.write("test_table", "daily", df1)

        df2 = pd.DataFrame({"value": [2, 3]})
        path2 = writer.write("test_table", "daily", df2)

        assert path2.exists()
        result = pd.read_parquet(path2)
        assert len(result) == 2
        assert list(result["value"]) == [2, 3]


# ---------------------------------------------------------------------------
# 5. Parquet partitioning
# ---------------------------------------------------------------------------


class TestParquetPartitioning:
    """Data partitioned by symbol — verify directory structure."""

    def test_partition_by_symbol(self, temp_cache_dir):
        """Writing with partition_by='symbol' should create symbol=value directories."""
        writer = AtomicWriter(temp_cache_dir)

        df_a = pd.DataFrame(
            {
                "symbol": ["sh600000"] * 3,
                "date": pd.date_range("2024-01-01", periods=3),
                "close": [10.0, 10.1, 10.2],
            }
        )
        df_b = pd.DataFrame(
            {
                "symbol": ["sz000001"] * 2,
                "date": pd.date_range("2024-01-01", periods=2),
                "close": [5.0, 5.1],
            }
        )

        path_a = writer.write(
            "stock_daily",
            "daily",
            df_a,
            partition_by="symbol",
            partition_value="sh600000",
        )
        path_b = writer.write(
            "stock_daily",
            "daily",
            df_b,
            partition_by="symbol",
            partition_value="sz000001",
        )

        assert "symbol=sh600000" in str(path_a)
        assert "symbol=sz000001" in str(path_b)
        assert path_a.exists()
        assert path_b.exists()

    def test_partition_directory_structure(self, temp_cache_dir):
        """Verify the full directory hierarchy: base/layer/table/partition=value/file."""
        pm = PartitionManager(temp_cache_dir)

        expected = Path(temp_cache_dir) / "daily" / "stock_daily" / "symbol=sh600000"
        actual = pm.raw_partition_path("stock_daily", "daily", "symbol", "sh600000")
        assert actual == expected

    def test_list_all_partitions_returns_symbols(self, temp_cache_dir):
        """After writing partitioned data, list_all_partitions should return partition values."""
        writer = AtomicWriter(temp_cache_dir)
        pm = PartitionManager(temp_cache_dir)

        for symbol in ["sh600000", "sz000001", "sh600036"]:
            df = pd.DataFrame(
                {
                    "symbol": [symbol],
                    "date": [date(2024, 1, 2)],
                    "close": [10.0],
                }
            )
            writer.write(
                "stock_daily",
                "daily",
                df,
                partition_by="symbol",
                partition_value=symbol,
            )

        partitions = pm.list_all_partitions("stock_daily", "daily", "symbol")
        assert sorted(partitions) == ["sh600000", "sh600036", "sz000001"]

    def test_non_partitioned_write_goes_to_meta(self, temp_cache_dir):
        """Writing without partition should place files under meta/table/."""
        writer = AtomicWriter(temp_cache_dir)
        df = pd.DataFrame({"date": [date(2024, 1, 2)], "is_trading_day": [True]})

        path = writer.write_meta("trade_calendar", df)
        assert "meta" in str(path)
        assert path.exists()


# ---------------------------------------------------------------------------
# 6. Incremental append
# ---------------------------------------------------------------------------


class TestIncrementalAppend:
    """New data appended to existing parquet — merged correctly via IncrementalStrategy."""

    def test_incremental_merge_combines_data(self, temp_cache_dir):
        """IncrementalStrategy.merge should concatenate cached and fresh data."""
        strategy = IncrementalStrategy(date_col="date")

        cached = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "close": [10.0, 10.1],
            }
        )
        fresh = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-03", "2024-01-04"]),
                "close": [10.2, 10.3],
            }
        )

        merged = strategy.merge(cached, fresh)
        assert len(merged) == 4
        assert list(merged["close"]) == [10.0, 10.1, 10.2, 10.3]

    def test_incremental_merge_no_cache(self, temp_cache_dir):
        """IncrementalStrategy.merge with no cache should return fresh data."""
        strategy = IncrementalStrategy(date_col="date")
        fresh = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "close": [10.0],
            }
        )
        merged = strategy.merge(None, fresh)
        assert len(merged) == 1

    def test_incremental_deduplicates_on_date(self, temp_cache_dir):
        """IncrementalStrategy.merge should deduplicate on date column, keeping last."""
        strategy = IncrementalStrategy(date_col="date")

        cached = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "close": [10.0, 10.1],
            }
        )
        # Fresh has an overlapping date with updated value
        fresh = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                "close": [10.15, 10.3],
            }
        )

        merged = strategy.merge(cached, fresh)
        assert len(merged) == 3  # 2024-01-02 deduplicated
        jan2_row = merged[merged["date"] == pd.Timestamp("2024-01-02")]
        assert list(jan2_row["close"]) == [10.15]  # kept fresh value

    def test_incremental_should_fetch_detects_missing(self, temp_cache_dir):
        """IncrementalStrategy.should_fetch should return True when date range is incomplete."""
        strategy = IncrementalStrategy(date_col="date")

        cached = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-05", "2024-01-10"]),
            }
        )
        # Requesting a wider range than what's cached
        assert (
            strategy.should_fetch(
                cached, start_date="2024-01-01", end_date="2024-01-15"
            )
            is True
        )

        # Requesting within cached range
        assert (
            strategy.should_fetch(
                cached, start_date="2024-01-06", end_date="2024-01-09"
            )
            is False
        )

    def test_incremental_should_fetch_no_cache(self, temp_cache_dir):
        """IncrementalStrategy.should_fetch should return True when no cache exists."""
        strategy = IncrementalStrategy(date_col="date")
        assert (
            strategy.should_fetch(None, start_date="2024-01-01", end_date="2024-01-15")
            is True
        )
        assert (
            strategy.should_fetch(
                pd.DataFrame(), start_date="2024-01-01", end_date="2024-01-15"
            )
            is True
        )


# ---------------------------------------------------------------------------
# 7. Full overwrite
# ---------------------------------------------------------------------------


class TestFullOverwrite:
    """Full cache strategy: existing data is replaced — old data gone."""

    def test_full_merge_replaces_data(self, temp_cache_dir):
        """FullCacheStrategy.merge should return only fresh data, ignoring cache."""
        strategy = FullCacheStrategy()

        cached = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "value": ["old"],
            }
        )
        fresh = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "value": ["new"],
            }
        )

        merged = strategy.merge(cached, fresh)
        assert len(merged) == 1
        assert list(merged["value"]) == ["new"]

    def test_full_should_fetch_only_when_no_cache(self, temp_cache_dir):
        """FullCacheStrategy.should_fetch should only return True when cache is absent."""
        strategy = FullCacheStrategy()

        cached = pd.DataFrame({"value": [1]})
        assert strategy.should_fetch(cached) is False
        assert strategy.should_fetch(None) is True
        assert strategy.should_fetch(pd.DataFrame()) is True

    def test_full_overwrite_end_to_end(self, temp_cache_dir):
        """Write initial data, then overwrite — only new data should remain."""
        manager = CacheManager(base_dir=temp_cache_dir)

        # First write
        df1 = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                "name": ["Old Name"],
                "type": ["stock"],
                "list_date": [date(1999, 1, 1)],
                "delist_date": [None],
                "exchange": ["SSE"],
            }
        )
        manager.write("securities", df1)

        # Overwrite with new data
        df2 = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                "name": ["New Name"],
                "type": ["stock"],
                "list_date": [date(1999, 1, 1)],
                "delist_date": [None],
                "exchange": ["SSE"],
            }
        )
        manager.write("securities", df2)

        # Read back
        result = manager.read("securities", storage_layer="meta")
        assert len(result) == 1
        # The latest write's data should be what's in the file
        # (write_meta overwrites the single file)
        assert list(result["name"]) == ["New Name"]


# ---------------------------------------------------------------------------
# 8. Storage strategy behaviour difference
# ---------------------------------------------------------------------------


class TestStorageStrategyBehavior:
    """Compare full vs incremental strategy behaviour."""

    def test_full_vs_incremental_merge_difference(self, temp_cache_dir):
        """Full strategy replaces; incremental strategy appends."""
        full = FullCacheStrategy()
        incremental = IncrementalStrategy(date_col="date")

        cached = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "value": [1],
            }
        )
        fresh = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02"]),
                "value": [2],
            }
        )

        full_result = full.merge(cached, fresh)
        incr_result = incremental.merge(cached, fresh)

        # Full: only fresh data
        assert len(full_result) == 1
        # Incremental: combined data
        assert len(incr_result) == 2

    def test_full_vs_incremental_should_fetch_difference(self, temp_cache_dir):
        """Full strategy only fetches when cache is empty; incremental checks date range."""
        full = FullCacheStrategy()
        incremental = IncrementalStrategy(date_col="date")

        cached = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
            }
        )

        # With cached data, full strategy says "don't fetch"
        assert full.should_fetch(cached) is False
        # But incremental strategy may still say "fetch" if range is incomplete
        assert (
            incremental.should_fetch(
                cached, start_date="2024-01-01", end_date="2024-01-10"
            )
            is True
        )

    def test_incremental_build_where_with_date_range(self, temp_cache_dir):
        """IncrementalStrategy.build_where should include date range."""
        strategy = IncrementalStrategy(date_col="date", filter_keys=["symbol"])
        where = strategy.build_where(
            symbol="sh600000",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        assert "symbol" in where
        assert where["symbol"] == "sh600000"
        assert "date" in where
        assert where["date"] == ("2024-01-01", "2024-01-31")

    def test_full_build_where_uses_filter_keys_only(self, temp_cache_dir):
        """FullCacheStrategy.build_where should only include filter_keys params."""
        strategy = FullCacheStrategy(filter_keys=["symbol"])
        where = strategy.build_where(
            symbol="sh600000",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        assert where == {"symbol": "sh600000"}


# ---------------------------------------------------------------------------
# 9. Validator
# ---------------------------------------------------------------------------


class TestValidator:
    """SchemaValidator: valid data passes, invalid data rejected with clear error."""

    def test_valid_data_passes(self):
        """Data matching the schema should produce zero errors."""
        schema = get_table_schema("trade_calendar")
        assert schema is not None

        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "is_trading_day": [False, True],
            }
        )

        validator = SchemaValidator("trade_calendar", schema.schema)
        errors = validator.validate(df)
        assert errors == []

    def test_invalid_data_rejected_with_clear_error(self):
        """Data with wrong columns should produce a clear, actionable error message."""
        schema = get_table_schema("stock_daily")
        assert schema is not None

        df = pd.DataFrame(
            {
                "wrong_col_a": [1],
                "wrong_col_b": [2],
            }
        )

        validator = SchemaValidator("stock_daily", schema.schema)
        errors = validator.validate(df)
        assert len(errors) == len(schema.schema)

        # Error messages should be human-readable and mention the table
        exc = SchemaValidationError("stock_daily", errors)
        assert "stock_daily" in str(exc)
        assert "Missing column" in str(exc)

    def test_validate_and_cast_produces_clean_dataframe(self):
        """validate_and_cast should return a DataFrame with properly cast columns."""
        schema = {
            "symbol": "string",
            "date": "date",
            "close": "float64",
        }

        # Input with types that need casting
        df = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                "date": ["2024-01-02"],  # string, not date
                "close": ["10.5"],  # string, not float
            }
        )

        validator = SchemaValidator("test", schema)
        result = validator.validate_and_cast(df)

        assert isinstance(result["date"].iloc[0], date)
        assert result["close"].iloc[0] == 10.5

    def test_validation_error_has_table_and_errors_attributes(self):
        """SchemaValidationError should expose table and errors attributes."""
        exc = SchemaValidationError("my_table", ["error1", "error2"])
        assert exc.table == "my_table"
        assert exc.errors == ["error1", "error2"]


# ---------------------------------------------------------------------------
# 10. Write pipeline: schema -> validate -> parquet -> verify round-trip
# ---------------------------------------------------------------------------


class TestWritePipelineRoundTrip:
    """End-to-end write pipeline: schema lookup -> validate -> write parquet -> read back."""

    def test_full_round_trip_stock_daily(self, temp_cache_dir):
        """Write stock_daily data through the full pipeline and read it back."""
        reset_cache_manager()
        CacheManager.reset_instance()
        manager = CacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame(
            {
                "symbol": ["sh600000", "sh600000"],
                "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                "open": [10.0, 10.1],
                "high": [11.0, 11.1],
                "low": [9.0, 9.1],
                "close": [10.5, 10.6],
                "volume": [100000.0, 110000.0],
                "amount": [1000000.0, 1100000.0],
                "adjust": ["qfq", "qfq"],
            }
        )

        # Write through CacheManager (which handles schema lookup + validation)
        file_path = manager.write("stock_daily", df)
        assert file_path
        assert Path(file_path).exists()

        # Read back via DuckDB engine
        result = manager.read("stock_daily", storage_layer="daily")
        assert not result.empty
        assert len(result) == 2
        assert "close" in result.columns

        reset_cache_manager()
        CacheManager.reset_instance()

    def test_round_trip_with_partition(self, temp_cache_dir):
        """Write partitioned data and verify round-trip through the pipeline."""
        reset_cache_manager()
        CacheManager.reset_instance()
        manager = CacheManager(base_dir=temp_cache_dir)

        symbols = ["sh600000", "sz000001"]
        for sym in symbols:
            df = pd.DataFrame(
                {
                    "symbol": [sym] * 3,
                    "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                    "open": [10.0, 10.1, 10.2],
                    "high": [11.0, 11.1, 11.2],
                    "low": [9.0, 9.1, 9.2],
                    "close": [10.5, 10.6, 10.7],
                    "volume": [100000.0, 100000.0, 100000.0],
                    "amount": [1000000.0, 1000000.0, 1000000.0],
                    "adjust": ["qfq"] * 3,
                }
            )
            manager.write("stock_daily", df, partition_by="symbol", partition_value=sym)

        # Verify files exist in partition directories
        pm = PartitionManager(temp_cache_dir)
        partitions = pm.list_all_partitions("stock_daily", "daily", "symbol")
        assert sorted(partitions) == sorted(symbols)

        reset_cache_manager()
        CacheManager.reset_instance()

    def test_round_trip_meta_table(self, temp_cache_dir):
        """Write and read back a meta table (non-partitioned)."""
        reset_cache_manager()
        CacheManager.reset_instance()
        manager = CacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame(
            {
                "symbol": ["sh600000", "sz000001"],
                "name": ["浦发银行", "平安银行"],
                "type": ["stock", "stock"],
                "list_date": pd.to_datetime(["1999-11-10", "1991-04-03"]),
                "delist_date": [None, None],
                "exchange": ["SSE", "SZSE"],
            }
        )

        file_path = manager.write("securities", df)
        assert file_path
        assert Path(file_path).exists()

        result = manager.read("securities", storage_layer="meta")
        assert len(result) == 2

        reset_cache_manager()
        CacheManager.reset_instance()

    def test_round_trip_empty_dataframe_skipped(self, temp_cache_dir):
        """Writing an empty DataFrame should be skipped and return empty string."""
        reset_cache_manager()
        CacheManager.reset_instance()
        manager = CacheManager(base_dir=temp_cache_dir)

        result = manager.write("stock_daily", pd.DataFrame())
        assert result == ""

        reset_cache_manager()
        CacheManager.reset_instance()

    def test_pipeline_validation_failure_raises(self, temp_cache_dir):
        """Pipeline should raise when data fails schema validation in strict mode."""
        writer = AtomicWriter(temp_cache_dir, strict_schema=True)

        df = pd.DataFrame(
            {
                "wrong_col": [1],
            }
        )

        with pytest.raises(SchemaValidationError):
            writer.write("stock_daily", "daily", df)

    def test_pipeline_schema_auto_lookup(self, temp_cache_dir):
        """Writer should auto-lookup schema from registry when not provided."""
        writer = AtomicWriter(temp_cache_dir)

        df = pd.DataFrame(
            {
                "symbol": ["sh600000"],
                "date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100000.0],
                "amount": [1000000.0],
                "adjust": ["qfq"],
            }
        )

        # No schema or primary_key passed — should be auto-looked up
        path = writer.write(
            "stock_daily",
            "daily",
            df,
            partition_by="symbol",
            partition_value="sh600000",
        )
        assert path.exists()

        # Verify data was cast correctly by reading back
        result_df = pd.read_parquet(path)
        assert len(result_df) == 1
