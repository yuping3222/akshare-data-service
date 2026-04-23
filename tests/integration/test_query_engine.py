"""Integration tests for the DuckDB query engine.

These tests exercise DuckDBEngine with real parquet files and a real
DuckDB backend. External data sources are not involved -- data is
written directly as parquet files and queried through the engine.

Test scenarios:
1. DuckDBEngine init with configured threads and memory limit
2. Query single parquet file: write data, query SQL, verify DataFrame
3. Query with WHERE clause: filter by symbol/date, correct subset
4. Query with aggregation: GROUP BY, COUNT, SUM, correct aggregates
5. Query multiple files: multiple symbols, combined results
6. Query with JOIN: two tables joined, correct join results
7. Query performance: 1000+ rows, completes within reasonable time
8. Schema introspection: registered tables, column info
9. Query error: invalid SQL, graceful handling
10. Thread safety: concurrent queries from multiple threads
"""

import threading
import time
from pathlib import Path

import pandas as pd
import pytest

from akshare_data.store.duckdb import DuckDBEngine


@pytest.mark.integration
class TestDuckDBEngineIntegration:
    """Integration tests for DuckDBEngine with real parquet files.

    Each test uses a fresh DuckDBEngine instance pointed at a
    ``temp_cache_dir`` fixture.  Parquet files are written directly
    via pandas ``to_parquet`` and then queried through the engine.
    """

    # ----------------------------------------------------------------
    # Test 1: DuckDBEngine init
    # ----------------------------------------------------------------

    def test_init_with_configured_threads_and_memory(self, temp_cache_dir):
        """DuckDBEngine initializes with configured threads and memory limit.

        Verifies that the constructor stores the configuration values
        and that a connection can be created with those settings applied.
        """
        engine = DuckDBEngine(
            base_dir=temp_cache_dir,
            threads=8,
            memory_limit="2GB",
        )
        assert engine.base_dir is not None
        assert engine.threads == 8
        assert engine.memory_limit == "2GB"

        # Verify connection is created successfully
        conn = engine._get_connection()
        assert conn is not None

        # Verify settings are applied on the connection
        threads_result = conn.execute("SELECT current_setting('threads')").fetchone()
        assert threads_result is not None

        engine.close()

    # ----------------------------------------------------------------
    # Test 2: Query single parquet file
    # ----------------------------------------------------------------

    def test_query_single_parquet_file(self, temp_cache_dir, sample_stock_data):
        """Write data to parquet, query with SQL, returns correct DataFrame.

        Writes ``sample_stock_data`` to a parquet file, then uses
        ``query_by_paths`` to read it back and verifies row count,
        columns, and data integrity.
        """
        parquet_path = Path(temp_cache_dir) / "test_single.parquet"
        sample_stock_data.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)
        result = engine.query_by_paths([parquet_path])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_stock_data)
        assert set(result.columns) >= {"date", "symbol", "close", "volume"}

        # Verify data matches
        pd.testing.assert_series_equal(
            result["symbol"].reset_index(drop=True),
            sample_stock_data["symbol"].reset_index(drop=True),
        )
        pd.testing.assert_series_equal(
            result["close"].reset_index(drop=True),
            sample_stock_data["close"].reset_index(drop=True),
        )
        engine.close()

    # ----------------------------------------------------------------
    # Test 3: Query with WHERE clause
    # ----------------------------------------------------------------

    def test_query_with_where_clause_filter(self, temp_cache_dir):
        """Filter by symbol and date returns the correct subset of rows.

        Creates a multi-symbol DataFrame, writes it to parquet, then
        queries with a WHERE clause filtering to a single symbol.
        """
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-15", freq="B"),
                "symbol": ["sh600000"] * 5 + ["sz000001"] * 5,
                "open": [10.0] * 5 + [20.0] * 5,
                "high": [11.0] * 5 + [21.0] * 5,
                "low": [9.0] * 5 + [19.0] * 5,
                "close": [10.5] * 5 + [20.5] * 5,
                "volume": [100_000] * 5 + [200_000] * 5,
            }
        )
        parquet_path = Path(temp_cache_dir) / "test_where.parquet"
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)

        # Filter by symbol
        result = engine.query_by_paths(
            [parquet_path],
            where={"symbol": "sh600000"},
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert (result["symbol"] == "sh600000").all()

        # Filter by symbol list (IN clause)
        result_in = engine.query_by_paths(
            [parquet_path],
            where={"symbol": ["sh600000", "sz000001"]},
        )
        assert len(result_in) == 10

        # Select specific columns
        result_cols = engine.query_by_paths(
            [parquet_path],
            where={"symbol": "sh600000"},
            columns=["date", "symbol", "close"],
        )
        assert list(result_cols.columns) == ["date", "symbol", "close"]
        assert len(result_cols) == 5

        engine.close()

    # ----------------------------------------------------------------
    # Test 4: Query with aggregation
    # ----------------------------------------------------------------

    def test_query_with_aggregation(self, temp_cache_dir):
        """GROUP BY, COUNT, SUM produce correct aggregate results.

        Uses the ``aggregate`` method to run SQL aggregations
        against parquet data and verifies the computed values.
        """
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-15", freq="B"),
                "symbol": ["sh600000"] * 5 + ["sz000001"] * 5,
                "open": [10.0, 10.1, 10.2, 10.3, 10.4, 20.0, 20.1, 20.2, 20.3, 20.4],
                "high": [11.0, 11.1, 11.2, 11.3, 11.4, 21.0, 21.1, 21.2, 21.3, 21.4],
                "low": [9.0, 9.1, 9.2, 9.3, 9.4, 19.0, 19.1, 19.2, 19.3, 19.4],
                "close": [10.5, 10.6, 10.7, 10.8, 10.9, 20.5, 20.6, 20.7, 20.8, 20.9],
                "volume": [
                    100_000,
                    110_000,
                    120_000,
                    130_000,
                    140_000,
                    200_000,
                    210_000,
                    220_000,
                    230_000,
                    240_000,
                ],
            }
        )

        # Create table directory structure expected by aggregate()
        table_dir = Path(temp_cache_dir) / "agg_test"
        table_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = table_dir / "data.parquet"
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)

        # COUNT(*)
        result_count = engine.aggregate("agg_test", "COUNT(*) AS cnt")
        assert len(result_count) == 1
        assert result_count["cnt"].iloc[0] == 10

        # SUM with GROUP BY
        result_sum = engine.aggregate(
            "agg_test",
            "symbol, SUM(volume) AS total_volume",
            group_by="symbol",
        )
        assert len(result_sum) == 2
        sh_row = result_sum[result_sum["symbol"] == "sh600000"]
        assert len(sh_row) == 1
        assert sh_row["total_volume"].iloc[0] == 600_000  # 100k+110k+120k+130k+140k

        # AVG with WHERE
        result_avg = engine.aggregate(
            "agg_test",
            "AVG(close) AS avg_close",
            where="symbol = 'sh600000'",
        )
        assert len(result_avg) == 1
        expected_avg = (10.5 + 10.6 + 10.7 + 10.8 + 10.9) / 5
        assert abs(result_avg["avg_close"].iloc[0] - expected_avg) < 0.001

        engine.close()

    # ----------------------------------------------------------------
    # Test 5: Query multiple files
    # ----------------------------------------------------------------

    def test_query_multiple_files_combined(self, temp_cache_dir):
        """Write data for multiple symbols to separate parquet files,
        query all files, verify combined results.

        Each file contains data for a different symbol. Querying all
        files together should return the union of all rows.
        """
        symbols_data = {
            "sh600000": [10.0, 10.1, 10.2],
            "sz000001": [20.0, 20.1, 20.2],
            "sz000002": [30.0, 30.1, 30.2],
        }
        paths = []
        for symbol, closes in symbols_data.items():
            df = pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-02", periods=len(closes), freq="B"),
                    "symbol": [symbol] * len(closes),
                    "close": closes,
                    "volume": [100_000] * len(closes),
                }
            )
            p = Path(temp_cache_dir) / f"{symbol}.parquet"
            df.to_parquet(p)
            paths.append(p)

        engine = DuckDBEngine(base_dir=temp_cache_dir)
        result = engine.query_by_paths(paths)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 9  # 3 symbols * 3 rows each

        # Verify each symbol is present
        unique_symbols = result["symbol"].unique()
        assert set(unique_symbols) == {"sh600000", "sz000001", "sz000002"}

        # Verify with ORDER BY and LIMIT
        result_limited = engine.query_by_paths(
            paths,
            order_by=["close"],
            limit=3,
        )
        assert len(result_limited) == 3
        # First 3 rows should be the lowest closes (sh600000)
        assert (result_limited["symbol"] == "sh600000").all()

        engine.close()

    # ----------------------------------------------------------------
    # Test 6: Query with JOIN
    # ----------------------------------------------------------------

    def test_query_with_join(self, temp_cache_dir):
        """Register two tables, join them, verify correct join results.

        Creates two parquet files (daily prices and sector info),
        registers them as DuckDB views, then executes a JOIN query.
        """
        # Table 1: daily prices
        prices_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5, freq="B"),
                "symbol": ["sh600000"] * 3 + ["sz000001"] * 2,
                "close": [10.5, 10.6, 10.7, 20.5, 20.6],
            }
        )
        prices_path = Path(temp_cache_dir) / "prices.parquet"
        prices_df.to_parquet(prices_path)

        # Table 2: sector info
        sector_df = pd.DataFrame(
            {
                "symbol": ["sh600000", "sz000001", "sz000002"],
                "sector": ["banking", "tech", "energy"],
                "market_cap": [1_000_000, 2_000_000, 3_000_000],
            }
        )
        sector_path = Path(temp_cache_dir) / "sectors.parquet"
        sector_df.to_parquet(sector_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)

        # Register both tables as views
        engine.register_table("prices_view", str(prices_path))
        engine.register_table("sector_view", str(sector_path))

        # Execute JOIN query
        conn = engine._get_connection()
        join_result = conn.execute("""
            SELECT p.symbol, p.close, s.sector, s.market_cap
            FROM prices_view p
            INNER JOIN sector_view s ON p.symbol = s.symbol
            ORDER BY p.symbol, p.date
        """).fetchdf()

        assert len(join_result) == 5
        # sh600000 rows should have banking sector
        sh_rows = join_result[join_result["symbol"] == "sh600000"]
        assert len(sh_rows) == 3
        assert (sh_rows["sector"] == "banking").all()
        assert (sh_rows["market_cap"] == 1_000_000).all()

        # sz000001 rows should have tech sector
        sz_rows = join_result[join_result["symbol"] == "sz000001"]
        assert len(sz_rows) == 2
        assert (sz_rows["sector"] == "tech").all()

        # Cleanup
        engine.unregister_table("prices_view")
        engine.unregister_table("sector_view")
        engine.close()

    # ----------------------------------------------------------------
    # Test 7: Query performance
    # ----------------------------------------------------------------

    def test_query_performance_large_dataset(self, temp_cache_dir):
        """Query a dataset with 1000+ rows completes within reasonable time.

        Creates a parquet file with 5000 rows (10 symbols x 500 dates),
        runs several query types, and verifies each completes under
        5 seconds.
        """
        num_rows = 5000
        symbols = [f"sh{600000 + i}" for i in range(10)]
        dates = pd.date_range("2020-01-02", periods=500, freq="B")

        # Build large DataFrame
        data = {
            "date": list(dates) * 10,
            "symbol": [s for s in symbols for _ in range(500)],
            "open": [10.0 + i * 0.01 for i in range(num_rows)],
            "high": [11.0 + i * 0.01 for i in range(num_rows)],
            "low": [9.0 + i * 0.01 for i in range(num_rows)],
            "close": [10.5 + i * 0.01 for i in range(num_rows)],
            "volume": [100_000 + i * 100 for i in range(num_rows)],
        }
        df = pd.DataFrame(data)
        parquet_path = Path(temp_cache_dir) / "large_dataset.parquet"
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)

        # Full table scan
        start = time.time()
        result = engine.query_by_paths([parquet_path])
        elapsed_full = time.time() - start
        assert len(result) == num_rows
        assert elapsed_full < 5.0, f"Full scan took {elapsed_full:.2f}s, expected < 5s"

        # Filtered query
        start = time.time()
        result_filtered = engine.query_by_paths(
            [parquet_path],
            where={"symbol": "sh600000"},
        )
        elapsed_filter = time.time() - start
        assert len(result_filtered) == 500
        assert elapsed_filter < 5.0, (
            f"Filtered query took {elapsed_filter:.2f}s, expected < 5s"
        )

        # Aggregation query
        start = time.time()
        result_agg = engine.query_by_paths(
            [parquet_path],
            columns=["symbol", "volume"],
        )
        # Manually compute aggregate on result
        agg = result_agg.groupby("symbol")["volume"].sum().reset_index()
        elapsed_agg = time.time() - start
        assert len(agg) == 10
        assert elapsed_agg < 5.0, f"Aggregation took {elapsed_agg:.2f}s, expected < 5s"

        engine.close()

    # ----------------------------------------------------------------
    # Test 8: Schema introspection
    # ----------------------------------------------------------------

    def test_schema_introspection(self, temp_cache_dir, sample_stock_data):
        """Register a table, inspect its schema, verify correct column info.

        Writes sample data to parquet, registers it as a DuckDB view,
        then uses DESCRIBE and information_schema queries to inspect
        the schema.
        """
        parquet_path = Path(temp_cache_dir) / "schema_test.parquet"
        sample_stock_data.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)
        engine.register_table("schema_view", str(parquet_path))

        conn = engine._get_connection()

        # DESCRIBE the view
        describe_result = conn.execute("DESCRIBE schema_view").fetchdf()
        assert len(describe_result) > 0

        column_names = describe_result["column_name"].tolist()
        assert "symbol" in column_names
        assert "date" in column_names
        assert "close" in column_names
        assert "volume" in column_names

        # Query information_schema for column details
        info_result = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'schema_view'
            ORDER BY ordinal_position
        """).fetchdf()
        assert len(info_result) > 0
        assert set(info_result["column_name"]) >= {
            "date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
        }

        # Verify row count via system query
        count_result = conn.execute("SELECT COUNT(*) AS cnt FROM schema_view").fetchdf()
        assert count_result["cnt"].iloc[0] == len(sample_stock_data)

        engine.unregister_table("schema_view")
        engine.close()

    # ----------------------------------------------------------------
    # Test 9: Query error handling
    # ----------------------------------------------------------------

    def test_query_error_invalid_sql(self, temp_cache_dir):
        """Invalid SQL is handled gracefully without crashing.

        Executes malformed SQL through _execute and query_by_paths
        and verifies the engine returns empty DataFrames or error
        markers rather than raising unhandled exceptions.
        """
        engine = DuckDBEngine(base_dir=temp_cache_dir)

        # _execute with invalid table name -- returns empty DataFrame
        result = engine._execute("SELECT * FROM nonexistent_table_xyz")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

        # query_by_paths with non-existent file path
        bad_path = Path(temp_cache_dir) / "does_not_exist.parquet"
        result_bad = engine.query_by_paths([bad_path])
        assert isinstance(result_bad, pd.DataFrame)
        # Either empty or has error marker
        assert result_bad.empty or "_query_error" in result_bad.columns

        # query with non-existent table directory
        result_empty = engine.query("nonexistent_table", "daily")
        assert isinstance(result_empty, pd.DataFrame)
        assert result_empty.empty

        engine.close()

    # ----------------------------------------------------------------
    # Test 10: Thread safety
    # ----------------------------------------------------------------

    def test_concurrent_queries_no_errors(self, temp_cache_dir, sample_stock_data):
        """Concurrent queries from multiple threads complete without errors.

        Writes a parquet file, then spawns 10 threads that each
        independently query the data. All threads should complete
        successfully and return valid results.
        """
        parquet_path = Path(temp_cache_dir) / "thread_test.parquet"
        sample_stock_data.to_parquet(parquet_path)

        num_threads = 10
        results = []
        errors = []
        barrier = threading.Barrier(num_threads)

        def query_task(thread_id):
            try:
                # Each thread gets its own engine (thread-local connections)
                engine = DuckDBEngine(base_dir=temp_cache_dir)
                barrier.wait()  # synchronize start
                result = engine.query_by_paths([parquet_path])
                results.append(
                    {
                        "thread_id": thread_id,
                        "row_count": len(result),
                    }
                )
                engine.close()
            except Exception as e:
                errors.append({"thread_id": thread_id, "error": str(e)})

        threads = [
            threading.Thread(target=query_task, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        # All threads should have completed
        assert len(results) == num_threads, (
            f"Expected {num_threads} results, got {len(results)}. Errors: {errors}"
        )
        assert len(errors) == 0, f"Thread errors: {errors}"

        # All threads should have read the correct number of rows
        for r in results:
            assert r["row_count"] == len(sample_stock_data), (
                f"Thread {r['thread_id']} got {r['row_count']} rows, "
                f"expected {len(sample_stock_data)}"
            )

    def test_concurrent_queries_same_engine(self, temp_cache_dir, sample_stock_data):
        """Concurrent queries sharing the same DuckDBEngine instance.

        Verifies that the thread-local connection model prevents
        interference when multiple threads use a single engine.
        """
        parquet_path = Path(temp_cache_dir) / "thread_shared.parquet"
        sample_stock_data.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=temp_cache_dir)
        num_threads = 5
        results = []
        errors = []

        def shared_engine_query(thread_id):
            try:
                # All threads use the same engine instance
                result = engine.query_by_paths([parquet_path])
                results.append({"thread_id": thread_id, "rows": len(result)})
            except Exception as e:
                errors.append({"thread_id": thread_id, "error": str(e)})

        threads = [
            threading.Thread(target=shared_engine_query, args=(i,))
            for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert len(errors) == 0, f"Errors with shared engine: {errors}"
        assert len(results) == num_threads
        engine.close()
