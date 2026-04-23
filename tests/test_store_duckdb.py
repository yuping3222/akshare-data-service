"""tests/test_store_duckdb.py

DuckDBEngine comprehensive tests
"""

import pytest
from datetime import datetime

import pandas as pd

from akshare_data.store.duckdb import DuckDBEngine


class TestDuckDBEngineInit:
    """Test DuckDBEngine initialization"""

    def test_init_with_string_path(self, tmp_path):
        """Test initialization with string base_dir"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        assert engine.base_dir == tmp_path

    def test_init_with_path_object(self, tmp_path):
        """Test initialization with Path object"""
        engine = DuckDBEngine(base_dir=tmp_path)
        assert engine.base_dir == tmp_path

    def test_init_with_custom_threads(self, tmp_path):
        """Test initialization with custom threads"""
        engine = DuckDBEngine(base_dir=str(tmp_path), threads=8)
        assert engine.threads == 8

    def test_init_with_custom_memory_limit(self, tmp_path):
        """Test initialization with custom memory limit"""
        engine = DuckDBEngine(base_dir=str(tmp_path), memory_limit="8GB")
        assert engine.memory_limit == "8GB"

    def test_init_default_thread_local(self, tmp_path):
        """Test that thread-local storage is initialized"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        assert hasattr(engine, "_local")
        assert not hasattr(engine._local, "conn")


class TestDuckDBEngineConnection:
    """Test connection management"""

    def test_get_connection_creates_new(self, tmp_path):
        """Test that _get_connection creates a new connection if none exists"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        conn = engine._get_connection()
        assert conn is not None

    def test_get_connection_reuses_existing(self, tmp_path):
        """Test that _get_connection reuses existing connection"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        conn1 = engine._get_connection()
        conn2 = engine._get_connection()
        assert conn1 is conn2

    def test_close_closes_connection(self, tmp_path):
        """Test close method closes the connection"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        engine._get_connection()
        engine.close()
        assert not hasattr(engine._local, "conn") or engine._local.conn is None


class TestDuckDBEngineGlobPaths:
    """Test glob path generation"""

    def test_list_all_glob_paths_raw_no_partition(self, tmp_path):
        """Test _list_all_glob_paths for raw layer without partition"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._list_all_glob_paths("test_table", "daily", None, "raw")
        expected = str(tmp_path / "daily" / "test_table" / "*.parquet")
        assert result == expected

    def test_list_all_glob_paths_raw_with_partition(self, tmp_path):
        """Test _list_all_glob_paths for raw layer with partition"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._list_all_glob_paths("test_table", "daily", "date", "raw")
        expected = str(tmp_path / "daily" / "test_table" / "**/*.parquet")
        assert result == expected

    def test_list_all_glob_paths_aggregated_no_partition(self, tmp_path):
        """Test _list_all_glob_paths for aggregated layer without partition"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._list_all_glob_paths("test_table", "daily", None, "aggregated")
        expected = str(tmp_path / "aggregated" / "daily" / "test_table" / "*.parquet")
        assert result == expected


class TestDuckDBEngineWhereClause:
    """Test WHERE clause building"""

    def test_build_where_clause_simple(self, tmp_path):
        """Test simple equality condition"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._build_where_clause({"symbol": "sh600000"})
        assert "symbol = 'sh600000'" in result

    def test_build_where_clause_list(self, tmp_path):
        """Test IN clause for list values"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._build_where_clause({"symbol": ["sh600000", "sh600001"]})
        assert "IN ('sh600000', 'sh600001')" in result

    def test_build_where_clause_range_numeric(self, tmp_path):
        """Test range condition with numeric values"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._build_where_clause({"price": (10, 20)})
        assert "price >= 10" in result
        assert "price <= 20" in result

    def test_build_where_clause_range_string_dates(self, tmp_path):
        """Test range condition with date strings"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._build_where_clause({"date": ("2024-01-01", "2024-12-31")})
        assert "date >= '2024-01-01'" in result
        assert "date <= '2024-12-31'" in result

    def test_build_where_clause_multiple(self, tmp_path):
        """Test multiple conditions"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._build_where_clause({"symbol": "sh600000", "price": 10})
        assert "symbol = 'sh600000'" in result
        assert "price = 10" in result


class TestDuckDBEngineFormatValue:
    """Test value formatting"""

    def test_format_value_string(self, tmp_path):
        """Test string formatting with escaping"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._format_value("test's value")
        assert result == "'test''s value'"

    def test_format_value_int(self, tmp_path):
        """Test integer formatting"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._format_value(42)
        assert result == "42"

    def test_format_value_float(self, tmp_path):
        """Test float formatting"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._format_value(3.14)
        assert result == "3.14"

    def test_format_value_bool_true(self, tmp_path):
        """Test boolean True formatting"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._format_value(True)
        assert result == "TRUE"

    def test_format_value_bool_false(self, tmp_path):
        """Test boolean False formatting"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._format_value(False)
        assert result == "FALSE"

    def test_format_value_datetime(self, tmp_path):
        """Test datetime formatting"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        dt = datetime(2024, 1, 15)
        result = engine._format_value(dt)
        assert result == "'2024-01-15'"


class TestDuckDBEngineBuildSQL:
    """Test SQL building"""

    def test_build_sql_list_paths(self, tmp_path):
        """Test SQL building with list of paths"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        paths = ["file1.parquet", "file2.parquet"]
        paths_str = ", ".join(f"'{p}'" for p in paths)
        result = engine._build_sql(f"[{paths_str}]", "", None, None, None)
        assert "read_parquet" in result
        assert "file1.parquet" in result

    def test_build_sql_with_columns(self, tmp_path):
        """Test SQL building with specific columns"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        paths = ["file1.parquet"]
        result = engine._build_sql(str(paths), "", ["date", "symbol"], None, None)
        assert "date, symbol" in result

    def test_build_sql_with_where(self, tmp_path):
        """Test SQL building with WHERE clause"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        paths = ["file1.parquet"]
        result = engine._build_sql(str(paths), "symbol = 'sh600000'", None, None, None)
        assert "WHERE" in result
        assert "symbol = 'sh600000'" in result

    def test_build_sql_with_order_by(self, tmp_path):
        """Test SQL building with ORDER BY"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        paths = ["file1.parquet"]
        result = engine._build_sql(str(paths), "", None, ["date", "symbol"], None)
        assert "ORDER BY" in result
        assert "date, symbol" in result

    def test_build_sql_with_limit(self, tmp_path):
        """Test SQL building with LIMIT"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        paths = ["file1.parquet"]
        result = engine._build_sql(str(paths), "", None, None, 100)
        assert "LIMIT 100" in result


class TestDuckDBEngineQuery:
    """Test query methods"""

    def test_query_empty_returns_dataframe(self, tmp_path):
        """Test query with no data returns empty DataFrame"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.query("nonexistent", "daily")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_query_by_paths_empty(self, tmp_path):
        """Test query_by_paths with empty list returns empty DataFrame"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.query_by_paths([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_query_by_paths_with_mock_data(self, tmp_path):
        """Test query_by_paths with mock parquet files"""

        parquet_path = tmp_path / "test.parquet"
        df = pd.DataFrame(
            {"date": ["2024-01-01"], "symbol": ["sh600000"], "close": [10.0]}
        )
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.query_by_paths([parquet_path])

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_exists_returns_false_for_missing(self, tmp_path):
        """Test exists returns False for non-existent data"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.exists("nonexistent", "daily")
        assert result is False

    def test_count_returns_zero_for_missing(self, tmp_path):
        """Test count returns 0 for non-existent data"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.count("nonexistent", "daily")
        assert result == 0


class TestDuckDBEngineSimpleQuery:
    """Test query_simple method"""

    def test_query_simple_empty_table_dir(self, tmp_path):
        """Test query_simple with non-existent table directory"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.query_simple("nonexistent_table")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_query_simple_no_parquet_files(self, tmp_path):
        """Test query_simple with empty table directory"""
        table_dir = tmp_path / "test_table"
        table_dir.mkdir()
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.query_simple("test_table")
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestDuckDBEngineAggregate:
    """Test aggregate method"""

    def test_aggregate_empty_table_dir(self, tmp_path):
        """Test aggregate with non-existent table"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine.aggregate("nonexistent_table", "COUNT(*)")
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestDuckDBEngineTableRegistration:
    """Test table registration methods"""

    def test_register_table_creates_view(self, tmp_path):
        """Test register_table creates a DuckDB view"""

        parquet_path = tmp_path / "test.parquet"
        df = pd.DataFrame(
            {"date": ["2024-01-01"], "symbol": ["sh600000"], "close": [10.0]}
        )
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=str(tmp_path))
        engine.register_table("test_view", str(parquet_path))

        conn = engine._get_connection()
        result = conn.execute("SELECT * FROM test_view LIMIT 1").fetchdf()
        assert len(result) == 1
        engine.unregister_table("test_view")

    def test_register_table_with_alias(self, tmp_path):
        """Test register_table with custom alias"""

        parquet_path = tmp_path / "test.parquet"
        df = pd.DataFrame(
            {"date": ["2024-01-01"], "symbol": ["sh600000"], "close": [10.0]}
        )
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=str(tmp_path))
        engine.register_table("original_name", str(parquet_path), alias="my_alias")

        conn = engine._get_connection()
        result = conn.execute("SELECT * FROM my_alias LIMIT 1").fetchdf()
        assert len(result) == 1
        engine.unregister_table("my_alias")

    def test_unregister_table(self, tmp_path):
        """Test unregister_table removes view"""

        parquet_path = tmp_path / "test.parquet"
        df = pd.DataFrame(
            {"date": ["2024-01-01"], "symbol": ["sh600000"], "close": [10.0]}
        )
        df.to_parquet(parquet_path)

        engine = DuckDBEngine(base_dir=str(tmp_path))
        engine.register_table("to_remove", str(parquet_path))
        engine.unregister_table("to_remove")

        conn = engine._get_connection()
        with pytest.raises(Exception):
            conn.execute("SELECT * FROM to_remove LIMIT 1").fetchdf()


class TestDuckDBEngineLooksLikeDate:
    """Test _looks_like_date helper"""

    def test_looks_like_date_valid(self, tmp_path):
        """Test date pattern matching"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        assert engine._looks_like_date("2024-01-15") is True
        assert engine._looks_like_date("2023-12-31") is True

    def test_looks_like_date_invalid(self, tmp_path):
        """Test non-date pattern"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        assert engine._looks_like_date("2024-1-15") is False
        assert engine._looks_like_date("not-a-date") is False
        assert engine._looks_like_date("sh600000") is False


class TestDuckDBEngineExecute:
    """Test _execute method"""

    def test_execute_valid_sql(self, tmp_path):
        """Test _execute with valid SQL"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._execute("SELECT 1 AS num")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["num"][0] == 1

    def test_execute_invalid_sql_returns_empty(self, tmp_path):
        """Test _execute with invalid SQL returns empty DataFrame"""
        engine = DuckDBEngine(base_dir=str(tmp_path))
        result = engine._execute("SELECT * FROM nonexistent_table")
        assert isinstance(result, pd.DataFrame)
        assert result.empty
