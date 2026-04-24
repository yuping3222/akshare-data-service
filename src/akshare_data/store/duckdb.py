import logging
import re
import threading
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class DuckDBEngine:
    def __init__(
        self,
        base_dir: str | Path,
        threads: int = 4,
        memory_limit: str = "4GB",
    ):
        self.base_dir = Path(base_dir)
        self.threads = threads
        self.memory_limit = memory_limit
        self._local = threading.local()

    def query(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None = None,
        where: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        prefer_aggregated: bool = True,
    ) -> pd.DataFrame | None:
        if prefer_aggregated:
            agg_glob = self._list_all_glob_paths(
                table, storage_layer, partition_by, layer="aggregated"
            )
            agg_paths = list(
                Path(self.base_dir).glob(agg_glob.replace(str(self.base_dir) + "/", ""))
            )
            if not agg_paths:
                agg_paths = (
                    list(Path(agg_glob).parent.glob("*.parquet"))
                    if Path(agg_glob).parent.exists()
                    else []
                )
            if not agg_paths:
                agg_paths = (
                    list(Path(agg_glob).parent.rglob("*.parquet"))
                    if Path(agg_glob).parent.exists()
                    else []
                )
            agg_paths = [p for p in agg_paths if p.suffix == ".parquet"]
            if agg_paths:
                result = self.query_by_paths(agg_paths, where, columns, order_by, limit)
                if result is not None and len(result) > 0:
                    return result

        raw_glob = self._list_all_glob_paths(
            table, storage_layer, partition_by, layer="raw"
        )
        raw_paths = list(
            Path(self.base_dir).glob(raw_glob.replace(str(self.base_dir) + "/", ""))
        )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.glob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.rglob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        raw_paths = [p for p in raw_paths if p.suffix == ".parquet"]
        if raw_paths:
            return self.query_by_paths(raw_paths, where, columns, order_by, limit)

        return pd.DataFrame()

    def query_by_paths(
        self,
        paths: list[Path],
        where: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame | None:
        if not paths:
            return pd.DataFrame()

        where_clause = self._build_where_clause(where) if where else ""
        sql = self._build_sql(
            str([str(p) for p in paths]),
            where_clause,
            columns,
            order_by,
            limit,
        )

        conn = self._get_connection()
        try:
            df = conn.execute(sql).fetchdf()
            return df
        except Exception as e:
            logger.warning(
                f"Query with strict schema failed: {e}, retrying with union_by_name"
            )
            sql_union = self._build_sql_union_by_name(
                str([str(p) for p in paths]),
                where_clause,
                columns,
                order_by,
                limit,
            )
            try:
                df = conn.execute(sql_union).fetchdf()
                return df
            except Exception as e2:
                logger.error(f"Query failed for paths {paths}: {e2}")
                df = pd.DataFrame()
                df["_query_error"] = True
                return df

    def exists(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None = None,
        where: dict[str, Any] | None = None,
        prefer_aggregated: bool = True,
    ) -> bool:
        if prefer_aggregated:
            agg_glob = self._list_all_glob_paths(
                table, storage_layer, partition_by, layer="aggregated"
            )
            agg_paths = list(
                Path(self.base_dir).glob(agg_glob.replace(str(self.base_dir) + "/", ""))
            )
            if not agg_paths:
                agg_paths = (
                    list(Path(agg_glob).parent.glob("*.parquet"))
                    if Path(agg_glob).parent.exists()
                    else []
                )
            if not agg_paths:
                agg_paths = (
                    list(Path(agg_glob).parent.rglob("*.parquet"))
                    if Path(agg_glob).parent.exists()
                    else []
                )
            agg_paths = [p for p in agg_paths if p.suffix == ".parquet"]
            if agg_paths:
                result = self.query_by_paths(agg_paths, where, columns=["1"], limit=1)
                if result is not None and len(result) > 0:
                    return True

        raw_glob = self._list_all_glob_paths(
            table, storage_layer, partition_by, layer="raw"
        )
        raw_paths = list(
            Path(self.base_dir).glob(raw_glob.replace(str(self.base_dir) + "/", ""))
        )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.glob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.rglob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        raw_paths = [p for p in raw_paths if p.suffix == ".parquet"]
        if raw_paths:
            result = self.query_by_paths(raw_paths, where, columns=["1"], limit=1)
            return result is not None and len(result) > 0

        return False

    def count(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None = None,
        where: dict[str, Any] | None = None,
        prefer_aggregated: bool = True,
    ) -> int:
        total = 0

        if prefer_aggregated:
            agg_glob = self._list_all_glob_paths(
                table, storage_layer, partition_by, layer="aggregated"
            )
            agg_paths = list(
                Path(self.base_dir).glob(agg_glob.replace(str(self.base_dir) + "/", ""))
            )
            if not agg_paths:
                agg_paths = (
                    list(Path(agg_glob).parent.glob("*.parquet"))
                    if Path(agg_glob).parent.exists()
                    else []
                )
            if not agg_paths:
                agg_paths = (
                    list(Path(agg_glob).parent.rglob("*.parquet"))
                    if Path(agg_glob).parent.exists()
                    else []
                )
            agg_paths = [p for p in agg_paths if p.suffix == ".parquet"]
            if agg_paths:
                where_clause = self._build_where_clause(where) if where else ""
                paths_str = ", ".join(f"'{p}'" for p in agg_paths)
                sql = f"SELECT COUNT(*) FROM read_parquet([{paths_str}])"
                if where_clause:
                    sql += f" WHERE {where_clause}"
                conn = self._get_connection()
                try:
                    result = conn.execute(sql).fetchone()
                    if result:
                        total += result[0]
                except Exception as e:
                    logger.warning("Count query failed for standardized path: %s", e)

        raw_glob = self._list_all_glob_paths(
            table, storage_layer, partition_by, layer="raw"
        )
        raw_paths = list(
            Path(self.base_dir).glob(raw_glob.replace(str(self.base_dir) + "/", ""))
        )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.glob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.rglob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        raw_paths = [p for p in raw_paths if p.suffix == ".parquet"]

        if raw_paths:
            where_clause = self._build_where_clause(where) if where else ""
            paths_str = ", ".join(f"'{p}'" for p in raw_paths)
            sql = f"SELECT COUNT(*) FROM read_parquet([{paths_str}])"
            if where_clause:
                sql += f" WHERE {where_clause}"
            conn = self._get_connection()
            try:
                result = conn.execute(sql).fetchone()
                if result:
                    total += result[0]
            except Exception as e:
                logger.warning("Count query failed for raw path: %s", e)

        raw_glob = self._list_all_glob_paths(
            table, storage_layer, partition_by, layer="raw"
        )
        raw_paths = list(
            Path(self.base_dir).glob(raw_glob.replace(str(self.base_dir) + "/", ""))
        )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.glob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        if not raw_paths:
            raw_paths = (
                list(Path(raw_glob).parent.rglob("*.parquet"))
                if Path(raw_glob).parent.exists()
                else []
            )
        raw_paths = [p for p in raw_paths if p.suffix == ".parquet"]
        if raw_paths:
            where_clause = self._build_where_clause(where) if where else ""
            paths_str = ", ".join(f"'{p}'" for p in raw_paths)
            sql = f"SELECT COUNT(*) FROM read_parquet([{paths_str}])"
            if where_clause:
                sql += f" WHERE {where_clause}"
            conn = self._get_connection()
            try:
                result = conn.execute(sql).fetchone()
                if result:
                    total += result[0]
            except Exception:
                pass

        return total

    def query_simple(
        self,
        table: str,
        symbol: str | None = None,
        start: str | None = None,
        end: str | None = None,
        columns: list[str] | None = None,
        order_by: str | None = "date",
        limit: int | None = None,
    ) -> pd.DataFrame:
        table_dir = self.base_dir / table
        if not table_dir.exists():
            return pd.DataFrame()

        parquet_files = list(table_dir.rglob("*.parquet"))
        parquet_files = [f for f in parquet_files if not f.name.endswith(".tmp")]

        if not parquet_files:
            return pd.DataFrame()

        cols = ", ".join(columns) if columns else "*"
        files_str = ", ".join(f"'{f}'" for f in parquet_files)
        sql = f"SELECT {cols} FROM read_parquet([{files_str}])"

        conditions = []
        if symbol:
            conditions.append(f"symbol = '{symbol}'")
        if start:
            conditions.append(f"date >= '{start}'")
        if end:
            conditions.append(f"date <= '{end}'")

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {limit}"

        return self._execute(sql)

    def aggregate(
        self,
        table: str,
        agg_expr: str,
        group_by: str | None = None,
        where: str | None = None,
    ) -> pd.DataFrame:
        table_dir = self.base_dir / table
        parquet_files = list(table_dir.rglob("*.parquet"))
        parquet_files = [f for f in parquet_files if not f.name.endswith(".tmp")]

        if not parquet_files:
            return pd.DataFrame()

        files_str = ", ".join(f"'{f}'" for f in parquet_files)
        sql = f"SELECT {agg_expr} FROM read_parquet([{files_str}])"
        if where:
            sql += f" WHERE {where}"
        if group_by:
            sql += f" GROUP BY {group_by}"

        return self._execute(sql)

    def _execute(self, sql: str) -> pd.DataFrame:
        conn = self._get_connection()
        try:
            conn.execute(f"SET threads={self.threads}")
            conn.execute(f"SET memory_limit='{self.memory_limit}'")
            return conn.execute(sql).fetchdf()
        except Exception as e:
            logger.error("Query failed: %s\nSQL: %s", e, sql)
            return pd.DataFrame()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = duckdb.connect(database=":memory:")
            self._local.conn.execute(f"SET threads={self.threads}")
            self._local.conn.execute(f"SET memory_limit='{self.memory_limit}'")
        return self._local.conn

    def _list_all_glob_paths(
        self,
        table: str,
        storage_layer: str,
        partition_by: str | None,
        layer: str = "raw",
    ) -> str:
        if layer == "raw":
            base = self.base_dir / storage_layer / table
        else:
            base = self.base_dir / "aggregated" / storage_layer / table

        if partition_by is None:
            return str(base / "*.parquet")

        if layer == "raw":
            return str(base / "**/*.parquet")
        return str(base / "*.parquet")

    def _build_where_clause(self, where: dict[str, Any]) -> str:
        conditions = []
        for key, value in where.items():
            if (
                isinstance(value, (list, tuple))
                and len(value) == 2
                and not isinstance(value, str)
            ):
                if all(isinstance(v, (int, float)) for v in value):
                    conditions.append(
                        f"{key} >= {self._format_value(value[0])} AND {key} <= {self._format_value(value[1])}"
                    )
                elif all(hasattr(v, "strftime") for v in value):
                    conditions.append(
                        f"{key} >= {self._format_value(value[0])} AND {key} <= {self._format_value(value[1])}"
                    )
                elif all(
                    isinstance(v, str) and self._looks_like_date(v) for v in value
                ):
                    conditions.append(
                        f"{key} >= {self._format_value(value[0])} AND {key} <= {self._format_value(value[1])}"
                    )
                else:
                    formatted_values = ", ".join(self._format_value(v) for v in value)
                    conditions.append(f"{key} IN ({formatted_values})")
            elif isinstance(value, list):
                formatted_values = ", ".join(self._format_value(v) for v in value)
                conditions.append(f"{key} IN ({formatted_values})")
            else:
                conditions.append(f"{key} = {self._format_value(value)}")
        return " AND ".join(conditions)

    def _looks_like_date(self, value: str) -> bool:
        # Accept either plain YYYY-MM-DD or a full ISO-ish timestamp so
        # minute/intraday range filters (``"2024-01-02 09:30:00"``) are
        # rendered as date-comparison SQL rather than an ``IN (...)`` list.
        return bool(
            re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$", value)
        )

    def _build_sql(
        self,
        glob_pattern: str,
        where_clause: str,
        columns: list[str] | None,
        order_by: list[str] | None,
        limit: int | None,
    ) -> str:
        if glob_pattern.startswith("["):
            sql = f"SELECT {', '.join(columns) if columns else '*'} FROM read_parquet({glob_pattern})"
        else:
            sql = (
                f"SELECT {', '.join(columns) if columns else '*'} FROM '{glob_pattern}'"
            )

        if where_clause:
            sql += f" WHERE {where_clause}"

        if order_by:
            sql += f" ORDER BY {', '.join(order_by)}"

        if limit is not None:
            sql += f" LIMIT {limit}"

        return sql

    def _build_sql_union_by_name(
        self,
        glob_pattern: str,
        where_clause: str,
        columns: list[str] | None,
        order_by: list[str] | None,
        limit: int | None,
    ) -> str:
        if glob_pattern.startswith("["):
            sql = f"SELECT {', '.join(columns) if columns else '*'} FROM read_parquet({glob_pattern}, union_by_name=true)"
        else:
            sql = (
                f"SELECT {', '.join(columns) if columns else '*'} FROM '{glob_pattern}'"
            )

        if where_clause:
            sql += f" WHERE {where_clause}"

        if order_by:
            sql += f" ORDER BY {', '.join(order_by)}"

        if limit is not None:
            sql += f" LIMIT {limit}"

        return sql

    def _format_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, (int, float)):
            return str(value)
        if hasattr(value, "strftime"):
            return f"'{value.strftime('%Y-%m-%d')}'"
        return str(value)

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None

    def register_table(
        self,
        table_name: str,
        parquet_path: str | Path,
        alias: str | None = None,
    ) -> None:
        """Register a parquet file as a DuckDB table.

        Args:
            table_name: Name for the registered table in DuckDB.
            parquet_path: Path to the parquet file.
            alias: Optional alias for the table.
        """
        conn = self._get_connection()
        table_name = alias or table_name
        try:
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{parquet_path}'"
            )
            logger.info("Registered table %s from %s", table_name, parquet_path)
        except Exception as e:
            logger.error("Failed to register table %s: %s", table_name, e)
            raise

    def unregister_table(self, table_name: str) -> None:
        """Unregister a table from DuckDB."""
        conn = self._get_connection()
        try:
            conn.execute(f"DROP VIEW IF EXISTS {table_name}")
            logger.info("Unregistered table %s", table_name)
        except Exception as e:
            logger.warning("Failed to unregister table %s: %s", table_name, e)


__all__ = ["DuckDBEngine"]
