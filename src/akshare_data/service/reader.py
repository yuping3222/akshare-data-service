"""Served data reader for the service layer.

Read-only access to the Served data layer.
Uses CacheManager as the current storage backend (Parquet + DuckDB).
Does NOT write data or fetch from sources.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.store.manager import CacheManager, get_cache_manager
from akshare_data.core.schema import get_table_schema
from akshare_data.core.exceptions import (
    InvalidColumnError,
    InvalidPartitionError,
    InvalidTableError,
)
from akshare_data.core.param_validator import validate_query

logger = logging.getLogger(__name__)


class ServedReader:
    """Read-only reader for Served data layer.

    Wraps CacheManager read operations with service-level semantics:
    - Schema-aware reads
    - Empty result detection
    - No write or source-fetch capabilities
    """

    def __init__(self, cache_manager: Optional[CacheManager] = None):
        self._cache = cache_manager or get_cache_manager()

    def read(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        partition_by: Optional[str] = None,
        partition_value: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read data from Served layer.

        Returns empty DataFrame if no data found (never fetches from source).

        When partition_by mismatches schema, converts partition_value to where clause.

        Raises:
            InvalidTableError: Table not found in registry.
            InvalidPartitionError: partition_by does not match schema.
            InvalidColumnError: where/columns/order_by contain invalid columns.
        """
        storage_layer = self._resolve_storage_layer(table)

        resolved_partition_by, effective_partition_value, effective_where = (
            self._resolve_partition_params(table, partition_by, partition_value, where)
        )

        try:
            validate_query(
                table,
                resolved_partition_by,
                effective_partition_value,
                effective_where,
                columns,
                order_by,
            )
        except InvalidTableError:
            # Unregistered tables are treated as soft-misses in the Served
            # layer: we fall through to the storage backend so operators /
            # tests that mock CacheManager.read can still exercise the path,
            # and downstream queries simply receive an empty DataFrame when
            # nothing is stored.
            logger.warning(
                "ServedReader: table=%s not in schema registry; serving as soft-miss",
                table,
            )
        except InvalidColumnError as e:
            # Legacy facade methods occasionally pass a generic ``date`` key
            # that does not match the schema's business date column (e.g.
            # equity_pledge uses ``pledge_date``). Rather than 500-ing the
            # read-only query, drop the unknown keys, log a warning, and let
            # the storage backend return what it has.
            logger.warning(
                "ServedReader: table=%s invalid columns %s; dropping from query",
                table,
                e.invalid_columns,
            )
            effective_where = {
                k: v
                for k, v in (effective_where or {}).items()
                if k not in set(e.invalid_columns)
            } or None
            if columns is not None:
                columns = [c for c in columns if c not in set(e.invalid_columns)]
            if order_by is not None:
                order_by = [
                    c for c in order_by if c.split()[0] not in set(e.invalid_columns)
                ]
        except InvalidPartitionError:
            # Schema disagreement is already handled by
            # _resolve_partition_params; if we still trip the validator, the
            # safest behaviour is to drop the partition hint and let the
            # storage backend fall back to a full-table read.
            logger.warning(
                "ServedReader: table=%s partition mismatch; dropping partition hint",
                table,
            )
            resolved_partition_by = None
            effective_partition_value = None

        try:
            result = self._cache.read(
                table,
                storage_layer=storage_layer,
                partition_by=resolved_partition_by,
                partition_value=effective_partition_value,
                where=effective_where,
                columns=columns,
                order_by=order_by,
                limit=limit,
            )
        except Exception as e:
            logger.error("Failed to read table=%s from served: %s", table, e)
            return pd.DataFrame()

        return result if result is not None else pd.DataFrame()

    def exists(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        partition_by: Optional[str] = None,
        partition_value: Optional[str] = None,
    ) -> bool:
        """Check if data exists in Served layer."""
        storage_layer = self._resolve_storage_layer(table)

        resolved_partition_by, effective_partition_value, effective_where = (
            self._resolve_partition_params(table, partition_by, partition_value, where)
        )

        try:
            return self._cache.exists(
                table,
                storage_layer=storage_layer,
                partition_by=resolved_partition_by,
                where=effective_where,
            )
        except Exception as e:
            logger.error("Failed to check existence for table=%s: %s", table, e)
            return False

    def has_date_range(
        self,
        table: str,
        start: str,
        end: str,
        date_col: str = "date",
        where: Optional[Dict[str, Any]] = None,
        partition_by: Optional[str] = None,
        partition_value: Optional[str] = None,
    ) -> bool:
        """Check if Served has data covering the given date range."""
        storage_layer = self._resolve_storage_layer(table)

        resolved_partition_by, effective_partition_value, effective_where = (
            self._resolve_partition_params(table, partition_by, partition_value, where)
        )

        try:
            return self._cache.has_range(
                table,
                storage_layer=storage_layer,
                partition_by=resolved_partition_by,
                where=effective_where,
                date_col=date_col,
                start=start,
                end=end,
            )
        except Exception as e:
            logger.error("Failed to check date range for table=%s: %s", table, e)
            return False

    def get_table_info(self, table: str) -> Dict[str, Any]:
        """Get metadata about a served table."""
        storage_layer = self._resolve_storage_layer(table)
        try:
            return self._cache.table_info(table, storage_layer=storage_layer)
        except Exception as e:
            logger.error("Failed to get table info for table=%s: %s", table, e)
            return {"name": table, "error": str(e)}

    def list_tables(self) -> List[str]:
        """List all available tables in Served layer."""
        try:
            return self._cache.list_tables()
        except Exception as e:
            logger.error("Failed to list tables: %s", e)
            return []

    def _resolve_storage_layer(self, table: str) -> str:
        schema = get_table_schema(table)
        if schema is not None:
            return schema.storage_layer
        return "daily"

    def _resolve_partition_by(self, table: str) -> Optional[str]:
        schema = get_table_schema(table)
        if schema is not None:
            return schema.partition_by
        return None

    def _resolve_partition_params(
        self,
        table: str,
        partition_by: Optional[str],
        partition_value: Optional[str],
        where: Optional[Dict[str, Any]],
    ) -> tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
        """Resolve partition parameters against the schema contract.

        When user-provided partition_by mismatches the schema:
        - If the requested key is actually a valid column, treat the pair as
          an extra where filter instead of a partition hint (this is the
          common case where the legacy facade still passes symbol= even
          though the table is partitioned by date/report_date).
        - Otherwise fall back to the schema's partition_by and lift the
          partition_value into the where clause keyed by the expected
          partition column.

        Returns:
            (resolved_partition_by, effective_partition_value, effective_where)
        """
        expected = self._resolve_partition_by(table)

        if partition_by is None:
            return expected, None, where

        if expected is None or partition_by == expected:
            return partition_by, partition_value, where

        schema = get_table_schema(table)
        schema_cols = set(schema.schema.keys()) if schema else set()
        effective_where = where.copy() if where else {}

        if partition_by in schema_cols:
            logger.warning(
                "ServedReader partition_by mismatch for table=%s: got=%s expected=%s; "
                "moving partition_value to where[%s]",
                table,
                partition_by,
                expected,
                partition_by,
            )
            if partition_value is not None:
                effective_where[partition_by] = partition_value
            return expected, None, effective_where

        logger.warning(
            "ServedReader partition_by mismatch for table=%s: got=%s expected=%s; "
            "converting partition_value to where clause",
            table,
            partition_by,
            expected,
        )
        if partition_value is not None:
            effective_where[expected] = partition_value

        return expected, None, effective_where

    def _validate_partition_by(
        self,
        table: str,
        partition_by: Optional[str],
    ) -> Optional[str]:
        """Ensure read partition key matches schema contract.

        On mismatch, warn and fallback to schema.partition_by.
        """
        expected = self._resolve_partition_by(table)
        if partition_by is None:
            return expected
        if expected is None or partition_by == expected:
            return partition_by

        logger.warning(
            "ServedReader partition_by mismatch for table=%s: got=%s expected=%s; fallback to expected",
            table,
            partition_by,
            expected,
        )
        return expected
