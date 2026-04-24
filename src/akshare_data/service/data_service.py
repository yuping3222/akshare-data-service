"""Read-only DataService for served data.

This is the new core service that replaces the old Cache-First DataService.
Key differences:
- Only reads from Served layer (CacheManager as current backend)
- Never synchronously fetches from source adapters
- Returns clear status when data is missing
- Can trigger async backfill requests (placeholder)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.service.reader import ServedReader
from akshare_data.service.version_selector import VersionSelector
from akshare_data.service.missing_data_policy import (
    MissingDataPolicy,
    MissingAction,
    MissingDataReport,
)
from akshare_data.ingestion.backfill_request import get_backfill_registry, BackfillRequest
from akshare_data.store.manager import CacheManager
from akshare_data.core.symbols import normalize_symbol

logger = logging.getLogger(__name__)


class QueryResult:
    """Wrapper for query results that includes metadata."""

    def __init__(
        self,
        data: pd.DataFrame,
        table: str,
        has_data: bool,
        missing_report: Optional[MissingDataReport] = None,
        version: Optional[str] = None,
    ):
        self.data = data
        self.table = table
        self.has_data = has_data
        self.missing_report = missing_report
        self.version = version

    @property
    def is_empty(self) -> bool:
        return self.data is None or self.data.empty

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "table": self.table,
            "has_data": self.has_data,
            "row_count": len(self.data) if self.data is not None else 0,
            "version": self.version,
        }
        if self.missing_report:
            result["missing_report"] = self.missing_report.to_dict()
        return result


class DataService:
    """Read-only DataService that serves data from the Served layer.

    This replaces the old Cache-First DataService. The new service:
    1. Only reads from Served (via ServedReader wrapping CacheManager)
    2. Never fetches from source adapters synchronously
    3. Returns clear status when data is missing
    4. Can queue async backfill requests
    """

    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        missing_policy: Optional[MissingDataPolicy] = None,
        version_selector: Optional[VersionSelector] = None,
    ):
        self._reader = ServedReader(cache_manager)
        self._missing_policy = missing_policy or MissingDataPolicy()
        self._version_selector = version_selector or VersionSelector()

    def query(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        partition_by: Optional[str] = None,
        partition_value: Optional[str] = None,
        version: Optional[str] = None,
    ) -> QueryResult:
        """Query served data.

        Returns QueryResult with data and metadata. Never fetches from source.
        """
        resolved_version = self._version_selector.resolve(version)

        data = self._reader.read(
            table,
            where=where,
            columns=columns,
            order_by=order_by,
            limit=limit,
            partition_by=partition_by,
            partition_value=partition_value,
        )

        if data.empty:
            query_params = {
                "where": where,
                "columns": columns,
                "partition_by": partition_by,
                "partition_value": partition_value,
            }
            report = self._missing_policy.handle_missing(table, query_params)
            return QueryResult(
                data=pd.DataFrame(),
                table=table,
                has_data=False,
                missing_report=report,
                version=resolved_version,
            )

        return QueryResult(
            data=data,
            table=table,
            has_data=True,
            version=resolved_version,
        )

    def query_daily(
        self,
        table: str,
        symbol: str,
        start_date: str,
        end_date: str,
        version: Optional[str] = None,
    ) -> QueryResult:
        """Query daily time-series data from Served."""
        sym = normalize_symbol(symbol)
        where = {"date": (start_date, end_date), "symbol": sym}
        return self.query(
            table=table,
            where=where,
            version=version,
        )

    def request_backfill(
        self,
        table: str,
        params: Optional[Dict[str, Any]] = None,
        priority: str = "normal",
    ) -> str:
        """Request async backfill for missing data.

        Returns a backfill request ID. Does NOT synchronously fetch data.
        """
        from datetime import date as _date
        _params = params or {}
        try:
            request = BackfillRequest.new(
                dataset=table,
                domain=_params.get("domain", "cn"),
                source_name=_params.get("source_name", ""),
                interface_name=_params.get("interface_name", ""),
                start_date=_params.get("start_date", _date.today()),
                end_date=_params.get("end_date", _date.today()),
                priority=priority if priority in ("p0", "p1", "p2", "p3") else "p2",
                reason="service.request_backfill",
                params=_params,
            )
            registry = get_backfill_registry()
            submitted = registry.submit(request)
            return submitted.request_id
        except Exception as exc:
            logger.warning("Failed to submit backfill request for table=%s: %s", table, exc)
            return ""

    def get_backfill_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a backfill request."""
        registry = get_backfill_registry()
        req = registry.get(request_id)
        if req is None:
            return None
        return {"request_id": req.request_id, "status": req.status.value,
                "dataset": req.dataset}

    def list_pending_backfills(self) -> List[Dict[str, Any]]:
        """List all pending backfill requests."""
        registry = get_backfill_registry()
        return [
            {"request_id": r.request_id, "status": r.status.value, "dataset": r.dataset}
            for r in registry.get_pending()
        ]

    def set_missing_action(self, table: str, action: MissingAction) -> None:
        """Configure missing data action for a specific table."""
        self._missing_policy.set_table_action(table, action)

    def get_table_info(self, table: str) -> Dict[str, Any]:
        """Get metadata about a served table."""
        return self._reader.get_table_info(table)

    def list_tables(self) -> List[str]:
        """List all available served tables."""
        return self._reader.list_tables()

    def table_exists(self, table: str) -> bool:
        """Check if a table has any data in Served."""
        return self._reader.exists(table)

    def has_data_for_range(
        self,
        table: str,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> bool:
        """Check if Served has data for the given symbol and date range."""
        sym = normalize_symbol(symbol)
        return self._reader.has_date_range(
            table,
            start=start_date,
            end=end_date,
            where={"symbol": sym},
        )
