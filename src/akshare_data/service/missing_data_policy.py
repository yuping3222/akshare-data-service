"""Missing data policy for read-only served service.

When Served layer has no data for a query, this module defines:
1. What status to return to the caller
2. How to record/trigger an async backfill request (placeholder)

Key principle: NEVER synchronously fall back to source adapters.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional

from akshare_data.ingestion.backfill_request import BackfillRequest, get_backfill_registry

logger = logging.getLogger(__name__)


class MissingAction(Enum):
    """Action to take when data is missing in Served."""

    RETURN_EMPTY = "return_empty"
    RETURN_STALE = "return_stale"
    REQUEST_BACKFILL = "request_backfill"
    RAISE_ERROR = "raise_error"


@dataclass
class MissingDataReport:
    """Structured report when Served has no data."""

    table: str
    query_params: Dict[str, Any]
    action: MissingAction
    message: str
    suggested_action: Optional[str] = None
    backfill_request_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "query_params": self.query_params,
            "action": self.action.value,
            "message": self.message,
            "suggested_action": self.suggested_action,
            "backfill_request_id": self.backfill_request_id,
            "metadata": self.metadata,
        }


class MissingDataPolicy:
    """Policy engine for handling missing data in Served layer.

    Determines what to do when a query returns empty from Served.
    """

    def __init__(
        self,
        default_action: MissingAction = MissingAction.RETURN_EMPTY,
        table_actions: Optional[Dict[str, MissingAction]] = None,
    ):
        self._default_action = default_action
        self._table_actions = table_actions or {}

    def resolve_action(self, table: str) -> MissingAction:
        return self._table_actions.get(table, self._default_action)

    def handle_missing(
        self,
        table: str,
        query_params: Dict[str, Any],
        stale_data: Optional[Any] = None,
    ) -> MissingDataReport:
        action = self.resolve_action(table)

        if action == MissingAction.RETURN_EMPTY:
            return MissingDataReport(
                table=table,
                query_params=query_params,
                action=action,
                message=f"No served data available for table='{table}'",
                suggested_action="Run offline downloader to populate data first",
            )

        if action == MissingAction.RETURN_STALE:
            return MissingDataReport(
                table=table,
                query_params=query_params,
                action=action,
                message="No fresh served data; returning stale cached result if available",
                metadata={"has_stale": stale_data is not None},
            )

        if action == MissingAction.REQUEST_BACKFILL:
            registry = get_backfill_registry()
            request = BackfillRequest.new(
                dataset=table,
                domain=query_params.get("domain", "cn"),
                source_name=query_params.get("source_name", ""),
                interface_name=query_params.get("interface_name", ""),
                start_date=query_params.get("start_date", date.today()),
                end_date=query_params.get("end_date", date.today()),
                reason="missing_data",
                requested_by="service",
                params=query_params,
            )
            registry.submit(request)
            return MissingDataReport(
                table=table,
                query_params=query_params,
                action=action,
                message=f"Backfill request submitted: {request.request_id}",
                suggested_action="Data will be available after next ingestion cycle",
                backfill_request_id=request.request_id,
            )

        if action == MissingAction.RAISE_ERROR:
            return MissingDataReport(
                table=table,
                query_params=query_params,
                action=action,
                message=f"No served data available for table='{table}'",
                suggested_action="Ensure data has been ingested before querying",
            )

        return MissingDataReport(
            table=table,
            query_params=query_params,
            action=MissingAction.RETURN_EMPTY,
            message=f"Unknown missing action for table='{table}'",
        )

    def set_table_action(self, table: str, action: MissingAction) -> None:
        self._table_actions[table] = action
