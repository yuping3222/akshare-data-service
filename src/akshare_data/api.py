"""Thin compatibility wrapper for the online read-only API.

Canonical implementations:
- DataService facade: ``akshare_data.service_facade``
- Namespace assembly: ``akshare_data.namespace_assembly``
- Legacy source proxy: ``akshare_data.legacy_adapter``
"""

from __future__ import annotations

import threading
from typing import Optional

from akshare_data.legacy_adapter import SourceProxy
from akshare_data.namespace_assembly import (
    CNETFQuoteAPI,
    CNIndexMetaAPI,
    CNIndexQuoteAPI,
    CNMarketAPI,
    CNStockCapitalAPI,
    CNStockEventAPI,
    CNStockFinanceAPI,
    CNStockQuoteAPI,
    HKMarketAPI,
    HKStockQuoteAPI,
    MacroAPI,
    MacroChinaAPI,
    USMarketAPI,
    USStockQuoteAPI,
)
from akshare_data.service.data_service import QueryResult
from akshare_data.service.missing_data_policy import MissingAction
from akshare_data.service_facade import DataService

__all__ = [
    "DataService",
    "get_service",
    "SourceProxy",
    "CNStockQuoteAPI",
    "CNStockFinanceAPI",
    "CNStockCapitalAPI",
    "CNIndexQuoteAPI",
    "CNIndexMetaAPI",
    "CNETFQuoteAPI",
    "CNStockEventAPI",
    "HKStockQuoteAPI",
    "HKMarketAPI",
    "USStockQuoteAPI",
    "USMarketAPI",
    "MacroChinaAPI",
    "MacroAPI",
    "CNMarketAPI",
    "QueryResult",
    "MissingAction",
]

_default_service: Optional[DataService] = None
_service_lock = threading.Lock()


def get_service() -> DataService:
    """Get global DataService singleton."""
    global _default_service
    if _default_service is None:
        with _service_lock:
            if _default_service is None:
                _default_service = DataService()
    return _default_service
