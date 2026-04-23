"""Ingestion layer: source adapter base classes and mixins.

This module defines the unified data source interface that all adapters
(AkShare, Lixinger, Tushare, Mock) must implement. It is the canonical
home for source adapter contracts in the new architecture.

``ingestion.base.DataSource`` inherits from ``core.base.DataSource`` so
that ``isinstance(adapter, core.base.DataSource)`` remains True for
backward compatibility.
"""

from __future__ import annotations

from abc import abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

# Re-use the legacy base so isinstance() checks against core.base.DataSource
# continue to work.  We layer additional mixins on top.
from akshare_data.core.base import (
    BondMixin,
    CompanyInfoMixin,
    DataSource as _LegacyDataSource,
    FinanceMixin,
    FundMixin,
    FuturesMixin,
    HsgtMixin,
    IndustryMixin,
    MiscMixin,
    MoneyFlowMixin,
    OptionMixin,
    QuoteExtensionMixin,
    ShareholderMixin,
)


# ---------------------------------------------------------------------------
# Additional mixins not present in the legacy base
# ---------------------------------------------------------------------------


class MacroMixin:
    """Mixin for macroeconomic data methods."""

    def get_macro_raw(self, indicator: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_macro_raw")

    def get_cpi_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_cpi_data")

    def get_ppi_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_ppi_data")

    def get_pmi_index(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_pmi_index")

    def get_lpr_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_lpr_rate")

    def get_m2_supply(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support get_m2_supply")


# ---------------------------------------------------------------------------
# DataSource — the canonical adapter contract
# ---------------------------------------------------------------------------


class DataSource(
    _LegacyDataSource,
    MacroMixin,
):
    """Abstract base class for all data source adapters.

    Inherits from ``core.base.DataSource`` for backward compatibility
    (``isinstance(x, core.base.DataSource)`` remains True).  Adds
    ``MacroMixin`` for macroeconomic data methods.

    Adapters live in ``ingestion/adapters/`` and are orchestrated by
    ``ingestion/router.py``.  The ``service`` layer must never import
    adapters directly.
    """

    name: str = "abstract"
    source_type: str = "abstract"

    @abstractmethod
    def get_daily_data(
        self,
        symbol: str,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV data for a symbol."""

    @abstractmethod
    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        """Fetch index constituent stocks with optional weights."""

    @abstractmethod
    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> List[str]:
        """Fetch a list of trading dates."""

    @abstractmethod
    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch a list of securities (stocks, ETFs, indices, etc.)."""

    @abstractmethod
    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """Fetch basic information for a single security."""

    @abstractmethod
    def get_minute_data(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch intraday minute-level data."""

    def is_configured(self) -> bool:
        """Return True if the adapter has valid credentials/config."""
        return True

    def health_check(self) -> Dict[str, Any]:
        """Run a lightweight health check."""
        return {"status": "ok", "message": f"Source: {self.name}"}

    def get_source_info(self) -> Dict[str, Any]:
        """Return metadata about this source adapter."""
        return {
            "name": self.name,
            "type": self.source_type,
            "description": f"Data source: {self.name}",
        }


__all__ = [
    "DataSource",
    "FinanceMixin",
    "ShareholderMixin",
    "IndustryMixin",
    "FundMixin",
    "FuturesMixin",
    "OptionMixin",
    "MacroMixin",
    "MoneyFlowMixin",
    "MiscMixin",
    "BondMixin",
    "HsgtMixin",
    "QuoteExtensionMixin",
    "CompanyInfoMixin",
]
