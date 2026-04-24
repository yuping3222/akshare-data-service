"""Shared fixtures for system-level tests.

These fixtures build on the root conftest.py fixtures and provide
system-test-specific helpers such as mock sources wired into DataService
and Docker-related test infrastructure.
"""

from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager, reset_cache_manager


def _seed_cache(
    cache: CacheManager,
    table: str,
    df: pd.DataFrame,
    *,
    adjust: str = "qfq",
) -> None:
    """Preload ``df`` into the Served cache for ``table``.

    System tests previously relied on monkey-patching source adapters
    (e.g. ``service.akshare.get_macro_gdp = MagicMock(...)``); under the
    read-only facade those patches are no-ops because the service only
    reads from Served. This helper plays the role that an ingestion /
    offline backfill job would in production: it normalizes symbol
    suffixes and fills the ``adjust`` column when the registered schema
    requires one so writes go through SchemaValidator cleanly.
    """
    data = df.copy()
    if "symbol" in data.columns:
        data["symbol"] = data["symbol"].astype(str).str.split(".").str[0]
    if "adjust" not in data.columns:
        data["adjust"] = adjust
    cache.write(table=table, data=data)


@pytest.fixture
def system_cache_manager(temp_cache_dir: str) -> Generator[CacheManager, None, None]:
    """CacheManager for system tests, fully isolated.

    Resets singleton state before and after to avoid cross-test pollution.
    """
    reset_cache_manager()
    CacheManager.reset_instance()
    manager = CacheManager(base_dir=temp_cache_dir)
    yield manager
    reset_cache_manager()
    CacheManager.reset_instance()


@pytest.fixture
def stock_source_df() -> pd.DataFrame:
    """Standard daily OHLCV DataFrame for a single stock."""
    dates = pd.date_range("2024-01-02", "2024-01-19", freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": ["600000.XSHG"] * len(dates),
            "open": [10.0 + i * 0.1 for i in range(len(dates))],
            "high": [11.0 + i * 0.1 for i in range(len(dates))],
            "low": [9.0 + i * 0.1 for i in range(len(dates))],
            "close": [10.5 + i * 0.1 for i in range(len(dates))],
            "volume": [100_000 + i * 10_000 for i in range(len(dates))],
            "amount": [1_000_000.0 + i * 100_000.0 for i in range(len(dates))],
        }
    )


@pytest.fixture
def index_source_df() -> pd.DataFrame:
    """Standard daily index DataFrame with valuation fields."""
    dates = pd.date_range("2024-01-02", "2024-01-19", freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": ["000300.XSHG"] * len(dates),
            "open": [3400.0 + i * 5.0 for i in range(len(dates))],
            "high": [3450.0 + i * 5.0 for i in range(len(dates))],
            "low": [3350.0 + i * 5.0 for i in range(len(dates))],
            "close": [3420.0 + i * 5.0 for i in range(len(dates))],
            "volume": [5_000_000 + i * 500_000 for i in range(len(dates))],
            "amount": [50_000_000.0 + i * 5_000_000.0 for i in range(len(dates))],
            "pe": [12.0 + i * 0.05 for i in range(len(dates))],
            "pb": [1.4 + i * 0.01 for i in range(len(dates))],
        }
    )


@pytest.fixture
def etf_source_df() -> pd.DataFrame:
    """Standard daily ETF DataFrame."""
    dates = pd.date_range("2024-01-02", "2024-01-19", freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": ["510300.XSHG"] * len(dates),
            "open": [3.5 + i * 0.01 for i in range(len(dates))],
            "high": [3.6 + i * 0.01 for i in range(len(dates))],
            "low": [3.4 + i * 0.01 for i in range(len(dates))],
            "close": [3.55 + i * 0.01 for i in range(len(dates))],
            "volume": [200_000 + i * 20_000 for i in range(len(dates))],
            "amount": [700_000.0 + i * 70_000.0 for i in range(len(dates))],
        }
    )


@pytest.fixture
def etf_minute_source_df() -> pd.DataFrame:
    """Standard minute-level ETF DataFrame.

    Columns align with the ``stock_minute`` schema (which now covers ETFs
    too via a ``period`` column) so tests that seed Served with this frame
    pass through the SchemaValidator unchanged.
    """
    times = pd.date_range("2024-01-02 09:30", periods=30, freq="min")
    return pd.DataFrame(
        {
            "datetime": times,
            "symbol": ["510300.XSHG"] * len(times),
            "week": ["2024-W01"] * len(times),
            "period": ["1min"] * len(times),
            "adjust": ["none"] * len(times),
            "open": [3.5 + i * 0.005 for i in range(len(times))],
            "high": [3.51 + i * 0.005 for i in range(len(times))],
            "low": [3.49 + i * 0.005 for i in range(len(times))],
            "close": [3.505 + i * 0.005 for i in range(len(times))],
            "volume": [5_000 + i * 500 for i in range(len(times))],
            "amount": [17_500.0 + i * 500.0 for i in range(len(times))],
        }
    )


@pytest.fixture
def macro_cpi_df() -> pd.DataFrame:
    """Mock macro CPI data."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "value": [0.3, 0.7, 0.1],
            "indicator": ["CPI"] * 3,
        }
    )


@pytest.fixture
def macro_pmi_df() -> pd.DataFrame:
    """Mock macro PMI data."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "value": [50.2, 50.8, 49.9],
            "indicator": ["PMI"] * 3,
        }
    )


@pytest.fixture
def macro_gdp_df() -> pd.DataFrame:
    """Mock macro GDP data."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-Q4", "2024-Q1", "2024-Q2"]),
            "gdp": [128_000_000, 130_000_000, 132_000_000],
            "growth": [5.2, 5.3, 5.1],
        }
    )


@pytest.fixture
def index_components_df() -> pd.DataFrame:
    """Mock index components list aligned with the index_components schema."""
    return pd.DataFrame(
        {
            "index_code": ["000300"] * 3,
            "date": pd.to_datetime(["2024-01-02"] * 3).date,
            "symbol": ["600000.XSHG", "600036.XSHG", "000001.XSHE"],
            "weight": [2.5, 3.1, 1.8],
            # kept for tests that assert on ``code`` column presence
            "code": ["600000.XSHG", "600036.XSHG", "000001.XSHE"],
            "name": ["浦发银行", "招商银行", "平安银行"],
        }
    )


@pytest.fixture
def mock_stock_source(stock_source_df: pd.DataFrame) -> MagicMock:
    """Mock data source configured for stock queries."""
    src = MagicMock()
    src.get_daily_data.return_value = stock_source_df.copy()
    src.get_minute_data.return_value = pd.DataFrame()
    src.get_balance_sheet.return_value = pd.DataFrame()
    src.get_income_statement.return_value = pd.DataFrame()
    src.get_cash_flow.return_value = pd.DataFrame()
    src.get_finance_indicator.return_value = pd.DataFrame()
    src.get_money_flow.return_value = pd.DataFrame()
    src.get_northbound_holdings.return_value = pd.DataFrame()
    src.get_block_deal.return_value = pd.DataFrame()
    src.get_dragon_tiger_list.return_value = pd.DataFrame()
    src.get_margin_data.return_value = pd.DataFrame()
    src.get_north_money_flow.return_value = pd.DataFrame()
    src.get_dividend_data.return_value = pd.DataFrame()
    src.get_restricted_release.return_value = pd.DataFrame()
    src.get_realtime_data.return_value = pd.DataFrame()
    src.get_index_daily.return_value = pd.DataFrame()
    src.get_index_components.return_value = pd.DataFrame()
    src.get_etf_daily.return_value = pd.DataFrame()
    src.get_macro_gdp.return_value = pd.DataFrame()
    src.get_shibor_rate.return_value = pd.DataFrame()
    src.get_social_financing.return_value = pd.DataFrame()
    src.get_call_auction.return_value = pd.DataFrame()
    src.get_hk_stocks.return_value = pd.DataFrame()
    src.get_us_stocks.return_value = pd.DataFrame()
    src.get_trading_days.return_value = None
    src.get_index_stocks.return_value = []
    src.get_industry_stocks.return_value = []
    src.get_concept_stocks.return_value = []
    src.get_stock_concepts.return_value = []
    return src


@pytest.fixture
def data_service_with_stock_source(
    system_cache_manager: CacheManager,
    mock_stock_source: MagicMock,
    stock_source_df: pd.DataFrame,
) -> Generator[DataService, None, None]:
    """DataService populated with stock data in Served.

    The online DataService facade is read-only and never synchronously
    fetches from source adapters anymore. To keep the system tests
    meaningful, we pre-write the expected data directly into the Served
    CacheManager so the facade returns it.
    """
    service = DataService(cache_manager=system_cache_manager)

    # Pre-populate Served with the stock_daily test data. We write without
    # an explicit partition_value so the CacheManager falls back to a
    # single-file layout; query paths read any file under the table tree.
    df = stock_source_df.copy()
    # Ensure schema compatibility: normalize symbol suffix and fill adjust.
    df["symbol"] = df["symbol"].astype(str).str.split(".").str[0]
    if "adjust" not in df.columns:
        df["adjust"] = "qfq"
    system_cache_manager.write(
        table="stock_daily",
        data=df,
    )

    # Also keep the legacy source mocks patched for any test that still
    # inspects adapter call args (they are tolerated but never invoked).
    for attr in ("lixinger", "akshare"):
        adapter = getattr(service, attr)
        adapter.get_daily_data = MagicMock(return_value=stock_source_df.copy())

    yield service


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).resolve().parent.parent.parent
