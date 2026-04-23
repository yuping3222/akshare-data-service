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
    """Standard minute-level ETF DataFrame."""
    times = pd.date_range("2024-01-02 09:30", periods=30, freq="min")
    return pd.DataFrame(
        {
            "datetime": times,
            "symbol": ["510300.XSHG"] * len(times),
            "open": [3.5 + i * 0.005 for i in range(len(times))],
            "high": [3.51 + i * 0.005 for i in range(len(times))],
            "low": [3.49 + i * 0.005 for i in range(len(times))],
            "close": [3.505 + i * 0.005 for i in range(len(times))],
            "volume": [5_000 + i * 500 for i in range(len(times))],
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
    """Mock index components list."""
    return pd.DataFrame(
        {
            "code": ["600000.XSHG", "600036.XSHG", "000001.XSHE"],
            "name": ["浦发银行", "招商银行", "平安银行"],
            "weight": [2.5, 3.1, 1.8],
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
    """DataService with stock source mock injected.

    The mock source is patched into both LixingerAdapter and AkShareAdapter
    so that any provider path returns the test data.
    """
    service = DataService(cache_manager=system_cache_manager)

    # Patch both adapters to return the mock source
    for attr in ("lixinger", "akshare"):
        adapter = getattr(service, attr)
        adapter.get_daily_data = MagicMock(return_value=stock_source_df.copy())

    yield service


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).resolve().parent.parent.parent
