"""Root-level shared fixtures for the akshare-data-service test suite.

This conftest provides the common testing infrastructure described in
docs/design/TESTING_PLAN.md. All fixtures import from the actual package
(akshare_data) and use proper typing.

Fixture categories:
- Infrastructure: temp_cache_dir, config_dir
- Service instances: data_service, cache_manager
- Test data: sample_stock_data, sample_minute_data
- Mock helpers: mock_lixinger_response, mock_akshare_response
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock
from typing import Generator

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager, reset_cache_manager


# ---------------------------------------------------------------------------
# Infrastructure fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_cache_dir() -> Generator[str, None, None]:
    """Create a temporary directory for cache storage; cleans up after test.

    Function-scoped so each test gets an isolated directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def config_dir() -> Generator[Path, None, None]:
    """Provide a temp config directory with a minimal system.yaml.

    Useful for tests that need file-based configuration without touching
    the user's real config.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir)
        # Write a minimal system.yaml so the config loader doesn't fail
        system_yaml = config_path / "system.yaml"
        system_yaml.write_text(
            "# Minimal test configuration\n"
            "cache:\n"
            "  base_dir: ./cache\n"
            "  compression: snappy\n"
        )
        yield config_path


# ---------------------------------------------------------------------------
# Service instance fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cache_manager(temp_cache_dir: str) -> Generator[CacheManager, None, None]:
    """Standalone CacheManager instance, properly reset between tests.

    Resets the singleton before and after each test to avoid cross-test
    pollution.
    """
    reset_cache_manager()
    CacheManager.reset_instance()

    manager = CacheManager(base_dir=temp_cache_dir)
    yield manager

    # Clean up singleton state after test
    reset_cache_manager()
    CacheManager.reset_instance()


@pytest.fixture
def data_service(
    cache_manager: CacheManager,
) -> Generator[DataService, None, None]:
    """Pre-configured DataService instance using temp_cache_dir.

    The CacheManager is injected so that the service uses the temporary
    directory instead of the default ./cache path.
    """
    service = DataService(cache_manager=cache_manager)
    yield service


# ---------------------------------------------------------------------------
# Test data fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_stock_data() -> pd.DataFrame:
    """Generate standard daily OHLCV test DataFrame.

    Columns: date, symbol, open, high, low, close, volume, amount
    Proper dtypes: datetime64 for date, float64 for prices, int64 for volume.
    """
    dates = pd.date_range("2024-01-02", "2024-01-15", freq="B")  # business days
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": ["sh600000"] * len(dates),
            "open": [10.0 + i * 0.1 for i in range(len(dates))],
            "high": [11.0 + i * 0.1 for i in range(len(dates))],
            "low": [9.0 + i * 0.1 for i in range(len(dates))],
            "close": [10.5 + i * 0.1 for i in range(len(dates))],
            "volume": [100_000 + i * 10_000 for i in range(len(dates))],
            "amount": [1_000_000.0 + i * 100_000.0 for i in range(len(dates))],
        }
    )


@pytest.fixture
def sample_minute_data() -> pd.DataFrame:
    """Generate minute-level test data.

    Columns: datetime, symbol, open, high, low, close, volume
    30 one-minute bars starting from 2024-01-02 09:30.
    """
    times = pd.date_range("2024-01-02 09:30", periods=30, freq="min")
    return pd.DataFrame(
        {
            "datetime": times,
            "symbol": ["sh600000"] * len(times),
            "open": [10.0 + i * 0.01 for i in range(len(times))],
            "high": [10.05 + i * 0.01 for i in range(len(times))],
            "low": [9.95 + i * 0.01 for i in range(len(times))],
            "close": [10.02 + i * 0.01 for i in range(len(times))],
            "volume": [5_000 + i * 500 for i in range(len(times))],
        }
    )


# ---------------------------------------------------------------------------
# Mock helper fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_lixinger_response() -> MagicMock:
    """Return a standard mock response for LixingerAdapter.

    Provides a MagicMock configured with is_configured=True and a
    get_daily method that returns a simple DataFrame.

    Usage:
        def test_something(mock_lixinger_response):
            with patch(
                "akshare_data.sources.lixinger_source.get_lixinger_client",
                return_value=mock_lixinger_response,
            ):
                ...
    """
    mock_client = MagicMock()
    mock_client.is_configured.return_value = True
    mock_client.get_daily.return_value = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", "2024-01-15", freq="B"),
            "symbol": ["sh600000"] * 10,
            "open": [10.0] * 10,
            "high": [11.0] * 10,
            "low": [9.0] * 10,
            "close": [10.5] * 10,
            "volume": [100_000] * 10,
        }
    )
    return mock_client


@pytest.fixture
def mock_akshare_response() -> MagicMock:
    """Return a standard mock response for AKShare data source.

    Provides a MagicMock pre-configured with common AKShare function
    return values (stock_zh_a_hist, etc.).

    Usage:
        def test_something(mock_akshare_response):
            with patch(
                "akshare_data.sources.akshare_source.ak.stock_zh_a_hist",
                return_value=mock_akshare_response,
            ):
                ...
    """
    mock_df = pd.DataFrame(
        {
            "日期": pd.date_range("2024-01-02", "2024-01-15", freq="B"),
            "开盘": [10.0] * 10,
            "最高": [11.0] * 10,
            "最低": [9.0] * 10,
            "收盘": [10.5] * 10,
            "成交量": [100_000] * 10,
            "成交额": [1_000_000.0] * 10,
        }
    )
    mock_response = MagicMock()
    mock_response.__repr__ = lambda self: "<mock akshare DataFrame>"
    return mock_df
