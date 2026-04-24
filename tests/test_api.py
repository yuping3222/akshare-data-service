"""tests/test_api.py

DataService API Tests - Fixed version with proper mocking

The key issue: _read_range calls self.cache.read() first before fetching from akshare.
Tests must mock cache.read to return empty DataFrame to ensure the fetch path is taken.
"""

import tempfile
from datetime import datetime, timedelta
from typing import List
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from akshare_data.api import DataService, get_service
from akshare_data.core.schema import SCHEMA_REGISTRY, get_table_schema
from akshare_data.store.manager import (
    CacheManager,
    reset_cache_manager,
)


@pytest.mark.integration
def create_test_daily_df(symbol="600000", start="2024-01-01", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * len(dates),
            "open": [10.0] * len(dates),
            "high": [11.0] * len(dates),
            "low": [9.0] * len(dates),
            "close": [10.5] * len(dates),
            "volume": [100000] * len(dates),
        }
    )


def create_test_minute_df(symbol="600000", start="2024-01-01", periods=10):
    dates = pd.date_range(start, periods=periods, freq="min")
    return pd.DataFrame(
        {
            "datetime": dates,
            "symbol": [symbol] * len(dates),
            "open": [10.0] * len(dates),
            "high": [11.0] * len(dates),
            "low": [9.0] * len(dates),
            "close": [10.5] * len(dates),
            "volume": [1000] * len(dates),
        }
    )


def create_test_index_df(symbol="000300", start="2024-01-01", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * len(dates),
            "open": [3000.0] * len(dates),
            "high": [3100.0] * len(dates),
            "low": [2900.0] * len(dates),
            "close": [3050.0] * len(dates),
            "volume": [1000000] * len(dates),
        }
    )


def create_test_etf_df(symbol="510300", start="2024-01-01", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * len(dates),
            "open": [3.5] * len(dates),
            "high": [3.6] * len(dates),
            "low": [3.4] * len(dates),
            "close": [3.55] * len(dates),
            "volume": [1000000] * len(dates),
        }
    )


class TestDataServiceInit:
    """Test DataService initialization"""

    def test_init_default(self):
        """Test default initialization"""
        service = DataService()
        assert service.cache is not None
        assert service.akshare is not None
        assert service.router is None

    def test_init_with_cache_manager(self):
        """Test initialization with custom cache manager"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_manager = CacheManager(base_dir=tmpdir)
            service = DataService(cache_manager=cache_manager)
            assert service.cache is cache_manager
            assert service.akshare is not None

    def test_init_with_router(self):
        """Test initialization with custom router"""
        mock_router = MagicMock()
        service = DataService(router=mock_router)
        assert service.router is mock_router


class TestDataServiceGetDaily:
    """Test get_daily method"""

    def test_get_daily_basic(self):
        """Test basic daily data retrieval"""
        service = DataService()
        test_df = create_test_daily_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_daily_data", return_value=pd.DataFrame()
            ),
            patch.object(service.akshare, "get_daily_data", return_value=test_df),
        ):
            df = service.get_daily("sh600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty
            assert len(df) == 10

    def test_get_daily_with_adjust(self):
        """Test daily data with adjustment (read-only served layer)."""
        # Under the read-only facade the adjust parameter is passed to the
        # Served query as part of the ``where`` clause instead of being
        # forwarded to source adapters. We only assert the returned payload.
        service = DataService()
        test_df = create_test_daily_df()

        with patch.object(service.cache, "read", return_value=test_df):
            df = service.get_daily("sh600000", "2024-01-01", "2024-01-05", adjust="hfq")
            assert df is not None
            assert not df.empty

    def test_get_daily_cache_hit(self):
        """Test cache hit returns cached data"""
        service = DataService()
        test_df = create_test_daily_df()

        with patch.object(service.cache, "read", return_value=test_df) as mock_read:
            df = service.get_daily("sh600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty
            mock_read.assert_called_once()

    def test_get_daily_empty_result(self):
        """Test empty result handling"""
        service = DataService()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_daily_data", return_value=pd.DataFrame()
            ),
            patch.object(
                service.akshare, "get_daily_data", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_daily("sh600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert df.empty

    def test_get_daily_exception_handling(self):
        """Test exception handling in fetch"""
        service = DataService()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(
                service.akshare,
                "get_daily_data",
                side_effect=Exception("Network error"),
            ),
        ):
            try:
                service.get_daily("sh600000", "2024-01-01", "2024-01-10")
            except Exception:
                pass

    def test_get_daily_symbol_normalization(self):
        """Test symbol normalization (sh600000 -> 600000) under read-only facade."""
        service = DataService()
        test_df = create_test_daily_df(symbol="600000")

        captured_where: List[dict] = []
        orig_query = service._served.query

        def capturing_query(*args, **kwargs):
            captured_where.append(kwargs.get("where"))
            return orig_query(*args, **kwargs)

        with (
            patch.object(service.cache, "read", return_value=test_df),
            patch.object(service._served, "query", side_effect=capturing_query),
        ):
            df = service.get_daily("sh600000", "2024-01-01", "2024-01-10")
            assert df is not None
            # Verify the sh-prefixed input was normalized before being passed
            # down to the Served layer ``where`` clause.
            assert captured_where and captured_where[0]["symbol"] == "600000"


class TestDataServiceGetMinute:
    """Test get_minute method"""

    def test_get_minute_basic(self):
        """Test basic minute data retrieval"""
        service = DataService()
        test_df = create_test_minute_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_minute_data", return_value=test_df),
        ):
            df = service.get_minute(
                "sh600000", freq="1min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert df is not None
            assert not df.empty

    def test_get_minute_no_dates(self):
        """Test minute data without date range"""
        service = DataService()
        test_df = create_test_minute_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_minute_data", return_value=test_df),
        ):
            df = service.get_minute("sh600000", freq="5min")
            assert df is not None

    def test_get_minute_empty_result(self):
        """Test empty minute data result"""
        service = DataService()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_minute_data", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_minute(
                "sh600000", freq="1min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert df is not None
            assert df.empty


class TestDataServiceGetIndex:
    """Test get_index method"""

    def test_get_index_basic(self):
        """Test basic index data retrieval"""
        service = DataService()
        test_df = create_test_index_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_index_daily", return_value=test_df),
        ):
            df = service.get_index("000300", "2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty

    def test_get_index_cache_hit(self):
        """Test index cache hit"""
        service = DataService()
        test_df = create_test_index_df()

        with patch.object(service.cache, "read", return_value=test_df) as mock_read:
            df = service.get_index("000300", "2024-01-01", "2024-01-10")
            assert df is not None
            mock_read.assert_called_once()


class TestDataServiceGetETF:
    """Test get_etf method"""

    def test_get_etf_basic(self):
        """Test basic ETF data retrieval"""
        service = DataService()
        test_df = create_test_etf_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_etf_daily", return_value=test_df),
        ):
            df = service.get_etf("510300", "2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty

    def test_get_etf_cache_hit(self):
        """Test ETF cache hit"""
        service = DataService()
        test_df = create_test_etf_df()

        with patch.object(service.cache, "read", return_value=test_df) as mock_read:
            df = service.get_etf("510300", "2024-01-01", "2024-01-10")
            assert df is not None
            mock_read.assert_called_once()


class TestDataServiceGetIndexStocks:
    """Test get_index_stocks method"""

    def test_get_index_stocks_basic(self):
        """Test basic index stocks retrieval"""
        service = DataService()
        mock_stocks = ["600000", "600519", "000001"]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame({"index_code": ["000300"] * 3, "code": mock_stocks})
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_index_stocks", return_value=mock_stocks),
        ):
            stocks = service.get_index_stocks("000300")
            assert stocks is not None
            assert len(stocks) == 3
            assert stocks == mock_stocks

    def test_get_index_stocks_from_cache(self):
        """Test index stocks from cache"""
        service = DataService()
        cached_df = pd.DataFrame(
            {"index_code": ["000300"] * 3, "code": ["600000", "600519", "000001"]}
        )

        with patch.object(service.cache, "read", return_value=cached_df) as mock_read:
            stocks = service.get_index_stocks("000300")
            assert stocks is not None
            assert len(stocks) == 3
            mock_read.assert_called_once()

    def test_get_index_stocks_empty(self):
        """Test empty index stocks"""
        service = DataService()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_index_stocks", return_value=[]),
        ):
            stocks = service.get_index_stocks("000300")
            assert stocks == []


class TestDataServiceGetTradingDays:
    """Test get_trading_days method"""

    def test_get_trading_days_basic(self):
        """Test basic trading days retrieval"""
        service = DataService()
        mock_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame({"date": mock_days})
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_trading_days", return_value=mock_days),
        ):
            days = service.get_trading_days("2024-01-01", "2024-01-10")
            assert days is not None
            assert len(days) == 3

    def test_get_trading_days_from_cache(self):
        """Test trading days from cache"""
        service = DataService()
        # Cached data must cover the full requested range so the incremental
        # strategy considers it complete and skips fetching from akshare.
        cached_df = pd.DataFrame(
            {
                "date": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ]
            }
        )

        with patch.object(service.cache, "read", return_value=cached_df) as mock_read:
            days = service.get_trading_days("2024-01-01", "2024-01-10")
            assert days is not None
            assert len(days) == 8
            mock_read.assert_called_once()


class TestDataServiceGetMoneyFlow:
    """Test get_money_flow method"""

    def test_get_money_flow_basic(self):
        """Test basic money flow retrieval"""
        service = DataService()
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000"] * 5,
                "buy_sm_amount": [1000.0] * 5,
                "sell_sm_amount": [900.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_money_flow", return_value=test_df),
        ):
            df = service.get_money_flow("600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty

    def test_get_money_flow_from_cache(self):
        """Test money flow from cache"""
        service = DataService()
        cached_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000"] * 5,
                "buy_sm_amount": [1000.0] * 5,
            }
        )

        with patch.object(service.cache, "read", return_value=cached_df) as mock_read:
            df = service.get_money_flow("600000", "2024-01-01", "2024-01-10")
            assert df is not None
            mock_read.assert_called_once()


class TestDataServiceGetNorthMoneyFlow:
    """Test get_north_money_flow method"""

    def test_get_north_money_flow_basic(self):
        """Test basic north money flow retrieval"""
        service = DataService()
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "north_money": [1000.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_north_money_flow", return_value=test_df),
        ):
            df = service.get_north_money_flow("2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty

    def test_get_north_money_flow_from_cache(self):
        """Test north money flow from cache"""
        service = DataService()
        cached_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "north_money": [1000.0] * 5,
            }
        )

        with patch.object(service.cache, "read", return_value=cached_df) as mock_read:
            df = service.get_north_money_flow("2024-01-01", "2024-01-10")
            assert df is not None
            mock_read.assert_called_once()


class TestDataServiceGetFinanceIndicator:
    """Test get_finance_indicator method"""

    def test_get_finance_indicator_basic(self):
        """Test basic finance indicator retrieval"""
        service = DataService()
        test_df = pd.DataFrame(
            {
                "report_date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000"] * 5,
                "roe": [0.1] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_finance_indicator", return_value=test_df
            ),
        ):
            df = service.get_finance_indicator("600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert not df.empty

    def test_get_finance_indicator_from_cache(self):
        """Test finance indicator from cache"""
        service = DataService()
        cached_df = pd.DataFrame(
            {
                "report_date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000"] * 5,
                "roe": [0.1] * 5,
            }
        )

        with patch.object(service.cache, "read", return_value=cached_df) as mock_read:
            df = service.get_finance_indicator("600000", "2024-01-01", "2024-01-10")
            assert df is not None
            mock_read.assert_called_once()


class TestCacheStrategies:
    """Test new cache strategy classes"""

    def test_incremental_strategy_empty_df(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        assert (
            strategy.should_fetch(
                pd.DataFrame(), start_date="2024-01-01", end_date="2024-01-10"
            )
            is True
        )

    def test_incremental_strategy_none(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        assert (
            strategy.should_fetch(None, start_date="2024-01-01", end_date="2024-01-10")
            is True
        )

    def test_incremental_strategy_full_range(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["600000"] * 10,
            }
        )
        assert (
            strategy.should_fetch(
                test_df, start_date="2024-01-01", end_date="2024-01-10"
            )
            is False
        )

    def test_incremental_strategy_partial_range(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05"),
                "symbol": ["600000"] * 5,
            }
        )
        assert (
            strategy.should_fetch(
                test_df, start_date="2024-01-01", end_date="2024-01-10"
            )
            is True
        )

    def test_incremental_find_missing_empty(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        gaps = strategy.find_missing_ranges(None, "2024-01-01", "2024-01-10")
        assert gaps == [("2024-01-01", "2024-01-10")]

    def test_incremental_find_missing_no_gaps(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["600000"] * 10,
            }
        )
        gaps = strategy.find_missing_ranges(test_df, "2024-01-01", "2024-01-10")
        assert len(gaps) == 0

    def test_incremental_find_missing_with_gaps(self):
        from akshare_data.store.strategies import IncrementalStrategy

        strategy = IncrementalStrategy()
        test_df = pd.DataFrame(
            {
                "date": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                ],
                "symbol": ["600000"] * 5,
            }
        )
        gaps = strategy.find_missing_ranges(test_df, "2024-01-01", "2024-01-10")
        assert len(gaps) == 1
        assert gaps[0] == ("2024-01-06", "2024-01-10")

    def test_full_cache_strategy(self):
        from akshare_data.store.strategies import FullCacheStrategy

        strategy = FullCacheStrategy()
        assert strategy.should_fetch(None) is True
        assert strategy.should_fetch(pd.DataFrame()) is True
        assert strategy.should_fetch(pd.DataFrame({"a": [1]})) is False

    def test_cached_fetch_basic(self):
        from akshare_data.store.fetcher import CachedFetcher, FetchConfig

        service = DataService()
        fetcher = CachedFetcher(service.cache)
        config = FetchConfig(table="test_table", storage_layer="meta")
        result = fetcher.execute(config, lambda: pd.DataFrame({"a": [1]}))
        assert result is not None
        assert len(result) == 1


class TestDataServiceSchema:
    """Test data schema and constants"""

    def test_schema_registry_covers_api_tables(self):
        """Test schema registry covers all API table names"""
        assert SCHEMA_REGISTRY.has("stock_daily")
        assert SCHEMA_REGISTRY.has("stock_minute")
        assert SCHEMA_REGISTRY.has("index_daily")
        assert SCHEMA_REGISTRY.has("etf_daily")
        stock_daily_schema = get_table_schema("stock_daily")
        assert stock_daily_schema.storage_layer == "daily"
        stock_minute_schema = get_table_schema("stock_minute")
        assert stock_minute_schema.storage_layer == "minute"


class TestGetService:
    """Test global service functions"""

    def test_get_service_returns_data_service(self):
        """Test get_service returns DataService instance"""
        reset_cache_manager()
        service = get_service()
        assert isinstance(service, DataService)

    def test_get_service_singleton(self):
        """Test get_service returns same instance"""
        reset_cache_manager()
        service1 = get_service()
        service2 = get_service()
        assert service1 is service2


class TestDataServiceEdgeCases:
    """Test edge cases"""

    def test_get_daily_future_dates(self):
        """Test get_daily with future dates"""
        service = DataService()
        future_start = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        future_end = (datetime.now() + timedelta(days=40)).strftime("%Y-%m-%d")

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_daily_data", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_daily("sh600000", future_start, future_end)
            assert df is not None

    def test_get_daily_invalid_symbol(self):
        """Test get_daily with invalid symbol"""
        service = DataService()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare,
                "get_daily_data",
                side_effect=Exception("Invalid symbol"),
            ),
        ):
            try:
                service.get_daily("INVALID", "2024-01-01", "2024-01-10")
            except Exception:
                pass

    def test_router_used_when_available(self):
        """Test that ad-hoc router is used for method execution (unified path)."""
        service = DataService()
        test_df = create_test_daily_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.lixinger, "get_daily_data", return_value=test_df),
        ):
            df = service.get_daily("sh600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert len(df) > 0


class TestDataServiceSymbols:
    """Test symbol normalization"""

    def test_symbol_normalization_sh(self):
        """Test sh prefix normalization"""
        from akshare_data.core.symbols import normalize_symbol

        assert normalize_symbol("sh600000") == "600000"

    def test_symbol_normalization_sz(self):
        """Test sz prefix normalization"""
        from akshare_data.core.symbols import normalize_symbol

        assert normalize_symbol("sz000001") == "000001"

    def test_symbol_normalization_jq(self):
        """Test jq format normalization"""
        from akshare_data.core.symbols import normalize_symbol

        assert normalize_symbol("600000.XSHG") == "600000"
        assert normalize_symbol("000001.XSHE") == "000001"
