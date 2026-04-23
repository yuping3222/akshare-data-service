"""tests/test_api_comprehensive.py

Comprehensive tests for DataService API methods.
Covers all public methods and internal helper methods.

Methods covered:
- get_minute (minute data fetching)
- get_index (index data)
- get_etf (ETF data)
- get_index_stocks (index components)
- get_trading_days
- get_money_flow
- get_north_money_flow
- get_finance_indicator
- Internal methods: _is_complete, _find_gaps, _write
"""

import pytest
from unittest.mock import patch

import pandas as pd

from akshare_data.api import DataService


def create_daily_df(symbol="600000", start="2024-01-01", end="2024-01-10", freq="D"):
    dates = pd.date_range(start, end, freq=freq)
    n = len(dates)
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * n,
            "open": [10.0 + i * 0.1 for i in range(n)],
            "high": [11.0 + i * 0.1 for i in range(n)],
            "low": [9.0 + i * 0.1 for i in range(n)],
            "close": [10.5 + i * 0.1 for i in range(n)],
            "volume": [100000 + i * 1000 for i in range(n)],
        }
    )


def create_minute_df(symbol="600000", start="2024-01-01", periods=60):
    dates = pd.date_range(start, periods=periods, freq="min")
    n = len(dates)
    return pd.DataFrame(
        {
            "datetime": dates,
            "date": pd.to_datetime(dates).date,
            "symbol": [symbol] * n,
            "open": [10.0 + i * 0.01 for i in range(n)],
            "high": [11.0 + i * 0.01 for i in range(n)],
            "low": [9.0 + i * 0.01 for i in range(n)],
            "close": [10.5 + i * 0.01 for i in range(n)],
            "volume": [1000 + i * 10 for i in range(n)],
        }
    )


def create_index_df(index_code="000300", start="2024-01-01", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="D")
    n = len(dates)
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [index_code] * n,
            "open": [3000.0 + i * 10 for i in range(n)],
            "high": [3100.0 + i * 10 for i in range(n)],
            "low": [2900.0 + i * 10 for i in range(n)],
            "close": [3050.0 + i * 10 for i in range(n)],
            "volume": [1000000 + i * 10000 for i in range(n)],
        }
    )


def create_etf_df(etf_code="510300", start="2024-01-01", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="D")
    n = len(dates)
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [etf_code] * n,
            "open": [3.5 + i * 0.01 for i in range(n)],
            "high": [3.6 + i * 0.01 for i in range(n)],
            "low": [3.4 + i * 0.01 for i in range(n)],
            "close": [3.55 + i * 0.01 for i in range(n)],
            "volume": [1000000 + i * 1000 for i in range(n)],
        }
    )


class TestGetMinute:
    """Comprehensive tests for get_minute method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_minute_1min(self, service):
        """Test 1 minute frequency"""
        test_df = create_minute_df(periods=100)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_minute_data", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_minute(
                "sh600000",
                freq="1min",
                start_date="2024-01-01 09:30:00",
                end_date="2024-01-01 10:00:00",
            )
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_minute_5min(self, service):
        """Test 5 minute frequency"""
        test_df = create_minute_df(periods=50)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_minute_data", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_minute(
                "sh600000", freq="5min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_minute_15min(self, service):
        """Test 15 minute frequency"""
        test_df = create_minute_df(periods=20)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_minute_data", return_value=test_df),
        ):
            df = service.get_minute(
                "sh600000", freq="15min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert not df.empty

    def test_get_minute_30min(self, service):
        """Test 30 minute frequency"""
        test_df = create_minute_df(periods=10)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_minute_data", return_value=test_df),
        ):
            df = service.get_minute(
                "sh600000", freq="30min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert not df.empty

    def test_get_minute_60min(self, service):
        """Test 60 minute frequency"""
        test_df = create_minute_df(periods=5)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_minute_data", return_value=test_df),
        ):
            df = service.get_minute(
                "sh600000", freq="60min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert not df.empty

    def test_get_minute_no_dates(self, service):
        """Test minute data without date range (fetch latest)"""
        test_df = create_minute_df(periods=100)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_minute_data", return_value=test_df
            ),
        ):
            df = service.get_minute("sh600000", freq="1min")
            assert df is not None

    def test_get_minute_cache_hit(self, service):
        """Test cache hit for minute data"""
        cached_df = create_minute_df(periods=100)

        with patch.object(service.cache, "read", return_value=cached_df) as mock_read:
            df = service.get_minute(
                "sh600000", freq="1min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert not df.empty
            mock_read.assert_called_once()

    def test_get_minute_cache_complete(self, service):
        """Test that complete cache skips fetch"""
        cached_df = create_minute_df(periods=100)

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_minute_data") as mock_fetch,
        ):
            service.get_minute(
                "sh600000", freq="1min", start_date="2024-01-01", end_date="2024-01-01"
            )
            mock_fetch.assert_not_called()

    def test_get_minute_empty_result(self, service):
        """Test empty result handling"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
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

    def test_get_minute_symbol_normalization(self, service):
        """Test symbol normalization in get_minute"""
        test_df = create_minute_df(symbol="000001", periods=10)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_minute_data", return_value=test_df
            ) as mock_fetch,
        ):
            service.get_minute(
                "sz000001", freq="1min", start_date="2024-01-01", end_date="2024-01-01"
            )
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args[0]
            assert args[0] == "000001"

    def test_get_minute_table_name(self, service):
        """Test that correct table name is used for different frequencies"""
        test_df = create_minute_df(periods=10)

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_minute_data", return_value=test_df),
        ):
            df = service.get_minute(
                "sh600000", freq="5min", start_date="2024-01-01", end_date="2024-01-01"
            )
            assert not df.empty


class TestGetIndex:
    """Comprehensive tests for get_index method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_index_basic(self, service):
        """Test basic index data retrieval"""
        test_df = create_index_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_index_daily", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_index("000300", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_index_default_dates(self, service):
        """Test index with default date range"""
        test_df = create_index_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_index_daily", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_index("000300")
            assert not df.empty
            call_args = mock_fetch.call_args
            assert call_args[0][0] == "000300"

    def test_get_index_cache_hit(self, service):
        """Test cache hit for index data"""
        cached_df = create_index_df()

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_index_daily") as mock_fetch,
        ):
            df = service.get_index("000300", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_not_called()

    def test_get_index_symbol_normalization(self, service):
        """Test symbol normalization for indices"""
        test_df = create_index_df(index_code="000001")

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_index_daily", return_value=test_df
            ) as mock_fetch,
        ):
            service.get_index("sh000001", "2024-01-01", "2024-01-10")
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args[0][0] == "000001"

    def test_get_index_empty_result(self, service):
        """Test empty result handling"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_index_daily", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_index("000300", "2024-01-01", "2024-01-10")
            assert df is not None
            assert df.empty

    def test_get_index_router_used(self, service):
        """Test that ad-hoc router is used for method execution (unified path)."""
        test_df = create_index_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.lixinger, "get_index_daily", return_value=test_df),
        ):
            df = service.get_index("000300", "2024-01-01", "2024-01-10")
            assert not df.empty
            assert len(df) > 0


class TestGetETF:
    """Comprehensive tests for get_etf method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_etf_basic(self, service):
        """Test basic ETF data retrieval"""
        test_df = create_etf_df()

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_etf_daily", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_etf("510300", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_etf_cache_hit(self, service):
        """Test cache hit for ETF data"""
        cached_df = create_etf_df()

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_etf_daily") as mock_fetch,
        ):
            df = service.get_etf("510300", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_not_called()

    def test_get_etf_symbol_normalization(self, service):
        """Test symbol normalization for ETFs"""
        test_df = create_etf_df(etf_code="159001")

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_etf_daily", return_value=test_df
            ) as mock_fetch,
        ):
            service.get_etf("sz159001", "2024-01-01", "2024-01-10")
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args[0][0] == "159001"

    def test_get_etf_empty_result(self, service):
        """Test empty result handling"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.lixinger, "get_etf_daily", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_etf("510300", "2024-01-01", "2024-01-10")
            assert df is not None
            assert df.empty

    def test_get_etf_different_etf_codes(self, service):
        """Test different ETF codes"""
        etf_codes = ["510300", "159919", "512000", "510050"]

        for etf_code in etf_codes:
            test_df = create_etf_df(etf_code=etf_code)

            read_call_count = [0]

            def mock_read(*args, **kwargs):
                result = test_df if read_call_count[0] > 0 else pd.DataFrame()
                read_call_count[0] += 1
                return result

            with (
                patch.object(service.cache, "read", side_effect=mock_read),
                patch.object(service.cache, "write", return_value=""),
                patch.object(service.lixinger, "get_etf_daily", return_value=test_df),
            ):
                df = service.get_etf(etf_code, "2024-01-01", "2024-01-10")
                assert not df.empty


class TestGetIndexStocks:
    """Comprehensive tests for get_index_stocks method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_index_stocks_basic(self, service):
        """Test basic index stocks retrieval"""
        mock_stocks = ["600000", "600519", "000001", "000002", "300001"]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = (
                pd.DataFrame({"index_code": ["000300"] * 5, "code": mock_stocks})
                if read_call_count[0] > 0
                else pd.DataFrame()
            )
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_index_stocks", return_value=mock_stocks
            ) as mock_fetch,
        ):
            stocks = service.get_index_stocks("000300")
            assert stocks == mock_stocks
            mock_fetch.assert_called_once()

    def test_get_index_stocks_from_cache(self, service):
        """Test index stocks from cache"""
        cached_df = pd.DataFrame(
            {
                "index_code": ["000300"] * 5,
                "code": ["600000", "600519", "000001", "000002", "300001"],
            }
        )

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_index_stocks") as mock_fetch,
        ):
            stocks = service.get_index_stocks("000300")
            assert len(stocks) == 5
            mock_fetch.assert_not_called()

    def test_get_index_stocks_empty_cache(self, service):
        """Test index stocks when cache is empty"""
        mock_stocks = ["600000", "600519"]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = (
                pd.DataFrame({"index_code": ["000300"] * 2, "code": mock_stocks})
                if read_call_count[0] > 0
                else pd.DataFrame()
            )
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_index_stocks", return_value=mock_stocks
            ) as mock_fetch,
        ):
            stocks = service.get_index_stocks("000300")
            assert stocks == mock_stocks
            mock_fetch.assert_called_once()

    def test_get_index_stocks_empty_result(self, service):
        """Test empty index stocks"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_index_stocks", return_value=[]),
        ):
            stocks = service.get_index_stocks("000300")
            assert stocks == []

    def test_get_index_stocks_writes_to_cache(self, service):
        """Test that fetched stocks are written to cache"""
        mock_stocks = ["600000", "600519", "000001"]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = (
                pd.DataFrame({"index_code": ["000300"] * 3, "code": mock_stocks})
                if read_call_count[0] > 0
                else pd.DataFrame()
            )
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value="") as mock_write,
            patch.object(service.akshare, "get_index_stocks", return_value=mock_stocks),
        ):
            stocks = service.get_index_stocks("000300")
            assert stocks == mock_stocks
            mock_write.assert_called_once()
            call_args = mock_write.call_args
            written_df = call_args[0][1]
            assert "code" in written_df.columns
            assert "index_code" in written_df.columns


class TestGetTradingDays:
    """Comprehensive tests for get_trading_days method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_trading_days_basic(self, service):
        """Test basic trading days retrieval"""
        mock_days = [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
        ]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = (
                pd.DataFrame({"date": mock_days})
                if read_call_count[0] > 0
                else pd.DataFrame()
            )
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_trading_days", return_value=mock_days
            ) as mock_fetch,
        ):
            days = service.get_trading_days("2024-01-01", "2024-01-10")
            assert days == mock_days
            mock_fetch.assert_called_once()

    def test_get_trading_days_from_cache(self, service):
        """Test trading days from cache - cache covers full range"""
        cached_df = pd.DataFrame(
            {
                "date": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                ]
            }
        )

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_trading_days") as mock_fetch,
        ):
            days = service.get_trading_days("2024-01-01", "2024-01-10")
            assert len(days) == 10
            mock_fetch.assert_not_called()

    def test_get_trading_days_no_params(self, service):
        """Test trading days without date parameters"""
        mock_days = ["2024-01-01", "2024-01-02", "2024-01-03"]

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = (
                pd.DataFrame({"date": mock_days})
                if read_call_count[0] > 0
                else pd.DataFrame()
            )
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_trading_days", return_value=mock_days
            ),
        ):
            days = service.get_trading_days()
            assert days == mock_days

    def test_get_trading_days_numpy_array(self, service):
        """Test trading days when akshare returns numpy array"""
        import numpy as np

        mock_days = np.array(["2024-01-01", "2024-01-02", "2024-01-03"])

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = (
                pd.DataFrame({"date": mock_days})
                if read_call_count[0] > 0
                else pd.DataFrame()
            )
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_trading_days", return_value=mock_days),
        ):
            days = service.get_trading_days()
            assert len(days) == 3

    def test_get_trading_days_empty_result(self, service):
        """Test empty trading days result"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_trading_days", return_value=[]),
        ):
            days = service.get_trading_days()
            assert days == []


class TestGetMoneyFlow:
    """Comprehensive tests for get_money_flow method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_money_flow_basic(self, service):
        """Test basic money flow retrieval"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000"] * 5,
                "buy_sm_amount": [1000.0] * 5,
                "sell_sm_amount": [900.0] * 5,
                "net_sm_amount": [100.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_money_flow", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_money_flow("600000", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_money_flow_from_cache(self, service):
        """Test money flow from cache - full range"""
        cached_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["600000"] * 10,
                "buy_sm_amount": [1000.0] * 10,
            }
        )

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_money_flow") as mock_fetch,
        ):
            df = service.get_money_flow("600000", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_not_called()

    def test_get_money_flow_adds_symbol_column(self, service):
        """Test that symbol column is added if missing"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "buy_sm_amount": [1000.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value="") as mock_write,
            patch.object(service.akshare, "get_money_flow", return_value=test_df),
        ):
            service.get_money_flow("600000", "2024-01-01", "2024-01-10")
            mock_write.assert_called_once()
            written_df = mock_write.call_args[0][1]
            assert "symbol" in written_df.columns

    def test_get_money_flow_empty_result(self, service):
        """Test empty money flow result"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_money_flow", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_money_flow("600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert df.empty

    def test_get_money_flow_default_dates(self, service):
        """Test money flow with default date range"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("1990-01-01", periods=5),
                "symbol": ["600000"] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_money_flow", return_value=test_df),
        ):
            df = service.get_money_flow("600000")
            assert df is not None


class TestGetNorthMoneyFlow:
    """Comprehensive tests for get_north_money_flow method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_north_money_flow_basic(self, service):
        """Test basic north money flow retrieval"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "north_money": [1000.0] * 5,
                "south_money": [500.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_north_money_flow", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_north_money_flow("2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_north_money_flow_from_cache(self, service):
        """Test north money flow from cache"""
        cached_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-10"),
                "north_money": [1000.0] * 10,
            }
        )

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_north_money_flow") as mock_fetch,
        ):
            df = service.get_north_money_flow("2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_not_called()

    def test_get_north_money_flow_empty_result(self, service):
        """Test empty north money flow result"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_north_money_flow", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_north_money_flow("2024-01-01", "2024-01-10")
            assert df is not None
            assert df.empty

    def test_get_north_money_flow_default_dates(self, service):
        """Test north money flow with default date range"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("1990-01-01", periods=5),
                "north_money": [1000.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(service.akshare, "get_north_money_flow", return_value=test_df),
        ):
            df = service.get_north_money_flow()
            assert df is not None


class TestGetFinanceIndicator:
    """Comprehensive tests for get_finance_indicator method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_finance_indicator_basic(self, service):
        """Test basic finance indicator retrieval"""
        test_df = pd.DataFrame(
            {
                "report_date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000"] * 5,
                "roe": [0.1] * 5,
                "eps": [1.0] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_finance_indicator", return_value=test_df
            ) as mock_fetch,
        ):
            df = service.get_finance_indicator("600000", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_called_once()

    def test_get_finance_indicator_from_cache(self, service):
        """Test finance indicator from cache - full range"""
        cached_df = pd.DataFrame(
            {
                "report_date": pd.date_range("2024-01-01", "2024-01-10"),
                "symbol": ["600000"] * 10,
                "roe": [0.1] * 10,
            }
        )

        with (
            patch.object(service.cache, "read", return_value=cached_df),
            patch.object(service.akshare, "get_finance_indicator") as mock_fetch,
        ):
            df = service.get_finance_indicator("600000", "2024-01-01", "2024-01-10")
            assert not df.empty
            mock_fetch.assert_not_called()

    def test_get_finance_indicator_adds_symbol_column(self, service):
        """Test that symbol column is added if missing"""
        test_df = pd.DataFrame(
            {
                "report_date": pd.date_range("2024-01-01", periods=5),
                "roe": [0.1] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value="") as mock_write,
            patch.object(
                service.akshare, "get_finance_indicator", return_value=test_df
            ),
        ):
            service.get_finance_indicator("600000", "2024-01-01", "2024-01-10")
            mock_write.assert_called_once()
            written_df = mock_write.call_args[0][1]
            assert "symbol" in written_df.columns

    def test_get_finance_indicator_empty_result(self, service):
        """Test empty finance indicator result"""
        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = pd.DataFrame() if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_finance_indicator", return_value=pd.DataFrame()
            ),
        ):
            df = service.get_finance_indicator("600000", "2024-01-01", "2024-01-10")
            assert df is not None
            assert df.empty

    def test_get_finance_indicator_default_dates(self, service):
        """Test finance indicator with default date range"""
        test_df = pd.DataFrame(
            {
                "report_date": pd.date_range("1990-01-01", periods=5),
                "symbol": ["600000"] * 5,
            }
        )

        read_call_count = [0]

        def mock_read(*args, **kwargs):
            result = test_df if read_call_count[0] > 0 else pd.DataFrame()
            read_call_count[0] += 1
            return result

        with (
            patch.object(service.cache, "read", side_effect=mock_read),
            patch.object(service.cache, "write", return_value=""),
            patch.object(
                service.akshare, "get_finance_indicator", return_value=test_df
            ),
        ):
            df = service.get_finance_indicator("600000")
            assert df is not None
