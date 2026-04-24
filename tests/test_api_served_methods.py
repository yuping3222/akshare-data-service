"""tests/test_api_served_methods.py

Tests for DataService public API methods:
- ServedDataService core methods (query, backfill, table info)
- DataService facade methods (index components, securities, futures, etc.)
"""

import pytest
from unittest.mock import patch

import pandas as pd

from akshare_data.api import DataService
from akshare_data.service.data_service import QueryResult
from akshare_data.service.missing_data_policy import MissingAction


def create_daily_df(symbol="600000", start="2024-01-01", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="D")
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


def create_index_components_df(index_code="000300"):
    return pd.DataFrame(
        {
            "index_code": [index_code] * 5,
            "symbol": ["600000", "600519", "000001", "000002", "300001"],
            "name": ["浦发银行", "贵州茅台", "平安银行", "万科A", "特锐德"],
            "weight": [0.1, 0.08, 0.05, 0.03, 0.02],
        }
    )


@pytest.mark.unit
class TestServedDataServiceQuery:
    """Tests for ServedDataService.query method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_query_basic(self, service):
        """Test basic query with table name only"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            result = service.query("stock_daily")
            assert result.has_data is True
            assert not result.data.empty
            mock_query.assert_called_once_with(
                table="stock_daily",
                where=None,
                columns=None,
                order_by=None,
                limit=None,
                partition_by=None,
                partition_value=None,
                version=None,
            )

    def test_query_with_where(self, service):
        """Test query with where clause"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)
        where = {"date": ("2024-01-01", "2024-01-10"), "symbol": "600000"}

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            result = service.query("stock_daily", where=where)
            assert result.has_data is True
            mock_query.assert_called_once()
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["where"] == where

    def test_query_with_columns(self, service):
        """Test query with specific columns"""
        test_df = pd.DataFrame({"date": ["2024-01-01"], "close": [10.5]})
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)
        columns = ["date", "close"]

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            result = service.query("stock_daily", columns=columns)
            assert "close" in result.data.columns
            mock_query.assert_called_once()

    def test_query_with_limit(self, service):
        """Test query with limit"""
        test_df = create_daily_df()
        mock_result = QueryResult(
            data=test_df.head(5), table="stock_daily", has_data=True
        )

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            result = service.query("stock_daily", limit=5)
            assert len(result.data) <= 5
            mock_query.assert_called_once()

    def test_query_with_order_by(self, service):
        """Test query with order_by"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)
        order_by = ["date"]

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.query("stock_daily", order_by=order_by)
            mock_query.assert_called_once()
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["order_by"] == order_by

    def test_query_with_partition(self, service):
        """Test query with partition_by and partition_value"""
        test_df = create_daily_df(symbol="600000")
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            result = service.query(
                "stock_daily",
                partition_by="symbol",
                partition_value="600000",
            )
            assert result.has_data is True
            mock_query.assert_called_once()

    def test_query_empty_result(self, service):
        """Test query returns empty result"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="stock_daily", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            result = service.query("stock_daily")
            assert result.has_data is False
            assert result.data.empty

    def test_query_result_to_dict(self, service):
        """Test QueryResult.to_dict method"""
        test_df = create_daily_df()
        mock_result = QueryResult(
            data=test_df, table="stock_daily", has_data=True, version="v1"
        )

        with patch.object(service._served, "query", return_value=mock_result):
            result = service.query("stock_daily")
            d = result.to_dict()
            assert d["table"] == "stock_daily"
            assert d["has_data"] is True
            assert d["row_count"] == 10
            assert d["version"] == "v1"


@pytest.mark.unit
class TestServedDataServiceQueryDaily:
    """Tests for ServedDataService.query_daily method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_query_daily_basic(self, service):
        """Test basic daily time series query"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(
            service._served, "query_daily", return_value=mock_result
        ) as mock_query:
            result = service._served.query_daily(
                "stock_daily", "600000", "2024-01-01", "2024-01-10"
            )
            assert result.has_data is True
            mock_query.assert_called_once()

    def test_query_daily_with_symbol_prefix(self, service):
        """Test query_daily normalizes symbol internally"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            result = service._served.query_daily(
                "stock_daily", "sh600000", "2024-01-01", "2024-01-10"
            )
            assert result.has_data is True
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["where"]["symbol"] == "600000"

    def test_query_daily_empty_result(self, service):
        """Test query_daily returns empty result"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="stock_daily", has_data=False
        )

        with patch.object(service._served, "query_daily", return_value=mock_result):
            result = service._served.query_daily(
                "stock_daily", "600000", "2024-01-01", "2024-01-10"
            )
            assert result.has_data is False


@pytest.mark.unit
class TestBackfillMethods:
    """Tests for backfill request methods"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_request_backfill_basic(self, service):
        """Test basic backfill request"""
        mock_request_id = "bf-stock_daily-000001"

        with patch.object(
            service._served._backfill_registry,
            "submit",
            return_value=mock_request_id,
        ) as mock_submit:
            request_id = service.request_backfill("stock_daily")
            assert request_id == mock_request_id
            mock_submit.assert_called_once_with("stock_daily", {}, priority="normal")

    def test_request_backfill_with_params(self, service):
        """Test backfill request with params"""
        mock_request_id = "bf-stock_daily-000001"
        params = {
            "symbol": "600000",
            "start_date": "2024-01-01",
            "end_date": "2024-01-10",
        }

        with patch.object(
            service._served._backfill_registry,
            "submit",
            return_value=mock_request_id,
        ):
            request_id = service.request_backfill("stock_daily", params=params)
            assert request_id == mock_request_id

    def test_request_backfill_with_priority(self, service):
        """Test backfill request with priority"""
        mock_request_id = "bf-stock_daily-000001"

        with patch.object(
            service._served._backfill_registry,
            "submit",
            return_value=mock_request_id,
        ) as mock_submit:
            service.request_backfill("stock_daily", priority="high")
            mock_submit.assert_called_once_with("stock_daily", {}, priority="high")

    def test_get_backfill_status_found(self, service):
        """Test get_backfill_status returns found request"""
        mock_request = {
            "request_id": "bf-stock_daily-000001",
            "table": "stock_daily",
            "params": {},
            "priority": "normal",
            "status": "pending",
        }
        service._served._backfill_registry._requests = [mock_request]

        result = service.get_backfill_status("bf-stock_daily-000001")
        assert result is not None
        assert result["request_id"] == "bf-stock_daily-000001"
        assert result["status"] == "pending"

    def test_get_backfill_status_not_found(self, service):
        """Test get_backfill_status returns None for unknown request"""
        service._served._backfill_registry._requests = []

        result = service.get_backfill_status("unknown-id")
        assert result is None

    def test_list_pending_backfills(self, service):
        """Test list_pending_backfills returns pending requests"""
        mock_requests = [
            {"request_id": "bf-1", "table": "stock_daily", "status": "pending"},
            {"request_id": "bf-2", "table": "index_daily", "status": "pending"},
            {"request_id": "bf-3", "table": "etf_daily", "status": "completed"},
        ]
        service._served._backfill_registry._requests = mock_requests

        pending = service.list_pending_backfills()
        assert len(pending) == 2
        assert all(r["status"] == "pending" for r in pending)

    def test_list_pending_backfills_empty(self, service):
        """Test list_pending_backfills returns empty list"""
        service._served._backfill_registry._requests = []

        pending = service.list_pending_backfills()
        assert pending == []


@pytest.mark.unit
class TestMissingActionMethods:
    """Tests for missing action and table info methods"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_set_missing_action(self, service):
        """Test set_missing_action configures table action"""
        service.set_missing_action("stock_daily", MissingAction.REQUEST_BACKFILL)

        action = service._served._missing_policy.resolve_action("stock_daily")
        assert action == MissingAction.REQUEST_BACKFILL

    def test_set_missing_action_multiple_tables(self, service):
        """Test set_missing_action for multiple tables"""
        service.set_missing_action("stock_daily", MissingAction.REQUEST_BACKFILL)
        service.set_missing_action("index_daily", MissingAction.RAISE_ERROR)

        assert (
            service._served._missing_policy.resolve_action("stock_daily")
            == MissingAction.REQUEST_BACKFILL
        )
        assert (
            service._served._missing_policy.resolve_action("index_daily")
            == MissingAction.RAISE_ERROR
        )

    def test_get_table_info_success(self, service):
        """Test get_table_info returns table metadata"""
        mock_info = {"name": "stock_daily", "row_count": 100, "storage_layer": "daily"}

        with patch.object(
            service._served._reader,
            "get_table_info",
            return_value=mock_info,
        ):
            info = service.get_table_info("stock_daily")
            assert info["name"] == "stock_daily"

    def test_get_table_info_with_error(self, service):
        """Test get_table_info handles errors"""
        mock_info = {"name": "unknown_table", "error": "Table not found"}

        with patch.object(
            service._served._reader,
            "get_table_info",
            return_value=mock_info,
        ):
            info = service.get_table_info("unknown_table")
            assert "error" in info

    def test_list_tables(self, service):
        """Test list_tables returns available tables"""
        mock_tables = ["stock_daily", "index_daily", "etf_daily", "securities"]

        with patch.object(
            service._served._reader,
            "list_tables",
            return_value=mock_tables,
        ):
            tables = service.list_tables()
            assert "stock_daily" in tables
            assert len(tables) == 4

    def test_list_tables_empty(self, service):
        """Test list_tables returns empty list on error"""
        with patch.object(service._served._reader, "list_tables", return_value=[]):
            tables = service.list_tables()
            assert tables == []

    def test_table_exists_true(self, service):
        """Test table_exists returns True"""
        with patch.object(service._served._reader, "exists", return_value=True):
            exists = service.table_exists("stock_daily")
            assert exists is True

    def test_table_exists_false(self, service):
        """Test table_exists returns False"""
        with patch.object(service._served._reader, "exists", return_value=False):
            exists = service.table_exists("unknown_table")
            assert exists is False

    def test_has_data_for_range_true(self, service):
        """Test has_data_for_range returns True"""
        with patch.object(
            service._served._reader,
            "has_date_range",
            return_value=True,
        ) as mock_has_range:
            result = service.has_data_for_range(
                "stock_daily", "600000", "2024-01-01", "2024-01-10"
            )
            assert result is True
            mock_has_range.assert_called_once()

    def test_has_data_for_range_false(self, service):
        """Test has_data_for_range returns False"""
        with patch.object(
            service._served._reader, "has_date_range", return_value=False
        ):
            result = service.has_data_for_range(
                "stock_daily", "600000", "2024-01-01", "2024-01-10"
            )
            assert result is False

    def test_has_data_for_range_with_prefix(self, service):
        """Test has_data_for_range normalizes symbol"""
        with patch.object(
            service._served._reader,
            "has_date_range",
            return_value=True,
        ) as mock_has_range:
            service.has_data_for_range(
                "stock_daily", "sh600000", "2024-01-01", "2024-01-10"
            )
            call_kwargs = mock_has_range.call_args[1]
            assert call_kwargs["where"]["symbol"] == "600000"


@pytest.mark.unit
class TestGetIndexComponents:
    """Tests for get_index_components facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_index_components_basic(self, service):
        """Test basic index components retrieval"""
        test_df = create_index_components_df()
        mock_result = QueryResult(data=test_df, table="index_components", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_index_components("000300")
            assert not df.empty
            assert "code" in df.columns
            assert "weight" in df.columns
            mock_query.assert_called_once()

    def test_get_index_components_with_prefix(self, service):
        """Test index components with symbol prefix"""
        test_df = create_index_components_df()
        mock_result = QueryResult(data=test_df, table="index_components", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.get_index_components("sh000300")
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["where"] == {"index_code": "000300"}

    def test_get_index_components_empty(self, service):
        """Test index components returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="index_components", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_index_components("000300")
            assert df.empty

    def test_get_index_components_default_weights(self, service):
        """Test get_index_components with default include_weights"""
        test_df = create_index_components_df()
        mock_result = QueryResult(data=test_df, table="index_components", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_index_components("000300", include_weights=True)
            assert "weight" in df.columns

    def test_get_index_components_without_weights(self, service):
        """Test get_index_components drops weight when requested"""
        test_df = create_index_components_df()
        mock_result = QueryResult(data=test_df, table="index_components", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_index_components("000300", include_weights=False)
            assert "weight" not in df.columns
            assert "stock_name" in df.columns


@pytest.mark.unit
class TestGetIndexStocks:
    """Tests for get_index_stocks facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_index_stocks_reads_symbol_column(self, service):
        test_df = create_index_components_df()
        mock_result = QueryResult(data=test_df, table="index_components", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            stocks = service.get_index_stocks("000300")
            assert stocks == ["600000", "600519", "000001", "000002", "300001"]


@pytest.mark.unit
class TestGetSecuritiesList:
    """Tests for get_securities_list facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_securities_list_stock(self, service):
        """Test get_securities_list for stocks"""
        test_df = pd.DataFrame(
            {
                "symbol": ["600000", "600519", "000001"],
                "name": ["浦发银行", "茅台", "平安银行"],
                "security_type": ["stock"] * 3,
            }
        )
        mock_result = QueryResult(data=test_df, table="securities", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_securities_list(security_type="stock")
            assert not df.empty
            assert list(df["code"]) == ["600000", "600519", "000001"]
            assert list(df["display_name"]) == ["浦发银行", "茅台", "平安银行"]
            assert "security_type" in df.columns
            mock_query.assert_called_once_with(table="securities")

    def test_get_securities_list_etf(self, service):
        """Test get_securities_list for ETFs"""
        test_df = pd.DataFrame(
            {
                "symbol": ["510300", "159919"],
                "name": ["300ETF", "创业板ETF"],
                "security_type": ["etf"] * 2,
            }
        )
        mock_result = QueryResult(data=test_df, table="securities", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_securities_list(security_type="etf")
            assert list(df["code"]) == ["510300", "159919"]
            assert list(df["security_type"]) == ["etf", "etf"]
            mock_query.assert_called_once_with(table="securities")

    def test_get_securities_list_empty(self, service):
        """Test get_securities_list returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="securities", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_securities_list(security_type="bond")
            assert df.empty


@pytest.mark.unit
class TestGetIndustryStocks:
    """Tests for get_industry_stocks facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_industry_stocks_basic(self, service):
        """Test basic industry stocks retrieval"""
        test_df = pd.DataFrame(
            {
                "industry_code": ["BK0001"] * 3,
                "symbol": ["600000", "600519", "000001"],
            }
        )
        mock_result = QueryResult(
            data=test_df, table="industry_components", has_data=True
        )

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            stocks = service.get_industry_stocks("BK0001")
            assert len(stocks) == 3
            assert "600000" in stocks
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["partition_value"] == "BK0001"

    def test_get_industry_stocks_empty(self, service):
        """Test industry stocks returns empty list"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="industry_components", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            stocks = service.get_industry_stocks("BK0001")
            assert stocks == []

    def test_get_industry_stocks_missing_code_column(self, service):
        """Test industry stocks returns empty when symbol/code missing"""
        test_df = pd.DataFrame({"industry_code": ["BK0001"]})
        mock_result = QueryResult(
            data=test_df, table="industry_components", has_data=True
        )

        with patch.object(service._served, "query", return_value=mock_result):
            stocks = service.get_industry_stocks("BK0001")
            assert stocks == []


@pytest.mark.unit
class TestGetSuspendedStocks:
    """Tests for get_suspended_stocks facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_suspended_stocks_basic(self, service):
        """Test basic suspended stocks retrieval"""
        test_df = pd.DataFrame(
            {
                "symbol": ["600000", "000001"],
                "suspend_date": ["2024-01-01", "2024-01-02"],
                "reason": ["重大事项", "重组"],
            }
        )
        mock_result = QueryResult(data=test_df, table="suspended_stocks", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_suspended_stocks()
            assert not df.empty
            mock_query.assert_called_once_with(table="suspended_stocks")

    def test_get_suspended_stocks_empty(self, service):
        """Test suspended stocks returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="suspended_stocks", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_suspended_stocks()
            assert df.empty


@pytest.mark.unit
class TestGetSTStocks:
    """Tests for get_st_stocks facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_st_stocks_basic(self, service):
        """Test basic ST stocks retrieval"""
        test_df = pd.DataFrame(
            {
                "symbol": ["600000", "000001"],
                "name": ["ST浦发", "*ST平安"],
                "st_type": ["ST", "*ST"],
                "st_date": ["2024-01-01", "2024-01-02"],
            }
        )
        mock_result = QueryResult(data=test_df, table="st_stocks", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_st_stocks()
            assert not df.empty
            assert list(df["code"]) == ["600000", "000001"]
            assert list(df["display_name"]) == ["ST浦发", "*ST平安"]
            mock_query.assert_called_once_with(table="st_stocks")

    def test_get_st_stocks_empty(self, service):
        """Test ST stocks returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="st_stocks", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_st_stocks()
            assert df.empty


@pytest.mark.unit
class TestGetSecurityInfo:
    """Tests for get_security_info facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_security_info_adds_legacy_keys(self, service):
        test_df = pd.DataFrame(
            {
                "symbol": ["600000"],
                "name": ["浦发银行"],
                "industry": ["银行"],
                "list_date": ["1999-11-10"],
            }
        )
        mock_result = QueryResult(data=test_df, table="company_info", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result) as mock_query:
            info = service.get_security_info("sh600000")
            assert info["code"] == "600000"
            assert info["display_name"] == "浦发银行"
            assert info["start_date"] == "1999-11-10"
            mock_query.assert_called_once_with(
                table="company_info",
                where={"symbol": "600000"},
            )


@pytest.mark.unit
class TestGetRestrictedReleaseCalendar:
    """Tests for get_restricted_release_calendar facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_restricted_release_calendar_basic(self, service):
        """Test basic restricted release calendar retrieval"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["600000", "000001", "600519", "000002", "300001"],
                "release_amount": [1000.0, 2000.0, 500.0, 800.0, 300.0],
            }
        )
        mock_result = QueryResult(
            data=test_df, table="restricted_release_calendar", has_data=True
        )

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_restricted_release_calendar("2024-01-01", "2024-01-05")
            assert not df.empty
            mock_query.assert_called_once()

    def test_get_restricted_release_calendar_no_dates(self, service):
        """Test restricted release calendar without date range"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10),
                "symbol": ["600000"] * 10,
            }
        )
        mock_result = QueryResult(
            data=test_df, table="restricted_release_calendar", has_data=True
        )

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.get_restricted_release_calendar()
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["where"] is None

    def test_get_restricted_release_calendar_empty(self, service):
        """Test restricted release calendar returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="restricted_release_calendar", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_restricted_release_calendar("2024-01-01", "2024-01-05")
            assert df.empty


@pytest.mark.unit
class TestGetStockHist:
    """Tests for get_stock_hist facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_stock_hist_daily(self, service):
        """Test get_stock_hist with daily period"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_stock_hist(
                "600000",
                period="daily",
                start_date="2024-01-01",
                end_date="2024-01-10",
                adjust="qfq",
            )
            assert df is not None

    def test_get_stock_hist_minute(self, service):
        """Test get_stock_hist with minute period"""
        test_df = pd.DataFrame(
            {
                "datetime": pd.date_range("2024-01-01 09:30", periods=60, freq="min"),
                "symbol": ["600000"] * 60,
                "close": [10.5] * 60,
            }
        )
        mock_result = QueryResult(data=test_df, table="stock_minute", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_stock_hist(
                "600000",
                period="5min",
                start_date="2024-01-01",
                end_date="2024-01-01",
            )
            assert df is not None

    def test_get_stock_hist_with_prefix(self, service):
        """Test get_stock_hist normalizes symbol"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.get_stock_hist(
                "sh600000",
                period="daily",
                start_date="2024-01-01",
                end_date="2024-01-10",
            )
            call_args = mock_query.call_args
            assert "600000" in str(call_args)

    def test_get_stock_hist_empty_adjust(self, service):
        """Test get_stock_hist with empty adjust defaults to qfq"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_stock_hist(
                "600000",
                period="daily",
                start_date="2024-01-01",
                end_date="2024-01-10",
                adjust="",
            )
            assert df is not None


@pytest.mark.unit
class TestGetSpotEm:
    """Tests for get_spot_em facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_spot_em_basic(self, service):
        """Test basic spot_em retrieval"""
        test_df = pd.DataFrame(
            {
                "symbol": ["600000", "000001", "600519"],
                "name": ["浦发银行", "平安银行", "茅台"],
                "price": [10.5, 15.0, 1800.0],
                "change": [0.01, -0.02, 0.03],
            }
        )
        mock_result = QueryResult(data=test_df, table="spot_em", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_spot_em()
            assert not df.empty
            mock_query.assert_called_once_with(table="spot_em")

    def test_get_spot_em_empty(self, service):
        """Test spot_em returns empty"""
        mock_result = QueryResult(data=pd.DataFrame(), table="spot_em", has_data=False)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_spot_em()
            assert df.empty


@pytest.mark.unit
class TestGetFuturesDaily:
    """Tests for get_futures_daily facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_futures_daily_basic(self, service):
        """Test basic futures daily retrieval"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "symbol": ["IF2401"] * 5,
                "open": [3000.0] * 5,
                "close": [3050.0] * 5,
            }
        )
        mock_result = QueryResult(data=test_df, table="futures_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_futures_daily("IF2401", "2024-01-01", "2024-01-05")
            assert not df.empty
            mock_query.assert_called_once()

    def test_get_futures_daily_no_dates(self, service):
        """Test futures daily without date range"""
        test_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10),
                "symbol": ["IF2401"] * 10,
            }
        )
        mock_result = QueryResult(data=test_df, table="futures_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.get_futures_daily("IF2401")
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["where"] is None

    def test_get_futures_daily_with_ccfx_suffix(self, service):
        """Test futures daily handles .CCFX suffix"""
        test_df = pd.DataFrame({"date": ["2024-01-01"], "symbol": ["IF2401"]})
        mock_result = QueryResult(data=test_df, table="futures_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.get_futures_daily("IF2401.CCFX", "2024-01-01", "2024-01-05")
            call_kwargs = mock_query.call_args[1]
            assert call_kwargs["partition_value"] == "IF2401"

    def test_get_futures_daily_empty(self, service):
        """Test futures daily returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="futures_daily", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_futures_daily("IF2401", "2024-01-01", "2024-01-05")
            assert df.empty


@pytest.mark.unit
class TestGetFuturesSpot:
    """Tests for get_futures_spot facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_futures_spot_basic(self, service):
        """Test basic futures spot retrieval"""
        test_df = pd.DataFrame(
            {
                "symbol": ["IF2401", "IC2401", "IH2401"],
                "name": ["沪深300", "中证500", "上证50"],
                "price": [3050.0, 5200.0, 2300.0],
            }
        )
        mock_result = QueryResult(data=test_df, table="futures_spot", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_futures_spot()
            assert not df.empty
            mock_query.assert_called_once_with(table="futures_spot")

    def test_get_futures_spot_empty(self, service):
        """Test futures spot returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="futures_spot", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_futures_spot()
            assert df.empty


@pytest.mark.unit
class TestGetFuturesMainContracts:
    """Tests for get_futures_main_contracts facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_futures_main_contracts_basic(self, service):
        """Test basic futures main contracts retrieval"""
        test_df = pd.DataFrame(
            {
                "symbol": ["IF2401", "IC2401", "IH2401"],
                "exchange": ["CFFEX", "CFFEX", "CFFEX"],
                "is_main": [True, True, True],
            }
        )
        mock_result = QueryResult(
            data=test_df, table="futures_main_contracts", has_data=True
        )

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            df = service.get_futures_main_contracts()
            assert not df.empty
            mock_query.assert_called_once_with(table="futures_main_contracts")

    def test_get_futures_main_contracts_empty(self, service):
        """Test futures main contracts returns empty"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="futures_main_contracts", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.get_futures_main_contracts()
            assert df.empty


@pytest.mark.unit
class TestGetCallAuction:
    """Tests for get_call_auction facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_get_call_auction_basic(self, service):
        """Test basic call auction retrieval"""
        test_df = pd.DataFrame(
            {
                "time": ["09:15:00", "09:20:00", "09:25:00"],
                "symbol": ["600000"] * 3,
                "price": [10.5, 10.6, 10.55],
                "volume": [1000, 2000, 3000],
            }
        )

        with patch.object(
            service.cn.stock.quote,
            "call_auction",
            return_value=test_df,
        ) as mock_call_auction:
            df = service.get_call_auction("600000", date="2024-01-01")
            assert not df.empty
            mock_call_auction.assert_called_once()

    def test_get_call_auction_no_date(self, service):
        """Test call auction without specific date"""
        test_df = pd.DataFrame(
            {
                "time": ["09:15:00", "09:20:00"],
                "symbol": ["600000"] * 2,
                "price": [10.5, 10.6],
            }
        )

        with patch.object(service.cn.stock.quote, "call_auction", return_value=test_df):
            df = service.get_call_auction("600000")
            assert df is not None

    def test_get_call_auction_empty(self, service):
        """Test call auction returns empty"""

        with patch.object(
            service.cn.stock.quote, "call_auction", return_value=pd.DataFrame()
        ):
            df = service.get_call_auction("600000", date="2024-01-01")
            assert df.empty


@pytest.mark.unit
class TestCachedFetch:
    """Tests for cached_fetch facade method"""

    @pytest.fixture
    def service(self):
        return DataService()

    def test_cached_fetch_basic(self, service):
        """Test basic cached_fetch"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.cached_fetch(
                table="stock_daily",
                start_date="2024-01-01",
                end_date="2024-01-10",
            )
            assert not df.empty

    def test_cached_fetch_with_partition(self, service):
        """Test cached_fetch with partition"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(
            service._served, "query", return_value=mock_result
        ) as mock_query:
            service.cached_fetch(
                table="stock_daily",
                partition_by="symbol",
                partition_value="600000",
            )
            mock_query.assert_called_once()

    def test_cached_fetch_empty_result(self, service):
        """Test cached_fetch returns empty DataFrame"""
        mock_result = QueryResult(
            data=pd.DataFrame(), table="stock_daily", has_data=False
        )

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.cached_fetch(table="stock_daily")
            assert df.empty

    def test_cached_fetch_with_fetch_fn_warning(self, service):
        """Test cached_fetch logs warning when fetch_fn provided"""
        test_df = create_daily_df()
        mock_result = QueryResult(data=test_df, table="stock_daily", has_data=True)

        with patch.object(service._served, "query", return_value=mock_result):
            df = service.cached_fetch(
                table="stock_daily",
                fetch_fn=lambda: pd.DataFrame(),
            )
            assert df is not None


@pytest.mark.unit
class TestQueryResultIsEmpty:
    """Tests for QueryResult.is_empty property"""

    def test_is_empty_with_data(self):
        """Test is_empty returns False when has data"""
        df = pd.DataFrame({"a": [1, 2]})
        result = QueryResult(data=df, table="test", has_data=True)
        assert result.is_empty is False

    def test_is_empty_with_empty_df(self):
        """Test is_empty returns True when DataFrame empty"""
        result = QueryResult(data=pd.DataFrame(), table="test", has_data=False)
        assert result.is_empty is True

    def test_is_empty_with_none(self):
        """Test is_empty returns True when data is None"""
        result = QueryResult(data=None, table="test", has_data=False)
        assert result.is_empty is True


@pytest.mark.unit
class TestMissingDataReport:
    """Tests for MissingDataReport"""

    def test_missing_report_to_dict(self):
        """Test MissingDataReport.to_dict method"""
        from akshare_data.service.missing_data_policy import MissingDataReport

        report = MissingDataReport(
            table="stock_daily",
            query_params={"symbol": "600000"},
            action=MissingAction.RETURN_EMPTY,
            message="No data",
            suggested_action="Run downloader",
        )
        d = report.to_dict()
        assert d["table"] == "stock_daily"
        assert d["action"] == "return_empty"
        assert d["message"] == "No data"

    def test_missing_report_with_backfill_id(self):
        """Test MissingDataReport with backfill_request_id"""
        from akshare_data.service.missing_data_policy import MissingDataReport

        report = MissingDataReport(
            table="stock_daily",
            query_params={},
            action=MissingAction.REQUEST_BACKFILL,
            message="Backfill queued",
            backfill_request_id="bf-001",
        )
        d = report.to_dict()
        assert d["backfill_request_id"] == "bf-001"


@pytest.mark.unit
class TestDataServiceCacheProperty:
    """Tests for DataService.cache property"""

    def test_cache_property_returns_cache_manager(self):
        """Test cache property returns underlying CacheManager"""
        service = DataService()
        assert service.cache is not None

    def test_cache_property_is_reader_cache(self):
        """Test cache property is reader's cache"""
        service = DataService()
        assert service.cache is service._served._reader._cache
