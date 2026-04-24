"""Extended tests for Namespace API surface.

Tests all namespace API classes with mocked _served.query calls.
Verifies:
- Symbol normalization is correctly applied
- Parameters are passed correctly to _served.query/_served.query_daily
- Table names and where clauses are correct
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data.api import (
    DataService,
    CNStockQuoteAPI,
    CNStockFinanceAPI,
    CNStockCapitalAPI,
    CNStockEventAPI,
    CNIndexQuoteAPI,
    CNIndexMetaAPI,
    CNETFQuoteAPI,
    HKMarketAPI,
    USMarketAPI,
    MacroAPI,
    MacroChinaAPI,
)


@pytest.mark.unit
def _mock_query_result(df: pd.DataFrame):
    from akshare_data.service.data_service import QueryResult

    return QueryResult(data=df, table="mock_table", has_data=True)


@pytest.fixture
def mock_service():
    service = MagicMock(spec=DataService)
    service._served = MagicMock()
    return service


class TestCNStockQuoteAPICallAuction:
    """Test cn.stock.quote.call_auction method."""

    def test_call_auction_with_symbol_only(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.call_auction(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "call_auction"
        assert call_kwargs["where"] == {"symbol": "600000"}

    def test_call_auction_with_symbol_and_date(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.call_auction(symbol="600000", date="2024-01-05")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"] == {"symbol": "600000", "date": "2024-01-05"}

    def test_call_auction_symbol_normalization(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.call_auction(symbol="sh600000.XSHG")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["symbol"] == "600000"


class TestCNStockQuoteAPIMinute:
    """Test cn.stock.quote.minute method for various frequencies."""

    def test_minute_default_freq(self, mock_service):
        # The schema registry exposes a single ``stock_minute`` table with a
        # ``period`` column for frequency; the namespace assembly now queries
        # it with ``period``/``symbol`` in the where clause.
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "stock_minute"
        assert call_kwargs["where"]["period"] == "1min"
        assert call_kwargs["where"]["symbol"] == "600000"

    def test_minute_5min_freq(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(symbol="600000", freq="5min")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "stock_minute"
        assert call_kwargs["where"]["period"] == "5min"

    def test_minute_15min_freq(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(symbol="600000", freq="15min")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "stock_minute"
        assert call_kwargs["where"]["period"] == "15min"

    def test_minute_30min_freq(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(symbol="600000", freq="30min")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "stock_minute"
        assert call_kwargs["where"]["period"] == "30min"

    def test_minute_60min_freq(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(symbol="600000", freq="60min")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "stock_minute"
        assert call_kwargs["where"]["period"] == "60min"

    def test_minute_with_date_range(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(
            symbol="600000", freq="5min", start_date="2024-01-02", end_date="2024-01-05"
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["datetime"] == ("2024-01-02", "2024-01-05")

    def test_minute_symbol_normalization(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.minute(symbol="sh600000.XSHG")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["symbol"] == "600000"


class TestCNStockQuoteAPIRealtime:
    """Test cn.stock.quote.realtime method."""

    def test_realtime_basic(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.realtime(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "spot_snapshot"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"

    def test_realtime_symbol_normalization(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.realtime(symbol="600000.XSHG")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600000"


class TestCNStockFinanceAPIIndicators:
    """Test cn.stock.finance.indicators method."""

    def test_indicators_with_full_date_range(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.indicators(
            symbol="sh600000", start_date="2024-01-01", end_date="2024-06-30"
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "finance_indicator"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"]["report_date"] == ("2024-01-01", "2024-06-30")

    def test_indicators_start_date_only(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.indicators(symbol="600000", start_date="2024-01-01")
        call_kwargs = mock_service._served.query.call_args[1]
        assert "report_date" in call_kwargs["where"]

    def test_indicators_no_dates(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.indicators(symbol="600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"] is None

    def test_indicators_symbol_normalization(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.indicators(symbol="sz000001.XSHE")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "000001"


class TestCNStockFinanceAPIBalanceSheet:
    """Test cn.stock.finance.balance_sheet method."""

    def test_balance_sheet_basic(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.balance_sheet(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "balance_sheet"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"

    def test_balance_sheet_symbol_normalization(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.balance_sheet(symbol="600519.XSHG")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600519"


class TestCNStockFinanceAPIIncomeStatement:
    """Test cn.stock.finance.income_statement method."""

    def test_income_statement_basic(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.income_statement(symbol="600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "income_statement"
        assert call_kwargs["partition_value"] == "600000"


class TestCNStockFinanceAPICashFlow:
    """Test cn.stock.finance.cash_flow method."""

    def test_cash_flow_basic(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.cash_flow(symbol="600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "cash_flow"
        assert call_kwargs["partition_value"] == "600000"


class TestCNStockCapitalAPIMoneyFlow:
    """Test cn.stock.capital.money_flow method."""

    def test_money_flow_with_dates(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.money_flow(
            symbol="sh600000", start_date="2024-01-02", end_date="2024-01-10"
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "money_flow"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")

    def test_money_flow_no_dates(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.money_flow(symbol="600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"] is None

    def test_money_flow_symbol_normalization(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.money_flow(symbol="sz000001")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "000001"


class TestCNStockCapitalAPINorthboundHoldings:
    """Test cn.stock.capital.northbound_holdings method."""

    def test_northbound_holdings_basic(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.northbound_holdings(
            symbol="sh600000", start_date="2024-01-02", end_date="2024-01-10"
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "northbound_holdings"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")

    def test_northbound_holdings_symbol_normalization(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.northbound_holdings(
            symbol="600519.XSHG", start_date="2024-01-01", end_date="2024-01-10"
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600519"


class TestCNStockCapitalAPIBlockDeal:
    """Test cn.stock.capital.block_deal method."""

    def test_block_deal_with_symbol(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.block_deal(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "block_deal"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"

    def test_block_deal_without_symbol(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.block_deal()
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"] is None

    def test_block_deal_with_dates(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.block_deal(symbol="600000", start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["date"] == ("2024-01-01", "2024-01-10")

    def test_block_deal_with_dates_no_symbol(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.block_deal(start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["date"] == ("2024-01-01", "2024-01-10")


class TestCNStockCapitalAPIDragonTiger:
    """Test cn.stock.capital.dragon_tiger method."""

    def test_dragon_tiger_basic(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.dragon_tiger(date="2024-01-05")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "dragon_tiger_list"
        assert call_kwargs["where"] == {"date": "2024-01-05"}


class TestCNStockCapitalAPIMargin:
    """Test cn.stock.capital.margin method."""

    def test_margin_basic(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.margin(symbol="sh600000", start_date="2024-01-02", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "margin_detail"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")

    def test_margin_symbol_normalization(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.margin(symbol="sz000001", start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "000001"


class TestCNStockCapitalAPINorth:
    """Test cn.stock.capital.north method."""

    def test_north_with_dates(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.north(start_date="2024-01-02", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "north_flow"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")

    def test_north_no_dates(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.north()
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"] is None


class TestCNStockEventAPIDividend:
    """Test cn.stock.event.dividend method."""

    def test_dividend_basic(self, mock_service):
        api = CNStockEventAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.dividend(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "dividend"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"

    def test_dividend_symbol_normalization(self, mock_service):
        api = CNStockEventAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.dividend(symbol="600519.XSHG")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600519"


class TestCNStockEventAPIRestrictedRelease:
    """Test cn.stock.event.restricted_release method."""

    def test_restricted_release_with_symbol(self, mock_service):
        api = CNStockEventAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.restricted_release(symbol="sh600000")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "restricted_release"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"

    def test_restricted_release_without_symbol(self, mock_service):
        api = CNStockEventAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.restricted_release()
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"] is None

    def test_restricted_release_with_dates(self, mock_service):
        api = CNStockEventAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.restricted_release(
            symbol="600000", start_date="2024-01-01", end_date="2024-01-31"
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["date"] == ("2024-01-01", "2024-01-31")

    def test_restricted_release_all_params(self, mock_service):
        api = CNStockEventAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.restricted_release(
            symbol="600000",
            start_date="2024-01-01",
            end_date="2024-12-31",
            source="akshare",
        )
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "600000"
        assert call_kwargs["where"]["date"] == ("2024-01-01", "2024-12-31")


class TestCNIndexQuoteAPIDaily:
    """Test cn.index.quote.daily method."""

    def test_index_daily_basic(self, mock_service):
        api = CNIndexQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="000300", start_date="2024-01-02", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "index_daily"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "000300"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")

    def test_index_daily_long_range(self, mock_service):
        api = CNIndexQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="000300", start_date="2023-01-01", end_date="2024-12-31")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["where"]["date"] == ("2023-01-01", "2024-12-31")

    def test_index_daily_symbol_normalization(self, mock_service):
        api = CNIndexQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="sh000300", start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "000300"


class TestCNIndexMetaAPIComponents:
    """Test cn.index.meta.components method."""

    def test_components_returns_dataframe(self, mock_service):
        api = CNIndexMetaAPI(mock_service)
        comp_df = pd.DataFrame(
            {"index_code": ["000300"] * 3, "code": ["600000", "600519", "000001"]}
        )
        mock_service._served.query.return_value = _mock_query_result(comp_df)
        result = api.components(index_code="000300")
        assert isinstance(result, pd.DataFrame)
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "index_components"
        assert call_kwargs["partition_by"] == "index_code"
        assert call_kwargs["partition_value"] == "000300"

    def test_components_empty_result(self, mock_service):
        api = CNIndexMetaAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        result = api.components(index_code="000300")
        assert result.empty

    def test_components_index_code_normalization(self, mock_service):
        api = CNIndexMetaAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.components(index_code="sh000300")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "000300"


class TestCNETFQuoteAPIDaily:
    """Test cn.fund.quote.daily method."""

    def test_etf_daily_basic(self, mock_service):
        api = CNETFQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="510300", start_date="2024-01-02", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "etf_daily"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "510300"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")

    def test_etf_daily_symbol_normalization(self, mock_service):
        api = CNETFQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="sh510300", start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "510300"


class TestHKMarketAPI:
    """Test hk.stock.quote.daily method."""

    def test_hk_daily_no_filter(self, mock_service):
        api = HKMarketAPI(mock_service)
        hk_df = pd.DataFrame({"stockCode": ["00700", "00941"], "close": [350.0, 70.0]})
        mock_service._served.query.return_value = _mock_query_result(hk_df)
        result = api.stock.quote.daily(symbol=None)
        assert len(result) == 2
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "hk_stock_daily"

    def test_hk_daily_with_symbol_filter(self, mock_service):
        api = HKMarketAPI(mock_service)
        hk_df = pd.DataFrame(
            {"stockCode": ["00700", "00941", "02318"], "close": [350.0, 70.0, 100.0]}
        )
        mock_service._served.query.return_value = _mock_query_result(hk_df)
        result = api.stock.quote.daily(symbol="00700")
        assert len(result) == 1
        assert result["stockCode"].iloc[0] == "00700"

    def test_hk_daily_empty_result(self, mock_service):
        api = HKMarketAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        result = api.stock.quote.daily(symbol=None)
        assert result.empty


class TestUSMarketAPI:
    """Test us.stock.quote.daily method."""

    def test_us_daily_no_filter(self, mock_service):
        api = USMarketAPI(mock_service)
        us_df = pd.DataFrame({"stockCode": ["AAPL", "GOOGL"], "close": [180.0, 140.0]})
        mock_service._served.query.return_value = _mock_query_result(us_df)
        result = api.stock.quote.daily(symbol=None)
        assert len(result) == 2
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "us_stock_daily"

    def test_us_daily_with_symbol_filter(self, mock_service):
        api = USMarketAPI(mock_service)
        us_df = pd.DataFrame(
            {"symbol": ["AAPL", "GOOGL", "MSFT"], "close": [180.0, 140.0, 400.0]}
        )
        mock_service._served.query.return_value = _mock_query_result(us_df)
        result = api.stock.quote.daily(symbol="AAPL")
        assert len(result) == 1
        assert result["symbol"].iloc[0] == "AAPL"

    def test_us_daily_empty_result(self, mock_service):
        api = USMarketAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        result = api.stock.quote.daily(symbol="AAPL")
        assert result.empty


class TestMacroChinaAPIInterestRate:
    """Test macro.china.interest_rate method."""

    def test_interest_rate_basic(self, mock_service):
        api = MacroChinaAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.interest_rate(start_date="2024-01-02", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "shibor_rate"
        assert call_kwargs["where"]["date"] == ("2024-01-02", "2024-01-10")


class TestMacroChinaAPIGdp:
    """Test macro.china.gdp method."""

    def test_gdp_basic(self, mock_service):
        api = MacroChinaAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.gdp(start_date="2024-01-01", end_date="2024-06-30")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "macro_gdp"
        assert call_kwargs["where"]["date"] == ("2024-01-01", "2024-06-30")


class TestMacroChinaAPISocialFinancing:
    """Test macro.china.social_financing method."""

    def test_social_financing_basic(self, mock_service):
        api = MacroChinaAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.social_financing(start_date="2024-01-01", end_date="2024-02-28")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["table"] == "social_financing"
        assert call_kwargs["where"]["date"] == ("2024-01-01", "2024-02-28")


class TestMacroAPI:
    """Test macro API structure."""

    def test_macro_has_china_namespace(self, mock_service):
        api = MacroAPI(mock_service)
        assert hasattr(api, "china")
        assert isinstance(api.china, MacroChinaAPI)


class TestNamespaceSymbolNormalizationConsistency:
    """Test that all namespace APIs consistently normalize symbols."""

    def test_stock_quote_daily_normalizes(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query_daily.return_value = _mock_query_result(
            pd.DataFrame()
        )
        for sym in ["sh600000", "600000.XSHG", "sh.600000", "600000"]:
            mock_service._served.query_daily.reset_mock()
            api.daily(symbol=sym, start_date="2024-01-01", end_date="2024-01-10")
            call_kwargs = mock_service._served.query_daily.call_args[1]
            assert call_kwargs["symbol"] == "600000"

    def test_stock_finance_normalizes(self, mock_service):
        api = CNStockFinanceAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        for sym in ["sz000001", "000001.XSHE", "sz.000001", "000001"]:
            mock_service._served.query.reset_mock()
            api.balance_sheet(symbol=sym)
            call_kwargs = mock_service._served.query.call_args[1]
            assert call_kwargs["partition_value"] == "000001"

    def test_stock_capital_normalizes(self, mock_service):
        api = CNStockCapitalAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        for sym in ["sh600519", "600519.XSHG", "600519"]:
            mock_service._served.query.reset_mock()
            api.money_flow(symbol=sym)
            call_kwargs = mock_service._served.query.call_args[1]
            assert call_kwargs["partition_value"] == "600519"

    def test_index_quote_normalizes(self, mock_service):
        api = CNIndexQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="sh000300", start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "000300"

    def test_etf_quote_normalizes(self, mock_service):
        api = CNETFQuoteAPI(mock_service)
        mock_service._served.query.return_value = _mock_query_result(pd.DataFrame())
        api.daily(symbol="sh510500", start_date="2024-01-01", end_date="2024-01-10")
        call_kwargs = mock_service._served.query.call_args[1]
        assert call_kwargs["partition_value"] == "510500"


class TestCNStockQuoteAPIDaily:
    """Test cn.stock.quote.daily method."""

    def test_daily_calls_query_daily(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query_daily.return_value = _mock_query_result(
            pd.DataFrame()
        )
        api.daily(symbol="sh600000", start_date="2024-01-02", end_date="2024-01-10")
        mock_service._served.query_daily.assert_called_once()
        call_kwargs = mock_service._served.query_daily.call_args[1]
        assert call_kwargs["table"] == "stock_daily"
        assert call_kwargs["symbol"] == "600000"
        assert call_kwargs["start_date"] == "2024-01-02"
        assert call_kwargs["end_date"] == "2024-01-10"

    def test_daily_with_adjust_param(self, mock_service):
        api = CNStockQuoteAPI(mock_service)
        mock_service._served.query_daily.return_value = _mock_query_result(
            pd.DataFrame()
        )
        api.daily(
            symbol="600000",
            start_date="2024-01-01",
            end_date="2024-01-10",
            adjust="hfq",
        )
        mock_service._served.query_daily.assert_called_once()
