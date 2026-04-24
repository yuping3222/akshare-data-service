"""tests/test_api_extended.py

Comprehensive tests for all uncovered methods in api.py.
Covers HKMarketAPI, USMarketAPI, MacroChinaAPI, CNStockCapitalAPI,
CNStockEventAPI, CNStockFinanceAPI, CNIndexMetaAPI, SourceProxy,
cached_fetch edge cases, _execute_source_method error paths,
and all uncovered DataService public methods.
"""

from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from akshare_data.api import (
    DataService,
    SourceProxy,
    HKMarketAPI,
    USMarketAPI,
    MacroChinaAPI,
)

pytestmark = pytest.mark.unit


def make_df(columns=None, rows=5):
    cols = columns or {
        "date": pd.date_range("2024-01-01", periods=rows),
        "value": [1.0] * rows,
    }
    return pd.DataFrame(cols)


class TestSourceProxy:
    """Test SourceProxy.__getattr__ wrapper"""

    def test_proxy_getattr_returns_wrapper(self):
        service = DataService()
        proxy = SourceProxy(service, requested_source="akshare")
        assert hasattr(proxy, "service")

    def test_proxy_unknown_method_returns_wrapper(self):
        service = DataService()
        proxy = SourceProxy(service, requested_source="akshare")
        wrapper = proxy.unknown_method
        assert callable(wrapper)

    def test_proxy_wrapper_calls_execute_source_method(self):
        service = DataService()
        proxy = SourceProxy(service, requested_source="akshare")
        with patch.object(service, "_execute_source_method") as mock_exec:
            mock_exec.return_value = pd.DataFrame()
            proxy.get_daily_data("600000", "2024-01-01", "2024-01-10", "qfq")
            mock_exec.assert_called_once()


class TestHKMarketAPI:
    """Test HKMarketAPI and HKStockQuoteAPI"""

    def test_hk_market_init(self):
        service = DataService()
        api = HKMarketAPI(service)
        assert api.service is service
        assert hasattr(api, "stock")

    def test_hk_stock_quote_daily(self):
        service = DataService()
        test_df = make_df({"symbol": ["00700"], "close": [300.0]})
        with patch.object(service.akshare, "get_hk_stocks", return_value=test_df):
            result = service.hk.stock.quote.daily(None)
            assert not result.empty

    def test_get_hk_stocks(self):
        service = DataService()
        test_df = make_df({"code": ["00700", "09988"], "name": ["腾讯", "阿里"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_hk_stocks", return_value=test_df
                ):
                    df = service.get_hk_stocks()
                    assert not df.empty


class TestUSMarketAPI:
    """Test USMarketAPI and USStockQuoteAPI"""

    def test_us_market_init(self):
        service = DataService()
        api = USMarketAPI(service)
        assert api.service is service
        assert hasattr(api, "stock")

    def test_us_stock_quote_daily(self):
        service = DataService()
        test_df = make_df({"symbol": ["AAPL"], "close": [150.0]})
        with patch.object(service.akshare, "get_us_stocks", return_value=test_df):
            result = service.us.stock.quote.daily(None)
            assert not result.empty

    def test_get_us_stocks(self):
        service = DataService()
        test_df = make_df({"symbol": ["AAPL", "MSFT"], "close": [150.0, 300.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_us_stocks", return_value=test_df
                ):
                    df = service.get_us_stocks()
                    assert not df.empty


class TestMacroChinaAPI:
    """Test MacroChinaAPI"""

    def test_macro_china_init(self):
        service = DataService()
        api = MacroChinaAPI(service)
        assert api.service is service

    def test_interest_rate(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "rate": [2.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_shibor_rate", return_value=test_df
                ):
                    df = service.macro.china.interest_rate("2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_gdp(self):
        service = DataService()
        test_df = make_df({"quarter": ["2024Q1"], "gdp": [250000.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_macro_gdp", return_value=test_df
                ):
                    df = service.macro.china.gdp("2024-01-01", "2024-12-31")
                    assert not df.empty

    def test_social_financing(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "total": [3000000.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_social_financing", return_value=test_df
                ):
                    df = service.macro.china.social_financing(
                        "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty

    def test_interest_rate_cached(self):
        service = DataService()
        cached = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "rate": [2.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=cached):
            df = service.macro.china.interest_rate("2024-01-01", "2024-01-10")
            assert not df.empty


class TestCNStockCapitalAPI:
    """Test CNStockCapitalAPI methods"""

    def test_northbound_holdings(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "holdings": [1000.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_northbound_holdings", return_value=test_df
                ):
                    df = service.cn.stock.capital.northbound_holdings(
                        "600000", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty

    def test_northbound_holdings_cached(self):
        service = DataService()
        cached = make_df({"symbol": ["600000"], "holdings": [1000.0]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.cn.stock.capital.northbound_holdings(
                "600000", "2024-01-01", "2024-01-10"
            )
            assert not df.empty

    def test_block_deal(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_block_deal", return_value=test_df
                ):
                    df = service.cn.stock.capital.block_deal()
                    assert not df.empty

    def test_block_deal_with_symbol(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_block_deal", return_value=test_df
                ):
                    df = service.cn.stock.capital.block_deal(
                        "600000", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty

    def test_block_deal_cached(self):
        service = DataService()
        cached = make_df({"symbol": ["600000"], "price": [10.0]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.cn.stock.capital.block_deal()
            assert not df.empty

    def test_dragon_tiger(self):
        service = DataService()
        test_df = make_df({"code": ["600000"], "direction": ["买"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_dragon_tiger_list", return_value=test_df
                ):
                    df = service.cn.stock.capital.dragon_tiger("2024-01-10")
                    assert not df.empty

    def test_dragon_tiger_cached(self):
        service = DataService()
        cached = make_df({"code": ["600000"], "direction": ["买"]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.cn.stock.capital.dragon_tiger("2024-01-10")
            assert not df.empty

    def test_margin(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "margin_balance": [1000000.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_margin_data", return_value=test_df
                ):
                    df = service.cn.stock.capital.margin(
                        "600000", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty

    def test_margin_cached(self):
        service = DataService()
        cached = make_df({"symbol": ["600000"], "margin_balance": [1000000.0]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.cn.stock.capital.margin("600000", "2024-01-01", "2024-01-10")
            assert not df.empty

    def test_north(self):
        service = DataService()
        test_df = make_df(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "north_money": [1000.0] * 5,
            }
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_north_money_flow", return_value=test_df
                ):
                    df = service.cn.stock.capital.north("2024-01-01", "2024-01-10")
                    assert not df.empty


class TestCNStockEventAPI:
    """Test CNStockEventAPI methods"""

    def test_dividend(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "dividend": [1.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_dividend_data", return_value=test_df
                ):
                    df = service.cn.stock.event.dividend("600000")
                    assert not df.empty

    def test_dividend_cached(self):
        service = DataService()
        cached = make_df({"symbol": ["600000"], "dividend": [1.0]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.cn.stock.event.dividend("600000")
            assert not df.empty

    def test_restricted_release(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "free_shares": [1000000]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_restricted_release", return_value=test_df
                ):
                    df = service.cn.stock.event.restricted_release("600000")
                    assert not df.empty

    def test_restricted_release_no_symbol(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "free_shares": [1000000]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_restricted_release", return_value=test_df
                ):
                    df = service.cn.stock.event.restricted_release()
                    assert not df.empty


class TestCNStockFinanceAPI:
    """Test CNStockFinanceAPI balance_sheet, income_statement, cash_flow"""

    def test_balance_sheet(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "total_assets": [1e9]})
        with patch.object(service.akshare, "get_balance_sheet", return_value=test_df):
            df = service.cn.stock.finance.balance_sheet("600000")
            assert not df.empty

    def test_income_statement(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "revenue": [1e8]})
        with patch.object(
            service.akshare, "get_income_statement", return_value=test_df
        ):
            df = service.cn.stock.finance.income_statement("600000")
            assert not df.empty

    def test_cash_flow(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "operating_cf": [1e7]})
        with patch.object(service.akshare, "get_cash_flow", return_value=test_df):
            df = service.cn.stock.finance.cash_flow("600000")
            assert not df.empty


class TestCNIndexMetaAPI:
    """Test CNIndexMetaAPI.components"""

    def test_components(self):
        service = DataService()
        test_df = make_df({"index_code": ["000300"], "code": ["600000"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_index_components", return_value=test_df
                ):
                    df = service.cn.index.meta.components("000300")
                    assert not df.empty

    def test_components_cached(self):
        service = DataService()
        cached = make_df({"index_code": ["000300"], "code": ["600000"]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.cn.index.meta.components("000300")
            assert not df.empty


class TestCNStockQuoteAPI:
    """Test CNStockQuoteAPI.realtime"""

    def test_realtime(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.5]})
        with patch.object(service.akshare, "get_realtime_data", return_value=test_df):
            df = service.cn.stock.quote.realtime("600000")
            assert not df.empty


class TestCachedFetchEdgeCases:
    """Test cached_fetch edge cases"""

    def test_cached_fetch_none_fetch_fn(self):
        service = DataService()
        result = service.cached_fetch(table="test", storage_layer="meta")
        assert result.empty

    def test_cached_fetch_empty_params(self):
        service = DataService()
        result = service.cached_fetch(
            table="test",
            storage_layer="meta",
            fetch_fn=None,
        )
        assert result.empty

    def test_cached_fetch_with_params_kwarg(self):
        service = DataService()
        test_df = make_df()
        result = service.cached_fetch(
            table="test",
            storage_layer="meta",
            fetch_fn=lambda: test_df,
            symbol="600000",
        )
        assert not result.empty


class TestExecuteSourceMethodErrorPaths:
    """Test _execute_source_method error paths"""

    def test_execute_source_method_no_matching_source(self):
        service = DataService()
        result = service._execute_source_method("unknown_method", "nonexistent_source")
        assert result is None

    def test_execute_source_method_non_df_method_empty(self):
        service = DataService()
        with patch.object(service.akshare, "get_index_stocks", return_value=[]):
            result = service._execute_source_method("get_index_stocks", None, "000300")
            assert result == []

    def test_execute_source_method_with_router_fallback(self):
        service = DataService()
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.data = make_df()
        mock_router = MagicMock()
        mock_router.execute.return_value = mock_result
        service.router = mock_router
        SourceProxy(service, requested_source="akshare")
        with patch.object(service.akshare, "get_daily_data", return_value=make_df()):
            result = service._execute_source_method(
                "get_daily_data", "akshare", "600000", "2024-01-01", "2024-01-10", "qfq"
            )
            assert result is not None


class TestDataServiceFinancialMethods:
    """Test financial data methods"""

    def test_get_basic_info(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "name": ["浦发银行"]})
        with patch.object(service.akshare, "get_basic_info", return_value=test_df):
            df = service.get_basic_info("600000")
            assert not df.empty

    def test_get_balance_sheet(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "total_assets": [1e9]})
        with patch.object(service.akshare, "get_balance_sheet", return_value=test_df):
            df = service.get_balance_sheet("600000")
            assert not df.empty

    def test_get_income_statement(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "revenue": [1e8]})
        with patch.object(
            service.akshare, "get_income_statement", return_value=test_df
        ):
            df = service.get_income_statement("600000")
            assert not df.empty

    def test_get_cash_flow(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "operating_cf": [1e7]})
        with patch.object(service.akshare, "get_cash_flow", return_value=test_df):
            df = service.get_cash_flow("600000")
            assert not df.empty

    def test_get_financial_metrics(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "roe": [0.15]})
        with patch.object(
            service.akshare, "get_financial_metrics", return_value=test_df
        ):
            df = service.get_financial_metrics("600000")
            assert not df.empty


class TestDataServiceValuationMethods:
    """Test valuation methods"""

    def test_get_stock_valuation(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "pe": [8.5]})
        with patch.object(service.akshare, "get_stock_valuation", return_value=test_df):
            df = service.get_stock_valuation("600000")
            assert not df.empty

    def test_get_index_valuation(self):
        service = DataService()
        test_df = make_df({"index_code": ["000300"], "pe": [12.0]})
        with patch.object(service.akshare, "get_index_valuation", return_value=test_df):
            df = service.get_index_valuation("000300")
            assert not df.empty


class TestDataServiceShareholderMethods:
    """Test shareholder methods"""

    def test_get_shareholder_changes(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "change_ratio": [0.05]})
        mock_proxy = MagicMock()
        mock_proxy.get_shareholder_changes.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_shareholder_changes("600000")
            assert not df.empty

    def test_get_top_shareholders(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "name": ["大股东"]})
        mock_proxy = MagicMock()
        mock_proxy.get_top_shareholders.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_top_shareholders("600000")
            assert not df.empty

    def test_get_institution_holdings(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "inst_count": [100]})
        mock_proxy = MagicMock()
        mock_proxy.get_institution_holdings.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_institution_holdings("600000")
            assert not df.empty

    def test_get_latest_holder_number(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "holders": [50000]})
        mock_proxy = MagicMock()
        mock_proxy.get_latest_holder_number.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_latest_holder_number("600000")
            assert not df.empty


class TestDataServiceDataFacade:
    """Test DataService facade methods for capital/stock data"""

    def test_get_northbound_holdings(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "holdings": [1000.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_northbound_holdings", return_value=test_df
                ):
                    df = service.get_northbound_holdings(
                        "600000", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty

    def test_get_dragon_tiger_list(self):
        service = DataService()
        test_df = make_df({"code": ["600000"], "direction": ["买"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_dragon_tiger_list", return_value=test_df
                ):
                    df = service.get_dragon_tiger_list("2024-01-10")
                    assert not df.empty

    def test_get_block_deal(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_block_deal", return_value=test_df
                ):
                    df = service.get_block_deal()
                    assert not df.empty

    def test_get_block_deal_with_params(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_block_deal", return_value=test_df
                ):
                    df = service.get_block_deal("600000", "2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_margin_data(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "margin_balance": [1000000.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_margin_data", return_value=test_df
                ):
                    df = service.get_margin_data("600000", "2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_dividend_data(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "dividend": [1.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_dividend_data", return_value=test_df
                ):
                    df = service.get_dividend_data("600000")
                    assert not df.empty

    def test_get_restricted_release(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "free_shares": [1000000]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_restricted_release", return_value=test_df
                ):
                    df = service.get_restricted_release("600000")
                    assert not df.empty


class TestDataServiceNewStocksIPO:
    """Test new stocks and IPO methods"""

    def test_get_new_stocks(self):
        service = DataService()
        test_df = make_df({"code": ["601688"], "name": ["华泰证券"]})
        with patch.object(service.akshare, "get_new_stocks", return_value=test_df):
            df = service.get_new_stocks()
            assert not df.empty

    def test_get_ipo_info(self):
        service = DataService()
        test_df = make_df({"code": ["601688"], "issue_price": [12.0]})
        with patch.object(service.akshare, "get_ipo_info", return_value=test_df):
            df = service.get_ipo_info()
            assert not df.empty


class TestDataServiceConceptBoard:
    """Test concept board methods"""

    def test_get_concept_list(self):
        service = DataService()
        test_df = make_df({"concept_code": ["BK0001"], "concept_name": ["人工智能"]})
        with patch.object(service.akshare, "get_concept_list", return_value=test_df):
            df = service.get_concept_list()
            assert not df.empty

    def test_get_concept_stocks(self):
        service = DataService()
        test_df = make_df({"concept_code": ["BK0001"], "code": ["600000"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_concept_components", return_value=test_df
                ):
                    df = service.get_concept_stocks("BK0001")
                    assert not df.empty

    def test_get_concept_stocks_cached(self):
        service = DataService()
        cached = make_df({"concept_code": ["BK0001"], "code": ["600000"]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.get_concept_stocks("BK0001")
            assert not df.empty

    def test_get_stock_concepts(self):
        service = DataService()
        test_df = make_df({"code": ["600000"], "concept": ["人工智能"]})
        with patch.object(service.akshare, "get_stock_concepts", return_value=test_df):
            df = service.get_stock_concepts("600000")
            assert not df.empty


class TestDataServiceExtendedMethods:
    """Test extended methods"""

    def test_get_restricted_release_detail(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "free_shares": [1000000]})
        mock_proxy = MagicMock()
        mock_proxy.get_restricted_release_detail.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_restricted_release_detail("2024-01-01", "2024-01-10")
            assert not df.empty

    def test_get_insider_trading(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "name": ["高管"]})
        mock_proxy = MagicMock()
        mock_proxy.get_insider_trading.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_insider_trading("600000")
            assert not df.empty

    def test_get_equity_freeze(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "frozen_shares": [1000000]})
        mock_proxy = MagicMock()
        mock_proxy.get_equity_freeze.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_equity_freeze("600000")
            assert not df.empty

    def test_get_capital_change(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "total_shares": [1e9]})
        mock_proxy = MagicMock()
        mock_proxy.get_capital_change.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_capital_change("600000")
            assert not df.empty

    def test_get_earnings_forecast(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "forecast_eps": [1.0]})
        mock_proxy = MagicMock()
        mock_proxy.get_earnings_forecast.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_earnings_forecast("600000")
            assert not df.empty

    def test_get_fund_open_daily(self):
        service = DataService()
        test_df = make_df({"fund_code": ["000001"], "nav": [1.5]})
        mock_proxy = MagicMock()
        mock_proxy.get_fund_open_daily.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_fund_open_daily()
            assert not df.empty

    def test_get_fund_open_nav(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "nav": [1.5] * 5}
        )
        mock_proxy = MagicMock()
        mock_proxy.get_fund_open_nav.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_fund_open_nav("000001", "2024-01-01", "2024-01-10")
            assert not df.empty

    def test_get_fund_open_info(self):
        service = DataService()
        test_dict = {"fund_code": "000001", "name": "测试基金"}
        mock_proxy = MagicMock()
        mock_proxy.get_fund_open_info.return_value = test_dict
        with patch.object(service, "_get_source", return_value=mock_proxy):
            result = service.get_fund_open_info("000001")
            assert result["fund_code"] == "000001"

    def test_get_sw_industry_list(self):
        service = DataService()
        test_df = make_df({"industry_code": ["801010"], "industry_name": ["农林牧渔"]})
        mock_proxy = MagicMock()
        mock_proxy.get_sw_industry.return_value = test_df
        with patch.object(service, "_get_source", return_value=mock_proxy):
            df = service.get_sw_industry_list()
            assert not df.empty

    def test_get_sw_industry_daily(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "close": [3000.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                mock_proxy = MagicMock()
                mock_proxy.get_sw_index_daily.return_value = test_df
                with patch.object(service, "_get_source", return_value=mock_proxy):
                    df = service.get_sw_industry_daily(
                        "801010", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty


class TestDataServiceEquityPledge:
    """Test equity pledge methods"""

    def test_get_equity_pledge(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "pledged_shares": [1e6]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_equity_pledge", return_value=test_df
                ):
                    df = service.get_equity_pledge("600000", "2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_equity_pledge_no_symbol(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "pledged_shares": [1e6]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_equity_pledge", return_value=test_df
                ):
                    df = service.get_equity_pledge()
                    assert not df.empty

    def test_get_equity_pledge_rank(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "rank": [1]})
        with patch.object(
            service.akshare, "get_equity_pledge_rank", return_value=test_df
        ):
            df = service.get_equity_pledge_rank("2024-01-10", top_n=50)
            assert not df.empty


class TestDataServiceGoodwill:
    """Test goodwill methods"""

    def test_get_goodwill_data(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "goodwill": [1e8]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_goodwill_data", return_value=test_df
                ):
                    df = service.get_goodwill_data("600000", "2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_goodwill_impairment(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "impairment": [1e6]})
        with patch.object(
            service.akshare, "get_goodwill_impairment", return_value=test_df
        ):
            df = service.get_goodwill_impairment("2024-01-10")
            assert not df.empty

    def test_get_goodwill_by_industry(self):
        service = DataService()
        test_df = make_df({"industry": ["制造业"], "avg_goodwill": [1e7]})
        with patch.object(
            service.akshare, "get_goodwill_by_industry", return_value=test_df
        ):
            df = service.get_goodwill_by_industry("2024-01-10")
            assert not df.empty


class TestDataServiceRepurchase:
    """Test repurchase methods"""

    def test_get_repurchase_data(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "repurchased_shares": [1e5]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_repurchase_data", return_value=test_df
                ):
                    df = service.get_repurchase_data(
                        "600000", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty


class TestDataServiceESG:
    """Test ESG methods"""

    def test_get_esg_rating(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "esg_score": [80.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_esg_rating", return_value=test_df
                ):
                    df = service.get_esg_rating("600000", "2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_esg_rank(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "rank": [10]})
        with patch.object(service.akshare, "get_esg_rank", return_value=test_df):
            df = service.get_esg_rank("2024-01-10", top_n=50)
            assert not df.empty


class TestDataServicePerformance:
    """Test performance forecast/express methods"""

    def test_get_performance_forecast(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "forecast_eps": [1.0]})
        with patch.object(
            service.akshare, "get_performance_forecast", return_value=test_df
        ):
            df = service.get_performance_forecast("600000", "2024-01-01", "2024-01-10")
            assert not df.empty

    def test_get_performance_express(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "express_eps": [0.8]})
        with patch.object(
            service.akshare, "get_performance_express", return_value=test_df
        ):
            df = service.get_performance_express("600000", "2024-01-01", "2024-01-10")
            assert not df.empty


class TestDataServiceAnalystReport:
    """Test analyst methods"""

    def test_get_analyst_rank(self):
        service = DataService()
        test_df = make_df({"analyst": ["张三"], "rank": [1]})
        with patch.object(service.akshare, "get_analyst_rank", return_value=test_df):
            df = service.get_analyst_rank("2024-01-01", "2024-01-10")
            assert not df.empty

    def test_get_research_report(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "title": ["深度报告"]})
        with patch.object(service.akshare, "get_research_report", return_value=test_df):
            df = service.get_research_report("600000", "2024-01-01", "2024-01-10")
            assert not df.empty


class TestDataServiceChipDistribution:
    """Test chip distribution method"""

    def test_get_chip_distribution(self):
        service = DataService()
        test_df = make_df({"price": [10.0, 10.5, 11.0], "ratio": [0.2, 0.5, 0.3]})
        with patch.object(
            service.akshare, "get_chip_distribution", return_value=test_df
        ):
            df = service.get_chip_distribution("600000")
            assert not df.empty


class TestDataServiceBonus:
    """Test stock bonus methods"""

    def test_get_stock_bonus(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "bonus_ratio": [0.1]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_stock_bonus", return_value=test_df
                ):
                    df = service.get_stock_bonus("600000")
                    assert not df.empty

    def test_get_stock_bonus_cached(self):
        service = DataService()
        cached = make_df({"symbol": ["600000"], "bonus_ratio": [0.1]})
        with patch.object(service.cache, "read", return_value=cached):
            df = service.get_stock_bonus("600000")
            assert not df.empty

    def test_get_rights_issue(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "rights_price": [8.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_rights_issue", return_value=test_df
                ):
                    df = service.get_rights_issue("600000")
                    assert not df.empty

    def test_get_dividend_by_date(self):
        service = DataService()
        test_df = make_df({"code": ["600000"], "dividend": [1.0]})
        with patch.object(
            service.akshare, "get_dividend_by_date", return_value=test_df
        ):
            df = service.get_dividend_by_date("2024-01-10")
            assert not df.empty


class TestDataServiceCompanyInfo:
    """Test company info methods"""

    def test_get_management_info(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "name": ["张三"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_management_info", return_value=test_df
                ):
                    df = service.get_management_info("600000")
                    assert not df.empty

    def test_get_name_history(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "old_name": ["原名"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_name_history", return_value=test_df
                ):
                    df = service.get_name_history("600000")
                    assert not df.empty


class TestDataServiceMacroExtended:
    """Test extended macro methods"""

    def test_get_shibor_rate(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "rate": [2.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_shibor_rate", return_value=test_df
                ):
                    df = service.get_shibor_rate("2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_macro_gdp(self):
        service = DataService()
        test_df = make_df({"quarter": ["2024Q1"], "gdp": [250000.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_macro_gdp", return_value=test_df
                ):
                    df = service.get_macro_gdp("2024-01-01", "2024-12-31")
                    assert not df.empty

    def test_get_social_financing(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "total": [3e6] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_social_financing", return_value=test_df
                ):
                    df = service.get_social_financing("2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_macro_exchange_rate(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "usd_cny": [7.2] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_macro_exchange_rate", return_value=test_df
                ):
                    df = service.get_macro_exchange_rate("2024-01-01", "2024-01-10")
                    assert not df.empty


class TestDataServiceFundMethods:
    """Test fund methods"""

    def test_get_fof_list(self):
        service = DataService()
        test_df = make_df({"fund_code": ["FOF001"], "name": ["FOF基金"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_fof_list", return_value=test_df
                ):
                    df = service.get_fof_list()
                    assert not df.empty

    def test_get_fof_nav(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "nav": [1.2] * 5}
        )
        with patch.object(service.akshare, "get_fof_nav", return_value=test_df):
            df = service.get_fof_nav("FOF001", "2024-01-01", "2024-01-10")
            assert not df.empty

    def test_get_lof_spot(self):
        service = DataService()
        test_df = make_df({"fund_code": ["LOF001"], "name": ["LOF基金"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_lof_spot", return_value=test_df
                ):
                    df = service.get_lof_spot()
                    assert not df.empty

    def test_get_lof_nav(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "nav": [1.5] * 5}
        )
        with patch.object(service.akshare, "get_lof_nav", return_value=test_df):
            df = service.get_lof_nav("LOF001")
            assert not df.empty


class TestDataServiceConvertBond:
    """Test convertible bond methods"""

    def test_get_convert_bond_premium(self):
        service = DataService()
        test_df = make_df({"bond_code": ["113009"], "premium": [20.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_convert_bond_premium", return_value=test_df
                ):
                    df = service.get_convert_bond_premium()
                    assert not df.empty

    def test_get_convert_bond_spot(self):
        service = DataService()
        test_df = make_df({"bond_code": ["113009"], "price": [120.0]})
        with patch.object(
            service.akshare, "get_convert_bond_spot", return_value=test_df
        ):
            df = service.get_convert_bond_spot()
            assert not df.empty

    def test_get_conversion_bond_list(self):
        service = DataService()
        test_df = make_df({"bond_code": ["113009"], "name": ["博威转债"]})
        with patch.object(
            service.akshare, "get_conversion_bond_list", return_value=test_df
        ):
            df = service.get_conversion_bond_list()
            assert not df.empty

    def test_get_conversion_bond_daily(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "close": [120.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_conversion_bond_daily", return_value=test_df
                ):
                    df = service.get_conversion_bond_daily(
                        "113009", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty


class TestDataServiceIndustry:
    """Test industry performance methods"""

    def test_get_industry_performance(self):
        service = DataService()
        test_df = make_df({"industry": ["银行"], "change_pct": [1.5]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_industry_performance", return_value=test_df
                ):
                    df = service.get_industry_performance("2024-01-10")
                    assert not df.empty

    def test_get_concept_performance(self):
        service = DataService()
        test_df = make_df({"concept": ["AI"], "change_pct": [3.0]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_concept_performance", return_value=test_df
                ):
                    df = service.get_concept_performance("2024-01-10")
                    assert not df.empty

    def test_get_stock_industry(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "industry": ["银行"]})
        with patch.object(service.akshare, "get_stock_industry", return_value=test_df):
            df = service.get_stock_industry("600000")
            assert not df.empty


class TestDataServiceHotRank:
    """Test hot rank methods"""

    def test_get_hot_rank(self):
        service = DataService()
        test_df = make_df({"rank": [1, 2, 3], "symbol": ["600000", "600519", "000001"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_hot_rank", return_value=test_df
                ):
                    df = service.get_hot_rank()
                    assert not df.empty


class TestDataServiceOption:
    """Test option methods"""

    def test_get_option_list(self):
        service = DataService()
        test_df = make_df({"option_code": ["10000001"], "name": ["50ETF购1月2.5"]})
        with patch.object(service.akshare, "get_option_list", return_value=test_df):
            df = service.get_option_list(market="sse")
            assert not df.empty

    def test_get_option_daily(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "close": [0.15] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_option_daily", return_value=test_df
                ):
                    df = service.get_option_daily("10000001")
                    assert not df.empty


class TestDataServiceLOF:
    """Test LOF fund methods"""

    def test_get_lof_daily(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "close": [1.5] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_lof_hist", return_value=test_df
                ):
                    df = service.get_lof_daily("LOF001", "2024-01-01", "2024-01-10")
                    assert not df.empty


class TestDataServiceFutures:
    """Test futures methods"""

    def test_get_futures_daily(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "close": [5000.0] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_futures_daily", return_value=test_df
                ):
                    df = service.get_futures_daily("IF2401", "2024-01-01", "2024-01-10")
                    assert not df.empty

    def test_get_futures_spot(self):
        service = DataService()
        test_df = make_df({"symbol": ["IF2401"], "price": [5000.0]})
        with patch.object(service.akshare, "get_futures_spot", return_value=test_df):
            df = service.get_futures_spot()
            assert not df.empty


class TestDataServiceSpot:
    """Test spot quotes methods"""

    def test_get_spot_em(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.5]})
        with patch.object(service.akshare, "get_spot_em", return_value=test_df):
            df = service.get_spot_em()
            assert not df.empty


class TestDataServiceStockHist:
    """Test stock historical data wrapper"""

    def test_get_stock_hist_daily(self):
        service = DataService()
        test_df = make_df(
            {"date": pd.date_range("2024-01-01", periods=5), "close": [10.5] * 5}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_daily_data", return_value=test_df
                ):
                    df = service.get_stock_hist(
                        "600000", "daily", "2024-01-01", "2024-01-10"
                    )
                    assert not df.empty

    def test_get_stock_hist_minute(self):
        service = DataService()
        test_df = make_df(
            {
                "datetime": pd.date_range("2024-01-01", periods=5, freq="min"),
                "close": [10.5] * 5,
            }
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_minute_data", return_value=test_df
                ):
                    df = service.get_stock_hist(
                        "600000", "1min", "2024-01-01", "2024-01-01"
                    )
                    assert not df.empty


class TestDataServiceRestrictedReleaseCalendar:
    """Test restricted release calendar"""

    def test_get_restricted_release_calendar(self):
        service = DataService()
        test_df = make_df({"date": ["2024-01-10"], "symbol": ["600000"]})
        with patch.object(
            service.akshare, "get_restricted_release_calendar", return_value=test_df
        ):
            df = service.get_restricted_release_calendar("2024-01-01", "2024-01-10")
            assert not df.empty


class TestDataServiceIndexComponents:
    """Test get_index_components"""

    def test_get_index_components(self):
        service = DataService()
        test_df = make_df(
            {"index_code": ["000300"], "code": ["600000"], "weight": [0.5]}
        )
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_index_components", return_value=test_df
                ):
                    df = service.get_index_components("000300")
                    assert not df.empty

    def test_get_index_components_cached(self):
        service = DataService()
        cached = make_df(
            {"index_code": ["000300"], "code": ["600000"], "weight": [0.5]}
        )
        with patch.object(service.cache, "read", return_value=cached):
            df = service.get_index_components("000300")
            assert not df.empty

    def test_get_index_components_no_weights(self):
        service = DataService()
        test_df = make_df({"index_code": ["000300"], "code": ["600000"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_index_components", return_value=test_df
                ):
                    df = service.get_index_components("000300", include_weights=False)
                    assert not df.empty


class TestDataServiceSecuritiesList:
    """Test securities list"""

    def test_get_securities_list(self):
        service = DataService()
        test_df = make_df({"code": ["600000"], "name": ["浦发银行"], "type": ["stock"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_securities_list", return_value=test_df
                ):
                    df = service.get_securities_list("stock")
                    assert not df.empty


class TestDataServiceSuspendedST:
    """Test suspended and ST stocks"""

    def test_get_suspended_stocks(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "suspend_date": ["2024-01-10"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_suspended_stocks", return_value=test_df
                ):
                    df = service.get_suspended_stocks()
                    assert not df.empty

    def test_get_st_stocks(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "st_type": ["ST"]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_st_stocks", return_value=test_df
                ):
                    df = service.get_st_stocks()
                    assert not df.empty


class TestDataServiceIndustryStocks:
    """Test industry stocks"""

    def test_get_industry_stocks(self):
        service = DataService()
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.lixinger,
                    "get_industry_stocks",
                    return_value=["600000", "600519"],
                ):
                    with patch.object(
                        service.akshare,
                        "get_industry_stocks",
                        return_value=["600000", "600519"],
                    ):
                        stocks = service.get_industry_stocks("801010")
                        assert len(stocks) == 2

    def test_get_industry_stocks_cached(self):
        service = DataService()
        cached = pd.DataFrame(
            {
                "industry_code": ["801010", "801010"],
                "level": [1, 1],
                "code": ["600000", "600519"],
            }
        )
        with patch.object(service.cache, "read", return_value=cached):
            stocks = service.get_industry_stocks("801010")
            assert len(stocks) == 2


class TestDataServiceSecurityInfo:
    """Test security info"""

    def test_get_security_info(self):
        service = DataService()
        mock_df = pd.DataFrame({"symbol": ["600000"], "name": ["浦发银行"]})
        with patch.object(service, "_build_security_info_df", return_value=mock_df):
            result = service.get_security_info("600000")
            assert "symbol" in result
            assert "name" in result


class TestDataServiceIndustryMapping:
    """Test industry mapping"""

    def test_get_industry_mapping(self):
        service = DataService()
        mock_proxy = MagicMock()
        mock_proxy.get_industry_mapping.return_value = ["801010"]
        with patch.object(service, "_get_source", return_value=mock_proxy):
            result = service.get_industry_mapping("600000")
            assert result == "801010"


class TestDataServiceCallAuction:
    """Test call auction"""

    def test_get_call_auction(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.5]})
        with patch.object(service.cache, "read", return_value=test_df):
            with patch.object(service.cache, "write", return_value=""):
                with patch.object(
                    service.akshare, "get_call_auction", return_value=test_df
                ):
                    df = service.get_call_auction("600000", "2024-01-10")
                    assert not df.empty


class TestDataServiceRealtime:
    """Test realtime data"""

    def test_get_realtime_data(self):
        service = DataService()
        test_df = make_df({"symbol": ["600000"], "price": [10.5]})
        with patch.object(service.akshare, "get_realtime_data", return_value=test_df):
            df = service.get_realtime_data("600000")
            assert not df.empty
