"""tests/test_sources_akshare.py

Comprehensive tests for AkShareAdapter in akshare_source.py
"""

import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import patch, MagicMock

from akshare_data.sources.akshare_source import (
    AkShareAdapter,
    find_date_column,
)
from akshare_data.core.errors import DataSourceError, SourceUnavailableError


class TestAkShareAdapterInit:
    """Test AkShareAdapter initialization"""

    def test_init_default_params(self):
        """Test adapter initialization with default parameters"""
        adapter = AkShareAdapter()
        assert adapter.name == "akshare"
        assert adapter.source_type == "real"
        assert adapter._use_cache is True
        assert adapter._cache_ttl_hours == 24
        assert adapter._offline_mode is False
        assert adapter._max_retries == 3
        assert adapter._retry_delay == 2.0

    def test_init_custom_params(self):
        """Test adapter initialization with custom parameters"""
        adapter = AkShareAdapter(
            use_cache=False,
            cache_ttl_hours=48,
            offline_mode=True,
            max_retries=5,
            retry_delay=3.0,
            data_sources=["sina", "baostock"],
        )
        assert adapter._use_cache is False
        assert adapter._cache_ttl_hours == 48
        assert adapter._offline_mode is True
        assert adapter._max_retries == 5
        assert adapter._retry_delay == 3.0
        assert adapter._data_sources == ["sina", "baostock"]

    def test_init_default_data_sources(self):
        """Test default data sources list"""
        adapter = AkShareAdapter()
        assert "sina" in adapter.DEFAULT_DATA_SOURCES
        assert "east_money" in adapter.DEFAULT_DATA_SOURCES
        assert "tushare" in adapter.DEFAULT_DATA_SOURCES
        assert "baostock" in adapter.DEFAULT_DATA_SOURCES


class TestNormalizeSymbol:
    """Test _normalize_symbol method"""

    def test_normalize_symbol_with_jq_format(self):
        """Test normalization of jq format codes"""
        with patch("akshare_data.sources.akshare_source._jq_code_to_ak") as mock_fn:
            mock_fn.return_value = "sh600000"
            adapter = AkShareAdapter()
            result = adapter._normalize_symbol("600000.XSHG")
            mock_fn.assert_called_once_with("600000.XSHG")
            assert result == "sh600000"

    def test_normalize_symbol_with_akshare_format(self):
        """Test normalization of akshare format codes"""
        with patch("akshare_data.sources.akshare_source._jq_code_to_ak") as mock_fn:
            mock_fn.return_value = "sz000001"
            adapter = AkShareAdapter()
            result = adapter._normalize_symbol("000001.XSHE")
            assert result == "sz000001"

    def test_normalize_symbol_with_prefix(self):
        """Test normalization of codes already with prefix"""
        with patch("akshare_data.sources.akshare_source._jq_code_to_ak") as mock_fn:
            mock_fn.return_value = "sh600000"
            adapter = AkShareAdapter()
            adapter._normalize_symbol("sh600000")
            mock_fn.assert_called_once_with("sh600000")


class TestNormalizeDate:
    """Test _normalize_date method"""

    def test_normalize_date_string(self):
        """Test that string dates pass through unchanged"""
        adapter = AkShareAdapter()
        assert adapter._normalize_date("2024-01-01") == "2024-01-01"
        assert adapter._normalize_date("2024-12-31") == "2024-12-31"

    def test_normalize_date_datetime(self):
        """Test datetime normalization"""
        adapter = AkShareAdapter()
        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert adapter._normalize_date(dt) == "2024-01-15"

    def test_normalize_date_date(self):
        """Test date normalization"""
        adapter = AkShareAdapter()
        d = date(2024, 6, 30)
        assert adapter._normalize_date(d) == "2024-06-30"

    def test_normalize_date_other_type(self):
        """Test normalization of other types via str()"""
        adapter = AkShareAdapter()
        result = adapter._normalize_date(20240101)
        assert result == "20240101"


class TestToJqFormat:
    """Test _to_jq_format method"""

    def test_to_jq_format_shanghai(self):
        """Test conversion of Shanghai codes to jq format"""
        adapter = AkShareAdapter()
        assert adapter._to_jq_format("600000") == "600000.XSHG"
        assert adapter._to_jq_format("600519") == "600519.XSHG"

    def test_to_jq_format_shenzhen(self):
        """Test conversion of Shenzhen codes to jq format"""
        adapter = AkShareAdapter()
        assert adapter._to_jq_format("000001") == "000001.XSHE"
        assert adapter._to_jq_format("000002") == "000002.XSHE"

    def test_to_jq_format_zero_padding(self):
        """Test zero padding in jq format conversion"""
        adapter = AkShareAdapter()
        assert adapter._to_jq_format("1") == "000001.XSHE"
        assert adapter._to_jq_format("99") == "000099.XSHE"

    def test_to_jq_format_already_has_prefix(self):
        """Test that 6-digit codes work correctly"""
        adapter = AkShareAdapter()
        assert adapter._to_jq_format("000001") == "000001.XSHE"


class TestCalculateConversionValue:
    """Test calculate_conversion_value method"""

    def test_calculate_conversion_value_call(self):
        """Test conversion value calculation"""
        adapter = AkShareAdapter()
        result = adapter.calculate_conversion_value(
            bond_price=120.0,
            conversion_ratio=10.0,
            stock_price=10.0,
        )

        assert result["bond_price"] == 120.0
        assert result["conversion_ratio"] == 10.0
        assert result["stock_price"] == 10.0
        assert result["conversion_value"] == 100.0
        assert result["premium_rate"] == 20.0

    def test_calculate_conversion_value_no_premium(self):
        """Test conversion value when bond trades at par"""
        adapter = AkShareAdapter()
        result = adapter.calculate_conversion_value(
            bond_price=100.0,
            conversion_ratio=10.0,
            stock_price=10.0,
        )

        assert result["conversion_value"] == 100.0
        assert result["premium_rate"] == 0.0

    def test_calculate_conversion_value_negative_stock(self):
        """Test conversion value calculation with negative stock price"""
        adapter = AkShareAdapter()
        result = adapter.calculate_conversion_value(
            bond_price=100.0,
            conversion_ratio=10.0,
            stock_price=-10.0,
        )

        assert result["conversion_value"] == -100.0
        # premium_rate = (100 - (-100)) / (-100) * 100 = -200.0
        assert result["premium_rate"] == -200.0


class TestBlackScholesPrice:
    """Test black_scholes_price method"""

    def test_black_scholes_call(self):
        """Test Black-Scholes call option price"""
        adapter = AkShareAdapter()
        try:
            result = adapter.black_scholes_price(
                S=100.0,
                K=100.0,
                T=1.0,
                r=0.05,
                sigma=0.2,
                option_type="call",
            )
            assert 9.0 < result < 11.0
        except DataSourceError:
            pytest.skip("scipy/numpy not available")

    def test_black_scholes_put(self):
        """Test Black-Scholes put option price"""
        adapter = AkShareAdapter()
        try:
            result = adapter.black_scholes_price(
                S=100.0,
                K=100.0,
                T=1.0,
                r=0.05,
                sigma=0.2,
                option_type="put",
            )
            assert 5.0 < result < 7.0
        except DataSourceError:
            pytest.skip("scipy/numpy not available")

    def test_black_scholes_invalid_option_type(self):
        """Test Black-Scholes with invalid option type"""
        adapter = AkShareAdapter()
        with pytest.raises(DataSourceError):
            adapter.black_scholes_price(
                S=100.0,
                K=100.0,
                T=1.0,
                r=0.05,
                sigma=0.2,
                option_type="invalid",
            )

    def test_black_scholes_invalid_params(self):
        """Test Black-Scholes with invalid parameters"""
        adapter = AkShareAdapter()
        with pytest.raises(DataSourceError):
            adapter.black_scholes_price(
                S=0,
                K=100.0,
                T=1.0,
                r=0.05,
                sigma=0.2,
                option_type="call",
            )


class TestFindDateColumn:
    """Test find_date_column module-level function"""

    def test_find_date_column_existing(self):
        """Test finding existing date columns"""
        df = pd.DataFrame(
            {
                "trade_date": ["2024-01-01"],
                "close": [10.0],
            }
        )
        assert find_date_column(df) == "trade_date"

    def test_find_date_column_datetime(self):
        """Test finding datetime column"""
        df = pd.DataFrame(
            {
                "datetime": ["2024-01-01"],
                "close": [10.0],
            }
        )
        assert find_date_column(df) == "datetime"

    def test_find_date_column_date(self):
        """Test finding date column"""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "close": [10.0],
            }
        )
        assert find_date_column(df) == "date"

    def test_find_date_column_chinese(self):
        """Test finding Chinese date columns"""
        df = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "close": [10.0],
            }
        )
        assert find_date_column(df) == "日期"

    def test_find_date_column_none_found(self):
        """Test when no date column is found"""
        df = pd.DataFrame(
            {
                "symbol": ["600000"],
                "close": [10.0],
            }
        )
        assert find_date_column(df) == "symbol"

    def test_find_date_column_empty_dataframe(self):
        """Test with empty DataFrame"""
        df = pd.DataFrame()
        assert find_date_column(df) == ""


class TestHealthCheck:
    """Test health_check method"""

    def test_health_check_returns_dict(self):
        """Test that health_check returns a dictionary"""
        adapter = AkShareAdapter()
        result = adapter.health_check()
        assert isinstance(result, dict)

    def test_health_check_has_required_keys(self):
        """Test that health_check has required keys"""
        adapter = AkShareAdapter()
        result = adapter.health_check()
        assert "status" in result
        assert "akshare_available" in result
        assert "cache_enabled" in result


class TestGetSourceInfo:
    """Test get_source_info method"""

    def test_get_source_info_returns_dict(self):
        """Test that get_source_info returns a dictionary"""
        adapter = AkShareAdapter()
        result = adapter.get_source_info()
        assert isinstance(result, dict)

    def test_get_source_info_has_required_keys(self):
        """Test that get_source_info has required keys"""
        adapter = AkShareAdapter()
        result = adapter.get_source_info()
        assert "name" in result
        assert "type" in result
        assert "description" in result
        assert "akshare_available" in result
        assert "cache_enabled" in result
        assert "offline_mode" in result
        assert "data_sources" in result

    def test_get_source_info_correct_values(self):
        """Test that get_source_info returns correct values"""
        adapter = AkShareAdapter(
            use_cache=False,
            offline_mode=True,
            data_sources=["sina", "baostock"],
        )
        result = adapter.get_source_info()
        assert result["name"] == "akshare"
        assert result["type"] == "real"
        assert result["cache_enabled"] is False
        assert result["offline_mode"] is True
        assert result["data_sources"] == ["sina", "baostock"]


class TestRecordMethods:
    """Test record methods"""

    def test_record_request(self):
        """Test _record_request doesn't raise"""
        adapter = AkShareAdapter()
        adapter._record_request("test_source", 100.0, True)
        adapter._record_request("test_source", 100.0, False, error_type="TimeoutError")

    def test_record_cache_hit(self):
        """Test _record_cache_hit doesn't raise"""
        adapter = AkShareAdapter()
        adapter._record_cache_hit("test_cache")

    def test_record_cache_miss(self):
        """Test _record_cache_miss doesn't raise"""
        adapter = AkShareAdapter()
        adapter._record_cache_miss("test_cache")


class TestSourceUnavailableError:
    """Test error handling when akshare is unavailable"""

    def test_get_daily_data_raises_when_unavailable(self):
        """Test get_daily_data raises SourceUnavailableError when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(SourceUnavailableError):
                adapter.get_daily_data("600000", "2024-01-01", "2024-01-10")

    def test_get_realtime_data_raises_when_unavailable(self):
        """Test get_realtime_data raises DataSourceError when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_realtime_data("600000")

    def test_get_index_list_raises_when_unavailable(self):
        """Test get_index_list raises DataSourceError when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_index_list()


class TestOfflineMode:
    """Test offline mode behavior"""

    def test_get_daily_data_offline_mode_no_cache(self):
        """Test get_daily_data in offline mode without cache"""
        adapter = AkShareAdapter(offline_mode=True, use_cache=False)
        with pytest.raises(SourceUnavailableError):
            adapter.get_daily_data("600000", "2024-01-01", "2024-01-10")

    def test_get_etf_daily_offline_mode_no_cache(self):
        """Test get_etf_daily in offline mode without cache"""
        adapter = AkShareAdapter(offline_mode=True, use_cache=False)
        with pytest.raises(SourceUnavailableError):
            adapter.get_etf_daily("510300", "2024-01-01", "2024-01-10")

    def test_get_index_daily_offline_mode_no_cache(self):
        """Test get_index_daily in offline mode without cache"""
        adapter = AkShareAdapter(offline_mode=True, use_cache=False)
        with pytest.raises(SourceUnavailableError):
            adapter.get_index_daily("000001", "2024-01-01", "2024-01-10")

    def test_get_futures_hist_data_offline_mode_no_cache(self):
        """Test get_futures_hist_data in offline mode without cache"""
        adapter = AkShareAdapter(offline_mode=True, use_cache=False)
        with pytest.raises(SourceUnavailableError):
            adapter.get_futures_hist_data("IF", "2024-01-01", "2024-01-10")


class TestGetSecurityInfo:
    """Test get_security_info method"""

    def test_get_security_info_returns_dict(self):
        """Test get_security_info returns dictionary"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            result = adapter.get_security_info("600000")
            assert isinstance(result, dict)
            assert result["code"] == "600000"

    def test_get_security_info_returns_unknown_type(self):
        """Test get_security_info returns unknown type when unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            result = adapter.get_security_info("600000")
            assert result["type"] == "unknown"


class TestGetSourceUnavailableErrorPaths:
    """Test various error paths"""

    def test_get_money_flow_unavailable(self):
        """Test get_money_flow when no sources available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_money_flow("600000")

    def test_get_north_money_flow_unavailable(self):
        """Test get_north_money_flow when no sources available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_north_money_flow()

    def test_get_industry_stocks_unavailable(self):
        """Test get_industry_stocks when no sources available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_industry_stocks("801010")

    def test_get_industry_mapping_unavailable(self):
        """Test get_industry_mapping when no sources available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            result = adapter.get_industry_mapping("600000")
            assert result == ""

    def test_get_finance_indicator_unavailable(self):
        """Test get_finance_indicator when no sources available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_finance_indicator("600000")

    def test_get_call_auction_unavailable(self):
        """Test get_call_auction when not available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_market_data_loaded", True):
            with patch.object(adapter, "_get_call_auction", None):
                with pytest.raises(DataSourceError):
                    adapter.get_call_auction("600000")


class TestAdapterStats:
    """Test adapter stats collection"""

    def test_stats_collector_initialized(self):
        """Test that stats collector is initialized"""
        adapter = AkShareAdapter()
        assert adapter._stats is not None

    def test_get_trading_days_unavailable(self):
        """Test get_trading_days when no sources available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_trading_days()


class TestGetStStocks:
    """Test get_st_stocks method"""

    def test_get_st_stocks_unavailable(self):
        """Test get_st_stocks when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_st_stocks()


class TestGetSuspendedStocks:
    """Test get_suspended_stocks method"""

    def test_get_suspended_stocks_unavailable(self):
        """Test get_suspended_stocks when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_suspended_stocks()


class TestSecuritiesList:
    """Test get_securities_list method"""

    def test_get_securities_list_unavailable(self):
        """Test get_securities_list when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(SourceUnavailableError):
                adapter.get_securities_list("stock")


class TestSectorFundFlow:
    """Test sector fund flow methods"""

    def test_get_sector_fund_flow_unavailable(self):
        """Test get_sector_fund_flow when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_sector_fund_flow()

    def test_get_sector_fund_flow_invalid_type(self):
        """Test get_sector_fund_flow with invalid sector type"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", True):
            with patch.object(adapter, "_akshare", MagicMock()):
                with pytest.raises(DataSourceError):
                    adapter.get_sector_fund_flow(sector_type="invalid")


class TestMainFundFlowRank:
    """Test get_main_fund_flow_rank method"""

    def test_get_main_fund_flow_rank_unavailable(self):
        """Test get_main_fund_flow_rank when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_main_fund_flow_rank()


class TestConvertBond:
    """Test convert bond methods"""

    def test_get_convert_bond_list_unavailable(self):
        """Test get_convert_bond_list when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_convert_bond_list()

    def test_get_convert_bond_info_unavailable(self):
        """Test get_convert_bond_info when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_convert_bond_info("113050")


class TestFuturesMethods:
    """Test futures-related methods"""

    def test_get_futures_realtime_data_unavailable(self):
        """Test get_futures_realtime_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_futures_realtime_data("IF")

    def test_get_futures_main_contracts_unavailable(self):
        """Test get_futures_main_contracts when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_futures_main_contracts()


class TestNorthboundMethods:
    """Test northbound-related methods"""

    def test_get_northbound_holdings_unavailable(self):
        """Test get_northbound_holdings when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_northbound_holdings()

    def test_get_northbound_top_stocks_unavailable(self):
        """Test get_northbound_top_stocks when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_northbound_top_stocks("2024-01-01")


class TestNewsAndSpecialData:
    """Test news and special data methods"""

    def test_get_news_data_unavailable(self):
        """Test get_news_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_news_data()

    def test_get_dragon_tiger_list_unavailable(self):
        """Test get_dragon_tiger_list when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_dragon_tiger_list("2024-01-01")

    def test_get_limit_up_pool_unavailable(self):
        """Test get_limit_up_pool when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_limit_up_pool("2024-01-01")

    def test_get_limit_down_pool_unavailable(self):
        """Test get_limit_down_pool when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_limit_down_pool("2024-01-01")


class TestMarginData:
    """Test margin data methods"""

    def test_get_margin_data_unavailable(self):
        """Test get_margin_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_margin_data("2024-01-01")

    def test_get_margin_summary_unavailable(self):
        """Test get_margin_summary when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_margin_summary()


class TestMacroData:
    """Test macroeconomic data methods"""

    def test_get_lpr_rate_unavailable(self):
        """Test get_lpr_rate when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_lpr_rate()

    def test_get_pmi_index_unavailable(self):
        """Test get_pmi_index when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_pmi_index()

    def test_get_cpi_data_unavailable(self):
        """Test get_cpi_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_cpi_data()

    def test_get_ppi_data_unavailable(self):
        """Test get_ppi_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_ppi_data()

    def test_get_m2_supply_unavailable(self):
        """Test get_m2_supply when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_m2_supply()


class TestOptionsMethods:
    """Test options-related methods"""

    def test_get_options_chain_unavailable(self):
        """Test get_options_chain when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_options_chain("600000")

    def test_get_options_realtime_data_unavailable(self):
        """Test get_options_realtime_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_options_realtime_data("OP1000")

    def test_get_options_expirations_unavailable(self):
        """Test get_options_expirations when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_options_expirations("600000")

    def test_get_options_hist_data_unavailable(self):
        """Test get_options_hist_data when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_options_hist_data("OP1000", "2024-01-01", "2024-01-10")

    def test_get_option_greeks_unavailable_scipy(self):
        """Test get_option_greeks when scipy not available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_scipy_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_option_greeks("OP1000", "2024-01-01")

    def test_calculate_option_implied_vol_unavailable_scipy(self):
        """Test calculate_option_implied_vol when scipy not available"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_scipy_available", False):
            with pytest.raises(DataSourceError):
                adapter.calculate_option_implied_vol(
                    "OP1000", 10.0, 100.0, "2024-06-01", "call"
                )


class TestFinancialMethods:
    """Test financial statement methods"""

    def test_get_balance_sheet_not_implemented(self):
        """Test get_balance_sheet raises NotImplementedError for akshare."""
        adapter = AkShareAdapter()
        with pytest.raises(NotImplementedError):
            adapter.get_balance_sheet("600000")

    def test_get_income_statement_not_implemented(self):
        """Test get_income_statement raises NotImplementedError for akshare."""
        adapter = AkShareAdapter()
        with pytest.raises(NotImplementedError):
            adapter.get_income_statement("600000")

    def test_get_cash_flow_not_implemented(self):
        """Test get_cash_flow raises NotImplementedError for akshare."""
        adapter = AkShareAdapter()
        with pytest.raises(NotImplementedError):
            adapter.get_cash_flow("600000")

    def test_get_basic_info_unavailable(self):
        """Test get_basic_info when akshare unavailable"""
        adapter = AkShareAdapter()
        with patch.object(adapter, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                adapter.get_basic_info("600000")
