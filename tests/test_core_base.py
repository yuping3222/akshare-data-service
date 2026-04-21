"""Tests for akshare_data.core.base module.

Covers:
- DataSource abstract base class
- All abstract methods
- Default implementations
- health_check method
- get_source_info method
"""

import pytest
import pandas as pd
from abc import ABC
from akshare_data.core.base import DataSource


class TestDataSourceIsAbstract:
    """Test that DataSource is an abstract class."""

    def test_is_abc(self):
        """DataSource should be abstract base class."""
        assert issubclass(DataSource, ABC)

    def test_cannot_instantiate_directly(self):
        """Should not be able to instantiate DataSource directly."""
        with pytest.raises(TypeError):
            DataSource()


class ConcreteDataSource(DataSource):
    """Concrete implementation of DataSource for testing."""

    name = "test"
    source_type = "test"

    def get_daily_data(self, symbol, start_date, end_date, adjust="qfq", **kwargs):
        return pd.DataFrame()

    def get_index_stocks(self, index_code, **kwargs):
        return []

    def get_index_components(self, index_code, include_weights=True, **kwargs):
        return pd.DataFrame()

    def get_trading_days(self, start_date=None, end_date=None, **kwargs):
        return []

    def get_securities_list(self, security_type="stock", date=None, **kwargs):
        return pd.DataFrame()

    def get_security_info(self, symbol, **kwargs):
        return {}

    def get_minute_data(
        self, symbol, freq="1min", start_date=None, end_date=None, **kwargs
    ):
        return pd.DataFrame()

    def get_money_flow(self, symbol, start_date=None, end_date=None, **kwargs):
        return pd.DataFrame()

    def get_north_money_flow(self, start_date=None, end_date=None, **kwargs):
        return pd.DataFrame()

    def get_industry_stocks(self, industry_code, level=1, **kwargs):
        return []

    def get_industry_mapping(self, symbol, level=1, **kwargs):
        return ""

    def get_finance_indicator(
        self, symbol, fields=None, start_date=None, end_date=None, **kwargs
    ):
        return pd.DataFrame()

    def get_call_auction(self, symbol, date=None, **kwargs):
        return pd.DataFrame()


class TestDataSourceAttributes:
    """Test DataSource class attributes."""

    def test_has_name_attribute(self):
        """Should have name class attribute."""
        assert hasattr(DataSource, "name")

    def test_has_source_type_attribute(self):
        """Should have source_type class attribute."""
        assert hasattr(DataSource, "source_type")


class TestDataSourceAbstractMethods:
    """Test that all abstract methods are defined."""

    def test_get_daily_data_is_abstract(self):
        """get_daily_data should be abstract."""
        ds = ConcreteDataSource()
        # Should be implemented
        result = ds.get_daily_data("600519", "2023-01-01", "2023-12-31")
        assert isinstance(result, pd.DataFrame)

    def test_get_index_stocks_is_abstract(self):
        """get_index_stocks should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_index_stocks("000300.XSHG")
        assert isinstance(result, list)

    def test_get_index_components_is_abstract(self):
        """get_index_components should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_index_components("000300.XSHG")
        assert isinstance(result, pd.DataFrame)

    def test_get_trading_days_is_abstract(self):
        """get_trading_days should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_trading_days()
        assert isinstance(result, list)

    def test_get_securities_list_is_abstract(self):
        """get_securities_list should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_securities_list()
        assert isinstance(result, pd.DataFrame)

    def test_get_security_info_is_abstract(self):
        """get_security_info should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_security_info("600519")
        assert isinstance(result, dict)

    def test_get_minute_data_is_abstract(self):
        """get_minute_data should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_minute_data("600519")
        assert isinstance(result, pd.DataFrame)

    def test_get_money_flow_is_abstract(self):
        """get_money_flow should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_money_flow("600519")
        assert isinstance(result, pd.DataFrame)

    def test_get_north_money_flow_is_abstract(self):
        """get_north_money_flow should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_north_money_flow()
        assert isinstance(result, pd.DataFrame)

    def test_get_industry_stocks_is_abstract(self):
        """get_industry_stocks should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_industry_stocks("801010")
        assert isinstance(result, list)

    def test_get_industry_mapping_is_abstract(self):
        """get_industry_mapping should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_industry_mapping("600519")
        assert isinstance(result, str)

    def test_get_finance_indicator_is_abstract(self):
        """get_finance_indicator should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_finance_indicator("600519")
        assert isinstance(result, pd.DataFrame)

    def test_get_call_auction_is_abstract(self):
        """get_call_auction should be abstract."""
        ds = ConcreteDataSource()
        result = ds.get_call_auction("600519")
        assert isinstance(result, pd.DataFrame)


class TestDataSourceDefaultImplementations:
    """Test DataSource default method implementations."""

    def setup_method(self):
        """Create concrete implementation for testing."""
        self.ds = ConcreteDataSource()

    def test_get_etf_daily_uses_get_daily_data(self):
        """get_etf_daily should use get_daily_data."""
        result = self.ds.get_etf_daily("510300", "2023-01-01", "2023-12-31")
        assert isinstance(result, pd.DataFrame)

    def test_get_index_daily_uses_get_daily_data(self):
        """get_index_daily should use get_daily_data."""
        result = self.ds.get_index_daily("000300", "2023-01-01", "2023-12-31")
        assert isinstance(result, pd.DataFrame)

    def test_get_lof_daily_uses_get_daily_data(self):
        """get_lof_daily should use get_daily_data."""
        result = self.ds.get_lof_daily("000001", "2023-01-01", "2023-12-31")
        assert isinstance(result, pd.DataFrame)

    def test_get_conversion_bond_list_raises_not_implemented(self):
        """get_conversion_bond_list should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_conversion_bond_list()

    def test_get_option_list_raises_not_implemented(self):
        """get_option_list should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_option_list()

    def test_get_option_daily_raises_not_implemented(self):
        """get_option_daily should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_option_daily("600519", "2023-01-01", "2023-12-31")

    def test_get_st_stocks_raises_not_implemented(self):
        """get_st_stocks should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_st_stocks()

    def test_get_suspended_stocks_raises_not_implemented(self):
        """get_suspended_stocks should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_suspended_stocks()

    def test_get_index_valuation_raises_not_implemented(self):
        """get_index_valuation should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_index_valuation("000300")

    def test_get_stock_valuation_raises_not_implemented(self):
        """get_stock_valuation should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_stock_valuation("600519")

    def test_get_stock_pe_pb_raises_not_implemented(self):
        """get_stock_pe_pb should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_stock_pe_pb("600519")

    def test_get_margin_detail_raises_not_implemented(self):
        """get_margin_detail should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_margin_detail("sh", "2023-01-01")

    def test_get_margin_underlying_raises_not_implemented(self):
        """get_margin_underlying should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_margin_underlying("sh")

    def test_get_macro_raw_raises_not_implemented(self):
        """get_macro_raw should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_macro_raw("gdp")

    def test_get_top10_holders_raises_not_implemented(self):
        """get_top10_holders should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_top10_holders("600519")

    def test_get_top10_float_holders_raises_not_implemented(self):
        """get_top10_float_holders should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_top10_float_holders("600519")

    def test_get_holder_count_raises_not_implemented(self):
        """get_holder_count should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_holder_count("600519")

    def test_get_institutional_holders_raises_not_implemented(self):
        """get_institutional_holders should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_institutional_holders("600519")

    def test_get_financial_report_raises_not_implemented(self):
        """get_financial_report should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_financial_report("600519", "利润表")

    def test_get_financial_benefit_raises_not_implemented(self):
        """get_financial_benefit should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_financial_benefit("600519", "indicator")

    def test_get_cashflow_raises_not_implemented(self):
        """get_cashflow should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_cashflow("600519")

    def test_get_dividend_raises_not_implemented(self):
        """get_dividend should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_dividend("600519")

    def test_get_share_change_raises_not_implemented(self):
        """get_share_change should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_share_change("600519")

    def test_get_unlock_schedule_raises_not_implemented(self):
        """get_unlock_schedule should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_unlock_schedule("600519")

    def test_get_spot_em_raises_not_implemented(self):
        """get_spot_em should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_spot_em()

    def test_get_stock_hist_raises_not_implemented(self):
        """get_stock_hist should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_stock_hist("600519")

    def test_get_trade_dates_raises_not_implemented(self):
        """get_trade_dates should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_trade_dates()

    def test_get_securities_code_name_raises_not_implemented(self):
        """get_securities_code_name should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_securities_code_name()

    def test_get_bond_yield_raises_not_implemented(self):
        """get_bond_yield should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_bond_yield("600519")

    def test_get_industry_list_raises_not_implemented(self):
        """get_industry_list should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_industry_list()

    def test_get_industry_components_raises_not_implemented(self):
        """get_industry_components should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_industry_components("industry_name")

    def test_get_concept_list_raises_not_implemented(self):
        """get_concept_list should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_concept_list()

    def test_get_concept_components_raises_not_implemented(self):
        """get_concept_components should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_concept_components("concept_name")

    def test_get_sw_industry_raises_not_implemented(self):
        """get_sw_industry should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_sw_industry("1")

    def test_get_etf_hist_raises_not_implemented(self):
        """get_etf_hist should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_etf_hist("510300")

    def test_get_lof_hist_raises_not_implemented(self):
        """get_lof_hist should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_lof_hist("000001")

    def test_get_conversion_bond_daily_raises_not_implemented(self):
        """get_conversion_bond_daily should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_conversion_bond_daily("113009")

    def test_get_futures_daily_raises_not_implemented(self):
        """get_futures_daily should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_futures_daily("IF2301")

    def test_get_billboard_list_raises_not_implemented(self):
        """get_billboard_list should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_billboard_list("2023-01-01")

    def test_get_company_info_raises_not_implemented(self):
        """get_company_info should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_company_info("600519")

    def test_get_forecast_raises_not_implemented(self):
        """get_forecast should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.ds.get_forecast("600519")


class TestDataSourceHealthCheck:
    """Test health_check method."""

    def setup_method(self):
        """Create concrete implementation for testing."""
        self.ds = ConcreteDataSource()

    def test_health_check_returns_dict(self):
        """health_check should return dictionary."""
        result = self.ds.health_check()
        assert isinstance(result, dict)

    def test_health_check_contains_status(self):
        """health_check should contain status field."""
        result = self.ds.health_check()
        assert "status" in result

    def test_health_check_contains_latency_ms(self):
        """health_check should contain latency_ms field."""
        result = self.ds.health_check()
        assert "latency_ms" in result

    def test_health_check_contains_message(self):
        """health_check should contain message field."""
        result = self.ds.health_check()
        assert "message" in result


class TestDataSourceGetSourceInfo:
    """Test get_source_info method."""

    def setup_method(self):
        """Create concrete implementation for testing."""
        self.ds = ConcreteDataSource()

    def test_returns_dict(self):
        """get_source_info should return dictionary."""
        result = self.ds.get_source_info()
        assert isinstance(result, dict)

    def test_contains_name(self):
        """Should contain name field."""
        result = self.ds.get_source_info()
        assert "name" in result

    def test_contains_type(self):
        """Should contain type field."""
        result = self.ds.get_source_info()
        assert "type" in result

    def test_contains_description(self):
        """Should contain description field."""
        result = self.ds.get_source_info()
        assert "description" in result


class TestDataSourceNotImplementedExceptionMessages:
    """Test that NotImplementedError messages include source name."""

    def setup_method(self):
        """Create concrete implementation for testing."""
        self.ds = ConcreteDataSource()

    def test_get_conversion_bond_list_error_message(self):
        """Error message should include source name."""
        with pytest.raises(NotImplementedError) as exc_info:
            self.ds.get_conversion_bond_list()
        assert self.ds.name in str(exc_info.value)

    def test_get_option_list_error_message(self):
        """Error message should include source name."""
        with pytest.raises(NotImplementedError) as exc_info:
            self.ds.get_option_list()
        assert self.ds.name in str(exc_info.value)
