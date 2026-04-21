"""Integration tests for tushare_source.py with real Tushare API.

Without TUSHARE_TOKEN, these fail with auth errors.
"""
import os
import pytest
import pandas as pd

from akshare_data.sources.tushare_source import TushareAdapter


@pytest.fixture(scope="module")
def adapter():
    return TushareAdapter()


class TestTushareBasicData:
    """Test basic Tushare data methods."""

    @pytest.mark.integration
    def test_get_daily_data(self, adapter):
        if adapter.is_configured():
            df = adapter.get_daily_data(symbol="000001", start_date="20240101", end_date="20240201")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_daily_data(symbol="000001", start_date="20240101", end_date="20240201")

    @pytest.mark.integration
    def test_get_securities_list(self, adapter):
        if adapter.is_configured():
            df = adapter.get_securities_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_securities_list()

    @pytest.mark.integration
    def test_get_trading_days(self, adapter):
        if adapter.is_configured():
            df = adapter.get_trading_days(start_date="20240101", end_date="20240201")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_trading_days(start_date="20240101", end_date="20240201")

    @pytest.mark.integration
    def test_get_index_stocks(self, adapter):
        if adapter.is_configured():
            result = adapter.get_index_stocks("000300.XSHG")
            assert result is not None
        else:
            with pytest.raises(Exception):
                adapter.get_index_stocks("000300.XSHG")

    @pytest.mark.integration
    def test_get_security_info(self, adapter):
        if adapter.is_configured():
            info = adapter.get_security_info(symbol="000001")
            assert isinstance(info, dict)
        else:
            with pytest.raises(Exception):
                adapter.get_security_info(symbol="000001")


class TestTushareFinancialData:
    """Test Tushare financial data methods."""

    @pytest.mark.integration
    def test_get_finance_indicator(self, adapter):
        if adapter.is_configured():
            df = adapter.get_finance_indicator(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_finance_indicator(symbol="000001")

    @pytest.mark.integration
    def test_get_stock_pe_pb(self, adapter):
        if adapter.is_configured():
            df = adapter.get_stock_pe_pb(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_stock_pe_pb(symbol="000001")

    @pytest.mark.integration
    def test_get_dividend(self, adapter):
        if adapter.is_configured():
            df = adapter.get_dividend(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_dividend(symbol="000001")

    @pytest.mark.integration
    def test_get_financial_report(self, adapter):
        if adapter.is_configured():
            df = adapter.get_financial_report(symbol="000001", report_type="income")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_financial_report(symbol="000001", report_type="income")


class TestTushareMarginData:
    """Test Tushare margin data methods."""

    @pytest.mark.integration
    def test_get_margin_detail(self, adapter):
        if adapter.is_configured():
            df = adapter.get_margin_detail(market="SZ", date="20240102")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_margin_detail(market="SZ", date="20240102")

    @pytest.mark.integration
    def test_get_billboard_list(self, adapter):
        if adapter.is_configured():
            df = adapter.get_billboard_list(start_date="20240101", end_date="20240110")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_billboard_list(start_date="20240101", end_date="20240110")


class TestTushareMacroData:
    """Test Tushare macro data methods."""

    @pytest.mark.integration
    def test_get_macro_raw_cpi(self, adapter):
        if adapter.is_configured():
            df = adapter.get_macro_raw(indicator="cpi")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_macro_raw(indicator="cpi")


class TestTushareNotImplementedMethods:
    """Test methods that return empty/NotImplemented for Tushare."""

    def test_get_minute_data_returns_empty(self, adapter):
        """Tushare doesn't support minute data."""
        df = adapter.get_minute_data(symbol="000001")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_call_auction_returns_empty(self, adapter):
        df = adapter.get_call_auction(symbol="000001")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_get_industry_stocks_returns_empty(self, adapter):
        df = adapter.get_industry_stocks()
        assert isinstance(df, list)


class TestTushareSourceInfo:
    """Test source metadata."""

    def test_get_source_info(self, adapter):
        info = adapter.get_source_info()
        assert "name" in info
        assert info["name"] == "tushare"

    def test_health_check_configured(self, adapter):
        result = adapter.health_check()
        assert isinstance(result, dict)
        assert "status" in result
        if adapter.is_configured():
            assert result["status"] == "ok"
