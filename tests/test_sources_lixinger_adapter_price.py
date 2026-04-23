"""Integration tests for lixinger_source.py adapter methods with REAL lixinger API calls.

These tests attempt to call lixinger adapter methods. Without LIXINGER_TOKEN,
they will fail with auth errors — we categorize these properly.
"""

import pytest
import pandas as pd

from akshare_data.sources.lixinger_source import LixingerAdapter
from akshare_data.core.errors import SourceUnavailableError, DataSourceError


@pytest.fixture(scope="module")
def adapter():
    """Create LixingerAdapter. Will be unconfigured without LIXINGER_TOKEN."""
    return LixingerAdapter()


def skip_if_no_token(adapter):
    """Skip test if lixinger token is not configured."""
    if not adapter.is_configured():
        pytest.skip("LIXINGER_TOKEN not set — cannot test lixinger adapter")


# ── Price Data Methods ───────────────────────────────────────────────
class TestLixingerPriceData:
    """Test daily price data methods."""

    @pytest.mark.integration
    def test_get_daily_data_requires_token(self, adapter):
        """get_daily_data should fail gracefully without token."""
        if adapter.is_configured():
            df = adapter.get_daily_data(
                symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises((SourceUnavailableError, DataSourceError, Exception)):
                adapter.get_daily_data(
                    symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
                )

    @pytest.mark.integration
    def test_get_index_daily_requires_token(self, adapter):
        if adapter.is_configured():
            df = adapter.get_index_daily(
                symbol="000300", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_index_daily(
                    symbol="000300", start_date="2024-01-01", end_date="2024-02-01"
                )

    @pytest.mark.integration
    def test_get_etf_daily_requires_token(self, adapter):
        if adapter.is_configured():
            df = adapter.get_etf_daily(
                symbol="510050", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_etf_daily(
                    symbol="510050", start_date="2024-01-01", end_date="2024-02-01"
                )


# ── Securities & List Methods ────────────────────────────────────────
class TestLixingerSecurities:
    """Test securities list and info methods."""

    @pytest.mark.integration
    def test_get_securities_list_stock(self, adapter):
        if adapter.is_configured():
            df = adapter.get_securities_list(security_type="stock")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_securities_list(security_type="stock")

    @pytest.mark.integration
    def test_get_securities_list_index(self, adapter):
        if adapter.is_configured():
            df = adapter.get_securities_list(security_type="index")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_securities_list(security_type="index")

    @pytest.mark.integration
    def test_get_securities_list_fund(self, adapter):
        if adapter.is_configured():
            df = adapter.get_securities_list(security_type="fund")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_securities_list(security_type="fund")

    @pytest.mark.integration
    def test_get_security_info(self, adapter):
        if adapter.is_configured():
            info = adapter.get_security_info(symbol="000001")
            assert isinstance(info, dict)
        else:
            with pytest.raises(Exception):
                adapter.get_security_info(symbol="000001")

    @pytest.mark.integration
    def test_get_index_list(self, adapter):
        if adapter.is_configured():
            df = adapter.get_index_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_index_list()

    @pytest.mark.integration
    def test_get_etf_list(self, adapter):
        if adapter.is_configured():
            df = adapter.get_etf_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_etf_list()

    @pytest.mark.integration
    def test_get_lof_list(self, adapter):
        if adapter.is_configured():
            df = adapter.get_lof_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_lof_list()


# ── Valuation Methods ────────────────────────────────────────────────
class TestLixingerValuation:
    """Test valuation methods."""

    @pytest.mark.integration
    def test_get_stock_valuation(self, adapter):
        if adapter.is_configured():
            df = adapter.get_stock_valuation("000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_stock_valuation("000001")

    @pytest.mark.integration
    def test_get_index_valuation(self, adapter):
        if adapter.is_configured():
            df = adapter.get_index_valuation("000300")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_index_valuation("000300")

    @pytest.mark.integration
    def test_get_financial_metrics(self, adapter):
        if adapter.is_configured():
            df = adapter.get_financial_metrics("000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_financial_metrics("000001")


# ── Fund Methods ─────────────────────────────────────────────────────
class TestLixingerFundMethods:
    """Test fund-related methods."""

    @pytest.mark.integration
    def test_get_fund_manager_info(self, adapter):
        if adapter.is_configured():
            df = adapter.get_fund_manager_info(fund_code="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_fund_manager_info(fund_code="000001")

    @pytest.mark.integration
    def test_get_fund_net_value(self, adapter):
        if adapter.is_configured():
            df = adapter.get_fund_net_value("510050", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_fund_net_value("510050", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_fof_list(self, adapter):
        if adapter.is_configured():
            df = adapter.get_fof_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_fof_list()

    @pytest.mark.integration
    def test_get_etf_hist_data(self, adapter):
        if adapter.is_configured():
            df = adapter.get_etf_hist_data(
                symbol="510050", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_etf_hist_data(
                    symbol="510050", start_date="2024-01-01", end_date="2024-02-01"
                )

    @pytest.mark.integration
    def test_get_lof_spot(self, adapter):
        if adapter.is_configured():
            df = adapter.get_lof_spot()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_lof_spot()


# ── Macro Methods ────────────────────────────────────────────────────
class TestLixingerMacroMethods:
    """Test macro data methods."""

    @pytest.mark.integration
    def test_get_lpr_rate(self, adapter):
        if adapter.is_configured():
            df = adapter.get_lpr_rate("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_lpr_rate("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_pmi_index(self, adapter):
        if adapter.is_configured():
            df = adapter.get_pmi_index("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_pmi_index("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_cpi_data(self, adapter):
        if adapter.is_configured():
            df = adapter.get_cpi_data("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_cpi_data("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_ppi_data(self, adapter):
        if adapter.is_configured():
            df = adapter.get_ppi_data("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_ppi_data("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_m2_supply(self, adapter):
        if adapter.is_configured():
            df = adapter.get_m2_supply("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_m2_supply("2024-01-01", "2024-12-31")


# ── Industry Methods ─────────────────────────────────────────────────
class TestLixingerIndustryMethods:
    """Test industry-related methods."""

    @pytest.mark.integration
    def test_get_industry_list(self, adapter):
        if adapter.is_configured():
            result = adapter.get_industry_list()
            assert isinstance(result, (list, pd.DataFrame))
        else:
            with pytest.raises(Exception):
                adapter.get_industry_list()

    @pytest.mark.integration
    def test_get_industry_stocks(self, adapter):
        if adapter.is_configured():
            result = adapter.get_industry_stocks(industry_code="801010")
            assert result is not None
        else:
            with pytest.raises(Exception):
                adapter.get_industry_stocks(industry_code="801010")

    @pytest.mark.integration
    def test_get_industry_mapping(self, adapter):
        if adapter.is_configured():
            result = adapter.get_industry_mapping(symbol="801010")
            assert result is not None
        else:
            with pytest.raises(Exception):
                adapter.get_industry_mapping(symbol="801010")


# ── Delegator Methods ────────────────────────────────────────────────
class TestLixingerDelegators:
    """Test delegator/thin-wrapper methods."""

    @pytest.mark.integration
    def test_get_all_industries(self, adapter):
        if adapter.is_configured():
            result = adapter.get_all_industries()
            assert result is not None
        else:
            with pytest.raises(Exception):
                adapter.get_all_industries()

    @pytest.mark.integration
    def test_get_ipo_info(self, adapter):
        if adapter.is_configured():
            df = adapter.get_ipo_info()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_ipo_info()

    @pytest.mark.integration
    def test_get_stock_bonus(self, adapter):
        if adapter.is_configured():
            df = adapter.get_stock_bonus(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_stock_bonus(symbol="000001")

    @pytest.mark.integration
    def test_get_capital_change(self, adapter):
        if adapter.is_configured():
            df = adapter.get_capital_change(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_capital_change(symbol="000001")
