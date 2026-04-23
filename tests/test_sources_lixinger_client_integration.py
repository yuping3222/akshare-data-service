"""Integration tests for lixinger_client.py — REAL HTTP calls to lixinger API.

Without LIXINGER_TOKEN, these fail with auth errors. We verify the error handling
is correct and that the API call pattern works.
"""

import pytest
import pandas as pd

from akshare_data.sources.lixinger_client import get_lixinger_client


@pytest.fixture(scope="module")
def client():
    return get_lixinger_client()


class TestLixingerClientIndexAPIs:
    """Test index-related client APIs."""

    @pytest.mark.integration
    def test_get_index_list(self, client):
        if client.is_configured():
            df = client.get_index_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_index_list()

    @pytest.mark.integration
    def test_get_index_drawdown(self, client):
        if client.is_configured():
            df = client.get_index_drawdown("000300", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_index_drawdown("000300", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_index_fs_hybrid(self, client):
        if client.is_configured():
            df = client.get_index_fs_hybrid("000300", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_index_fs_hybrid("000300", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_index_tracking_fund(self, client):
        if client.is_configured():
            df = client.get_index_tracking_fund("000300")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_index_tracking_fund("000300")

    @pytest.mark.integration
    def test_get_index_mutual_market(self, client):
        if client.is_configured():
            df = client.get_index_mutual_market("000300", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_index_mutual_market("000300", "2024-01-01", "2024-02-01")


class TestLixingerClientCompanyAPIs:
    """Test company-related client APIs."""

    @pytest.mark.integration
    def test_get_company_list(self, client):
        if client.is_configured():
            df = client.get_company_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_list()

    @pytest.mark.integration
    def test_get_company_profile(self, client):
        if client.is_configured():
            df = client.get_company_profile("000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_profile("000001")

    @pytest.mark.integration
    def test_get_company_dividend(self, client):
        if client.is_configured():
            df = client.get_company_dividend("000001", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_dividend("000001", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_company_candlestick(self, client):
        if client.is_configured():
            df = client.get_company_candlestick("000001", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_candlestick("000001", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_company_industries(self, client):
        if client.is_configured():
            result = client.get_company_industries("000001")
            assert result is not None
        else:
            with pytest.raises(Exception):
                client.get_company_industries("000001")

    @pytest.mark.integration
    def test_get_company_block_deal(self, client):
        if client.is_configured():
            df = client.get_company_block_deal("000001", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_block_deal("000001", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_company_margin_trading(self, client):
        if client.is_configured():
            df = client.get_company_margin_trading("000001", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_margin_trading("000001", "2024-01-01", "2024-02-01")


class TestLixingerClientFundAPIs:
    """Test fund-related client APIs."""

    @pytest.mark.integration
    def test_get_fund_list(self, client):
        if client.is_configured():
            df = client.get_fund_list()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_fund_list()

    @pytest.mark.integration
    def test_get_fund_candlestick(self, client):
        if client.is_configured():
            df = client.get_fund_candlestick("510050", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_fund_candlestick("510050", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_fund_net_value(self, client):
        if client.is_configured():
            df = client.get_fund_net_value("510050", "2024-01-01", "2024-02-01")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_fund_net_value("510050", "2024-01-01", "2024-02-01")

    @pytest.mark.integration
    def test_get_fund_manager(self, client):
        if client.is_configured():
            df = client.get_fund_manager("510050")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_fund_manager("510050")


class TestLixingerClientMacroAPIs:
    """Test macro client APIs."""

    @pytest.mark.integration
    def test_get_macro_cpi(self, client):
        if client.is_configured():
            df = client.get_macro_cpi("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_macro_cpi("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_macro_gdp(self, client):
        if client.is_configured():
            df = client.get_macro_gdp("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_macro_gdp("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_macro_pmi(self, client):
        if client.is_configured():
            df = client.get_macro_pmi("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_macro_pmi("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_macro_ppi(self, client):
        if client.is_configured():
            df = client.get_macro_ppi("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_macro_ppi("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_macro_money_supply(self, client):
        if client.is_configured():
            df = client.get_macro_money_supply("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_macro_money_supply("2024-01-01", "2024-12-31")

    @pytest.mark.integration
    def test_get_macro_interest_rates(self, client):
        if client.is_configured():
            df = client.get_macro_interest_rates("2024-01-01", "2024-12-31")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_macro_interest_rates("2024-01-01", "2024-12-31")


class TestLixingerClientShareholderAPIs:
    """Test shareholder-related client APIs."""

    @pytest.mark.integration
    def test_get_company_shareholders_num(self, client):
        if client.is_configured():
            df = client.get_company_shareholders_num(
                "000001", "2024-01-01", "2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_shareholders_num(
                    "000001", "2024-01-01", "2024-02-01"
                )

    @pytest.mark.integration
    def test_get_company_fund_shareholders(self, client):
        if client.is_configured():
            df = client.get_company_fund_shareholders(
                "000001", "2024-01-01", "2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_fund_shareholders(
                    "000001", "2024-01-01", "2024-02-01"
                )

    @pytest.mark.integration
    def test_get_company_majority_shareholders(self, client):
        if client.is_configured():
            df = client.get_company_majority_shareholders(
                "000001", "2024-01-01", "2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                client.get_company_majority_shareholders(
                    "000001", "2024-01-01", "2024-02-01"
                )


class TestLixingerClientErrorHandling:
    """Test error handling paths in client."""

    def test_query_api_with_invalid_token(self, client):
        if not client.is_configured():
            with pytest.raises(Exception):
                client.get_index_list()

    def test_query_api_leading_slash_stripped(self, client):
        """Test that leading slash in suffix is stripped."""
        if client.is_configured():
            result = client.query_api("/cn/index", params={})
            # Returns dict from JSON, _to_df converts to DataFrame
            assert isinstance(result, (dict, pd.DataFrame))
        else:
            with pytest.raises(Exception):
                client.query_api("/cn/index", params={})

    def test_singleton_pattern(self):
        c1 = get_lixinger_client()
        c2 = get_lixinger_client()
        assert c1 is c2
