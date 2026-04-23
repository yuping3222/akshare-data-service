"""Integration tests for lixinger_source.py financial statement methods."""

import pytest
import pandas as pd
from unittest.mock import MagicMock

from akshare_data.sources.lixinger_source import LixingerAdapter


@pytest.fixture(scope="module")
def adapter():
    return LixingerAdapter()


class TestLixingerFinancialStatements:
    """Test balance sheet, income statement, cash flow methods."""

    @pytest.mark.integration
    def test_get_balance_sheet(self, adapter):
        if adapter.is_configured():
            df = adapter.get_balance_sheet(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_balance_sheet(symbol="000001")

    @pytest.mark.integration
    def test_get_income_statement(self, adapter):
        if adapter.is_configured():
            df = adapter.get_income_statement(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_income_statement(symbol="000001")

    @pytest.mark.integration
    def test_get_cash_flow(self, adapter):
        if adapter.is_configured():
            df = adapter.get_cash_flow(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_cash_flow(symbol="000001")

    @pytest.mark.integration
    def test_get_combined_financial_statements(self, adapter):
        """Test _get_combined_financial_statements internal method."""
        if adapter.is_configured():
            df = adapter._get_combined_financial_statements(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter._get_combined_financial_statements(symbol="000001")


class TestLixingerShareholderMethods:
    """Test shareholder-related methods."""

    @pytest.mark.integration
    def test_get_shareholder_changes(self, adapter):
        if adapter.is_configured():
            df = adapter.get_shareholder_changes(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_shareholder_changes(symbol="000001")

    @pytest.mark.integration
    def test_get_top_shareholders(self, adapter):
        if adapter.is_configured():
            df = adapter.get_top_shareholders(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_top_shareholders(symbol="000001")

    @pytest.mark.integration
    def test_get_latest_holder_number(self, adapter):
        if adapter.is_configured():
            df = adapter.get_latest_holder_number(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_latest_holder_number(symbol="000001")

    @pytest.mark.integration
    def test_get_institution_holdings(self, adapter):
        if adapter.is_configured():
            df = adapter.get_institution_holdings(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_institution_holdings(symbol="000001")

    @pytest.mark.integration
    def test_get_topholder_change(self, adapter):
        if adapter.is_configured():
            df = adapter.get_topholder_change(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_topholder_change(symbol="000001")

    @pytest.mark.integration
    def test_get_major_holder_trade(self, adapter):
        if adapter.is_configured():
            df = adapter.get_major_holder_trade(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_major_holder_trade(symbol="000001")


class TestLixingerCorporateAction:
    """Test corporate action methods."""

    @pytest.mark.integration
    def test_get_dividend_data(self, adapter):
        if adapter.is_configured():
            df = adapter.get_dividend_data(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_dividend_data(symbol="000001")

    @pytest.mark.integration
    def test_get_equity_pledge(self, adapter):
        if adapter.is_configured():
            df = adapter.get_equity_pledge(
                symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_equity_pledge(
                    symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
                )

    @pytest.mark.integration
    def test_get_restricted_release(self, adapter):
        if adapter.is_configured():
            df = adapter.get_restricted_release(
                symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_restricted_release(
                    symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
                )

    @pytest.mark.integration
    def test_get_rights_issue(self, adapter):
        if adapter.is_configured():
            df = adapter.get_rights_issue(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_rights_issue(symbol="000001")

    @pytest.mark.integration
    def test_get_block_deal(self, adapter):
        if adapter.is_configured():
            df = adapter.get_block_deal(
                symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_block_deal(
                    symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
                )

    @pytest.mark.integration
    def test_get_margin_data(self, adapter):
        if adapter.is_configured():
            df = adapter.get_margin_data(
                symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
            )
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_margin_data(
                    symbol="000001", start_date="2024-01-01", end_date="2024-02-01"
                )


class TestLixingerHKUSMethods:
    """Test HK/US stock methods."""

    @pytest.mark.integration
    def test_get_hk_stocks(self, adapter):
        if adapter.is_configured():
            df = adapter.get_hk_stocks()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_hk_stocks()

    @pytest.mark.integration
    def test_get_us_stocks(self, adapter):
        if adapter.is_configured():
            df = adapter.get_us_stocks()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_us_stocks()

    @pytest.mark.integration
    def test_get_new_stocks(self, adapter):
        if adapter.is_configured():
            df = adapter.get_new_stocks()
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_new_stocks()


class TestLixingerInfoDisclosure:
    """Test information disclosure methods."""

    @pytest.fixture
    def mock_client(self):
        """Create mock Lixinger client."""
        client = MagicMock()
        client.is_configured.return_value = True
        return client

    @pytest.fixture
    def adapter_with_mock_client(self, mock_client):
        """Create LixingerAdapter with mocked client."""
        adapter = LixingerAdapter(token="test_token")
        adapter._client = mock_client
        return adapter

    @pytest.mark.integration
    def test_get_disclosure_news(self, adapter):
        if adapter.is_configured():
            df = adapter.get_disclosure_news(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_disclosure_news(symbol="000001")

    def test_get_performance_forecast(self, mock_client, adapter_with_mock_client):
        """Test get_performance_forecast delegates to client and returns DataFrame."""
        mock_df = pd.DataFrame(
            {
                "reportDate": ["2024-12-31"],
                "profit": [1000000.0],
                "profitRatio": [15.5],
                "type": ["pre-increase"],
            }
        )
        mock_client.get_performance_forecast.return_value = mock_df

        result = adapter_with_mock_client.get_performance_forecast(symbol="000001")
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        mock_client.get_performance_forecast.assert_called_once_with("000001")

    def test_get_performance_forecast_empty(
        self, mock_client, adapter_with_mock_client
    ):
        """Test get_performance_forecast handles empty result."""
        mock_client.get_performance_forecast.return_value = pd.DataFrame()

        result = adapter_with_mock_client.get_performance_forecast(symbol="000001")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_get_performance_forecast_with_dates(
        self, mock_client, adapter_with_mock_client
    ):
        """Test get_performance_forecast accepts optional date parameters."""
        mock_df = pd.DataFrame(
            {
                "reportDate": ["2024-12-31"],
                "profit": [1000000.0],
            }
        )
        mock_client.get_performance_forecast.return_value = mock_df

        result = adapter_with_mock_client.get_performance_forecast(
            symbol="000001", start_date="2024-01-01", end_date="2024-12-31"
        )
        assert isinstance(result, pd.DataFrame)
        # Adapter passes only symbol to client
        mock_client.get_performance_forecast.assert_called_once_with("000001")

    def test_get_analyst_rank(self, mock_client, adapter_with_mock_client):
        """Test get_analyst_rank delegates to client and returns DataFrame."""
        mock_df = pd.DataFrame(
            {
                "stockCode": ["600519", "000858"],
                "stockName": ["茅台", "五粮液"],
                "rank": [1, 2],
                "rating": ["买入", "增持"],
            }
        )
        mock_client.get_analyst_rank.return_value = mock_df

        result = adapter_with_mock_client.get_analyst_rank()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        mock_client.get_analyst_rank.assert_called_once()

    def test_get_analyst_rank_empty(self, mock_client, adapter_with_mock_client):
        """Test get_analyst_rank handles empty result."""
        mock_client.get_analyst_rank.return_value = pd.DataFrame()

        result = adapter_with_mock_client.get_analyst_rank()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_get_analyst_rank_with_dates(self, mock_client, adapter_with_mock_client):
        """Test get_analyst_rank accepts optional date parameters (passed but not used)."""
        mock_df = pd.DataFrame(
            {
                "stockCode": ["600519"],
                "rank": [1],
            }
        )
        mock_client.get_analyst_rank.return_value = mock_df

        result = adapter_with_mock_client.get_analyst_rank(
            start_date="2024-01-01", end_date="2024-12-31"
        )
        assert isinstance(result, pd.DataFrame)
        # Adapter calls client with no args
        mock_client.get_analyst_rank.assert_called_once()

    def test_get_research_report(self, mock_client, adapter_with_mock_client):
        """Test get_research_report delegates to client and returns DataFrame."""
        mock_df = pd.DataFrame(
            {
                "reportDate": ["2024-06-15"],
                "title": ["研究报告标题"],
                "analyst": ["张三"],
                "rating": ["买入"],
            }
        )
        mock_client.get_research_report.return_value = mock_df

        result = adapter_with_mock_client.get_research_report(symbol="000001")
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        mock_client.get_research_report.assert_called_once_with("000001")

    def test_get_research_report_empty(self, mock_client, adapter_with_mock_client):
        """Test get_research_report handles empty result."""
        mock_client.get_research_report.return_value = pd.DataFrame()

        result = adapter_with_mock_client.get_research_report(symbol="000001")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_get_research_report_with_dates(
        self, mock_client, adapter_with_mock_client
    ):
        """Test get_research_report accepts optional date parameters."""
        mock_df = pd.DataFrame(
            {
                "reportDate": ["2024-06-15"],
                "title": ["研究报告标题"],
            }
        )
        mock_client.get_research_report.return_value = mock_df

        result = adapter_with_mock_client.get_research_report(
            symbol="000001", start_date="2024-01-01", end_date="2024-12-31"
        )
        assert isinstance(result, pd.DataFrame)
        # Adapter passes only symbol to client
        mock_client.get_research_report.assert_called_once_with("000001")

    @pytest.mark.integration
    def test_get_name_history(self, adapter):
        if adapter.is_configured():
            df = adapter.get_name_history(symbol="000001")
            assert isinstance(df, pd.DataFrame)
        else:
            with pytest.raises(Exception):
                adapter.get_name_history(symbol="000001")

    @pytest.mark.integration
    def test_get_basic_info(self, adapter):
        if adapter.is_configured():
            result = adapter.get_basic_info(symbol="000001")
            # Method may return DataFrame or dict depending on implementation
            assert isinstance(result, (pd.DataFrame, dict))
        else:
            with pytest.raises(Exception):
                adapter.get_basic_info(symbol="000001")
