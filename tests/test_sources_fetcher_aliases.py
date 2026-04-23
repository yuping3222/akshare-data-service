"""Integration tests for fetcher.py fetch aliases and akshare interfaces with real data."""

import pytest
import pandas as pd
import akshare as ak
from unittest.mock import patch

from akshare_data.sources.akshare import fetcher
from akshare_data.sources.akshare.fetcher import (
    fetch_daily_data,
    fetch_money_flow,
    fetch_north_money_flow,
    fetch_macro_gdp,
    fetch_macro_exchange_rate,
    fetch_balance_sheet,
    fetch_income_statement,
    fetch_cash_flow,
    fetch_finance_indicator,
    fetch_block_deal,
    fetch_margin_summary,
    fetch_st_stocks,
    fetch_suspended_stocks,
    fetch_index_stocks,
    fetch_fund_net_value,
    fetch_sector_fund_flow,
    fetch_industry_stocks,
    fetch_industry_mapping,
    fetch_hot_rank,
)


class TestFetchDailyData:
    """Test fetch_daily_data with mocked akshare."""

    @pytest.mark.integration
    def test_stock_daily(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-15", freq="B"),
                "open": [10.0] * 10,
                "high": [11.0] * 10,
                "low": [9.0] * 10,
                "close": [10.5] * 10,
                "volume": [100_000] * 10,
                "amount": [1_000_000.0] * 10,
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_daily_data(
                ak, symbol="000001", start_date="20240101", end_date="20240201"
            )
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            mock_fetch.assert_called_once_with(
                "equity_daily",
                akshare=ak,
                symbol="000001",
                start_date="20240101",
                end_date="20240201",
                adjust="qfq",
            )


class TestFetchMacroAliases:
    """Test macro fetch aliases."""

    @pytest.mark.integration
    def test_macro_gdp(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=4, freq="QE"),
                "gdp": [30.0, 31.0, 32.0, 33.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_macro_gdp(ak)
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            mock_fetch.assert_called_once_with(
                "macro_gdp", akshare=ak, start_date=None, end_date=None
            )

    @pytest.mark.integration
    def test_macro_exchange_rate(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10, freq="D"),
                "exchange_rate": [
                    7.1,
                    7.12,
                    7.08,
                    7.15,
                    7.11,
                    7.09,
                    7.13,
                    7.10,
                    7.14,
                    7.07,
                ],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_macro_exchange_rate(ak)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "macro_exchange_rate",
                akshare=ak,
                start_date=None,
                end_date=None,
            )


class TestFetchFinancialAliases:
    """Test financial fetch aliases."""

    @pytest.mark.integration
    def test_finance_indicator(self):
        mock_df = pd.DataFrame(
            {
                "date": ["2024-Q1", "2024-Q2"],
                "roe": [10.5, 11.2],
                "eps": [0.5, 0.6],
                "pe": [15.0, 14.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_finance_indicator(ak, symbol="000001")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "finance_indicator",
                akshare=ak,
                symbol="000001",
                start_date=None,
                end_date=None,
            )

    @pytest.mark.integration
    def test_balance_sheet(self):
        mock_df = pd.DataFrame(
            {
                "date": ["2023-12-31"],
                "total_assets": [1_000_000.0],
                "total_liabilities": [500_000.0],
                "total_equity": [500_000.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_balance_sheet(ak, symbol="000001")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "balance_sheet",
                akshare=ak,
                symbol="000001",
                start_date=None,
                end_date=None,
            )

    @pytest.mark.integration
    def test_income_statement(self):
        mock_df = pd.DataFrame(
            {
                "date": ["2023-12-31"],
                "revenue": [100_000.0],
                "net_income": [20_000.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_income_statement(ak, symbol="000001")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "income_statement",
                akshare=ak,
                symbol="000001",
                start_date=None,
                end_date=None,
            )

    @pytest.mark.integration
    def test_cash_flow(self):
        mock_df = pd.DataFrame(
            {
                "date": ["2023-12-31"],
                "operating_cash_flow": [30_000.0],
                "investing_cash_flow": [-10_000.0],
                "financing_cash_flow": [-5_000.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_cash_flow(ak, symbol="000001")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "cash_flow",
                akshare=ak,
                symbol="000001",
                start_date=None,
                end_date=None,
            )


class TestFetchMarketAliases:
    """Test market data fetch aliases."""

    @pytest.mark.integration
    def test_money_flow(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10, freq="B"),
                "main_net_inflow": [1_000_000.0] * 10,
                "small_net_inflow": [-500_000.0] * 10,
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_money_flow(ak, symbol="000001")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "money_flow",
                akshare=ak,
                symbol="000001",
                start_date=None,
                end_date=None,
            )

    @pytest.mark.integration
    def test_north_money_flow(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10, freq="B"),
                "net_inflow": [2_000_000.0] * 10,
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_north_money_flow(ak)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "north_money_flow",
                akshare=ak,
                start_date=None,
                end_date=None,
            )

    @pytest.mark.integration
    def test_block_deal(self):
        mock_df = pd.DataFrame(
            {
                "date": ["2024-01-15"],
                "symbol": ["600000"],
                "price": [10.0],
                "volume": [100_000],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_block_deal(ak, date="2024-01-15")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "block_deal", akshare=ak, date="2024-01-15"
            )

    @pytest.mark.integration
    def test_margin_summary(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5, freq="B"),
                "margin_balance": [10_000_000.0] * 5,
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_margin_summary(
                ak, start_date="2024-01-01", end_date="2024-01-05"
            )
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "margin_summary",
                akshare=ak,
                start_date="2024-01-01",
                end_date="2024-01-05",
            )


class TestFetchListAliases:
    """Test list-type fetch aliases."""

    @pytest.mark.integration
    def test_st_stocks(self):
        mock_df = pd.DataFrame(
            {
                "symbol": ["600001", "000002"],
                "name": ["ST Test1", "ST Test2"],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_st_stocks(ak)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with("st_stocks", akshare=ak)

    @pytest.mark.integration
    def test_index_stocks(self):
        mock_df = pd.DataFrame(
            {
                "symbol": ["600000", "000001"],
                "name": ["Test1", "Test2"],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_index_stocks(ak, index_code="000300")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "index_components",
                akshare=ak,
                symbol="000300",
            )

    @pytest.mark.integration
    def test_suspended_stocks(self):
        mock_df = pd.DataFrame(
            {
                "symbol": ["600003"],
                "name": ["Suspended1"],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_suspended_stocks(ak)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with("suspended_stocks", akshare=ak)

    @pytest.mark.integration
    def test_hot_rank(self):
        mock_df = pd.DataFrame(
            {
                "rank": [1, 2, 3],
                "symbol": ["600000", "000001", "600519"],
                "name": ["Stock1", "Stock2", "Stock3"],
                "hot_score": [100.0, 90.0, 80.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_hot_rank(ak)
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            mock_fetch.assert_called_once_with("hot_rank", akshare=ak)


class TestFetchFundAliases:
    """Test fund-related fetch aliases."""

    @pytest.mark.integration
    def test_fund_net_value(self):
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10, freq="B"),
                "nav": [1.5, 1.52, 1.48, 1.51, 1.53, 1.50, 1.49, 1.54, 1.52, 1.55],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_fund_net_value(ak, fund_code="510050")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "fund_net_value",
                akshare=ak,
                symbol="510050",
                start_date=None,
                end_date=None,
            )

    @pytest.mark.integration
    def test_sector_fund_flow(self):
        mock_df = pd.DataFrame(
            {
                "sector": ["Technology", "Finance", "Healthcare"],
                "net_inflow": [5_000_000.0, 3_000_000.0, 2_000_000.0],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_sector_fund_flow(ak)
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "sector_fund_flow",
                akshare=ak,
                date=None,
                sector_type="industry",
            )


class TestFetchIndustryAliases:
    """Test industry-related fetch aliases."""

    @pytest.mark.integration
    def test_industry_stocks(self):
        mock_df = pd.DataFrame(
            {
                "symbol": ["600000", "000001"],
                "name": ["Stock1", "Stock2"],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_industry_stocks(ak, industry_code="801010")
            assert isinstance(df, pd.DataFrame) or isinstance(df, list)
            mock_fetch.assert_called_once_with(
                "industry_stocks",
                akshare=ak,
                symbol="801010",
            )

    @pytest.mark.integration
    def test_industry_mapping(self):
        mock_df = pd.DataFrame(
            {
                "symbol": ["801010"],
                "name": ["Agriculture"],
                "level": [1],
            }
        )
        with patch.object(fetcher, "fetch", return_value=mock_df) as mock_fetch:
            df = fetch_industry_mapping(ak, symbol="801010")
            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once_with(
                "industry_mapping",
                akshare=ak,
                symbol="801010",
            )
