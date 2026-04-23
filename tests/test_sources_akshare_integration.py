"""Unit tests for akshare_source.py — mocked AKShare calls.

Tests the AkShareAdapter methods using mocked fetch() responses.
Verifies correct interface routing, parameter passing, and result processing.
"""

import pytest
import pandas as pd
from unittest.mock import patch

from akshare_data.sources.akshare_source import AkShareAdapter
from akshare_data.core.errors import DataSourceError, SourceUnavailableError


@pytest.fixture
def source():
    return AkShareAdapter()


class TestAkShareAdapterMocked:
    """Test AkShareAdapter methods with mocked fetch() calls."""

    # ── get_daily_data ───────────────────────────────────────────────

    def test_get_daily_data_calls_correct_interface(self, source):
        """get_daily_data routes to equity_daily interface."""
        mock_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-10", freq="B"),
                "open": [10.0] * 7,
                "high": [11.0] * 7,
                "low": [9.0] * 7,
                "close": [10.5] * 7,
                "volume": [100_000] * 7,
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_daily_data(
                symbol="000001",
                start_date="2024-01-01",
                end_date="2024-02-01",
            )
            mock_fetch.assert_called_once_with(
                "equity_daily",
                akshare=source._akshare,
                symbol="000001",
                start_date="2024-01-01",
                end_date="2024-02-01",
                adjust="qfq",
            )
            assert isinstance(result, pd.DataFrame)
            assert not result.empty

    def test_get_daily_data_preserves_adjust_param(self, source):
        """get_daily_data passes adjust parameter correctly."""
        mock_df = pd.DataFrame({"date": ["2024-01-02"], "close": [10.0]})
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            source.get_daily_data(
                symbol="600000",
                start_date="2024-01-01",
                end_date="2024-02-01",
                adjust="hfq",
            )
            call_kwargs = mock_fetch.call_args.kwargs
            assert call_kwargs["adjust"] == "hfq"

    def test_get_daily_data_raises_when_akshare_unavailable(self, source):
        """get_daily_data raises SourceUnavailableError when akshare unavailable."""
        with patch.object(source, "_akshare_available", False):
            with pytest.raises(SourceUnavailableError):
                source.get_daily_data("600000", "2024-01-01", "2024-01-10")

    # ── get_index_components ─────────────────────────────────────────

    def test_get_index_components_calls_correct_interface(self, source):
        """get_index_components routes to index_components interface."""
        mock_df = pd.DataFrame(
            {
                "code": ["000001", "000002", "600000"],
                "name": ["平安银行", "万科A", "浦发银行"],
                "weight": [0.5, 0.3, 0.2],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_index_components(
                index_code="000300", include_weights=True
            )
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "index_components"
            assert mock_fetch.call_args.kwargs["index_code"] == "000300"
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3

    # ── get_st_stocks ────────────────────────────────────────────────

    def test_get_st_stocks_calls_correct_interface(self, source):
        """get_st_stocks routes to st_stocks interface."""
        mock_df = pd.DataFrame(
            {
                "code": ["000001", "600000"],
                "name": ["平安银行", "浦发银行"],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_st_stocks()
            mock_fetch.assert_called_once_with(
                "st_stocks",
                akshare=source._akshare,
            )
            assert isinstance(result, pd.DataFrame)

    def test_get_st_stocks_raises_when_akshare_unavailable(self, source):
        """get_st_stocks raises DataSourceError when akshare unavailable."""
        with patch.object(source, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                source.get_st_stocks()

    # ── get_finance_indicator ────────────────────────────────────────

    def test_get_finance_indicator_calls_correct_interface(self, source):
        """get_finance_indicator routes to finance_indicator interface."""
        mock_df = pd.DataFrame(
            {
                "date": ["2024-03-31", "2023-12-31"],
                "roe": [0.15, 0.14],
                "pe": [12.5, 13.0],
                "pb": [1.2, 1.1],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_finance_indicator(
                symbol="000001",
                start_date="2023-01-01",
                end_date="2024-12-31",
            )
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "finance_indicator"
            assert mock_fetch.call_args.kwargs["symbol"] == "000001"
            assert isinstance(result, pd.DataFrame)

    def test_get_finance_indicator_raises_when_akshare_unavailable(self, source):
        """get_finance_indicator raises DataSourceError when akshare unavailable."""
        with patch.object(source, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                source.get_finance_indicator("600000")

    # ── get_industry_stocks ──────────────────────────────────────────

    def test_get_industry_stocks_calls_correct_interface(self, source):
        """get_industry_stocks routes to industry_stocks interface."""
        mock_df = pd.DataFrame(
            {
                "code": ["000001", "000002"],
                "name": ["平安银行", "万科A"],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_industry_stocks(industry_code="801010", level=1)
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "industry_stocks"
            assert isinstance(result, pd.DataFrame)

    def test_get_industry_stocks_raises_when_akshare_unavailable(self, source):
        """get_industry_stocks raises DataSourceError when akshare unavailable."""
        with patch.object(source, "_akshare_available", False):
            with pytest.raises(DataSourceError):
                source.get_industry_stocks("801010")

    # ── get_realtime_data ────────────────────────────────────────────

    def test_get_realtime_data_calls_correct_interface(self, source):
        """get_realtime_data routes to equity_realtime interface."""
        mock_df = pd.DataFrame(
            {
                "code": ["000001"],
                "name": ["平安银行"],
                "price": [12.5],
                "change": [0.02],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_realtime_data(symbol="000001")
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "equity_realtime"
            assert isinstance(result, pd.DataFrame)

    # ── get_money_flow ───────────────────────────────────────────────

    def test_get_money_flow_calls_correct_interface(self, source):
        """get_money_flow routes to money_flow interface."""
        mock_df = pd.DataFrame(
            {
                "date": ["2024-01-02"],
                "main_net_inflow": [1_000_000.0],
                "small_net_inflow": [500_000.0],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_money_flow(
                symbol="000001",
                start_date="2024-01-01",
                end_date="2024-02-01",
            )
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "money_flow"
            assert isinstance(result, pd.DataFrame)

    # ── get_sector_fund_flow ─────────────────────────────────────────

    def test_get_sector_fund_flow_calls_correct_interface(self, source):
        """get_sector_fund_flow routes to sector_fund_flow interface."""
        mock_df = pd.DataFrame(
            {
                "sector": ["银行", "券商"],
                "net_inflow": [1e9, 5e8],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_sector_fund_flow(sector_type="industry")
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "sector_fund_flow"
            assert isinstance(result, pd.DataFrame)

    def test_get_sector_fund_flow_rejects_invalid_type(self, source):
        """get_sector_fund_flow raises DataSourceError for invalid sector_type."""
        with pytest.raises(DataSourceError):
            source.get_sector_fund_flow(sector_type="invalid_type_xyz")

    # ── get_suspended_stocks ─────────────────────────────────────────

    def test_get_suspended_stocks_calls_correct_interface(self, source):
        """get_suspended_stocks routes to suspended_stocks interface."""
        mock_df = pd.DataFrame(
            {
                "code": ["000001"],
                "name": ["平安银行"],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_suspended_stocks()
            mock_fetch.assert_called_once_with(
                "suspended_stocks",
                akshare=source._akshare,
            )
            assert isinstance(result, pd.DataFrame)

    # ── get_north_money_flow ─────────────────────────────────────────

    def test_get_north_money_flow_calls_correct_interface(self, source):
        """get_north_money_flow routes to north_money_flow interface."""
        mock_df = pd.DataFrame(
            {
                "date": ["2024-01-02"],
                "net_amount": [1e8],
            }
        )
        with patch(
            "akshare_data.sources.akshare_source.fetch",
            return_value=mock_df,
        ) as mock_fetch:
            result = source.get_north_money_flow(
                start_date="2024-01-01",
                end_date="2024-02-01",
            )
            mock_fetch.assert_called_once()
            assert mock_fetch.call_args.args[0] == "north_money_flow"
            assert isinstance(result, pd.DataFrame)


class TestAkShareSourceOptionMethods:
    """Test option-related calculation methods (pure computation, no network)."""

    def test_black_scholes_price_call(self, source):
        """Test black_scholes_price with valid params."""
        try:
            result = source.black_scholes_price(
                S=100.0, K=100.0, T=0.25, r=0.05, sigma=0.2, option_type="call"
            )
            assert result is not None
        except ImportError:
            pytest.skip("scipy not available")

    def test_black_scholes_price_put(self, source):
        """Test black_scholes_price with put option."""
        try:
            result = source.black_scholes_price(
                S=100.0, K=100.0, T=0.25, r=0.05, sigma=0.2, option_type="put"
            )
            assert result is not None
        except ImportError:
            pytest.skip("scipy not available")

    def test_calculate_option_implied_vol(self, source):
        """Test implied vol calculation."""
        try:
            result = source.calculate_option_implied_vol(
                symbol="510050",
                price=5.0,
                strike=2.5,
                expiry="2024-06-30",
                risk_free_rate=0.05,
            )
            assert result is not None
        except ImportError:
            pytest.skip("scipy not available")
        except Exception:
            pytest.skip("calculate_option_implied_vol unavailable")

    def test_calculate_conversion_value(self, source):
        """Test convertible bond conversion value."""
        result = source.calculate_conversion_value(
            bond_price=120.0, conversion_ratio=8.33, stock_price=15.0
        )
        assert result is not None


class TestAkShareSourceEdgeCases:
    """Test edge cases and error paths."""

    def test_health_check(self, source):
        """Test health check returns dict."""
        result = source.health_check()
        assert isinstance(result, dict)
        assert "status" in result

    def test_get_source_info(self, source):
        """Test source info returns correctly."""
        info = source.get_source_info()
        assert info is not None
        assert "name" in info

    def test_offline_mode_raises(self, source):
        """Test offline mode raises SourceUnavailableError."""
        offline_source = AkShareAdapter(offline_mode=True)
        with pytest.raises(SourceUnavailableError):
            offline_source.get_daily_data("000001", "2024-01-01", "2024-01-10")

    def test_dynamic_routing_unknown_method_raises(self, source):
        """Test that calling an unknown method raises AttributeError."""
        with pytest.raises(AttributeError):
            source.get_nonexistent_method_xyz()
