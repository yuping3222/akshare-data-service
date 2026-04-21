"""Tests for Tushare data source adapter."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, date

from akshare_data.sources.tushare_source import TushareAdapter, set_tushare_token
from akshare_data.core.errors import SourceUnavailableError


class TestTushareAdapter:
    """Test suite for TushareAdapter class."""

    @pytest.fixture
    def mock_pro_api(self):
        """Create mock Tushare Pro API."""
        return MagicMock()

    @pytest.fixture
    def adapter_with_mock_pro(self, mock_pro_api):
        """Create TushareAdapter with mocked pro API."""
        adapter = TushareAdapter(token="test_token")
        adapter._pro = mock_pro_api
        adapter._initialized = True
        return adapter

    @pytest.fixture
    def adapter_uninitialized(self):
        """Create uninitialized TushareAdapter."""
        adapter = TushareAdapter()
        adapter._initialized = False
        adapter._pro = None
        return adapter

    class TestTokenManagement:
        """Test token management functions."""

        def test_set_tushare_token(self):
            """Test set_tushare_token function."""
            from akshare_data.core.tokens import TokenManager

            TokenManager.reset()
            set_tushare_token("my_token")
            from akshare_data.core.tokens import get_token

            assert get_token("tushare") == "my_token"
            TokenManager.reset()

        def test_get_tushare_token_from_env(self):
            """Test token retrieval from environment variable."""
            from akshare_data.core.tokens import TokenManager, get_token

            TokenManager.reset()
            with patch.dict("os.environ", {"TUSHARE_TOKEN": "env_token"}):
                assert get_token("tushare") == "env_token"
            TokenManager.reset()

    class TestInitialization:
        """Test TushareAdapter initialization."""

        def test_adapter_creation_with_token(self):
            """Test adapter creation with explicit token."""
            adapter = TushareAdapter(token="test_token")
            assert adapter._token == "test_token"
            assert adapter._pro is None
            assert adapter._initialized is False

        def test_adapter_creation_without_token(self):
            """Test adapter creation without token."""
            adapter = TushareAdapter()
            assert adapter._token is None

    class TestEnsureInitialized:
        """Test _ensure_initialized method."""

        def test_skips_if_already_initialized(self, adapter_with_mock_pro):
            """Test skips initialization if already done."""
            adapter_with_mock_pro._ensure_initialized()
            assert adapter_with_mock_pro._pro is not None

        def test_initializes_with_valid_token(self):
            """Test initializes with valid token."""
            with patch.dict("os.environ", {}, clear=True):
                with patch(
                    "akshare_data.sources.tushare_source.ts", create=True
                ) as mock_ts:
                    mock_ts.set_token = MagicMock()
                    mock_ts.pro_api.return_value = MagicMock()

                    adapter = TushareAdapter(token="test_token")
                    adapter._ensure_initialized()

                    mock_ts.set_token.assert_called_once_with("test_token")
                    assert adapter._pro is not None

        def test_handles_missing_tushare_package(self, adapter_uninitialized):
            """Test handles missing tushare package gracefully."""
            with patch(
                "akshare_data.sources.tushare_source.ts", create=True
            ) as mock_ts:
                mock_ts.set_token.side_effect = ImportError("No module named 'tushare'")

                adapter_uninitialized._ensure_initialized()
                assert adapter_uninitialized._initialized is True

    class TestEnsureConfigured:
        """Test _ensure_configured method."""

        def test_raises_error_when_pro_not_set(self, adapter_uninitialized):
            """Test raises SourceUnavailableError when pro not set."""
            with patch.dict("os.environ", {}, clear=True):
                with patch("akshare_data.sources.tushare_source.get_token", return_value=None):
                    with pytest.raises(SourceUnavailableError):
                        adapter_uninitialized._ensure_configured()

    class TestToTsCode:
        """Test _to_ts_code method."""

        def test_converts_sh_stock(self, adapter_with_mock_pro):
            """Test converts Shanghai stock to ts code."""
            result = adapter_with_mock_pro._to_ts_code("600519")
            assert result == "600519.SH"

        def test_converts_sz_stock(self, adapter_with_mock_pro):
            """Test converts Shenzhen stock to ts code."""
            result = adapter_with_mock_pro._to_ts_code("000858")
            assert result == "000858.SZ"

        def test_preserves_existing_suffix(self, adapter_with_mock_pro):
            """Test preserves existing .SH/.SZ suffix."""
            result = adapter_with_mock_pro._to_ts_code("600519.SH")
            assert result == "600519.SH"

    class TestFromTsCode:
        """Test _from_ts_code method."""

        def test_removes_sh_suffix(self, adapter_with_mock_pro):
            """Test removes .SH suffix."""
            result = adapter_with_mock_pro._from_ts_code("600519.SH")
            assert result == "600519"

        def test_removes_sz_suffix(self, adapter_with_mock_pro):
            """Test removes .SZ suffix."""
            result = adapter_with_mock_pro._from_ts_code("000858.SZ")
            assert result == "000858"

    class TestNormalizeDate:
        """Test _normalize_date method."""

        def test_normalize_date_with_datetime(self, adapter_with_mock_pro):
            """Test date normalization with datetime."""
            dt = datetime(2024, 1, 15)
            result = adapter_with_mock_pro._normalize_date(dt)
            assert result == "20240115"

        def test_normalize_date_with_date(self, adapter_with_mock_pro):
            """Test date normalization with date."""
            d = date(2024, 1, 15)
            result = adapter_with_mock_pro._normalize_date(d)
            assert result == "20240115"

        def test_normalize_date_with_string(self, adapter_with_mock_pro):
            """Test date normalization with string."""
            result = adapter_with_mock_pro._normalize_date("2024-01-15")
            assert result == "20240115"

    class TestNormalizeDailyDf:
        """Test _normalize_daily_df method."""

        def test_returns_empty_on_empty_df(self, adapter_with_mock_pro):
            """Test returns empty DataFrame when input is empty."""
            result = adapter_with_mock_pro._normalize_daily_df(pd.DataFrame())
            assert result.empty

        def test_normalizes_trade_date_column(self, adapter_with_mock_pro):
            """Test trade_date column normalization."""
            df = pd.DataFrame(
                {"trade_date": ["20240101", "20240102"], "close": [100.0, 101.0]}
            )
            result = adapter_with_mock_pro._normalize_daily_df(df)
            assert "datetime" in result.columns

        def test_normalizes_columns(self, adapter_with_mock_pro):
            """Test column normalization."""
            df = pd.DataFrame(
                {
                    "trade_date": ["20240101"],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.0],
                    "vol": [1000000],
                    "amount": [100000000],
                }
            )
            result = adapter_with_mock_pro._normalize_daily_df(df)
            assert "volume" in result.columns

    class TestGetDailyData:
        """Test get_daily_data method."""

        def test_get_daily_data_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful daily data retrieval."""
            mock_df = pd.DataFrame(
                {"trade_date": ["20240101", "20240102"], "close": [100.0, 101.0]}
            )
            mock_pro_api.daily.return_value = mock_df

            result = adapter_with_mock_pro.get_daily_data(
                "600519", "2024-01-01", "2024-01-10"
            )
            assert isinstance(result, pd.DataFrame)

        def test_get_daily_data_empty_result(self, adapter_with_mock_pro, mock_pro_api):
            """Test returns empty DataFrame when no data."""
            mock_pro_api.daily.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_daily_data(
                "600519", "2024-01-01", "2024-01-10"
            )
            assert result.empty

    class TestGetIndexStocks:
        """Test get_index_stocks method."""

        def test_get_index_stocks_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful index stocks retrieval."""
            mock_df = pd.DataFrame({"con_code": ["600519.SH", "000858.SZ"]})
            mock_pro_api.index_weight.return_value = mock_df

            result = adapter_with_mock_pro.get_index_stocks("000300")
            assert isinstance(result, list)
            assert "600519.XSHG" in result
            assert "000858.XSHE" in result

        def test_get_index_stocks_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty list when no data."""
            mock_pro_api.index_weight.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_index_stocks("000300")
            assert result == []

    class TestGetIndexComponents:
        """Test get_index_components method."""

        def test_get_index_components_success(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test successful index components retrieval."""
            mock_df = pd.DataFrame(
                {
                    "con_code": ["600519.SH"],
                    "con_name": ["贵州茅台"],
                    "weight": [5.5],
                    "in_date": ["20240101"],
                }
            )
            mock_pro_api.index_weight.return_value = mock_df

            result = adapter_with_mock_pro.get_index_components(
                "000300", include_weights=True
            )
            assert "index_code" in result.columns
            assert "code" in result.columns

        def test_get_index_components_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty DataFrame when no data."""
            mock_pro_api.index_weight.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_index_components("000300")
            assert result.empty

    class TestGetTradingDays:
        """Test get_trading_days method."""

        def test_get_trading_days_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful trading days retrieval."""
            mock_df = pd.DataFrame(
                {"cal_date": ["2024-01-02", "2024-01-03"], "is_open": [1, 1]}
            )
            mock_pro_api.trade_cal.return_value = mock_df

            result = adapter_with_mock_pro.get_trading_days("2024-01-01", "2024-01-10")
            assert isinstance(result, list)
            assert len(result) == 2

        def test_get_trading_days_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty list when no data."""
            mock_pro_api.trade_cal.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_trading_days()
            assert result == []

    class TestGetSecuritiesList:
        """Test get_securities_list method."""

        def test_get_securities_list_for_stocks(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test securities list for stocks."""
            mock_df = pd.DataFrame(
                {
                    "ts_code": ["600519.SH", "000858.SZ"],
                    "name": ["贵州茅台", "五粮液"],
                    "list_date": ["2001-08-27", "1998-04-27"],
                }
            )
            mock_pro_api.stock_basic.return_value = mock_df

            result = adapter_with_mock_pro.get_securities_list(security_type="stock")
            assert isinstance(result, pd.DataFrame)
            assert "code" in result.columns

        def test_get_securities_list_for_index(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test securities list for index."""
            mock_df = pd.DataFrame(
                {"code": ["000300", "000905"], "name": ["沪深300", "中证500"]}
            )
            mock_pro_api.index_basic.return_value = mock_df

            result = adapter_with_mock_pro.get_securities_list(security_type="index")
            assert isinstance(result, pd.DataFrame)

        def test_get_securities_list_empty_for_other_types(self, adapter_with_mock_pro):
            """Test returns empty DataFrame for unsupported types."""
            result = adapter_with_mock_pro.get_securities_list(security_type="fund")
            assert result.empty

    class TestGetSecurityInfo:
        """Test get_security_info method."""

        def test_get_security_info_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful security info retrieval."""
            mock_df = pd.DataFrame(
                {
                    "ts_code": ["600519.SH"],
                    "name": ["贵州茅台"],
                    "industry": ["白酒"],
                    "list_date": ["2001-08-27"],
                }
            )
            mock_pro_api.stock_basic.return_value = mock_df

            result = adapter_with_mock_pro.get_security_info("600519")
            assert isinstance(result, dict)
            assert result["display_name"] == "贵州茅台"
            assert result["industry"] == "白酒"

        def test_get_security_info_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty dict when no data."""
            mock_pro_api.stock_basic.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_security_info("600519")
            assert result == {}

    class TestGetMinuteData:
        """Test get_minute_data method."""

        def test_returns_empty_dataframe(self, adapter_with_mock_pro):
            """Test Tushare does not support minute data."""
            result = adapter_with_mock_pro.get_minute_data("000001", freq="1min")
            assert result.empty

    class TestGetMoneyFlow:
        """Test get_money_flow method."""

        def test_get_money_flow_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful money flow retrieval."""
            mock_df = pd.DataFrame({"trade_date": ["20240102"], "money": [1000000]})
            mock_pro_api.moneyflow_hsgt.return_value = mock_df

            result = adapter_with_mock_pro.get_money_flow("000001")
            assert isinstance(result, pd.DataFrame)

        def test_get_money_flow_empty_result(self, adapter_with_mock_pro, mock_pro_api):
            """Test returns empty DataFrame when no data."""
            mock_pro_api.moneyflow_hsgt.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_money_flow("000001")
            assert result.empty

    class TestGetNorthMoneyFlow:
        """Test get_north_money_flow method."""

        def test_get_north_money_flow_success(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test successful north money flow retrieval."""
            mock_df = pd.DataFrame(
                {"trade_date": ["20240102"], "hk_cnt_net": [1000000]}
            )
            mock_pro_api.moneyflow_hkctl.return_value = mock_df

            result = adapter_with_mock_pro.get_north_money_flow()
            assert isinstance(result, pd.DataFrame)

        def test_get_north_money_flow_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty DataFrame when no data."""
            mock_pro_api.moneyflow_hkctl.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_north_money_flow()
            assert result.empty

    class TestGetIndustryStocks:
        """Test get_industry_stocks method."""

        def test_returns_empty_list(self, adapter_with_mock_pro):
            """Test Tushare uses concept components instead."""
            result = adapter_with_mock_pro.get_industry_stocks("801010")
            assert result == []

    class TestGetIndustryMapping:
        """Test get_industry_mapping method."""

        def test_get_industry_mapping_success(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test successful industry mapping retrieval."""
            mock_df = pd.DataFrame({"ts_code": ["600519.SH"], "industry": ["白酒"]})
            mock_pro_api.stock_basic.return_value = mock_df

            result = adapter_with_mock_pro.get_industry_mapping("600519")
            assert result == "白酒"

        def test_get_industry_mapping_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty string when no data."""
            mock_pro_api.stock_basic.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_industry_mapping("600519")
            assert result == ""

    class TestGetFinanceIndicator:
        """Test get_finance_indicator method."""

        def test_get_finance_indicator_success(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test successful finance indicator retrieval."""
            mock_df = pd.DataFrame({"ts_code": ["600519.SH"], "roe": [30.5]})
            mock_pro_api.fina_indicator.return_value = mock_df

            result = adapter_with_mock_pro.get_finance_indicator("600519")
            assert isinstance(result, pd.DataFrame)

        def test_get_finance_indicator_with_fields(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test finance indicator with specific fields."""
            mock_df = pd.DataFrame(
                {"ts_code": ["600519.SH"], "roe": [30.5], "eps": [5.5]}
            )
            mock_pro_api.fina_indicator.return_value = mock_df

            result = adapter_with_mock_pro.get_finance_indicator(
                "600519", fields=["roe", "eps"]
            )
            assert "roe" in result.columns
            assert "eps" in result.columns

        def test_get_finance_indicator_empty_result(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test returns empty DataFrame when no data."""
            mock_pro_api.fina_indicator.return_value = pd.DataFrame()

            result = adapter_with_mock_pro.get_finance_indicator("600519")
            assert result.empty

    class TestGetCallAuction:
        """Test get_call_auction method."""

        def test_returns_empty_dataframe(self, adapter_with_mock_pro):
            """Test Tushare does not support call auction."""
            result = adapter_with_mock_pro.get_call_auction("000001")
            assert result.empty

    class TestHealthCheck:
        """Test health_check method."""

        def test_health_check_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test health check success."""
            mock_df = pd.DataFrame({"trade_date": ["20240102"], "close": [10.5]})
            mock_pro_api.daily.return_value = mock_df

            result = adapter_with_mock_pro.health_check()
            assert result["status"] == "ok"
            assert "message" in result

        def test_health_check_error_on_exception(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test health check handles exceptions."""
            mock_pro_api.daily.side_effect = Exception("Connection error")

            result = adapter_with_mock_pro.health_check()
            assert result["status"] == "error"
            assert result["latency_ms"] is None

    class TestGetSourceInfo:
        """Test get_source_info method."""

        def test_returns_source_info_dict(self, adapter_with_mock_pro):
            """Test get_source_info returns proper dict."""
            result = adapter_with_mock_pro.get_source_info()
            assert result["name"] == "tushare"
            assert result["type"] == "real"
            assert result["requires_auth"] is True

    class TestGetStockPePb:
        """Test get_stock_pe_pb method."""

        def test_get_stock_pe_pb_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful PE/PB retrieval."""
            mock_df = pd.DataFrame(
                {
                    "trade_date": ["20240102"],
                    "close": [1800.0],
                    "pe": [30.5],
                    "pb": [10.2],
                }
            )
            mock_pro_api.daily_basic.return_value = mock_df

            result = adapter_with_mock_pro.get_stock_pe_pb("600519")
            assert isinstance(result, pd.DataFrame)
            assert "pe" in result.columns
            assert "pb" in result.columns

    class TestGetFinancialReport:
        """Test get_financial_report method."""

        def test_get_financial_report_income(self, adapter_with_mock_pro, mock_pro_api):
            """Test income statement retrieval."""
            mock_df = pd.DataFrame({"ts_code": ["600519.SH"], "revenue": [1000000000]})
            mock_pro_api.income.return_value = mock_df

            result = adapter_with_mock_pro.get_financial_report("600519", "income")
            assert isinstance(result, pd.DataFrame)

        def test_get_financial_report_balancesheet(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test balance sheet retrieval."""
            mock_df = pd.DataFrame(
                {"ts_code": ["600519.SH"], "total_asset": [5000000000]}
            )
            mock_pro_api.balancesheet.return_value = mock_df

            result = adapter_with_mock_pro.get_financial_report(
                "600519", "balancesheet"
            )
            assert isinstance(result, pd.DataFrame)

        def test_get_financial_report_cashflow(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test cash flow statement retrieval."""
            mock_df = pd.DataFrame(
                {"ts_code": ["600519.SH"], "net_operate_cash_flow": [500000000]}
            )
            mock_pro_api.cashflow.return_value = mock_df

            result = adapter_with_mock_pro.get_financial_report("600519", "cashflow")
            assert isinstance(result, pd.DataFrame)

        def test_get_financial_report_invalid_type(self, adapter_with_mock_pro):
            """Test returns empty DataFrame for invalid report type."""
            result = adapter_with_mock_pro.get_financial_report("600519", "invalid")
            assert result.empty

    class TestGetDividend:
        """Test get_dividend method."""

        def test_get_dividend_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful dividend retrieval."""
            mock_df = pd.DataFrame({"ts_code": ["600519.SH"], "div_ratio": [1.5]})
            mock_pro_api.dividend.return_value = mock_df

            result = adapter_with_mock_pro.get_dividend("600519")
            assert isinstance(result, pd.DataFrame)

    class TestGetTop10Holders:
        """Test get_top10_holders method."""

        def test_get_top10_holders_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful top 10 holders retrieval."""
            mock_df = pd.DataFrame(
                {"ts_code": ["600519.SH"], "holder_name": ["贵州茅台酒厂"]}
            )
            mock_pro_api.top10_holders.return_value = mock_df

            result = adapter_with_mock_pro.get_top10_holders("600519")
            assert isinstance(result, pd.DataFrame)

    class TestGetTop10FloatHolders:
        """Test get_top10_float_holders method."""

        def test_get_top10_float_holders_success(
            self, adapter_with_mock_pro, mock_pro_api
        ):
            """Test successful top 10 float holders retrieval."""
            mock_df = pd.DataFrame(
                {"ts_code": ["600519.SH"], "holder_name": ["机构投资者"]}
            )
            mock_pro_api.top10_floatholders.return_value = mock_df

            result = adapter_with_mock_pro.get_top10_float_holders("600519")
            assert isinstance(result, pd.DataFrame)

    class TestGetMarginDetail:
        """Test get_margin_detail method."""

        def test_get_margin_detail_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful margin detail retrieval."""
            mock_df = pd.DataFrame({"trade_date": ["20240102"], "rzye": [1000000]})
            mock_pro_api.margin_detail.return_value = mock_df

            result = adapter_with_mock_pro.get_margin_detail("sh", "2024-01-02")
            assert isinstance(result, pd.DataFrame)

    class TestGetMacroRaw:
        """Test get_macro_raw method."""

        def test_get_macro_raw_cpi(self, adapter_with_mock_pro, mock_pro_api):
            """Test CPI data retrieval."""
            mock_df = pd.DataFrame({"month": ["2024-01"], "cpi": [100.5]})
            mock_pro_api.cpi_monthly.return_value = mock_df

            result = adapter_with_mock_pro.get_macro_raw("cpi")
            assert isinstance(result, pd.DataFrame)

        def test_get_macro_raw_ppi(self, adapter_with_mock_pro, mock_pro_api):
            """Test PPI data retrieval."""
            mock_df = pd.DataFrame({"month": ["2024-01"], "ppi": [95.0]})
            mock_pro_api.ppi_monthly.return_value = mock_df

            result = adapter_with_mock_pro.get_macro_raw("ppi")
            assert isinstance(result, pd.DataFrame)

        def test_get_macro_raw_invalid_indicator(self, adapter_with_mock_pro):
            """Test returns empty DataFrame for invalid indicator."""
            result = adapter_with_mock_pro.get_macro_raw("invalid")
            assert result.empty

    class TestGetBillboardList:
        """Test get_billboard_list method."""

        def test_get_billboard_list_success(self, adapter_with_mock_pro, mock_pro_api):
            """Test successful billboard list retrieval."""
            mock_df = pd.DataFrame({"trade_date": ["20240102"], "code": ["000001"]})
            mock_pro_api.top_list.return_value = mock_df

            result = adapter_with_mock_pro.get_billboard_list(
                "2024-01-01", "2024-01-10"
            )
            assert isinstance(result, pd.DataFrame)
