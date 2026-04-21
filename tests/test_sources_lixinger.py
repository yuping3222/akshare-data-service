"""Tests for Lixinger data source adapter."""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, date

from akshare_data.sources.lixinger_source import LixingerAdapter
from akshare_data.core.errors import SourceUnavailableError


class TestLixingerAdapter:
    """Test suite for LixingerAdapter class."""

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

    class TestInitialization:
        """Test LixingerAdapter initialization."""

        def test_adapter_creation_with_token(self):
            """Test adapter creation with explicit token."""
            adapter = LixingerAdapter(token="test_token")
            assert adapter._token == "test_token"
            assert adapter._client is None

        def test_adapter_creation_without_token(self):
            """Test adapter creation without token."""
            adapter = LixingerAdapter()
            assert adapter._token is None
            assert adapter._client is None

        def test_client_property_lazy_loads(self, mock_client):
            """Test client property lazy loads the client."""
            adapter = LixingerAdapter(token="test_token")
            with patch(
                "akshare_data.sources.lixinger_source.get_lixinger_client",
                return_value=mock_client,
            ):
                client = adapter.client
                assert client is mock_client

    class TestNormalizeDate:
        """Test _normalize_date method."""

        def test_normalize_date_with_datetime(self, adapter_with_mock_client):
            """Test date normalization with datetime object."""
            dt = datetime(2024, 1, 15)
            result = adapter_with_mock_client._normalize_date(dt)
            assert result == "2024-01-15"

        def test_normalize_date_with_date(self, adapter_with_mock_client):
            """Test date normalization with date object."""
            d = date(2024, 1, 15)
            result = adapter_with_mock_client._normalize_date(d)
            assert result == "2024-01-15"

        def test_normalize_date_with_string(self, adapter_with_mock_client):
            """Test date normalization with string."""
            result = adapter_with_mock_client._normalize_date("2024-01-15")
            assert result == "2024-01-15"

    class TestFormatIndexCode:
        """Test _format_index_code method."""

        def test_removes_xshg_suffix(self, adapter_with_mock_client):
            """Test removes .XSHG suffix."""
            result = adapter_with_mock_client._format_index_code("000300.XSHG")
            assert result == "000300"

        def test_removes_xshe_suffix(self, adapter_with_mock_client):
            """Test removes .XSHE suffix."""
            result = adapter_with_mock_client._format_index_code("000300.XSHE")
            assert result == "000300"

        def test_pads_to_6_digits(self, adapter_with_mock_client):
            """Test pads code to 6 digits."""
            result = adapter_with_mock_client._format_index_code("300")
            assert result == "000300"

    class TestNormalizeDailyDf:
        """Test _normalize_daily_df method."""

        def test_returns_empty_on_empty_df(self, adapter_with_mock_client):
            """Test returns empty DataFrame when input is empty."""
            result = adapter_with_mock_client._normalize_daily_df(
                pd.DataFrame(), "000001"
            )
            assert result.empty

        def test_normalizes_date_column(self, adapter_with_mock_client):
            """Test date column normalization."""
            df = pd.DataFrame(
                {"date": ["2024-01-01", "2024-01-02"], "close": [100.0, 101.0]}
            )
            result = adapter_with_mock_client._normalize_daily_df(df, "000001")
            assert "date" in result.columns

        def test_normalizes_chinese_columns(self, adapter_with_mock_client):
            """Test Chinese column name normalization."""
            df = pd.DataFrame(
                {
                    "日期": ["2024-01-01"],
                    "收盘": [100.0],
                    "开盘": [99.0],
                    "最高": [101.0],
                    "最低": [98.0],
                    "成交量": [1000000],
                    "成交额": [100000000],
                }
            )
            result = adapter_with_mock_client._normalize_daily_df(df, "000001")
            assert "close" in result.columns
            assert "open" in result.columns

        def test_preserves_standard_columns(self, adapter_with_mock_client):
            """Test preserves standard columns."""
            df = pd.DataFrame(
                {
                    "date": ["2024-01-01"],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.0],
                    "volume": [1000000],
                    "amount": [100000000],
                }
            )
            result = adapter_with_mock_client._normalize_daily_df(df, "000001")
            for col in ["date", "open", "high", "low", "close", "volume", "amount"]:
                assert col in result.columns

        def test_returns_original_on_no_date_column(self, adapter_with_mock_client):
            """Test returns original when no date column found."""
            df = pd.DataFrame({"close": [100.0]})
            result = adapter_with_mock_client._normalize_daily_df(df, "000001")
            assert result.empty

    class TestEnsureConfigured:
        """Test _ensure_configured method."""

        def test_raises_error_when_not_configured(self):
            """Test raises SourceUnavailableError when not configured."""
            adapter = LixingerAdapter()
            adapter._client = MagicMock()
            adapter._client.is_configured.return_value = False

            with pytest.raises(SourceUnavailableError):
                adapter._ensure_configured()

    class TestGetDailyData:
        """Test get_daily_data method."""

        def test_get_daily_data_success(self, adapter_with_mock_client, mock_client):
            """Test successful daily data retrieval."""
            mock_df = pd.DataFrame(
                {"date": ["2024-01-01", "2024-01-02"], "close": [100.0, 101.0]}
            )
            mock_client.get_index_candlestick.return_value = mock_df

            result = adapter_with_mock_client.get_daily_data(
                "000300", "2024-01-01", "2024-01-10"
            )
            assert isinstance(result, pd.DataFrame)

        def test_get_daily_data_empty_result(
            self, adapter_with_mock_client, mock_client
        ):
            """Test returns empty DataFrame when no data."""
            mock_client.get_index_candlestick.return_value = pd.DataFrame()

            result = adapter_with_mock_client.get_daily_data(
                "000300", "2024-01-01", "2024-01-10"
            )
            assert result.empty

        def test_get_daily_data_unconfigured_raises(self):
            """Test raises error when not configured."""
            adapter = LixingerAdapter(token="test_token")
            adapter._client = MagicMock()
            adapter._client.is_configured.return_value = False

            with pytest.raises(SourceUnavailableError):
                adapter.get_daily_data("000300", "2024-01-01", "2024-01-10")

    class TestGetIndexStocks:
        """Test get_index_stocks method."""

        def test_get_index_stocks_success(self, adapter_with_mock_client, mock_client):
            """Test successful index stocks retrieval."""
            mock_df = pd.DataFrame(
                {"stockCode": ["600519", "000858"], "stockName": ["茅台", "五粮液"]}
            )
            mock_client.get_index_constituents.return_value = mock_df

            result = adapter_with_mock_client.get_index_stocks("000300")
            assert isinstance(result, list)
            assert len(result) == 2

        def test_get_index_stocks_empty_result(
            self, adapter_with_mock_client, mock_client
        ):
            """Test returns empty list when no data."""
            mock_client.get_index_constituents.return_value = pd.DataFrame()

            result = adapter_with_mock_client.get_index_stocks("000300")
            assert result == []

        def test_get_index_stocks_formats_jq_codes(
            self, adapter_with_mock_client, mock_client
        ):
            """Test formats stocks to jq format."""
            mock_df = pd.DataFrame({"stockCode": ["600519", "000858"]})
            mock_client.get_index_constituents.return_value = mock_df

            result = adapter_with_mock_client.get_index_stocks("000300")
            assert "600519.XSHG" in result
            assert "000858.XSHE" in result

    class TestGetIndexComponents:
        """Test get_index_components method."""

        def test_get_index_components_with_weights(
            self, adapter_with_mock_client, mock_client
        ):
            """Test index components with weights."""
            mock_df = pd.DataFrame(
                {
                    "stockCode": ["600519"],
                    "stockName": ["茅台"],
                    "weight": [5.5],
                    "date": ["2024-01-01"],
                }
            )
            mock_client.get_index_constituent_weightings.return_value = mock_df

            result = adapter_with_mock_client.get_index_components(
                "000300", include_weights=True
            )
            assert "index_code" in result.columns
            assert "code" in result.columns
            assert "stock_name" in result.columns

        def test_get_index_components_without_weights(
            self, adapter_with_mock_client, mock_client
        ):
            """Test index components without weights."""
            mock_df = pd.DataFrame({"constituents": ["600519", "000858"]})
            mock_client.get_index_constituents.return_value = mock_df

            result = adapter_with_mock_client.get_index_components(
                "000300", include_weights=False
            )
            assert isinstance(result, pd.DataFrame)

        def test_get_index_components_empty_result(
            self, adapter_with_mock_client, mock_client
        ):
            """Test returns empty DataFrame when no data."""
            mock_client.get_index_constituent_weightings.return_value = pd.DataFrame()

            result = adapter_with_mock_client.get_index_components("000300")
            assert result.empty

    class TestGetTradingDays:
        """Test get_trading_days method."""

        def test_raises_not_implemented(self, adapter_with_mock_client):
            """Test Lixinger does not support trading days."""
            with pytest.raises(NotImplementedError):
                adapter_with_mock_client.get_trading_days()

    class TestGetSecuritiesList:
        """Test get_securities_list method."""

        def test_returns_empty_dataframe(self, adapter_with_mock_client):
            """Test Lixinger does not support securities list."""
            result = adapter_with_mock_client.get_securities_list()
            assert result.empty

    class TestGetSecurityInfo:
        """Test get_security_info method."""

        def test_returns_empty_dict(self, adapter_with_mock_client):
            """Test Lixinger does not support security info."""
            result = adapter_with_mock_client.get_security_info("000001")
            assert result == {}

    class TestGetMinuteData:
        """Test get_minute_data method."""

        def test_raises_not_implemented(self, adapter_with_mock_client):
            """Test Lixinger does not support minute data."""
            with pytest.raises(NotImplementedError):
                adapter_with_mock_client.get_minute_data("000001", freq="1min")

    class TestGetMoneyFlow:
        """Test get_money_flow method."""

        def test_raises_not_implemented(self, adapter_with_mock_client):
            """Test Lixinger does not support money flow."""
            with pytest.raises(NotImplementedError):
                adapter_with_mock_client.get_money_flow("000001")

    class TestGetNorthMoneyFlow:
        """Test get_north_money_flow method."""

        def test_raises_not_implemented(self, adapter_with_mock_client):
            """Test Lixinger does not support north money flow."""
            with pytest.raises(NotImplementedError):
                adapter_with_mock_client.get_north_money_flow()

    class TestGetIndustryStocks:
        """Test get_industry_stocks method."""

        def test_returns_empty_list(self, adapter_with_mock_client):
            """Test Lixinger does not support industry stocks."""
            result = adapter_with_mock_client.get_industry_stocks("801010")
            assert result == []

    class TestGetIndustryMapping:
        """Test get_industry_mapping method."""

        def test_returns_empty_string(self, adapter_with_mock_client):
            """Test Lixinger does not support industry mapping."""
            result = adapter_with_mock_client.get_industry_mapping("000001")
            assert result == ""

    class TestGetFinanceIndicator:
        """Test get_finance_indicator method."""

        def test_get_finance_indicator_success(
            self, adapter_with_mock_client, mock_client
        ):
            """Test successful finance indicator retrieval."""
            mock_df = pd.DataFrame({"pe_ttm.mcw": [10.5], "pb.mcw": [1.2]})
            mock_client.get_stock_financial.return_value = mock_df

            result = adapter_with_mock_client.get_finance_indicator("600519")
            assert isinstance(result, pd.DataFrame)

        def test_get_finance_indicator_default_metrics(
            self, adapter_with_mock_client, mock_client
        ):
            """Test uses default metrics when none provided."""
            mock_df = pd.DataFrame()
            mock_client.get_stock_financial.return_value = mock_df

            adapter_with_mock_client.get_finance_indicator("600519")
            call_args = mock_client.get_stock_financial.call_args
            assert "pe_ttm.mcw" in call_args.kwargs["metrics"]
            assert "pb.mcw" in call_args.kwargs["metrics"]

        def test_get_finance_indicator_empty_result(
            self, adapter_with_mock_client, mock_client
        ):
            """Test returns empty DataFrame when no data."""
            mock_client.get_stock_financial.return_value = pd.DataFrame()

            result = adapter_with_mock_client.get_finance_indicator("600519")
            assert result.empty

    class TestGetCallAuction:
        """Test get_call_auction method."""

        def test_raises_not_implemented(self, adapter_with_mock_client):
            """Test Lixinger does not support call auction."""
            with pytest.raises(NotImplementedError):
                adapter_with_mock_client.get_call_auction("000001")

    class TestHealthCheck:
        """Test health_check method."""

        def test_health_check_success(self, adapter_with_mock_client, mock_client):
            """Test health check success."""
            mock_df = pd.DataFrame({"col": [1, 2]})
            mock_client.get_index_constituents.return_value = mock_df

            result = adapter_with_mock_client.health_check()
            assert result["status"] == "ok"
            assert "message" in result
            assert "latency_ms" in result

        def test_health_check_error_on_exception(
            self, adapter_with_mock_client, mock_client
        ):
            """Test health check handles exceptions."""
            mock_client.get_index_constituents.side_effect = Exception(
                "Connection error"
            )
            mock_client.is_configured.return_value = True

            result = adapter_with_mock_client.health_check()
            assert result["status"] == "error"
            assert result["latency_ms"] is None

    class TestGetSourceInfo:
        """Test get_source_info method."""

        def test_returns_source_info_dict(self, adapter_with_mock_client):
            """Test get_source_info returns proper dict."""
            result = adapter_with_mock_client.get_source_info()
            assert result["name"] == "lixinger"
            assert result["type"] == "partial"
            assert result["requires_auth"] is True

    class TestGetStockValuation:
        """Test get_stock_valuation method."""

        def test_get_stock_valuation_success(
            self, adapter_with_mock_client, mock_client
        ):
            """Test successful stock valuation retrieval."""
            mock_df = pd.DataFrame({"pe_ttm.mcw": [10.5], "pb.mcw": [1.2]})
            mock_client.get_stock_financial.return_value = mock_df

            result = adapter_with_mock_client.get_stock_valuation("600519")
            assert isinstance(result, pd.DataFrame)

        def test_get_stock_valuation_empty_result(
            self, adapter_with_mock_client, mock_client
        ):
            """Test returns empty DataFrame when no data."""
            mock_client.get_stock_financial.return_value = pd.DataFrame()

            result = adapter_with_mock_client.get_stock_valuation("600519")
            assert result.empty

    class TestGetStockPePb:
        """Test get_stock_pe_pb method."""

        def test_delegates_to_get_stock_valuation(
            self, adapter_with_mock_client, mock_client
        ):
            """Test get_stock_pe_pb delegates to get_stock_valuation."""
            mock_df = pd.DataFrame({"pe": [10.5], "pb": [1.2]})
            mock_client.get_stock_financial.return_value = mock_df

            result = adapter_with_mock_client.get_stock_pe_pb("600519")
            assert isinstance(result, pd.DataFrame)

    class TestGetIndexValuation:
        """Test get_index_valuation method."""

        def test_get_index_valuation_success(
            self, adapter_with_mock_client, mock_client
        ):
            """Test successful index valuation retrieval."""
            mock_df = pd.DataFrame({"pe_ttm.mcw": [12.0], "pb.mcw": [1.3]})
            mock_client.get_index_fundamental.return_value = mock_df

            result = adapter_with_mock_client.get_index_valuation("000300")
            assert isinstance(result, pd.DataFrame)

        def test_get_index_valuation_empty_result(
            self, adapter_with_mock_client, mock_client
        ):
            """Test returns empty DataFrame when no data."""
            mock_client.get_index_fundamental.return_value = pd.DataFrame()

            result = adapter_with_mock_client.get_index_valuation("000300")
            assert result.empty
