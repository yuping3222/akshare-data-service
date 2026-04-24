"""Tests for Lixinger API client."""

import pytest
import pandas as pd
import json
from unittest.mock import patch, MagicMock

from akshare_data.sources.lixinger_client import LixingerClient, get_lixinger_client


@pytest.mark.unit
class TestLixingerClient:
    """Test suite for LixingerClient class."""

    @pytest.fixture
    def mock_session(self):
        """Create mock requests session."""
        session = MagicMock()
        response = MagicMock()
        response.json.return_value = {"code": 1, "data": []}
        response.status_code = 200
        session.post.return_value = response
        return session

    @pytest.fixture
    def client_with_token(self):
        """Create LixingerClient with test token."""
        with patch.object(LixingerClient, "_load_token", return_value="test_token"):
            with patch.object(LixingerClient, "_create_session") as mock_create:
                mock_session = MagicMock()
                mock_response = MagicMock()
                mock_response.json.return_value = {"code": 1, "data": []}
                mock_response.status_code = 200
                mock_session.post.return_value = mock_response
                mock_create.return_value = mock_session

                client = LixingerClient.__new__(LixingerClient)
                object.__setattr__(client, "_token", "test_token")
                object.__setattr__(client, "_session", mock_session)
                object.__setattr__(client, "logger", MagicMock())
                object.__setattr__(client, "_initialized", True)
                LixingerClient._instance = client
                return client

    class TestInitialization:
        """Test LixingerClient initialization."""

        def test_singleton_pattern(self):
            """Test LixingerClient is a singleton."""
            with patch.object(LixingerClient, "_load_token", return_value="test_token"):
                with patch.object(LixingerClient, "_create_session") as mock_create:
                    mock_session = MagicMock()
                    mock_create.return_value = mock_session

                    client1 = LixingerClient()
                    client2 = LixingerClient()
                    assert client1 is client2

        def test_token_loaded_on_init(self):
            """Test token is loaded during initialization."""
            with patch.object(LixingerClient, "_load_token", return_value="my_token"):
                with patch.object(LixingerClient, "_create_session") as mock_create:
                    mock_session = MagicMock()
                    mock_create.return_value = mock_session

                    client = LixingerClient.__new__(LixingerClient)
                    object.__setattr__(client, "_token", "my_token")
                    object.__setattr__(client, "_session", mock_session)
                    object.__setattr__(client, "logger", MagicMock())

                    assert client._token == "my_token"

    class TestTokenLoading:
        """Test _load_token method."""

        def test_loads_from_environment_variable(self):
            """Test loads token from LIXINGER_TOKEN env var."""
            with patch.dict("os.environ", {"LIXINGER_TOKEN": "env_token"}):
                with patch("pathlib.Path.exists", return_value=False):
                    token = LixingerClient._load_token()
                    assert token == "env_token"

        def test_loads_from_config_file(self):
            """Test loads token from token.cfg file."""
            with patch.dict("os.environ", {}, clear=True):
                with patch(
                    "akshare_data.sources.lixinger_client._get_token",
                    return_value="file_token",
                ):
                    token = LixingerClient._load_token()
                    assert token == "file_token"

        def test_returns_empty_string_when_no_token(self):
            """Test returns empty string when no token found."""
            with patch.dict("os.environ", {}, clear=True):
                with patch(
                    "akshare_data.sources.lixinger_client._get_token",
                    return_value=None,
                ):
                    # Also reset the TokenManager singleton to clear any cached state
                    from akshare_data.core.tokens import TokenManager

                    TokenManager.reset()
                    token = LixingerClient._load_token()
                    assert token == ""

    class TestSessionCreation:
        """Test _create_session method."""

        def test_creates_session_with_retry(self):
            """Test creates session with retry strategy."""
            session = LixingerClient._create_session()
            assert session is not None

        def test_session_has_adapter_mounted(self):
            """Test session has HTTP adapters mounted."""
            session = LixingerClient._create_session()
            assert hasattr(session, "mount")

    class TestIsConfigured:
        """Test is_configured property."""

        def test_returns_true_when_token_set(self, client_with_token):
            """Test returns True when token is set."""
            assert client_with_token.is_configured() is True

        def test_returns_false_when_token_empty(self):
            """Test returns False when token is empty."""
            client = LixingerClient.__new__(LixingerClient)
            object.__setattr__(client, "_token", "")
            object.__setattr__(client, "_session", MagicMock())
            object.__setattr__(client, "logger", MagicMock())

            assert client.is_configured() is False

    class TestTokenProperty:
        """Test token property."""

        def test_returns_configured_token(self, client_with_token):
            """Test returns configured token."""
            assert client_with_token.token == "test_token"

    class TestSessionProperty:
        """Test session property."""

        def test_returns_session(self, client_with_token):
            """Test returns the HTTP session."""
            assert client_with_token.session is not None

    class TestQueryApi:
        """Test query_api method."""

        def test_raises_error_when_no_token(self):
            """Test raises RuntimeError when token not configured."""
            client = LixingerClient.__new__(LixingerClient)
            object.__setattr__(client, "_token", "")
            object.__setattr__(client, "_session", MagicMock())
            object.__setattr__(client, "logger", MagicMock())

            with pytest.raises(RuntimeError, match="not configured"):
                client.query_api("test/suffix", {})

        def test_query_api_success(self, client_with_token):
            """Test successful API query."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": [{"test": "data"}]}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.query_api("cn/test", {"param": "value"})
            assert result["code"] == 1

        def test_query_api_error_response(self, client_with_token):
            """Test API returns error response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": -1, "msg": "Invalid request"}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.query_api("cn/test", {})
            assert result["code"] == -1

        def test_query_api_timeout(self, client_with_token):
            """Test handles request timeout."""
            import requests

            client_with_token._session.post.side_effect = requests.exceptions.Timeout()

            with pytest.raises(RuntimeError, match="timeout"):
                client_with_token.query_api("cn/test", {})

        def test_query_api_adds_token_to_params(self, client_with_token):
            """Test adds token to request params."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.query_api("cn/test", {"key": "value"})

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert "token" in params
            assert params["token"] == "test_token"

    class TestGetIndexCandlestick:
        """Test get_index_candlestick method."""

        def test_returns_empty_dataframe_on_error_response(self, client_with_token):
            """Test returns empty DataFrame on error response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": -1, "msg": "Error"}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_candlestick(
                "000300", "2024-01-01", "2024-01-10"
            )
            assert result.empty

        def test_returns_empty_dataframe_when_no_data(self, client_with_token):
            """Test returns empty DataFrame when no data returned."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_candlestick(
                "000300", "2024-01-01", "2024-01-10"
            )
            assert result.empty

        def test_returns_dataframe_on_success(self, client_with_token):
            """Test returns DataFrame on successful response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "code": 1,
                "data": [
                    {"date": "2024-01-01", "close": 3000.0},
                    {"date": "2024-01-02", "close": 3010.0},
                ],
            }
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_candlestick(
                "000300", "2024-01-01", "2024-01-10"
            )
            assert isinstance(result, pd.DataFrame)
            assert not result.empty

        def test_uses_candlestick_type_parameter(self, client_with_token):
            """Test uses candlestick_type parameter."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_index_candlestick(
                "000300", "2024-01-01", "2024-01-10", candlestick_type="total_return"
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["type"] == "total_return"

    class TestGetIndexConstituents:
        """Test get_index_constituents method."""

        def test_returns_empty_on_error_response(self, client_with_token):
            """Test returns empty DataFrame on error response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": -1, "msg": "Error"}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_constituents("000300")
            assert result.empty

        def test_returns_empty_when_no_data(self, client_with_token):
            """Test returns empty DataFrame when no data."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_constituents("000300")
            assert result.empty

        def test_returns_dataframe_on_success(self, client_with_token):
            """Test returns DataFrame on successful response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "code": 1,
                "data": [
                    {"stockCode": "600519", "stockName": "茅台"},
                    {"stockCode": "000858", "stockName": "五粮液"},
                ],
            }
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_constituents("000300")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2

        def test_uses_latest_date_by_default(self, client_with_token):
            """Test uses 'latest' date by default."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_index_constituents("000300")

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["date"] == "latest"

    class TestGetIndexConstituentWeightings:
        """Test get_index_constituent_weightings method."""

        def test_returns_empty_on_error_response(self, client_with_token):
            """Test returns empty DataFrame on error response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": -1, "msg": "Error"}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_constituent_weightings(
                "000300", "2024-01-01", "2024-01-31"
            )
            assert result.empty

        def test_returns_empty_when_no_data(self, client_with_token):
            """Test returns empty DataFrame when no data."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_constituent_weightings(
                "000300", "2024-01-01", "2024-01-31"
            )
            assert result.empty

        def test_passes_required_parameters(self, client_with_token):
            """Test passes correct parameters to API."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_index_constituent_weightings(
                "000300", "2024-01-01", "2024-01-31"
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["stockCodes"] == ["000300"]
            assert params["startDate"] == "2024-01-01"
            assert params["endDate"] == "2024-01-31"

    class TestGetIndexFundamental:
        """Test get_index_fundamental method."""

        def test_returns_empty_on_error_response(self, client_with_token):
            """Test returns empty DataFrame on error response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": -1, "msg": "Error"}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_index_fundamental(
                symbols=["000300"], metrics=["pe_ttm.mcw"]
            )
            assert result.empty

        def test_passes_symbols_and_metrics(self, client_with_token):
            """Test passes symbols and metrics correctly."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_index_fundamental(
                symbols=["000300", "000905"], metrics=["pe_ttm.mcw", "pb.mcw"]
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["stockCodes"] == ["000300", "000905"]
            assert params["metricsList"] == ["pe_ttm.mcw", "pb.mcw"]

        def test_includes_date_when_provided(self, client_with_token):
            """Test includes date parameter when provided."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_index_fundamental(
                symbols=["000300"], metrics=["pe_ttm.mcw"], date="2024-01-15"
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["date"] == "2024-01-15"

    class TestGetStockFinancial:
        """Test get_stock_financial method."""

        def test_returns_empty_on_error_response(self, client_with_token):
            """Test returns empty DataFrame on error response."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": -1, "msg": "Error"}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            result = client_with_token.get_stock_financial(
                symbol="600519", metrics=["pe_ttm.mcw"]
            )
            assert result.empty

        def test_passes_symbol_and_metrics(self, client_with_token):
            """Test passes symbol and metrics correctly."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_stock_financial(
                symbol="600519", metrics=["pe_ttm.mcw", "pb.mcw"]
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["stockCodes"] == ["600519"]
            assert params["metricsList"] == ["pe_ttm.mcw", "pb.mcw"]

        def test_uses_date_parameter_when_provided(self, client_with_token):
            """Test uses date parameter when provided."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_stock_financial(
                symbol="600519", metrics=["pe_ttm.mcw"], date="latest"
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["date"] == "latest"
            assert "startDate" not in params

        def test_uses_start_end_dates_when_provided(self, client_with_token):
            """Test uses start/end date parameters."""
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 1, "data": []}
            mock_response.status_code = 200
            client_with_token._session.post.return_value = mock_response

            client_with_token.get_stock_financial(
                symbol="600519",
                metrics=["pe_ttm.mcw"],
                start_date="2024-01-01",
                end_date="2024-06-30",
            )

            call_args = client_with_token._session.post.call_args
            params = json.loads(
                call_args.kwargs.get("data") or call_args[1].get("data")
            )
            assert params["startDate"] == "2024-01-01"
            assert params["endDate"] == "2024-06-30"

    class TestGetLixingerClient:
        """Test get_lixinger_client function."""

        def test_returns_client_instance(self):
            """Test returns LixingerClient instance."""
            with patch.object(LixingerClient, "_load_token", return_value="test_token"):
                with patch.object(LixingerClient, "_create_session") as mock_create:
                    mock_session = MagicMock()
                    mock_create.return_value = mock_session

                    client = get_lixinger_client()
                    assert isinstance(client, LixingerClient)

        def test_returns_same_instance(self):
            """Test returns the same instance (singleton)."""
            with patch.object(LixingerClient, "_load_token", return_value="test_token"):
                with patch.object(LixingerClient, "_create_session") as mock_create:
                    mock_session = MagicMock()
                    mock_create.return_value = mock_session

                    client1 = get_lixinger_client()
                    client2 = get_lixinger_client()
                    assert client1 is client2

    class TestBaseUrl:
        """Test BASE_URL constant."""

        def test_base_url_is_correct(self):
            """Test BASE_URL is set correctly."""
            assert LixingerClient.BASE_URL == "https://open.lixinger.com/api/"
