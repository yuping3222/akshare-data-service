"""Integration tests for the namespace API surface.

Verifies that DataService namespace proxies (cn.*, macro.*, hk.*, us.*)
correctly delegate to the underlying namespace classes and ultimately
call the service's cached_fetch / source methods.

The data source is mocked; all namespace classes and DataService wiring
are exercised with real implementations.
"""

from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from akshare_data.api import (
    CNMarketAPI,
    CNStockQuoteAPI,
    CNIndexQuoteAPI,
    CNETFQuoteAPI,
    MacroChinaAPI,
    MacroAPI,
    CNStockFinanceAPI,
    CNStockCapitalAPI,
    CNStockEventAPI,
    CNIndexMetaAPI,
    SourceProxy,
)


# ---------------------------------------------------------------------------
# Helper: create a minimal stock-data DataFrame for mocked returns
# ---------------------------------------------------------------------------


def _daily_df(symbol="600000", start="2024-01-02", end="2024-01-10"):
    dates = pd.date_range(start, end, freq="B")
    n = len(dates)
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * n,
            "open": [10.0 + i * 0.1 for i in range(n)],
            "high": [11.0 + i * 0.1 for i in range(n)],
            "low": [9.0 + i * 0.1 for i in range(n)],
            "close": [10.5 + i * 0.1 for i in range(n)],
            "volume": [100_000] * n,
            "amount": [1_000_000.0] * n,
        }
    )


@pytest.mark.integration
class TestNamespaceAccess:
    """Verify DataService exposes the expected namespace attributes."""

    def test_cn_returns_cn_market_api(self, data_service):
        """DataService.cn attribute access returns CNMarketAPI instance."""
        assert isinstance(data_service.cn, CNMarketAPI)
        assert data_service.cn.service is data_service

    def test_cn_stock_namespace(self, data_service):
        """cn.stock has quote, finance, capital, event sub-namespaces."""
        stock = data_service.cn.stock
        assert isinstance(stock.quote, CNStockQuoteAPI)
        assert isinstance(stock.finance, CNStockFinanceAPI)
        assert isinstance(stock.capital, CNStockCapitalAPI)
        assert isinstance(stock.event, CNStockEventAPI)

    def test_cn_index_namespace(self, data_service):
        """cn.index has quote and meta sub-namespaces."""
        index = data_service.cn.index
        assert isinstance(index.quote, CNIndexQuoteAPI)
        assert isinstance(index.meta, CNIndexMetaAPI)

    def test_cn_fund_namespace(self, data_service):
        """cn.fund has quote sub-namespace (ETF data lives here)."""
        fund = data_service.cn.fund
        assert isinstance(fund.quote, CNETFQuoteAPI)

    def test_macro_namespace(self, data_service):
        """DataService.macro returns MacroAPI with china sub-namespace."""
        assert isinstance(data_service.macro, MacroAPI)
        assert isinstance(data_service.macro.china, MacroChinaAPI)

    def test_hk_and_us_namespaces(self, data_service):
        """DataService exposes hk and us market namespaces."""
        assert data_service.hk is not None
        assert data_service.us is not None
        assert hasattr(data_service.hk, "stock")
        assert hasattr(data_service.us, "stock")

    def test_cn_bond_raises_attribute_error(self, data_service):
        """cn.bond does not exist; accessing it raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = data_service.cn.bond

    def test_cn_industry_raises_attribute_error(self, data_service):
        """cn.industry does not exist; accessing it raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = data_service.cn.industry


@pytest.mark.integration
class TestStockQuoteDelegation:
    """cn.stock.quote.* methods delegate to service.cached_fetch correctly."""

    def test_stock_quote_daily_calls_cached_fetch(self, data_service):
        """cn.stock.quote.daily delegates to cached_fetch with table=stock_daily."""
        mock_df = _daily_df()

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            result = data_service.cn.stock.quote.daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-10",
                adjust="qfq",
            )

        assert result is mock_df
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["table"] == "stock_daily"
        assert call_kwargs["storage_layer"] == "daily"
        assert call_kwargs["partition_by"] == "symbol"
        assert call_kwargs["partition_value"] == "600000"

    def test_stock_quote_daily_normalizes_symbol(self, data_service):
        """cn.stock.quote.daily normalizes the symbol before caching."""
        mock_df = _daily_df()

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            data_service.cn.stock.quote.daily(
                symbol="600000",
                start_date="2024-01-02",
                end_date="2024-01-10",
            )

        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["partition_value"] == "600000"

    def test_stock_quote_daily_passes_fetch_fn(self, data_service):
        """cn.stock.quote.daily passes a fetch_fn that calls get_daily_data."""
        mock_df = _daily_df()
        mock_source = MagicMock()
        mock_source.get_daily_data.return_value = mock_df

        with patch.object(data_service, "_get_source", return_value=mock_source):
            with patch.object(
                data_service, "cached_fetch", return_value=mock_df
            ) as mock_fetch:
                data_service.cn.stock.quote.daily(
                    symbol="600000",
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                    adjust="hfq",
                )
                fetch_fn = mock_fetch.call_args[1]["fetch_fn"]
                result = fetch_fn()
                mock_source.get_daily_data.assert_called_once_with(
                    "600000", "2024-01-02", "2024-01-10", "hfq"
                )
                assert result is mock_df


@pytest.mark.integration
class TestIndexQuoteDelegation:
    """cn.index.quote.* methods delegate correctly."""

    def test_index_quote_daily_calls_cached_fetch(self, data_service):
        """cn.index.quote.daily delegates to cached_fetch with table=index_daily."""
        mock_df = _daily_df(symbol="000300")

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            result = data_service.cn.index.quote.daily(
                symbol="000300",
                start_date="2024-01-02",
                end_date="2024-01-10",
            )

        assert result is mock_df
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["table"] == "index_daily"
        assert call_kwargs["storage_layer"] == "daily"
        assert call_kwargs["partition_value"] == "000300"

    def test_index_quote_daily_calls_get_index_daily(self, data_service):
        """cn.index.quote.daily fetch_fn calls get_index_daily on source."""
        mock_df = _daily_df(symbol="000300")
        mock_source = MagicMock()
        mock_source.get_index_daily.return_value = mock_df

        with patch.object(data_service, "_get_source", return_value=mock_source):
            with patch.object(
                data_service, "cached_fetch", return_value=mock_df
            ) as mock_fetch:
                data_service.cn.index.quote.daily(
                    symbol="000300",
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                )
                fetch_fn = mock_fetch.call_args[1]["fetch_fn"]
                fetch_fn()
                mock_source.get_index_daily.assert_called_once_with(
                    "000300", "2024-01-02", "2024-01-10"
                )


@pytest.mark.integration
class TestEtfQuoteDelegation:
    """cn.fund.quote.* methods (ETF data) delegate correctly."""

    def test_etf_quote_daily_calls_cached_fetch(self, data_service):
        """cn.fund.quote.daily delegates to cached_fetch with table=etf_daily."""
        mock_df = _daily_df(symbol="510300")

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            result = data_service.cn.fund.quote.daily(
                symbol="510300",
                start_date="2024-01-02",
                end_date="2024-01-10",
            )

        assert result is mock_df
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["table"] == "etf_daily"
        assert call_kwargs["storage_layer"] == "daily"
        assert call_kwargs["partition_value"] == "510300"

    def test_etf_quote_daily_calls_get_etf_daily(self, data_service):
        """cn.fund.quote.daily fetch_fn calls get_etf_daily on source."""
        mock_df = _daily_df(symbol="510300")
        mock_source = MagicMock()
        mock_source.get_etf_daily.return_value = mock_df

        with patch.object(data_service, "_get_source", return_value=mock_source):
            with patch.object(
                data_service, "cached_fetch", return_value=mock_df
            ) as mock_fetch:
                data_service.cn.fund.quote.daily(
                    symbol="510300",
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                )
                fetch_fn = mock_fetch.call_args[1]["fetch_fn"]
                fetch_fn()
                mock_source.get_etf_daily.assert_called_once_with(
                    "510300", "2024-01-02", "2024-01-10"
                )

    def test_etf_accessible_via_get_etf_facade(self, data_service):
        """DataService.get_etf delegates to cn.fund.quote.daily."""
        mock_df = _daily_df(symbol="510300")
        with patch.object(
            data_service.cn.fund.quote, "daily", return_value=mock_df
        ) as mock_daily:
            result = data_service.get_etf(
                symbol="510300",
                start_date="2024-01-02",
                end_date="2024-01-10",
            )
            assert result is mock_df
            mock_daily.assert_called_once_with(
                "510300", "2024-01-02", "2024-01-10", None
            )


@pytest.mark.integration
class TestMacroDelegation:
    """macro.china.* methods delegate to cached_fetch correctly."""

    def test_macro_china_interest_rate(self, data_service):
        """macro.china.interest_rate delegates to cached_fetch with table=shibor_rate."""
        mock_df = pd.DataFrame({"date": ["2024-01-02"], "rate": [1.5]})

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            result = data_service.macro.china.interest_rate(
                start_date="2024-01-01", end_date="2024-01-10"
            )

        assert result is mock_df
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["table"] == "shibor_rate"
        assert call_kwargs["storage_layer"] == "daily"

    def test_macro_china_interest_rate_calls_shibor(self, data_service):
        """macro.china.interest_rate fetch_fn calls get_shibor_rate on source."""
        mock_df = pd.DataFrame({"date": ["2024-01-02"], "rate": [1.5]})
        mock_source = MagicMock()
        mock_source.get_shibor_rate.return_value = mock_df

        with patch.object(data_service, "_get_source", return_value=mock_source):
            with patch.object(
                data_service, "cached_fetch", return_value=mock_df
            ) as mock_fetch:
                data_service.macro.china.interest_rate(
                    start_date="2024-01-01", end_date="2024-01-10"
                )
                fetch_fn = mock_fetch.call_args[1]["fetch_fn"]
                fetch_fn()
                mock_source.get_shibor_rate.assert_called_once_with(
                    "2024-01-01", "2024-01-10"
                )

    def test_macro_china_gdp(self, data_service):
        """macro.china.gdp delegates to cached_fetch with table=macro_gdp."""
        mock_df = pd.DataFrame({"date": ["2024-Q1"], "value": [30000]})

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            result = data_service.macro.china.gdp(
                start_date="2024-01-01", end_date="2024-12-31"
            )

        assert result is mock_df
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["table"] == "macro_gdp"

    def test_macro_china_social_financing(self, data_service):
        """macro.china.social_financing delegates to cached_fetch with table=social_financing."""
        mock_df = pd.DataFrame({"date": ["2024-01"], "value": [5000]})

        with patch.object(
            data_service, "cached_fetch", return_value=mock_df
        ) as mock_fetch:
            result = data_service.macro.china.social_financing(
                start_date="2024-01-01", end_date="2024-12-31"
            )

        assert result is mock_df
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["table"] == "social_financing"


@pytest.mark.integration
class TestSourceProxy:
    """SourceProxy dynamic method dispatch."""

    def test_source_proxy_creates_callable(self, data_service):
        """SourceProxy returns a proxy that wraps method calls."""
        proxy = data_service._get_source("akshare")
        assert isinstance(proxy, SourceProxy)
        assert proxy.requested_source == "akshare"

    def test_source_proxy_dispatches_to_execute_source_method(self, data_service):
        """Calling a method on SourceProxy delegates to _execute_source_method."""
        proxy = data_service._get_source("akshare")

        with patch.object(
            data_service, "_execute_source_method", return_value="mock_result"
        ) as mock_exec:
            result = proxy.get_daily_data("sh600000", "2024-01-01", "2024-01-10")

        assert result == "mock_result"
        mock_exec.assert_called_once_with(
            "get_daily_data", "akshare", "sh600000", "2024-01-01", "2024-01-10"
        )

    def test_source_proxy_passes_kwargs(self, data_service):
        """SourceProxy forwards keyword arguments correctly."""
        proxy = data_service._get_source("lixinger")

        with patch.object(
            data_service, "_execute_source_method", return_value=42
        ) as mock_exec:
            result = proxy.some_method(arg1="a", arg2=123)

        mock_exec.assert_called_once_with("some_method", "lixinger", arg1="a", arg2=123)
        assert result == 42

    def test_source_proxy_with_none_source(self, data_service):
        """SourceProxy with None source passes None as requested_source."""
        proxy = data_service._get_source()
        assert proxy.requested_source is None

    def test_source_proxy_with_list_source(self, data_service):
        """SourceProxy with list of sources passes list as requested_source."""
        proxy = data_service._get_source(["lixinger", "akshare"])
        assert proxy.requested_source == ["lixinger", "akshare"]

    def test_source_proxy_arbitrary_method(self, data_service):
        """SourceProxy can dispatch any method name dynamically."""
        proxy = data_service._get_source("akshare")

        with patch.object(
            data_service, "_execute_source_method", return_value="ok"
        ) as mock_exec:
            proxy.nonexistent_method_name(1, 2, x=3)

        mock_exec.assert_called_once_with(
            "nonexistent_method_name", "akshare", 1, 2, x=3
        )


@pytest.mark.integration
class TestNamespaceErrorHandling:
    """Namespace error handling: invalid access raises appropriate errors."""

    def test_invalid_cn_subnamespace_raises_attribute_error(self, data_service):
        """Accessing a non-existent cn.* sub-namespace raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = data_service.cn.nonexistent

    def test_invalid_stock_sub_namespace_raises_attribute_error(self, data_service):
        """Accessing cn.stock.nonexistent raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = data_service.cn.stock.nonexistent

    def test_invalid_index_sub_namespace_raises_attribute_error(self, data_service):
        """Accessing cn.index.nonexistent raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = data_service.cn.index.nonexistent

    def test_invalid_fund_sub_namespace_raises_attribute_error(self, data_service):
        """Accessing cn.fund.nonexistent raises AttributeError."""
        with pytest.raises(AttributeError):
            _ = data_service.cn.fund.nonexistent

    def test_macro_china_invalid_method_is_callable_but_returns_from_source(
        self, data_service
    ):
        """macro.china allows any attribute access (via __getattr__ is not defined,
        so invalid attribute raises AttributeError)."""
        with pytest.raises(AttributeError):
            _ = data_service.macro.china.nonexistent_method_xyz

    def test_namespace_classes_store_service_reference(self, data_service):
        """All namespace classes store a reference back to the DataService."""
        assert data_service.cn.stock.quote.service is data_service
        assert data_service.cn.index.quote.service is data_service
        assert data_service.cn.fund.quote.service is data_service
        assert data_service.macro.china.service is data_service
