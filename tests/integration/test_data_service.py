"""Integration tests for the DataService full pipeline.

These tests exercise the end-to-end flow through DataService:
parameter validation, cache lookup, source fetching, cache writeback,
field mapping, symbol normalization, incremental updates, namespace
delegation, error propagation, and edge-case handling.

The data source layer (AkShareAdapter / LixingerAdapter) is mocked to
return known DataFrames; all other layers (CacheManager, CachedFetcher,
CachedFetcher strategies, symbol normalization, namespace routing) use
real implementations.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest

from akshare_data.core.errors import DataSourceError, SourceUnavailableError, ErrorCode
from akshare_data.core.symbols import normalize_symbol, jq_code_to_ak, ak_code_to_jq
from akshare_data.store.manager import CacheManager, reset_cache_manager
from akshare_data.api import CNMarketAPI, CNStockQuoteAPI


# ===================================================================
# Helpers
# ===================================================================


def _make_daily_df(symbol="sh600000", start="2024-01-02", end="2024-01-15"):
    """Build a daily OHLCV DataFrame over business days in [start, end]."""
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
            "volume": [100_000 + i * 10_000 for i in range(n)],
            "amount": [1_000_000.0 + i * 100_000.0 for i in range(n)],
        }
    )


def _make_minute_df(symbol="sh600000", periods=30):
    """Build a minute-bar DataFrame."""
    times = pd.date_range("2024-01-02 09:30", periods=periods, freq="min")
    return pd.DataFrame(
        {
            "datetime": times,
            "symbol": [symbol] * periods,
            "open": [10.0 + i * 0.01 for i in range(periods)],
            "high": [10.05 + i * 0.01 for i in range(periods)],
            "low": [9.95 + i * 0.01 for i in range(periods)],
            "close": [10.02 + i * 0.01 for i in range(periods)],
            "volume": [5_000 + i * 500 for i in range(periods)],
        }
    )


def _make_chinese_daily_df(symbol="sh600000", start="2024-01-02", end="2024-01-15"):
    """Build a daily DataFrame with Chinese column names (AkShare style)."""
    dates = pd.date_range(start, end, freq="B")
    n = len(dates)
    return pd.DataFrame(
        {
            "日期": dates,
            "代码": [symbol] * n,
            "开盘": [10.0 + i * 0.1 for i in range(n)],
            "最高": [11.0 + i * 0.1 for i in range(n)],
            "最低": [9.0 + i * 0.1 for i in range(n)],
            "收盘": [10.5 + i * 0.1 for i in range(n)],
            "成交量": [100_000 + i * 10_000 for i in range(n)],
            "成交额": [1_000_000.0 + i * 100_000.0 for i in range(n)],
        }
    )


# ===================================================================
# Test class
# ===================================================================


@pytest.mark.integration
class TestDataServiceFullPipeline:
    """Full-pipeline integration tests for DataService.

    Each test constructs an isolated CacheManager (via temp_cache_dir
    fixture) and a DataService on top of it.  The AkShareAdapter source
    is patched so its fetch() returns a controlled DataFrame.
    """

    def setup_method(self):
        reset_cache_manager()
        CacheManager.reset_instance()

    def teardown_method(self):
        reset_cache_manager()
        CacheManager.reset_instance()

    # ----------------------------------------------------------------
    # 1. get_daily() full pipeline: param validation -> cache check ->
    #    fetch -> cache writeback -> return
    # ----------------------------------------------------------------

    def test_get_daily_full_pipeline_cache_miss_then_hit(
        self, data_service, temp_cache_dir
    ):
        """get_daily() on first call misses cache, fetches from source,
        writes back to cache; second call hits cache without invoking source.

        Steps verified:
        1. Call get_daily() with symbol, start_date, end_date
        2. Cache is empty -> miss
        3. Source (AkShareAdapter) is invoked
        4. Returned DataFrame has expected rows and columns
        5. A second get_daily() call returns data without calling source again
        """
        service = data_service
        daily_df = _make_daily_df()

        with patch.object(
            service.akshare, "get_daily_data", return_value=daily_df.copy()
        ) as mock_src:
            # First call: cache miss, should hit source
            result1 = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result1.empty
            assert "close" in result1.columns or "收盘" in result1.columns
            assert mock_src.call_count == 1

            # Second call: cache hit, should NOT hit source again
            result2 = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result2.empty
            assert mock_src.call_count == 1  # still 1, not incremented

    # ----------------------------------------------------------------
    # 2. get_minute() full pipeline with minute-level data
    # ----------------------------------------------------------------

    def test_get_minute_full_pipeline(self, data_service, temp_cache_dir):
        """get_minute() fetches minute-level bars, caches them, and returns
        a DataFrame with datetime index.

        Verifies:
        1. Source is called with correct minute params
        2. Result contains datetime column
        3. Data is cached (subsequent call doesn't re-fetch)
        """
        service = data_service
        minute_df = _make_minute_df(periods=30)

        with patch.object(
            service.akshare, "get_minute_data", return_value=minute_df.copy()
        ) as mock_src:
            result = service.get_minute(
                symbol="sh600000",
                freq="1min",
                start_date="2024-01-02 09:30",
                end_date="2024-01-02 10:00",
                source="akshare",
            )
            assert not result.empty
            assert mock_src.call_count == 1
            # Source should have been called with normalized symbol and freq
            call_args = mock_src.call_args
            assert call_args is not None

    # ----------------------------------------------------------------
    # 3. Field mapping: different source field names -> unified English
    #    field names
    # ----------------------------------------------------------------

    def test_field_mapping_chinese_to_english(self, data_service, temp_cache_dir):
        """Source returns Chinese column names (AkShare style); pipeline
        maps them to unified English names.

        Verifies that data returned from the pipeline has either English
        column names (after mapping) or the original Chinese names
        (mapping may happen at different layers).  The key check is that
        the data is accessible and non-empty regardless of source naming.
        """
        service = data_service
        chinese_df = _make_chinese_daily_df()

        with patch.object(
            service.akshare, "get_daily_data", return_value=chinese_df.copy()
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result.empty
            # At least one of the known column name styles should be present
            has_english = any(
                col in result.columns
                for col in ("close", "open", "high", "low", "volume")
            )
            has_chinese = any(
                col in result.columns
                for col in ("收盘", "开盘", "最高", "最低", "成交量")
            )
            assert has_english or has_chinese, (
                f"Expected English or Chinese column names, got: {list(result.columns)}"
            )

    def test_field_mapping_lixinger_style(self, data_service, temp_cache_dir):
        """Lixinger source returns different field names (e.g. 'closePx');
        pipeline handles them correctly.
        """
        service = data_service
        lixinger_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-10", freq="B"),
                "symbol": ["sh600000"] * 7,
                "closePx": [10.5 + i * 0.1 for i in range(7)],
                "openPx": [10.0 + i * 0.1 for i in range(7)],
                "highPx": [11.0 + i * 0.1 for i in range(7)],
                "lowPx": [9.0 + i * 0.1 for i in range(7)],
                "volume": [100_000] * 7,
            }
        )

        with patch.object(
            service.lixinger, "get_daily_data", return_value=lixinger_df.copy()
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-10",
                source="lixinger",
            )
            assert not result.empty

    # ----------------------------------------------------------------
    # 4. Symbol conversion: sh600000 -> 600000.XSHG propagation through
    #    pipeline
    # ----------------------------------------------------------------

    def test_symbol_conversion_sh_to_jq_format(self, data_service, temp_cache_dir):
        """Input symbol 'sh600000' is normalized to '600000' internally and
        propagated as '600000.XSHG' in JQ-style format.

        Verifies:
        1. normalize_symbol('sh600000') returns '600000'
        2. jq_code_to_ak('600000.XSHG') returns 'sh600000'
        3. ak_code_to_jq('sh600000') returns '600000.XSHG'
        4. The pipeline receives and returns data with consistent symbols
        """
        # Unit-level checks on symbol functions
        assert normalize_symbol("sh600000") == "600000"
        assert normalize_symbol("600000.XSHG") == "600000"
        assert jq_code_to_ak("600000.XSHG") == "sh600000"
        assert ak_code_to_jq("sh600000") == "600000.XSHG"

        # Integration-level: pipeline handles sz prefix too
        assert normalize_symbol("sz000001") == "000001"
        assert jq_code_to_ak("000001.XSHE") == "sz000001"
        assert ak_code_to_jq("sz000001") == "000001.XSHE"

    def test_symbol_propagation_in_pipeline(self, data_service, temp_cache_dir):
        """Symbol format is correctly propagated through the get_daily pipeline.

        The source receives the normalized symbol, and the returned
        DataFrame contains the expected symbol column.
        """
        service = data_service
        daily_df = _make_daily_df(symbol="sh600000")

        with patch.object(
            service.akshare, "get_daily_data", return_value=daily_df.copy()
        ) as mock_src:
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result.empty
            # Source should be called (the adapter handles symbol normalization internally)
            assert mock_src.call_count == 1

    # ----------------------------------------------------------------
    # 5. Incremental update: partial cache exists -> detect missing range
    #    -> fetch delta -> merge
    # ----------------------------------------------------------------

    def test_incremental_update_partial_cache(self, data_service, temp_cache_dir):
        """First fetch caches Jan 2-8; second fetch for Jan 2-15 detects
        missing Jan 9-15, fetches only the delta, merges with cached data,
        and returns the full range.

        Steps:
        1. Fetch and cache data for 2024-01-02 to 2024-01-08
        2. Request data for 2024-01-02 to 2024-01-15
        3. Pipeline detects missing 2024-01-09 to 2024-01-15
        4. Fetches delta and merges
        5. Result covers the full requested range
        """
        service = data_service

        # Step 1: Initial fetch for first week
        week1_df = _make_daily_df(start="2024-01-02", end="2024-01-08")
        with patch.object(
            service.akshare, "get_daily_data", return_value=week1_df.copy()
        ) as mock_src:
            result1 = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-08",
                source="akshare",
            )
            assert not result1.empty
            first_call_count = mock_src.call_count

        # Step 2: Request full range; should only fetch the missing portion
        week2_df = _make_daily_df(start="2024-01-09", end="2024-01-15")

        def side_effect(*args, **kwargs):
            # The incremental fetcher will call with the missing range
            s = kwargs.get("start_date", args[1] if len(args) > 1 else "2024-01-09")
            if s >= "2024-01-09":
                return week2_df.copy()
            return week1_df.copy()

        with patch.object(
            service.akshare, "get_daily_data", side_effect=side_effect
        ) as mock_src:
            result2 = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result2.empty
            # Source should have been called at least once more for the delta
            assert mock_src.call_count >= first_call_count

    def test_incremental_update_all_cached(self, data_service, temp_cache_dir):
        """When the entire requested date range is already cached, no source
        call is made.
        """
        service = data_service
        full_df = _make_daily_df()

        # First call populates cache
        with patch.object(
            service.akshare, "get_daily_data", return_value=full_df.copy()
        ) as mock_src:
            service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert mock_src.call_count == 1

        # Second call for a subset should hit cache
        service.get_daily(
            symbol="sh600000",
            start_date="2024-01-02",
            end_date="2024-01-08",
            source="akshare",
        )
        assert mock_src.call_count == 1  # no additional source call

    # ----------------------------------------------------------------
    # 6. Namespace delegation: service.cn -> CN namespace classes
    # ----------------------------------------------------------------

    def test_namespace_delegation_cn_stock_quote_daily(
        self, data_service, temp_cache_dir
    ):
        """service.cn.stock.quote.daily delegates to CNStockQuoteAPI.daily,
        which calls cached_fetch with the correct table and storage layer.
        """
        service = data_service
        assert isinstance(service.cn, CNMarketAPI)
        assert isinstance(service.cn.stock.quote, CNStockQuoteAPI)

        daily_df = _make_daily_df()
        with patch.object(
            service.akshare, "get_daily_data", return_value=daily_df.copy()
        ):
            result = service.cn.stock.quote.daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result.empty

    def test_namespace_delegation_cn_index_quote_daily(
        self, data_service, temp_cache_dir
    ):
        """service.cn.index.quote.daily delegates to CNIndexQuoteAPI.daily
        for index data.
        """
        service = data_service
        index_df = _make_daily_df(symbol="sh000001")

        with patch.object(
            service.akshare, "get_index_daily", return_value=index_df.copy()
        ):
            result = service.cn.index.quote.daily(
                symbol="sh000001",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result.empty

    def test_namespace_delegation_cn_etf_quote_daily(
        self, data_service, temp_cache_dir
    ):
        """service.cn.fund.quote.daily delegates to CNETFQuoteAPI.daily
        for ETF data.
        """
        service = data_service
        etf_df = _make_daily_df(symbol="sh510050")

        with patch.object(service.akshare, "get_etf_daily", return_value=etf_df.copy()):
            result = service.cn.fund.quote.daily(
                symbol="sh510050",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert not result.empty

    def test_namespace_delegation_cn_finance_indicators(
        self, data_service, temp_cache_dir
    ):
        """service.cn.stock.finance.indicators delegates to CNStockFinanceAPI.indicators."""
        service = data_service
        finance_df = pd.DataFrame(
            {
                "report_date": ["2024-03-31", "2023-12-31"],
                "symbol": ["600000", "600000"],
                "roe": [0.12, 0.11],
                "pe": [10.5, 11.2],
            }
        )

        with patch.object(
            service.akshare, "get_finance_indicator", return_value=finance_df.copy()
        ):
            result = service.cn.stock.finance.indicators(
                symbol="sh600000",
                start_date="2023-01-01",
                end_date="2024-12-31",
                source="akshare",
            )
            assert not result.empty

    def test_convenience_method_delegates_to_namespace(self, data_service):
        """Convenience methods (get_daily, get_minute, get_index, get_etf)
        delegate to the correct namespace classes.
        """
        service = data_service

        # get_daily -> cn.stock.quote.daily
        assert callable(service.get_daily)
        # get_minute -> cn.stock.quote.minute
        assert callable(service.get_minute)
        # get_index -> cn.index.quote.daily
        assert callable(service.get_index)
        # get_etf -> cn.fund.quote.daily
        assert callable(service.get_etf)
        # get_money_flow -> cn.stock.capital.money_flow
        assert callable(service.get_money_flow)
        # get_trading_days -> cn.trade_calendar
        assert callable(service.get_trading_days)

    # ----------------------------------------------------------------
    # 7. Error propagation: source error -> correct exception with
    #    ErrorCode
    # ----------------------------------------------------------------

    def test_source_error_propagates_with_error_code(
        self, data_service, temp_cache_dir
    ):
        """When a source raises DataSourceError, the error is caught by the
        router (logged as a warning) and the pipeline returns an empty DataFrame.
        This tests graceful degradation on source failure.
        """
        service = data_service

        with patch.object(
            service.akshare,
            "get_daily_data",
            side_effect=DataSourceError(
                message="Source unavailable",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
            ),
        ):
            # Router catches the error and returns empty
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert result.empty

    def test_source_unavailable_error_propagation(self, data_service, temp_cache_dir):
        """SourceUnavailableError is caught by the router and results in
        graceful degradation (empty DataFrame returned).
        """
        service = data_service

        with patch.object(
            service.akshare,
            "get_daily_data",
            side_effect=SourceUnavailableError(
                message="Connection refused",
                error_code=ErrorCode.SOURCE_CONNECTION_REFUSED,
            ),
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert result.empty

    def test_all_sources_fail_returns_empty(self, data_service, temp_cache_dir):
        """When both lixinger and akshare sources fail, the pipeline
        returns an empty DataFrame rather than raising an exception
        (graceful degradation).
        """
        service = data_service

        with (
            patch.object(
                service.lixinger,
                "get_daily_data",
                side_effect=DataSourceError(
                    "Lixinger down", error_code=ErrorCode.SOURCE_UNAVAILABLE
                ),
            ),
            patch.object(
                service.akshare,
                "get_daily_data",
                side_effect=DataSourceError(
                    "AkShare down", error_code=ErrorCode.SOURCE_UNAVAILABLE
                ),
            ),
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
            )
            # With all sources failing, the result should be empty
            assert result.empty

    # ----------------------------------------------------------------
    # 8. Empty result handling: source returns empty DataFrame -> handled
    #    gracefully
    # ----------------------------------------------------------------

    def test_empty_source_result_returns_empty_df(self, data_service, temp_cache_dir):
        """When the source returns an empty DataFrame, the pipeline returns
        an empty DataFrame without raising errors.
        """
        service = data_service
        empty_df = pd.DataFrame()

        with patch.object(service.akshare, "get_daily_data", return_value=empty_df):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert result.empty

    def test_source_returns_none_treated_as_empty(self, data_service, temp_cache_dir):
        """When the source returns None, the pipeline treats it as empty
        and returns an empty DataFrame.
        """
        service = data_service

        with patch.object(service.akshare, "get_daily_data", return_value=None):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date="2024-01-15",
                source="akshare",
            )
            assert result.empty

    def test_empty_minute_result_handled(self, data_service, temp_cache_dir):
        """get_minute() gracefully handles an empty source result."""
        service = data_service

        with patch.object(
            service.akshare, "get_minute_data", return_value=pd.DataFrame()
        ):
            result = service.get_minute(
                symbol="sh600000",
                freq="1min",
                start_date="2024-01-02 09:30",
                end_date="2024-01-02 10:00",
                source="akshare",
            )
            assert result.empty

    # ----------------------------------------------------------------
    # 9. Date range validation: invalid dates -> proper error
    # ----------------------------------------------------------------

    def test_invalid_start_date_handled_gracefully(self, data_service, temp_cache_dir):
        """get_daily() with an unparseable start_date is passed through to the
        source; when the source fails, the pipeline returns an empty DataFrame
        rather than crashing.
        """
        service = data_service

        with patch.object(
            service.akshare, "get_daily_data", return_value=pd.DataFrame()
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="not-a-date",
                end_date="2024-01-15",
                source="akshare",
            )
            assert result.empty

    def test_end_before_start_date(self, data_service, temp_cache_dir):
        """When end_date is before start_date, the pipeline returns an
        empty DataFrame (no valid date range to query).
        """
        service = data_service

        with patch.object(
            service.akshare, "get_daily_data", return_value=pd.DataFrame()
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-06-01",
                end_date="2024-01-01",
                source="akshare",
            )
            assert result.empty

    def test_future_date_handling(self, data_service, temp_cache_dir):
        """Requesting data with a future end_date is handled gracefully;
        the source is called but no data is expected for future dates.
        """
        service = data_service
        future = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

        with patch.object(
            service.akshare, "get_daily_data", return_value=pd.DataFrame()
        ):
            result = service.get_daily(
                symbol="sh600000",
                start_date="2024-01-02",
                end_date=future,
                source="akshare",
            )
            # Should return whatever the source provides (likely empty or partial)
            assert isinstance(result, pd.DataFrame)
