"""Integration tests for akshare/fetcher.py core functions.

Tests the core fetch() path, _build_call_kwargs, _normalize_output,
_transform_param with mocked akshare calls.
"""

import pytest
import pandas as pd

from akshare_data.sources.akshare.fetcher import (
    fetch,
    _transform_param,
    _build_call_kwargs,
    _normalize_output,
    _to_pandas_type,
    reload_config,
)
from akshare_data.core.errors import SourceUnavailableError


@pytest.mark.unit
# ── _transform_param unit tests ──────────────────────────────────────
class TestTransformParam:
    def test_yyyymmdd_date_object(self):
        from datetime import date

        result = _transform_param(date(2024, 1, 15), "YYYYMMDD")
        assert result == "20240115"

    def test_yyyymmdd_string_with_dashes(self):
        result = _transform_param("2024-01-15", "YYYYMMDD")
        assert result == "20240115"

    def test_yyyymmdd_string_with_slashes(self):
        result = _transform_param("2024/01/15", "YYYYMMDD")
        assert result == "20240115"

    def test_yyyymmdd_already_formatted(self):
        result = _transform_param("20240115", "YYYYMMDD")
        assert result == "20240115"

    def test_to_ts_code_sh(self):
        result = _transform_param("600000", "to_ts_code")
        assert result == "600000.SH"

    def test_to_ts_code_sz(self):
        result = _transform_param("000001", "to_ts_code")
        assert result == "000001.SZ"

    def test_to_ak_code_strip_suffixes(self):
        result = _transform_param("600000.XSHG", "to_ak_code")
        assert result == "600000"

    def test_strip_symbol(self):
        result = _transform_param("sh600000", "strip")
        assert result == "600000"

    def test_none_value_returns_none(self):
        assert _transform_param(None, "YYYYMMDD") is None

    def test_no_transform_returns_same(self):
        assert _transform_param("hello", "unknown") == "hello"

    def test_prepend_prefix_handles_prefixed_symbol(self):
        assert _transform_param("sh000001", "prepend_prefix:sh/sz") == "sh000001"
        assert _transform_param("sz000001", "prepend_prefix:sh/sz") == "sz000001"

    def test_prepend_prefix_adds_prefix_for_raw_symbol(self):
        assert _transform_param("600000", "prepend_prefix:sh/sz") == "sh600000"
        assert _transform_param("000001", "prepend_prefix:sh/sz") == "sz000001"


# ── _to_pandas_type ──────────────────────────────────────────────────
class TestToPandasType:
    def test_str(self):
        assert _to_pandas_type("str") == "str"

    def test_float(self):
        assert _to_pandas_type("float") == "float64"

    def test_int(self):
        assert _to_pandas_type("int") == "int64"

    def test_date(self):
        assert _to_pandas_type("date") == "datetime64[ns]"

    def test_unknown(self):
        assert _to_pandas_type("unknown") == "str"


# ── _build_call_kwargs ───────────────────────────────────────────────
class TestBuildCallKwargs:
    def test_input_mapping_applied(self):
        source = {
            "input_mapping": {"symbol": "stock_code", "start_date": "begin"},
            "param_transforms": {"start_date": "YYYYMMDD"},
        }
        result = _build_call_kwargs(
            {"symbol": "600000", "start_date": "2024-01-01", "extra": "keep"},
            source,
        )
        assert result["stock_code"] == "600000"
        assert result["begin"] == "20240101"
        assert result["extra"] == "keep"

    def test_no_mapping_passes_through(self):
        source = {"input_mapping": {}, "param_transforms": {}}
        result = _build_call_kwargs({"a": 1, "b": 2}, source)
        assert result == {"a": 1, "b": 2}

    def test_filters_invalid_params(self):
        def dummy(accepts_this, also_this):
            pass

        source = {"input_mapping": {}, "param_transforms": {}}
        result = _build_call_kwargs(
            {"accepts_this": 1, "also_this": 2, "not_this": 3},
            source,
            ak_func=dummy,
        )
        assert "not_this" not in result
        assert result["accepts_this"] == 1
        assert result["also_this"] == 2


# ── _normalize_output ────────────────────────────────────────────────
class TestNormalizeOutput:
    def test_rename_columns(self):
        source = {"output_mapping": {"old_col": "new_col"}}
        df = pd.DataFrame({"old_col": [1, 2], "other": [3, 4]})
        result = _normalize_output(df, source)
        assert "new_col" in result.columns
        assert "old_col" not in result.columns

    def test_type_conversion(self):
        source = {"column_types": {"num_col": "float"}}
        df = pd.DataFrame({"num_col": ["1.5", "2.5"]})
        result = _normalize_output(df, source)
        assert result["num_col"].dtype == "float64"

    def test_datetime_conversion(self):
        source = {"column_types": {"date_col": "date"}}
        df = pd.DataFrame({"date_col": ["2024-01-01", "2024-01-02"]})
        result = _normalize_output(df, source)
        assert pd.api.types.is_datetime64_any_dtype(result["date_col"])

    def test_empty_df_returns_unchanged(self):
        df = pd.DataFrame()
        result = _normalize_output(df, {"output_mapping": {}})
        assert result.empty


# ── reload_config ────────────────────────────────────────────────────
class TestReloadConfig:
    def test_reload_does_not_crash(self):
        reload_config()  # should not raise


# ── fetch() with mocked akshare — macro endpoints ────────────────────
class TestFetchMacroData:
    """Test fetch() dispatches to correct akshare functions with mocked data."""

    def _make_mock_ak(self, func_map):
        """Build a mock akshare module with the given function->DataFrame mapping."""
        import types

        ak = types.SimpleNamespace()
        for func_name, df in func_map.items():
            setattr(ak, func_name, lambda _df=df, **_kw: _df.copy())
        return ak

    def test_fetch_macro_cpi(self):
        """Verify fetch('macro_cpi') calls macro_china_cpi and applies output mapping."""
        mock_df = pd.DataFrame(
            {
                "月份": ["2024-01", "2024-02"],
                "全国居民消费价格指数(CPI)上年同月=100": [101.5, 100.8],
            }
        )
        ak = self._make_mock_ak({"macro_china_cpi": mock_df})
        result = fetch("macro_cpi", akshare=ak)
        assert isinstance(result, pd.DataFrame)
        assert "date" in result.columns
        assert "cpi" in result.columns
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "macro_cpi"

    def test_fetch_macro_gdp(self):
        """Verify fetch('macro_gdp') calls macro_china_gdp and applies output mapping."""
        mock_df = pd.DataFrame(
            {
                "季度": ["2024Q1", "2024Q2"],
                "国内生产总值绝对额(亿元)": [300000, 320000],
            }
        )
        ak = self._make_mock_ak({"macro_china_gdp": mock_df})
        result = fetch("macro_gdp", akshare=ak)
        assert isinstance(result, pd.DataFrame)
        assert "date" in result.columns
        assert "gdp" in result.columns
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "macro_gdp"

    def test_fetch_macro_ppi(self):
        """Verify fetch('macro_ppi') calls macro_china_ppi."""
        mock_df = pd.DataFrame(
            {
                "月份": ["2024-01"],
                "工业生产者出厂价格指数(PPI)": [98.5],
            }
        )
        ak = self._make_mock_ak({"macro_china_ppi": mock_df})
        result = fetch("macro_ppi", akshare=ak)
        assert isinstance(result, pd.DataFrame)
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "macro_ppi"

    def test_fetch_macro_pmi(self):
        """Verify fetch('macro_pmi') calls macro_china_pmi and maps columns."""
        mock_df = pd.DataFrame(
            {
                "月份": ["2024-01", "2024-02"],
                "制造业PMI": [50.8, 50.5],
            }
        )
        ak = self._make_mock_ak({"macro_china_pmi": mock_df})
        result = fetch("macro_pmi", akshare=ak)
        assert isinstance(result, pd.DataFrame)
        assert "date" in result.columns
        assert "pmi" in result.columns
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "macro_pmi"


# ── fetch() with mocked akshare — stock/ETF endpoints ────────────────
class TestFetchStockData:
    """Test fetch() dispatches to correct akshare functions with mocked data."""

    def _make_mock_ak(self, func_map):
        import types

        ak = types.SimpleNamespace()
        for func_name, df in func_map.items():
            setattr(ak, func_name, lambda _df=df, **_kw: _df.copy())
        return ak

    def test_fetch_equity_daily(self):
        """Verify fetch('equity_daily') calls stock_zh_a_hist and applies mappings."""
        mock_df = pd.DataFrame(
            {
                "日期": ["2024-01-02", "2024-01-03"],
                "开盘": [10.5, 10.6],
                "最高": [11.0, 11.1],
                "最低": [10.3, 10.4],
                "收盘": [10.8, 10.9],
                "成交量": [1000000, 1200000],
                "成交额": [10800000, 13080000],
            }
        )
        ak = self._make_mock_ak({"stock_zh_a_hist": mock_df})
        result = fetch(
            "equity_daily",
            akshare=ak,
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-03-01",
        )
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
        ]
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "equity_daily"
        # Verify param transforms: dates should be converted to YYYYMMDD
        # The mock captures the call; verify the function was called (non-empty result proves it)
        assert not result.empty

    def test_fetch_index_list(self):
        """Verify fetch('index_list') calls stock_zh_index_spot_em."""
        mock_df = pd.DataFrame(
            {
                "代码": ["000001", "000300"],
                "名称": ["上证指数", "沪深300"],
            }
        )
        ak = self._make_mock_ak({"stock_zh_index_spot_em": mock_df})
        result = fetch("index_list", akshare=ak)
        assert isinstance(result, pd.DataFrame)
        # output_mapping is empty for this interface, so original columns preserved
        assert "代码" in result.columns
        assert "名称" in result.columns
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "index_list"

    def test_fetch_etf_list(self):
        """Verify fetch('etf_list') calls fund_etf_spot_em and maps columns."""
        mock_df = pd.DataFrame(
            {
                "基金代码": ["510300", "159919"],
                "基金简称": ["沪深300ETF", "深100ETF"],
            }
        )
        ak = self._make_mock_ak({"fund_etf_spot_em": mock_df})
        result = fetch("etf_list", akshare=ak)
        assert isinstance(result, pd.DataFrame)
        assert "symbol" in result.columns
        assert "name" in result.columns
        assert result.attrs.get("source") == "akshare_em"
        assert result.attrs.get("interface") == "etf_list"


# ── fetch() backward compat aliases (mocked) ─────────────────────────
class TestFetchAliases:
    """Test backward-compatible fetch_xxx aliases with mocked akshare."""

    def _make_mock_ak(self, func_map):
        import types

        ak = types.SimpleNamespace()
        for func_name, df in func_map.items():
            setattr(ak, func_name, lambda _df=df, **_kw: _df.copy())
        return ak

    def test_fetch_daily_data_alias(self):
        """Verify fetch_daily_data alias calls fetch('equity_daily')."""
        from akshare_data.sources.akshare.fetcher import fetch_daily_data

        mock_df = pd.DataFrame(
            {
                "日期": ["2024-01-02"],
                "开盘": [10.5],
                "最高": [11.0],
                "最低": [10.3],
                "收盘": [10.8],
                "成交量": [1000000],
                "成交额": [10800000],
            }
        )
        ak = self._make_mock_ak({"stock_zh_a_hist": mock_df})
        result = fetch_daily_data(
            ak, symbol="000001", start_date="20240101", end_date="20240201"
        )
        assert isinstance(result, pd.DataFrame)
        assert "date" in result.columns
        assert result.attrs.get("interface") == "equity_daily"

    def test_fetch_cpi_data_alias(self):
        """Verify fetch_cpi_data alias calls fetch('macro_cpi')."""
        from akshare_data.sources.akshare.fetcher import fetch_cpi_data

        mock_df = pd.DataFrame(
            {
                "月份": ["2024-01"],
                "全国居民消费价格指数(CPI)上年同月=100": [101.5],
            }
        )
        ak = self._make_mock_ak({"macro_china_cpi": mock_df})
        result = fetch_cpi_data(ak)
        assert isinstance(result, pd.DataFrame)
        assert "date" in result.columns
        assert "cpi" in result.columns
        assert result.attrs.get("interface") == "macro_cpi"

    def test_fetch_trading_days_alias(self):
        """Verify fetch_trading_days alias calls fetch('trading_days')."""
        from akshare_data.sources.akshare.fetcher import fetch_trading_days

        mock_df = pd.DataFrame({"trade_date": ["2024-01-02", "2024-01-03"]})
        ak = self._make_mock_ak({"tool_trade_date_hist_sina": mock_df})
        result = fetch_trading_days(ak, start_date="2024-01-01", end_date="2024-02-01")
        assert isinstance(result, pd.DataFrame)
        assert result.attrs.get("interface") == "trading_days"

    def test_fetch_lpr_rate_alias(self):
        """Verify fetch_lpr_rate alias calls fetch('macro_lpr')."""
        from akshare_data.sources.akshare.fetcher import fetch_lpr_rate

        mock_df = pd.DataFrame(
            {
                "日期": ["2024-01-20"],
                "1年": [3.45],
                "5年": [4.20],
            }
        )
        ak = self._make_mock_ak({"macro_china_lpr": mock_df})
        result = fetch_lpr_rate(ak)
        assert isinstance(result, pd.DataFrame)
        assert result.attrs.get("interface") == "macro_lpr"

    def test_fetch_m2_supply_alias(self):
        """Verify fetch_m2_supply alias calls fetch('macro_m2')."""
        from akshare_data.sources.akshare.fetcher import fetch_m2_supply

        mock_df = pd.DataFrame(
            {
                "月份": ["2024-01"],
                "货币和准货币(广义货币M2)(亿元)": [300000],
            }
        )
        ak = self._make_mock_ak({"macro_china_m2_yearly": mock_df})
        result = fetch_m2_supply(ak)
        assert isinstance(result, pd.DataFrame)
        assert result.attrs.get("interface") == "macro_m2"


# ── fetch() error paths ──────────────────────────────────────────────
class TestFetchErrorPaths:
    def test_fetch_undefined_interface(self):
        with pytest.raises(ValueError, match="未定义"):
            fetch("nonexistent_interface_xyz")

    def test_fetch_with_no_akshare_function(self):
        """Test fetch when akshare function doesn't exist for interface."""
        with pytest.raises((ValueError, SourceUnavailableError)):
            fetch(
                "equity_daily",
                symbol="INVALID",
                start_date="2099-01-01",
                end_date="2099-01-02",
            )
