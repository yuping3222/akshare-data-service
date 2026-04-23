"""Tests for akshare_data.standardized.normalizer.market_quote_daily module."""

import pandas as pd

from akshare_data.standardized.normalizer.market_quote_daily import (
    MarketQuoteDailyNormalizer,
)


STANDARD_FIELDS = {
    "security_id",
    "exchange",
    "adjust_type",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "turnover_amount",
    "batch_id",
    "source_name",
    "interface_name",
    "ingest_time",
    "normalize_version",
    "schema_version",
}


def _akshare_eastmoney_raw() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["600519"] * 3,
            "日期": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "开盘": [10.50, 10.60, 10.70],
            "最高": [10.80, 10.90, 11.00],
            "最低": [10.40, 10.50, 10.60],
            "收盘": [10.70, 10.80, 10.90],
            "成交量": [100000, 110000, 120000],
            "成交额": [1070000.0, 1188000.0, 1308000.0],
            "振幅": [3.81, 3.77, 3.74],
            "涨跌幅": [1.90, 0.93, 0.93],
            "涨跌额": [0.20, 0.10, 0.10],
            "换手率": [1.50, 1.65, 1.80],
        }
    )


def _akshare_sina_raw() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["000001"] * 2,
            "date": ["2024-01-02", "2024-01-03"],
            "open": [20.00, 20.10],
            "high": [20.50, 20.60],
            "low": [19.90, 20.00],
            "close": [20.30, 20.40],
            "volume": [50000, 55000],
            "amount": [1015000.0, 1122000.0],
        }
    )


def _tushare_raw() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["600519.SH", "600519.SH"],
            "trade_date": ["20240102", "20240103"],
            "open": [1800.00, 1810.00],
            "high": [1820.00, 1830.00],
            "low": [1790.00, 1800.00],
            "close": [1810.00, 1820.00],
            "vol": [30000, 32000],
            "amount": [54300000.0, 58240000.0],
            "pct_chg": [0.56, 0.55],
        }
    )


def _baostock_raw() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["sh.600519", "sh.600519"],
            "date": ["2024-01-02", "2024-01-03"],
            "open": [1800.00, 1810.00],
            "high": [1820.00, 1830.00],
            "low": [1790.00, 1800.00],
            "close": [1810.00, 1820.00],
            "volume": [30000, 32000],
            "amount": [54300000.0, 58240000.0],
            "adjustflag": "3",
            "turn": [0.50, 0.53],
            "pctChg": [0.56, 0.55],
        }
    )


def _raw_with_symbol_prefix() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["sh600519", "000001.XSHE", "sz.000001", "600519"],
            "date": ["2024-01-02"] * 4,
            "open": [10.0, 20.0, 30.0, 40.0],
            "high": [11.0, 21.0, 31.0, 41.0],
            "low": [9.0, 19.0, 29.0, 39.0],
            "close": [10.5, 20.5, 30.5, 40.5],
            "volume": [1000, 2000, 3000, 4000],
            "amount": [10500.0, 41000.0, 91500.0, 162000.0],
        }
    )


def _raw_with_missing_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["600519", None, "000001", "600519"],
            "date": ["2024-01-02", "2024-01-03", None, "2024-01-04"],
            "open": [10.0, 20.0, 30.0, 40.0],
            "high": [11.0, 21.0, 31.0, 41.0],
            "low": [9.0, 19.0, 29.0, 39.0],
            "close": [10.5, 20.5, 30.5, 40.5],
            "volume": [1000, 2000, 3000, 4000],
            "amount": [10500.0, 41000.0, 91500.0, 162000.0],
        }
    )


class TestMarketQuoteDailyNormalizer:
    """Test the market_quote_daily normalizer with various sources."""

    def setup_method(self):
        self.normalizer = MarketQuoteDailyNormalizer()

    # ------------------------------------------------------------------
    # AkShare EastMoney
    # ------------------------------------------------------------------

    def test_akshare_eastmoney_basic(self):
        """Should normalize eastmoney raw data to standard fields."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b1",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert len(result) == 3
        assert set(result.columns) == STANDARD_FIELDS
        assert result["trade_date"].iloc[0] == pd.Timestamp("2024-01-02").date()
        assert result["open_price"].iloc[0] == 10.50
        assert result["close_price"].iloc[0] == 10.70
        assert result["volume"].iloc[0] == 100000
        assert result["turnover_amount"].iloc[0] == 1070000.0

    def test_akshare_eastmoney_derives_exchange(self):
        """Should derive exchange from security_id prefix."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b1",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert "exchange" in result.columns

    def test_akshare_eastmoney_default_adjust_type(self):
        """Should default adjust_type to 'none' when not present."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b1",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert (result["adjust_type"] == "none").all()

    # ------------------------------------------------------------------
    # AkShare Sina
    # ------------------------------------------------------------------

    def test_akshare_sina_basic(self):
        """Should normalize sina raw data to standard fields."""
        df = _akshare_sina_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b2",
            source_name="akshare_sina",
            interface_name="stock_zh_a_daily",
        )
        assert len(result) == 2
        assert set(result.columns) == STANDARD_FIELDS
        assert result["open_price"].iloc[0] == 20.00
        assert result["close_price"].iloc[0] == 20.30

    # ------------------------------------------------------------------
    # Tushare
    # ------------------------------------------------------------------

    def test_tushare_basic(self):
        """Should normalize tushare raw data to standard fields."""
        df = _tushare_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b3",
            source_name="tushare",
            interface_name="daily",
        )
        assert len(result) == 2
        assert set(result.columns) == STANDARD_FIELDS
        assert result["security_id"].iloc[0] == "600519"
        assert result["exchange"].iloc[0] == "SSE"
        assert result["volume"].iloc[0] == 30000

    def test_tushare_symbol_conversion(self):
        """Should convert ts_code format (600519.SH) to 6-digit security_id."""
        df = _tushare_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b3",
            source_name="tushare",
            interface_name="daily",
        )
        assert result["security_id"].iloc[0] == "600519"
        assert result["security_id"].iloc[1] == "600519"

    # ------------------------------------------------------------------
    # Baostock
    # ------------------------------------------------------------------

    def test_baostock_basic(self):
        """Should normalize baostock raw data to standard fields."""
        df = _baostock_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b4",
            source_name="baostock",
            interface_name="bs_daily",
        )
        assert len(result) == 2
        assert set(result.columns) == STANDARD_FIELDS
        assert result["security_id"].iloc[0] == "600519"
        assert result["exchange"].iloc[0] == "SSE"

    def test_baostock_adjust_type_mapping(self):
        """Should map baostock adjustflag '3' to 'none'."""
        df = _baostock_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b4",
            source_name="baostock",
            interface_name="bs_daily",
        )
        assert (result["adjust_type"] == "none").all()

    # ------------------------------------------------------------------
    # Symbol normalization
    # ------------------------------------------------------------------

    def test_symbol_normalization_various_formats(self):
        """Should convert various symbol formats to 6-digit."""
        df = _raw_with_symbol_prefix()
        result = self.normalizer.normalize(
            df,
            batch_id="b5",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert result["security_id"].iloc[0] == "600519"
        assert result["security_id"].iloc[1] == "000001"
        assert result["security_id"].iloc[2] == "000001"
        assert result["security_id"].iloc[3] == "600519"

    def test_exchange_derivation(self):
        """Should derive correct exchange from security_id."""
        df = _raw_with_symbol_prefix()
        result = self.normalizer.normalize(
            df,
            batch_id="b5",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert result.loc[result["security_id"] == "600519", "exchange"].iloc[0] == "SSE"
        assert result.loc[result["security_id"] == "000001", "exchange"].iloc[0] == "SZSE"

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def test_drops_rows_with_missing_security_id(self):
        """Should drop rows where security_id is None."""
        df = _raw_with_missing_rows()
        result = self.normalizer.normalize(
            df,
            batch_id="b6",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert len(result) == 2
        assert result["security_id"].notna().all()
        assert result["trade_date"].notna().all()

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_empty_dataframe(self):
        """Should return empty DataFrame for empty input."""
        result = self.normalizer.normalize(
            pd.DataFrame(),
            batch_id="b7",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert result.empty

    def test_none_dataframe(self):
        """Should return empty DataFrame for None input."""
        result = self.normalizer.normalize(
            None,
            batch_id="b7",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert result.empty

    def test_no_legacy_field_names_in_output(self):
        """Output must not contain legacy field names."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b8",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        legacy_names = {"symbol", "date", "close", "amount", "open", "high", "low"}
        assert not (set(result.columns) & legacy_names)

    def test_system_fields_present(self):
        """All mandatory system fields must be present."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="batch_001",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        system_fields = {
            "batch_id",
            "source_name",
            "interface_name",
            "ingest_time",
            "normalize_version",
            "schema_version",
        }
        assert system_fields.issubset(set(result.columns))

    def test_normalize_version_and_schema_version(self):
        """Should set correct version fields."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b9",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert (result["normalize_version"] == "v1").all()
        assert (result["schema_version"] == "v1").all()

    # ------------------------------------------------------------------
    # Optional fields
    # ------------------------------------------------------------------

    def test_optional_fields_not_in_required_output(self):
        """change_pct and turnover_rate are optional, not in required set."""
        df = _akshare_eastmoney_raw()
        result = self.normalizer.normalize(
            df,
            batch_id="b10",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert "change_pct" not in result.columns
        assert "turnover_rate" not in result.columns
