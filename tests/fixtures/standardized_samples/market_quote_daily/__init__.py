"""Test fixtures for market_quote_daily normalizer.

Provides sample raw DataFrames mimicking different data sources.
"""

import pandas as pd


def akshare_eastmoney_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by akshare stock_zh_a_hist (eastmoney)."""
    return pd.DataFrame(
        {
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


def akshare_sina_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by akshare stock_zh_a_daily (sina)."""
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "open": [20.00, 20.10],
            "high": [20.50, 20.60],
            "low": [19.90, 20.00],
            "close": [20.30, 20.40],
            "volume": [50000, 55000],
            "amount": [1015000.0, 1122000.0],
        }
    )


def tushare_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by tushare daily."""
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


def baostock_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by baostock."""
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


def raw_with_symbol_prefix() -> pd.DataFrame:
    """Raw DataFrame with various symbol formats."""
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


def raw_with_missing_rows() -> pd.DataFrame:
    """Raw DataFrame with some rows missing security_id or date."""
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
