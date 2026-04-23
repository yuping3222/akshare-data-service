"""Test fixtures for macro_indicator normalizer.

Provides sample raw DataFrames mimicking different data sources.
"""

import pandas as pd


def akshare_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by akshare macro indicator interface."""
    return pd.DataFrame(
        {
            "indicator": ["CPI", "CPI", "GDP", "GDP", "PMI"],
            "date": ["2024-01", "2024-02", "2024-Q1", "2023-Q4", "2024-03"],
            "value": [102.5, 103.0, 1260000.0, 1240000.0, 50.8],
            "yoy": [2.5, 3.0, 5.3, 5.2, None],
            "mom": [0.5, 0.3, None, None, 0.2],
        }
    )


def tushare_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by tushare macro interface."""
    return pd.DataFrame(
        {
            "month": ["2024-01", "2024-02"],
            "cpi": [102.5, 103.0],
            "ppi": [98.5, 98.2],
            "pmi": [50.8, 50.5],
        }
    )


def standardized_sample() -> pd.DataFrame:
    """Standardized macro_indicator DataFrame conforming to entity schema."""
    return pd.DataFrame(
        {
            "indicator_code": ["china_cpi", "china_cpi", "china_gdp", "china_pmi"],
            "indicator_name": ["居民消费价格指数", "居民消费价格指数", "国内生产总值", "采购经理指数"],
            "frequency": ["M", "M", "Q", "M"],
            "region": ["CN", "CN", "CN", "CN"],
            "observation_date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-03-31"]).date,
            "publish_date": pd.to_datetime(["2024-02-10", "2024-03-10", "2024-04-16", "2024-04-01"]).date,
            "value": [102.5, 103.0, 1260000.0, 50.8],
            "value_yoy_pct": [2.5, 3.0, 5.3, None],
            "value_mom_pct": [0.5, 0.3, None, 0.2],
            "unit": ["index", "index", "亿元", "index"],
            "source_org": ["国家统计局", "国家统计局", "国家统计局", "国家统计局"],
            "batch_id": ["b1", "b1", "b1", "b1"],
            "source_name": ["akshare", "akshare", "akshare", "akshare"],
            "interface_name": ["macro_china_cpi", "macro_china_cpi", "macro_china_gdp", "macro_china_pmi"],
            "ingest_time": pd.to_datetime(["2024-04-22T10:00:00Z"] * 4),
            "normalize_version": ["v1", "v1", "v1", "v1"],
            "schema_version": ["v1", "v1", "v1", "v1"],
            "quality_status": ["passed", "passed", "passed", "passed"],
            "publish_time": pd.to_datetime([None, None, None, None]),
            "release_version": [None, None, None, None],
        }
    )
