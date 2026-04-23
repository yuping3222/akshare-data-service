"""Test fixtures for financial_indicator normalizer.

Provides sample raw DataFrames mimicking different data sources.
"""

import pandas as pd


def akshare_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by akshare financial indicator interface."""
    return pd.DataFrame(
        {
            "code": ["600519", "600519", "000001"],
            "report_date": ["2024-03-31", "2024-06-30", "2024-03-31"],
            "report_type": ["Q1", "H1", "Q1"],
            "pe_ttm": [25.5, 26.0, 8.2],
            "pb": [5.8, 5.9, 0.7],
            "ps_ttm": [10.2, 10.5, 2.1],
            "roe": [15.2, 16.0, 5.5],
            "roa": [10.1, 10.5, 0.5],
            "net_profit": [5000000000.0, 10000000000.0, 8000000000.0],
            "revenue": [20000000000.0, 40000000000.0, 50000000000.0],
            "total_assets": [100000000000.0, 105000000000.0, 5000000000000.0],
            "total_equity": [50000000000.0, 52000000000.0, 300000000000.0],
            "debt_ratio": [50.0, 50.5, 94.0],
            "gross_margin": [60.0, 61.0, 25.0],
            "net_margin": [25.0, 25.0, 16.0],
        }
    )


def tushare_raw() -> pd.DataFrame:
    """Raw DataFrame as returned by tushare fina_indicator."""
    return pd.DataFrame(
        {
            "ts_code": ["600519.SH", "600519.SH"],
            "ann_date": ["2024-04-15", "2024-08-15"],
            "end_date": ["20240331", "20240630"],
            "pe_ttm": [25.5, 26.0],
            "pb": [5.8, 5.9],
            "roe": [15.2, 16.0],
            "roa": [10.1, 10.5],
            "net_profit": [5000000000.0, 10000000000.0],
            "total_revenue": [20000000000.0, 40000000000.0],
            "total_assets": [100000000000.0, 105000000000.0],
            "total_holders": [100000, 105000],
        }
    )


def standardized_sample() -> pd.DataFrame:
    """Standardized financial_indicator DataFrame conforming to entity schema."""
    return pd.DataFrame(
        {
            "security_id": ["600519", "600519", "000001"],
            "report_date": pd.to_datetime(["2024-03-31", "2024-06-30", "2024-03-31"]).date,
            "report_type": ["Q1", "H1", "Q1"],
            "publish_date": pd.to_datetime(["2024-04-15", "2024-08-15", "2024-04-20"]).date,
            "currency": ["CNY", "CNY", "CNY"],
            "pe_ratio_ttm": [25.5, 26.0, 8.2],
            "pb_ratio": [5.8, 5.9, 0.7],
            "ps_ratio_ttm": [10.2, 10.5, 2.1],
            "roe_pct": [15.2, 16.0, 5.5],
            "roa_pct": [10.1, 10.5, 0.5],
            "net_profit": [5000000000.0, 10000000000.0, 8000000000.0],
            "revenue": [20000000000.0, 40000000000.0, 50000000000.0],
            "total_assets": [100000000000.0, 105000000000.0, 5000000000000.0],
            "total_equity": [50000000000.0, 52000000000.0, 300000000000.0],
            "debt_ratio_pct": [50.0, 50.5, 94.0],
            "gross_margin_pct": [60.0, 61.0, 25.0],
            "net_margin_pct": [25.0, 25.0, 16.0],
            "batch_id": ["b1", "b1", "b1"],
            "source_name": ["akshare", "akshare", "akshare"],
            "interface_name": ["fina_indicator", "fina_indicator", "fina_indicator"],
            "ingest_time": pd.to_datetime(["2024-04-22T10:00:00Z"] * 3),
            "normalize_version": ["v1", "v1", "v1"],
            "schema_version": ["v1", "v1", "v1"],
            "quality_status": ["passed", "passed", "passed"],
            "publish_time": pd.to_datetime([None, None, None]),
            "release_version": [None, None, None],
        }
    )
