"""批量下载工具函数"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd


def validate_ohlcv_data(df: pd.DataFrame) -> bool:
    """验证 OHLCV 数据有效性

    Args:
        df: 待验证的 DataFrame

    Returns:
        bool: 数据是否有效
    """
    if df is None or df.empty:
        return False

    required_cols = ["date", "stock_code"]
    if not all(col in df.columns for col in required_cols):
        return False

    ohlcv_cols = ["open", "high", "low", "close"]
    if not all(col in df.columns for col in ohlcv_cols):
        return False

    has_valid_ohlcv = False
    for col in ohlcv_cols:
        if df[col].notna().any():
            has_valid_ohlcv = True
            break

    return has_valid_ohlcv


def convert_wide_to_long(
    ohlcv_dict: Dict[str, pd.DataFrame], symbols: List[str]
) -> pd.DataFrame:
    """将宽表格式 OHLCV 数据转换为长表格式

    Args:
        ohlcv_dict: OHLCV 数据字典，key 为列名，value 为 DataFrame
        symbols: 股票代码列表

    Returns:
        pd.DataFrame: 长表格式数据
    """
    if not ohlcv_dict:
        return pd.DataFrame()

    records = []
    for col_name, df in ohlcv_dict.items():
        if df is None or df.empty:
            continue
        for symbol in symbols:
            if symbol not in df.columns:
                continue
            for idx in df.index:
                record = {
                    "date": idx,
                    "stock_code": symbol,
                    col_name: df.loc[idx, symbol],
                }
                records.append(record)

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records)

    if "date" in result.columns:
        result = result.sort_values(["date", "stock_code"])

    return result
