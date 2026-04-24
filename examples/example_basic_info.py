"""get_basic_info() 示例：空数据重试 + 降级输出。"""

from __future__ import annotations

import logging
import time
import warnings
from typing import Callable

import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_service


def _mock_basic_info(symbol: str) -> pd.DataFrame:
    info_map = {
        "600519": {"symbol": "600519", "name": "贵州茅台", "market": "沪A", "industry": "酿酒行业", "list_date": "2001-08-27"},
        "000001": {"symbol": "000001", "name": "平安银行", "market": "深A", "industry": "银行", "list_date": "1991-04-03"},
        "300750": {"symbol": "300750", "name": "宁德时代", "market": "深A", "industry": "电池", "list_date": "2018-06-11"},
    }
    info = info_map.get(symbol, {"symbol": symbol, "name": "未知", "market": "-", "industry": "-", "list_date": "-"})
    return pd.DataFrame([info])


def _fetch_with_retry(fetcher: Callable[[], pd.DataFrame], desc: str, retries: int = 3) -> pd.DataFrame:
    last_df: pd.DataFrame | None = None
    for i in range(retries):
        try:
            df = fetcher()
            last_df = df
            if df is not None and not df.empty:
                return df
        except Exception:
            pass
        if i < retries - 1:
            time.sleep(0.5)
    return last_df


def _print_df(df: pd.DataFrame, title: str, rows: int = 5) -> None:
    if df is None:
        print(f"{title}: None")
        return
    print(f"{title}: shape={df.shape}, columns={list(df.columns)}")
    if df.empty:
        print("  空数据")
        return
    print(df.head(rows).to_string(index=False))


def example_basic() -> None:
    print("=" * 60)
    print("股票基础信息示例")
    print("=" * 60)
    service = get_service()
    for symbol in ["600519", "000001", "300750"]:
        df = _fetch_with_retry(lambda s=symbol: service.get_basic_info(symbol=s), f"symbol={symbol}")
        if df is not None and not df.empty:
            _print_df(df, f"成功: symbol={symbol}")
            return
    print("  无真实缓存数据，使用演示数据")
    for symbol in ["600519", "000001", "300750"]:
        df = _mock_basic_info(symbol)
        _print_df(df, f"演示: symbol={symbol}")


if __name__ == "__main__":
    example_basic()
