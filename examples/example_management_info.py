"""get_management_info() 示例：修复 symbol 参数、空数据重试、可读输出。"""

from __future__ import annotations

import time
from typing import Callable

import pandas as pd

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取高管增持信息
# ============================================================
def _fetch_with_retry(fetcher: Callable[[], pd.DataFrame], desc: str, retries: int = 3) -> pd.DataFrame:
    last_df: pd.DataFrame | None = None
    for i in range(retries):
        try:
            df = fetcher()
            last_df = df
            if df is not None and not df.empty:
                return df
            print(f"{desc}: 返回空数据 (第 {i + 1}/{retries} 次)")
        except Exception as exc:
            print(f"{desc}: 调用异常 (第 {i + 1}/{retries} 次): {exc}")
        if i < retries - 1:
            time.sleep(1)
    return last_df


# ============================================================
# 示例 2: 获取高管减持信息
# ============================================================
def _print_df(df: pd.DataFrame, title: str, rows: int = 8) -> None:
    if df is None:
        print(f"{title}: None")
        return
    print(f"{title}: shape={df.shape}, columns={list(df.columns)}")
    if df.empty:
        print("  空数据")
        return
    print(df.head(rows).to_string(index=False))


# ============================================================
# 示例 3: 对比增减持情况
# ============================================================
def example_basic() -> None:
    print("=" * 60)
    print("管理层信息示例（修复参数：symbol 使用证券代码）")
    print("=" * 60)
    service = get_service()
    for symbol in ["000001", "600519", "300750"]:
        df = _fetch_with_retry(lambda: service.get_management_info(symbol=symbol), f"symbol={symbol}")
        if df is not None and not df.empty:
            _print_df(df, f"成功: symbol={symbol}")
            return
    _print_df(df, "最终结果")


if __name__ == "__main__":
    example_basic()
