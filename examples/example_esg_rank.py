"""get_esg_rank() 示例：参数与日期修复、空数据重试、可读输出。"""

from __future__ import annotations

import time
from typing import Callable

import pandas as pd

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取最新 ESG 排名
# ============================================================
def _date_candidates(date_str: str) -> list[str]:
    return list(dict.fromkeys([date_str, date_str.replace("-", "")]))


# ============================================================
# 示例 2: 指定日期获取排名
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
# 示例 3: 获取不同数量的排名
# ============================================================
def _print_df(df: pd.DataFrame, title: str, rows: int = 10) -> None:
    if df is None:
        print(f"{title}: None")
        return
    print(f"{title}: shape={df.shape}, columns={list(df.columns)}")
    if df.empty:
        print("  空数据")
        return
    print(df.head(rows).to_string(index=False))


# ============================================================
# 示例 4: 排名数据分析
# ============================================================
def example_basic() -> None:
    print("=" * 60)
    print("ESG 排名示例（修复参数：date/top_n）")
    print("=" * 60)
    service = get_service()

    for date in [None] + _date_candidates("2024-06-30"):
        desc = f"date={date}, top_n=20"
        df = _fetch_with_retry(lambda: service.get_esg_rank(date=date, top_n=20), desc)
        if df is not None and not df.empty:
            _print_df(df, f"成功: {desc}")
            return
    _print_df(df, "最终结果")


if __name__ == "__main__":
    example_basic()
