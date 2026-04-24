"""get_esg_rating() 示例：参数与日期修复、空数据重试、可读输出。"""

from __future__ import annotations

import time
from typing import Callable

import pandas as pd

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票 ESG 评级
# ============================================================
def _date_candidates(date_str: str) -> list[str]:
    return list(dict.fromkeys([date_str, date_str.replace("-", "")]))


# ============================================================
# 示例 2: 获取沪市股票 ESG 评级
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
# 示例 3: 对比不同股票的 ESG 评级
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
# 示例 4: ESG 评级趋势分析
# ============================================================
def example_basic() -> None:
    print("=" * 60)
    print("ESG 评级示例（修复参数：symbol/start_date/end_date）")
    print("=" * 60)
    service = get_service()

    for start_date in _date_candidates("2024-01-01"):
        for end_date in _date_candidates("2024-12-31"):
            desc = f"symbol=000001, {start_date}~{end_date}"
            df = _fetch_with_retry(
                lambda: service.get_esg_rating(symbol="000001", start_date=start_date, end_date=end_date),
                desc,
            )
            if df is not None and not df.empty:
                _print_df(df, f"成功: {desc}")
                return
    _print_df(df, "最终结果")


if __name__ == "__main__":
    example_basic()
