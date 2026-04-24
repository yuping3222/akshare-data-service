"""get_equity_pledge() 示例：日期兼容 + 空数据重试 + 可读输出。"""

from __future__ import annotations

import time
from typing import Callable

import pandas as pd

from akshare_data import get_service


def _date_candidates(date_str: str) -> list[str]:
    return list(dict.fromkeys([date_str, date_str.replace("-", "")]))


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
    print("股权质押示例：平安银行 000001")
    print("=" * 60)
    service = get_service()

    start_dates = _date_candidates("2024-01-01")
    end_dates = _date_candidates("2024-12-31")
    for start_date in start_dates:
        for end_date in end_dates:
            desc = f"symbol=000001, {start_date}~{end_date}"
            df = _fetch_with_retry(
                lambda: service.get_equity_pledge(symbol="000001", start_date=start_date, end_date=end_date),
                desc,
            )
            if df is not None and not df.empty:
                _print_df(df, f"成功: {desc}")
                return
    _print_df(df, "最终结果")


if __name__ == "__main__":
    example_basic()
