"""get_hot_rank 接口示例（带重试与降级）。"""

import time
from typing import Callable, Optional

import pandas as pd

from akshare_data import get_service


def _fetch_with_retry(fetcher: Callable[[], pd.DataFrame], desc: str) -> Optional[pd.DataFrame]:
    last_error: Optional[Exception] = None
    for i in range(3):
        try:
            df = fetcher()
            if df is not None and not df.empty:
                return df
            print(f"{desc}: 第 {i + 1}/3 次返回空结果")
        except Exception as e:  # noqa: BLE001
            last_error = e
            print(f"{desc}: 第 {i + 1}/3 次失败 -> {e}")
        time.sleep(1)
    if last_error is not None:
        print(f"{desc}: 重试后仍失败，最终异常: {last_error}")
    return None


def _get_hot_rank_df(service) -> Optional[pd.DataFrame]:
    # 首选 facade 方法，失败后降级到 akshare 直调
    df = _fetch_with_retry(lambda: service.get_hot_rank(), "service.get_hot_rank")
    if df is not None:
        return df
    return _fetch_with_retry(lambda: service.akshare.get_hot_rank(), "service.akshare.get_hot_rank")


def main() -> None:
    print("=" * 60)
    print("get_hot_rank 示例（重试 + 降级）")
    print("=" * 60)

    service = get_service()
    df = _get_hot_rank_df(service)
    if df is None:
        print("最终无可用数据，可能是数据源暂时不可用。")
        return

    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")
    print("\n前20行:")
    print(df.head(20).to_string(index=False))

    rank_col = "rank" if "rank" in df.columns else ("排名" if "排名" in df.columns else None)
    if rank_col:
        top10 = df[df[rank_col] <= 10]
        print(f"\nTop10 条数: {len(top10)}")


if __name__ == "__main__":
    main()
