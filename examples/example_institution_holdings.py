"""get_institution_holdings 示例（symbol 规范 + 重试 + 降级）。"""

import re
import time
from typing import Callable, Optional

import pandas as pd

from akshare_data import get_service


def _normalize_symbol(symbol: str) -> str:
    m = re.search(r"(\d{6})", symbol)
    return m.group(1) if m else symbol.strip()


def _fetch_with_retry(fetcher: Callable[[], pd.DataFrame], desc: str) -> Optional[pd.DataFrame]:
    for i in range(3):
        try:
            df = fetcher()
            if df is not None and not df.empty:
                return df
            print(f"{desc}: 第 {i + 1}/3 次返回空结果")
        except Exception as e:  # noqa: BLE001
            print(f"{desc}: 第 {i + 1}/3 次失败 -> {e}")
        time.sleep(1)
    return None


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的机构持股
# ============================================================
def main() -> None:
    print("=" * 60)
    print("institution_holdings 示例")
    print("=" * 60)

    service = get_service()
    for raw_symbol in ["600519", "sh600519", "000001.XSHE"]:
        symbol = _normalize_symbol(raw_symbol)
        df = _fetch_with_retry(
            lambda s=symbol: service.get_institution_holdings(symbol=s),
            f"get_institution_holdings({raw_symbol})",
        )
        if df is None:
            df = _fetch_with_retry(
                lambda s=symbol: service.get_institution_holdings(symbol=s, source="lixinger"),
                f"get_institution_holdings({raw_symbol}, source=lixinger)",
            )
        if df is None:
            print(f"{raw_symbol} -> {symbol}: 无数据")
            continue
        print(f"{raw_symbol} -> {symbol}: {len(df)} 行, 字段 {list(df.columns)}")
        print(df.head(3).to_string(index=False))


if __name__ == "__main__":
    main()
