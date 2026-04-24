"""实时行情示例（统一 symbol 格式、重试、降级输出）。"""

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
            print(f"{desc}: 第 {i + 1}/3 次为空")
        except Exception as e:  # noqa: BLE001
            print(f"{desc}: 第 {i + 1}/3 次失败 -> {e}")
        time.sleep(1)
    return None


def main() -> None:
    print("=" * 60)
    print("realtime 示例（重试 + 降级）")
    print("=" * 60)

    service = get_service()
    df = _fetch_with_retry(lambda: service.get_spot_em(), "service.get_spot_em")
    if df is None:
        df = _fetch_with_retry(lambda: service.akshare.get_spot_em(), "service.akshare.get_spot_em")
    if df is None:
        print("最终无可用数据。")
        return

    code_col = "代码" if "代码" in df.columns else ("code" if "code" in df.columns else None)
    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")
    if code_col is None:
        print("未找到代码列，降级输出前10行:")
        print(df.head(10).to_string(index=False))
        return

    for symbol in ["600519", "sh600519", "000001.XSHE", "510300"]:
        norm = _normalize_symbol(symbol)
        hit = df[df[code_col].astype(str) == norm]
        print(f"{symbol:12s} -> {norm}: {'命中' if not hit.empty else '未命中'}")


if __name__ == "__main__":
    main()
