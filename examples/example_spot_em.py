"""get_spot_em 示例（带空结果重试和降级输出）。"""

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


def _get_spot_df(service) -> Optional[pd.DataFrame]:
    df = _fetch_with_retry(lambda: service.get_spot_em(), "service.get_spot_em")
    if df is not None:
        return df
    return _fetch_with_retry(lambda: service.akshare.get_spot_em(), "service.akshare.get_spot_em")


def main() -> None:
    print("=" * 60)
    print("get_spot_em 示例（重试 + 降级）")
    print("=" * 60)

    service = get_service()
    df = _get_spot_df(service)
    if df is None:
        print("最终无数据（可能是非交易时段或数据源异常）")
        return

    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")

    code_col = "代码" if "代码" in df.columns else ("code" if "code" in df.columns else None)
    for symbol in ["600519", "sh600519", "000001.XSHE"]:
        norm = _normalize_symbol(symbol)
        if code_col is None:
            print("无法按 symbol 筛选：未找到代码列")
            break
        hit = df[df[code_col].astype(str) == norm]
        print(f"{symbol} -> {norm}: {'命中' if not hit.empty else '未命中'}")

    if "成交额" in df.columns:
        df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce")
        print("\n成交额Top5:")
        cols = [c for c in ["代码", "名称", "成交额"] if c in df.columns]
        print(df.nlargest(5, "成交额")[cols].to_string(index=False))
    else:
        print("\n降级输出前10行：")
        print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
