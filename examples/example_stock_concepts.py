"""get_stock_concepts 示例：symbol 回退 + 空数据重试。"""

import time
import pandas as pd
from akshare_data import get_service


def _fetch_stock_concepts(service, symbols, retries=2, wait_seconds=1.0):
    for symbol in symbols:
        for attempt in range(1, retries + 1):
            df = service.get_stock_concepts(symbol=symbol)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return symbol, df
            if attempt < retries:
                time.sleep(wait_seconds)
    return symbols[-1], pd.DataFrame()


def main():
    service = get_service()
    symbol, df = _fetch_stock_concepts(service, ["600519", "000858", "300750", "000001"])
    if df.empty:
        print("个股概念为空（已重试并回退 symbol）")
        return
    print(f"使用 symbol={symbol}")
    print(f"记录数: {len(df)}")
    print(f"字段: {list(df.columns)}")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
