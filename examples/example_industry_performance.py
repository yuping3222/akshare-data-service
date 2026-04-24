"""get_industry_performance 示例：symbol/date 回退 + 空数据重试。"""

import time
import pandas as pd
from akshare_data import get_service


def _fetch_perf(service, symbols, date_ranges, retries=2, wait_seconds=1.0):
    for symbol in symbols:
        for start_date, end_date in date_ranges:
            for attempt in range(1, retries + 1):
                df = service.get_industry_performance(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    period="日k",
                )
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return symbol, start_date, end_date, df
                if attempt < retries:
                    time.sleep(wait_seconds)
    return symbols[-1], date_ranges[-1][0], date_ranges[-1][1], pd.DataFrame()


def main():
    service = get_service()
    symbols = ["半导体", "银行", "医药", "新能源"]
    date_ranges = [("20240101", "20240601"), ("20230101", "20231231"), ("20220101", "20221231")]
    symbol, start_date, end_date, df = _fetch_perf(service, symbols, date_ranges)
    if df.empty:
        print("行业行情为空（已重试并回退 symbol/date）")
        return
    print(f"使用参数: symbol={symbol}, start_date={start_date}, end_date={end_date}")
    print(f"记录数: {len(df)}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
