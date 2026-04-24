"""get_sw_industry_daily 示例：index_code/date 回退 + 空数据重试。"""

import time
import pandas as pd
from akshare_data import get_service


def _fetch_sw_daily(service, index_codes, date_ranges, retries=2, wait_seconds=1.0):
    for code in index_codes:
        for start_date, end_date in date_ranges:
            for attempt in range(1, retries + 1):
                df = service.get_sw_industry_daily(
                    index_code=code,
                    start_date=start_date,
                    end_date=end_date,
                )
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return code, start_date, end_date, df
                if attempt < retries:
                    time.sleep(wait_seconds)
    return index_codes[-1], date_ranges[-1][0], date_ranges[-1][1], pd.DataFrame()


def main():
    service = get_service()
    index_codes = ["801120", "801080", "801780", "801150"]
    date_ranges = [("2024-01-01", "2024-06-30"), ("2023-01-01", "2023-12-31")]
    code, start_date, end_date, df = _fetch_sw_daily(service, index_codes, date_ranges)
    if df.empty:
        print("申万行业日线为空（已重试并回退 index_code/date）")
        return

    print(f"使用参数: index_code={code}, start_date={start_date}, end_date={end_date}")
    print(f"记录数: {len(df)}")
    print(f"字段: {list(df.columns)}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
