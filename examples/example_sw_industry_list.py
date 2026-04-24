"""get_sw_industry_list 示例：level 回退 + 空数据重试。"""

import time
import pandas as pd
from akshare_data import get_service


def _fetch_sw_list(service, levels, retries=2, wait_seconds=1.0):
    for level in levels:
        for attempt in range(1, retries + 1):
            df = service.get_sw_industry_list(level=level)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return level, df
            if attempt < retries:
                time.sleep(wait_seconds)
    return levels[-1], pd.DataFrame()


def main():
    service = get_service()
    level, df = _fetch_sw_list(service, ["1", "2", "3"])
    if df.empty:
        print("申万行业列表为空（已重试并回退 level）")
        return
    print(f"使用 level={level}")
    print(f"行业数量: {len(df)}")
    print(f"字段: {list(df.columns)}")
    print(df.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
