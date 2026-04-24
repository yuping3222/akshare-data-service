"""get_concept_list 示例：含空数据重试。"""

import time
import pandas as pd
from akshare_data import get_service


def _fetch_non_empty_df(fetcher, retries=3, wait_seconds=1.0):
    for attempt in range(1, retries + 1):
        df = fetcher()
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
        if attempt < retries:
            time.sleep(wait_seconds)
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


def main():
    service = get_service()
    df = _fetch_non_empty_df(lambda: service.get_concept_list())
    if df.empty:
        print("概念列表为空（已重试）")
        return
    print(f"概念数量: {len(df)}")
    print(f"字段: {list(df.columns)}")
    print(df.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
