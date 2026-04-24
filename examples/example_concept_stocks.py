"""get_concept_stocks 示例：概念名回退 + 空数据重试。"""

import time
import pandas as pd
from akshare_data import get_service


def _fetch_concept_stocks(service, concepts, retries=2, wait_seconds=1.0):
    for concept in concepts:
        for attempt in range(1, retries + 1):
            df = service.get_concept_stocks(concept_code=concept)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return concept, df
            if attempt < retries:
                time.sleep(wait_seconds)
    return concepts[-1], pd.DataFrame()


def main():
    service = get_service()
    fallback_concepts = ["人工智能", "芯片", "新能源", "半导体", "医药"]
    used_concept, df = _fetch_concept_stocks(service, fallback_concepts)
    if df.empty:
        print("概念成分股为空（已重试并回退概念）")
        return

    print(f"使用概念: {used_concept}")
    print(f"成分股数量: {len(df)}")
    print(f"字段: {list(df.columns)}")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
