"""get_industry_stocks 示例：industry_code 回退 + 空数据重试。"""

import time
from akshare_data import get_service


def _fetch_industry_stocks(service, industry_codes, level=1, retries=2, wait_seconds=1.0):
    for code in industry_codes:
        for attempt in range(1, retries + 1):
            stocks = service.get_industry_stocks(industry_code=code, level=level)
            if stocks:
                return code, stocks
            if attempt < retries:
                time.sleep(wait_seconds)
    return industry_codes[-1], []


def main():
    service = get_service()
    industry_codes = ["801120", "801780", "801080", "801150"]
    used_code, stocks = _fetch_industry_stocks(service, industry_codes, level=1)
    if not stocks:
        print("行业成分股为空（已重试并回退 industry_code）")
        return

    print(f"使用行业代码: {used_code}")
    print(f"成分股数量: {len(stocks)}")
    print(f"前20只: {stocks[:20]}")


if __name__ == "__main__":
    main()
