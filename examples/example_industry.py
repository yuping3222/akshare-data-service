"""行业映射示例：含 symbol 回退与空结果重试。"""

import time
from akshare_data import get_industry_mapping, get_industry_stocks


def _fetch_mapping(symbols, level=1, retries=2, wait_seconds=1.0):
    for symbol in symbols:
        for attempt in range(1, retries + 1):
            industry = get_industry_mapping(symbol, level)
            if industry:
                return symbol, industry
            if attempt < retries:
                time.sleep(wait_seconds)
    return symbols[-1], ""


def _fetch_stocks(industry_codes, level=1, retries=2, wait_seconds=1.0):
    for code in industry_codes:
        for attempt in range(1, retries + 1):
            stocks = get_industry_stocks(code, level)
            if stocks:
                return code, stocks
            if attempt < retries:
                time.sleep(wait_seconds)
    return industry_codes[-1], []


def main():
    symbols = ["000858", "600519", "000001"]
    industry_codes = ["801120", "801780", "801150"]

    used_symbol, industry = _fetch_mapping(symbols)
    print(f"映射查询 symbol={used_symbol}, industry={industry or '无'}")

    used_code, stocks = _fetch_stocks(industry_codes)
    print(f"行业成分股查询 industry_code={used_code}, 数量={len(stocks)}")
    if stocks:
        print(f"前10只: {stocks[:10]}")


if __name__ == "__main__":
    main()
