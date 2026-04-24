"""
交易日历 (Trading Calendar) 接口示例。

统一策略:
- 使用“最近可用日期窗口”，避免硬编码历史年份导致空数据。
- 默认以今天为 end_date，并向前回看固定天数作为 start_date。
"""

from datetime import date, datetime, timedelta
import time
import pandas as pd
from akshare_data import get_service


RECENT_LOOKBACK_DAYS = 365


def _normalize_days(days):
    if not days:
        return []
    return [str(d)[:10] for d in days]


def _recent_window(days=RECENT_LOOKBACK_DAYS):
    end = date.today()
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _to_list(value):
    if isinstance(value, pd.DataFrame):
        return [] if value.empty else value.iloc[:, 0].tolist()
    if isinstance(value, list):
        return value
    if hasattr(value, "to_list"):
        return value.to_list()
    return list(value) if value is not None else []


def example_basic():
    print("=" * 60)
    print("示例 1: 最近一年交易日")
    print("=" * 60)
    service = get_service()
    start, end = _recent_window()
    days = _normalize_days(service.get_trading_days(start_date=start, end_date=end))
    print(f"查询区间: {start} ~ {end}")
    if not days:
        print("未获取到交易日数据")
        return
    print(f"交易日数量: {len(days)}")
    print(f"前5个: {days[:5]}")
    print(f"后5个: {days[-5:]}")


def example_call_methods():
    print("\n" + "=" * 60)
    print("示例 2: 三种调用方式一致性")
    print("=" * 60)
    service = get_service()
    start, end = _recent_window(90)
    results = []
    for fn in (
        lambda: service.get_trading_days(start_date=start, end_date=end),
        lambda: service.cn.trade_calendar(start_date=start, end_date=end),
        lambda: service.akshare.get_trading_days(start_date=start, end_date=end),
    ):
        try:
            results.append(_normalize_days(_to_list(fn())))
        except Exception as exc:
            results.append([])
            print(f"调用失败: {exc}")
    print(f"查询区间: {start} ~ {end}")
    print(f"结果长度: {[len(x) for x in results]}")
    print("一致性:", "✓" if results[0] and results[0] == results[1] == results[2] else "✗")


def example_recent_trading_days():
    print("\n" + "=" * 60)
    print("示例 3: 最近 30 个交易日")
    print("=" * 60)
    service = get_service()
    start, end = _recent_window(120)
    days = _normalize_days(service.get_trading_days(start_date=start, end_date=end))
    if not days:
        print("未获取到交易日数据")
        return
    recent_30 = days[-30:]
    span = (
        datetime.strptime(recent_30[-1], "%Y-%m-%d")
        - datetime.strptime(recent_30[0], "%Y-%m-%d")
    ).days
    print(f"最近30个交易日: {recent_30[0]} ~ {recent_30[-1]} (自然日跨度 {span})")


def example_count_trading_days():
    print("\n" + "=" * 60)
    print("示例 4: 区间交易日统计")
    print("=" * 60)
    service = get_service()
    start, end = _recent_window(180)
    days = _normalize_days(service.get_trading_days(start_date=start, end_date=end))
    if not days:
        print("未获取到交易日数据")
        return
    mid = days[len(days) // 2]
    periods = [(start, mid, "前半段"), (mid, end, "后半段"), (start, end, "全区间")]
    for s, e, name in periods:
        n = len([d for d in days if s <= d <= e])
        calendar_days = (
            datetime.strptime(e, "%Y-%m-%d") - datetime.strptime(s, "%Y-%m-%d")
        ).days + 1
        print(f"{name} {s} ~ {e}: 交易日 {n} / 自然日 {calendar_days}")


def example_caching():
    print("\n" + "=" * 60)
    print("示例 5: 缓存效果")
    print("=" * 60)
    service = get_service()
    start, end = _recent_window()
    t1 = time.time()
    days1 = service.get_trading_days(start_date=start, end_date=end)
    t1 = time.time() - t1
    t2 = time.time()
    days2 = service.get_trading_days(start_date=start, end_date=end)
    t2 = time.time() - t2
    print(f"第一次: {t1:.4f}s, {len(days1)} 条")
    print(f"第二次: {t2:.4f}s, {len(days2)} 条")


if __name__ == "__main__":
    example_basic()
    example_call_methods()
    example_recent_trading_days()
    example_count_trading_days()
    example_caching()
