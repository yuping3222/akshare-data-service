"""
交易日历 (Trading Calendar) 接口示例

演示如何使用 DataService 获取交易日历数据。

交易日历接口说明:

1. DataService.get_trading_days(start_date, end_date)
    参数:
        start_date: 可选，起始日期，格式 "YYYY-MM-DD"
        end_date: 可选，结束日期，格式 "YYYY-MM-DD"
    返回:
        List[str]，交易日列表

2. service.cn.trade_calendar(start_date, end_date)
    参数:
        start_date: 可选，起始日期，格式 "YYYY-MM-DD"
        end_date: 可选，结束日期，格式 "YYYY-MM-DD"
    返回:
        List[str]，交易日列表

3. service.akshare.get_trading_days(start_date, end_date)
    参数:
        start_date: 可选，起始日期，格式 "YYYY-MM-DD"
        end_date: 可选，结束日期，格式 "YYYY-MM-DD"
    返回:
        List[str]，交易日列表

交易日历特性:
- 包含沪深两市所有交易日
- 自动排除周末和法定节假日
- 数据来源于交易所官方日历
- 可用于计算交易日数量、判断是否为交易日等
"""

from datetime import datetime, timedelta
import pandas as pd
from akshare_data import get_service


def _normalize_days(days):
    """Normalize trading days to string list (handles mixed Timestamp/str types)."""
    if not days:
        return []
    return [str(d)[:10] for d in days]


# ============================================================
# 示例 1: 基本用法 - 获取指定日期范围的交易日
# ============================================================
def example_basic():
    """基本用法: 获取2024年上半年的交易日"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取指定日期范围的交易日")
    print("=" * 60)

    service = get_service()

    try:
        trading_days = service.get_trading_days(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )

        # Normalize to strings to avoid tz-aware/tz-naive type mixing
        trading_days = _normalize_days(trading_days)

        if not trading_days:
            print("未获取到交易日数据")
            return

        print(f"2024年上半年交易日数量: {len(trading_days)} 天")
        print(f"\n前10个交易日:")
        for day in trading_days[:10]:
            print(f"  {day}")

        print(f"\n后10个交易日:")
        for day in trading_days[-10:]:
            print(f"  {day}")

    except Exception as e:
        print(f"获取交易日历失败: {e}")


# ============================================================
# 示例 2: 不同调用方式对比
# ============================================================
def example_call_methods():
    """对比不同的调用方式"""
    print("\n" + "=" * 60)
    print("示例 2: 不同调用方式对比")
    print("=" * 60)

    service = get_service()
    start = "2024-06-01"
    end = "2024-06-30"

    days1, days2, days3 = [], [], []

    # 方式1: DataService.get_trading_days()
    print("\n方式1: service.get_trading_days()")
    try:
        days1 = service.get_trading_days(start_date=start, end_date=end)
        print(f"  结果: {len(days1)} 个交易日")
    except Exception as e:
        print(f"  失败: {e}")

    # 方式2: cn.trade_calendar()
    print("\n方式2: service.cn.trade_calendar()")
    try:
        days2 = service.cn.trade_calendar(start_date=start, end_date=end)
        print(f"  结果: {len(days2)} 个交易日")
    except Exception as e:
        print(f"  失败: {e}")

    # 方式3: akshare adapter
    print("\n方式3: service.akshare.get_trading_days()")
    try:
        days3 = service.akshare.get_trading_days(start_date=start, end_date=end)
        print(f"  结果: {len(days3)} 个交易日")
    except Exception as e:
        print(f"  失败: {e}")

    # 统一转为 list
    def _to_list(val):
        if isinstance(val, pd.DataFrame):
            if val.empty:
                return []
            return val.iloc[:, 0].tolist()
        if isinstance(val, list):
            return val
        if hasattr(val, "to_list"):
            return val.to_list()
        return list(val) if val is not None else []

    days1 = _to_list(days1)
    days2 = _to_list(days2)
    days3 = _to_list(days3)

    # Normalize to strings to avoid mixed type comparison
    days1 = _normalize_days(days1)
    days2 = _normalize_days(days2)
    days3 = _normalize_days(days3)

    # 验证结果一致性
    if days1 and days1 == days2 == days3:
        print("\n✓ 三种方式返回的结果一致")
    else:
        print("\n✗ 不同方式返回的结果存在差异")


# ============================================================
# 示例 3: 获取特定月份的交易日
# ============================================================
def example_monthly_trading_days():
    """获取2024年各月份的交易日数量"""
    print("\n" + "=" * 60)
    print("示例 3: 2024年各月份交易日统计")
    print("=" * 60)

    service = get_service()

    months = [
        ("2024-01-01", "2024-01-31", "1月"),
        ("2024-02-01", "2024-02-29", "2月"),
        ("2024-03-01", "2024-03-31", "3月"),
        ("2024-04-01", "2024-04-30", "4月"),
        ("2024-05-01", "2024-05-31", "5月"),
        ("2024-06-01", "2024-06-30", "6月"),
        ("2024-07-01", "2024-07-31", "7月"),
        ("2024-08-01", "2024-08-31", "8月"),
        ("2024-09-01", "2024-09-30", "9月"),
        ("2024-10-01", "2024-10-31", "10月"),
        ("2024-11-01", "2024-11-30", "11月"),
        ("2024-12-01", "2024-12-31", "12月"),
    ]

    for start, end, month_name in months:
        try:
            days = service.get_trading_days(start_date=start, end_date=end)
            days = _normalize_days(days)
            if days:
                print(f"{month_name}: {len(days)} 个交易日 ({days[0]} ~ {days[-1]})")
            else:
                print(f"{month_name}: 无数据")
        except Exception as e:
            print(f"{month_name}: 获取失败 - {e}")


# ============================================================
# 示例 4: 判断是否为交易日
# ============================================================
def example_is_trading_day():
    """判断特定日期是否为交易日"""
    print("\n" + "=" * 60)
    print("示例 4: 判断是否为交易日")
    print("=" * 60)

    service = get_service()

    # 获取2024年全年的交易日
    try:
        trading_days = service.get_trading_days(
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        trading_days = _normalize_days(trading_days)
        if not trading_days:
            print("未获取到交易日数据")
            return

        # 测试一些特定日期
        test_dates = [
            "2024-01-01",   # 元旦
            "2024-02-09",   # 除夕
            "2024-02-14",   # 情人节（可能非交易）
            "2024-02-19",   # 春节后第一个工作日
            "2024-05-01",   # 劳动节
            "2024-06-10",   # 端午节
            "2024-10-01",   # 国庆节
            "2024-06-28",   # 正常交易日
            "2024-12-31",   # 年末
        ]

        trading_set = set(trading_days)

        print(f"2024年全年交易日总数: {len(trading_days)} 天")
        print("\n日期判断:")

        for date_str in test_dates:
            if date_str in trading_set:
                print(f"  {date_str}: 交易日 ✓")
            else:
                print(f"  {date_str}: 非交易日 ✗")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 计算N个交易日后的日期
# ============================================================
def example_future_trading_day():
    """计算N个交易日后的日期"""
    print("\n" + "=" * 60)
    print("示例 5: 计算N个交易日后的日期")
    print("=" * 60)

    service = get_service()

    try:
        # 获取足够多的交易日数据
        trading_days = service.get_trading_days(
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        trading_days = _normalize_days(trading_days)
        if not trading_days:
            print("未获取到交易日数据")
            return

        # 给定起始日期，计算N个交易日后的日期
        start_date = "2024-06-03"
        n_values = [5, 10, 20, 60]

        if start_date in trading_days:
            start_idx = trading_days.index(start_date)

            print(f"起始日期: {start_date}")
            print("\n未来交易日:")

            for n in n_values:
                future_idx = start_idx + n
                if future_idx < len(trading_days):
                    future_date = trading_days[future_idx]
                    print(f"  +{n:2d} 个交易日: {future_date}")
                else:
                    print(f"  +{n:2d} 个交易日: 超出数据范围")
        else:
            print(f"{start_date} 不是交易日")

    except Exception as e:
        print(f"计算失败: {e}")


# ============================================================
# 示例 6: 计算日期间隔的交易日数量
# ============================================================
def example_count_trading_days():
    """计算两个日期之间的交易日数量"""
    print("\n" + "=" * 60)
    print("示例 6: 计算日期间隔的交易日数量")
    print("=" * 60)

    service = get_service()

    try:
        # 获取数据
        trading_days = service.get_trading_days(
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        trading_days = _normalize_days(trading_days)
        if not trading_days:
            print("未获取到交易日数据")
            return

        # 计算不同区间的交易日数量
        periods = [
            ("2024-01-01", "2024-01-31", "1月"),
            ("2024-01-01", "2024-03-31", "第一季度"),
            ("2024-01-01", "2024-06-30", "上半年"),
            ("2024-01-01", "2024-09-30", "前三季度"),
            ("2024-01-01", "2024-12-31", "全年"),
        ]

        trading_set = set(trading_days)

        print("时间段内的交易日数量:")
        for start, end, name in periods:
            # 筛选在范围内的交易日
            days_in_range = [d for d in trading_days if start <= d <= end]
            # 计算自然日天数
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            calendar_days = (end_dt - start_dt).days + 1

            print(f"\n{name} ({start} ~ {end}):")
            print(f"  交易日: {len(days_in_range)} 天")
            print(f"  自然日: {calendar_days} 天")
            print(f"  占比: {len(days_in_range) / calendar_days * 100:.1f}%")

    except Exception as e:
        print(f"计算失败: {e}")


# ============================================================
# 示例 7: 获取最近的交易日
# ============================================================
def example_recent_trading_days():
    """获取最近的交易日"""
    print("\n" + "=" * 60)
    print("示例 7: 获取最近的交易日")
    print("=" * 60)

    service = get_service()

    try:
        # 获取最近30个交易日的数据
        # 先获取较大范围的数据，再取最后30天
        trading_days = service.get_trading_days(
            start_date="2024-06-01",
            end_date="2024-12-31",
        )

        trading_days = _normalize_days(trading_days)
        if not trading_days:
            print("未获取到交易日数据")
            return

        # 取最后30个交易日
        recent_30 = trading_days[-30:]

        print(f"最近30个交易日:")
        print(f"  起始: {recent_30[0]}")
        print(f"  结束: {recent_30[-1]}")
        print(f"  跨度: {(datetime.strptime(recent_30[-1], '%Y-%m-%d') - datetime.strptime(recent_30[0], '%Y-%m-%d')).days} 个自然日")

        # 显示全部
        print("\n完整列表:")
        for i, day in enumerate(recent_30, 1):
            dt = datetime.strptime(day, "%Y-%m-%d")
            weekday = ["一", "二", "三", "四", "五", "六", "日"][dt.weekday()]
            print(f"  {i:2d}. {day} (星期{weekday})")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 8: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 8: 错误处理")
    print("=" * 60)

    service = get_service()

    test_cases = [
        ("无效日期格式", "invalid", "2024-06-30"),
        ("结束日期早于开始日期", "2024-12-31", "2024-01-01"),
        ("未来日期", "2025-01-01", "2025-12-31"),
    ]

    for case_name, start, end in test_cases:
        print(f"\n测试: {case_name}")
        try:
            days = service.get_trading_days(start_date=start, end_date=end)
            if days:
                print(f"  结果: 获取到 {len(days)} 个交易日")
            else:
                print(f"  结果: 返回空列表")
        except Exception as e:
            print(f"  捕获异常: {type(e).__name__}: {e}")


# ============================================================
# 示例 9: 节假日统计
# ============================================================
def example_holiday_analysis():
    """分析节假日对交易日的影响"""
    print("\n" + "=" * 60)
    print("示例 9: 节假日影响分析")
    print("=" * 60)

    service = get_service()

    try:
        trading_days = service.get_trading_days(
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        trading_days = _normalize_days(trading_days)
        if not trading_days:
            print("未获取到交易日数据")
            return

        # 分析各月份的周一到周五数量 vs 实际交易日数量
        from collections import defaultdict

        monthly_weekdays = defaultdict(int)
        monthly_trading = defaultdict(int)

        # 计算每个月的工作日数量
        current = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)

        while current <= end:
            month_key = current.strftime("%Y-%m")
            if current.weekday() < 5:  # 周一到周五
                monthly_weekdays[month_key] += 1
            current += timedelta(days=1)

        # 计算每个月的实际交易日数量
        for day in trading_days:
            month_key = day[:7]
            monthly_trading[month_key] += 1

        print("各月份工作日 vs 实际交易日对比:")
        for month in sorted(monthly_weekdays.keys()):
            wd = monthly_weekdays[month]
            td = monthly_trading.get(month, 0)
            diff = wd - td
            print(f"  {month}: 工作日 {wd} 天, 交易日 {td} 天, 差值 {diff} 天")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 10: 缓存效果演示
# ============================================================
def example_caching():
    """演示缓存效果"""
    print("\n" + "=" * 60)
    print("示例 10: 缓存效果演示")
    print("=" * 60)

    import time
    service = get_service()

    # 第一次调用
    print("\n第一次调用（从数据源获取）:")
    start = time.time()
    days1 = service.get_trading_days(
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    elapsed1 = time.time() - start
    print(f"  耗时: {elapsed1:.4f} 秒，获取 {len(days1)} 个交易日")

    # 第二次调用（应从缓存读取）
    print("\n第二次调用（应从缓存读取）:")
    start = time.time()
    days2 = service.get_trading_days(
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    elapsed2 = time.time() - start
    print(f"  耗时: {elapsed2:.4f} 秒，获取 {len(days2)} 个交易日")

    if elapsed2 < elapsed1 and elapsed1 > 0:
        speedup = elapsed1 / elapsed2
        print(f"\n缓存加速比: {speedup:.1f}x")
    else:
        print("\n两次调用耗时相近")


if __name__ == "__main__":
    example_basic()
    example_call_methods()
    example_monthly_trading_days()
    example_is_trading_day()
    example_future_trading_day()
    example_count_trading_days()
    example_recent_trading_days()
    example_error_handling()
    example_holiday_analysis()
    example_caching()
