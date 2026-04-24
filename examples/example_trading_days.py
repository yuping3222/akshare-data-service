"""
get_trading_days() 接口使用示例

本示例展示如何获取A股交易日列表。
get_trading_days 返回指定日期范围内的所有交易日（字符串列表）。

参数说明：
    start_date: 可选，起始日期，格式 "YYYY-MM-DD"，不传则从缓存/默认起始日期开始
    end_date: 可选，结束日期，格式 "YYYY-MM-DD"，不传则到今天

返回：
    List[str] - 交易日列表，如 ["2024-01-02", "2024-01-03", ...]
"""

import logging
import warnings
from datetime import date, timedelta

warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_trading_days


def _recent_window(days: int = 365):
    end = date.today()
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def example_basic_usage():
    """基础用法：获取最近的交易日列表"""
    print("=" * 60)
    print("示例1: 获取最近30天的交易日")
    print("=" * 60)

    try:
        start_date, end_date = _recent_window(30)
        days = get_trading_days(start_date=start_date, end_date=end_date)
        print(f"查询区间: {start_date} ~ {end_date}")
        print(f"交易日数量: {len(days)}")
        if days:
            print(f"前5个交易日: {days[:5]}")
            print(f"后5个交易日: {days[-5:]}")
        else:
            print("（无交易日数据，可能是缓存未初始化或网络不可用）")
    except Exception as e:
        print(f"获取交易日失败: {e}")


def example_no_parameters():
    """不传参数：获取全部缓存的交易日"""
    print("\n" + "=" * 60)
    print("示例2: 不传参数（获取全部交易日）")
    print("=" * 60)

    try:
        # 不传参数时，返回缓存中的所有交易日
        days = get_trading_days()
        print(f"总交易日数量: {len(days)}")
        if days:
            print(f"最早交易日: {days[0]}")
            print(f"最晚交易日: {days[-1]}")
            print(f"前10个交易日: {days[:10]}")
        else:
            print("（缓存中无交易日数据，可能是首次运行或网络不可用）")
    except Exception as e:
        print(f"获取交易日失败: {e}")


def example_single_date_range():
    """指定单个日期范围"""
    print("\n" + "=" * 60)
    print("示例3: 获取最近90天的交易日")
    print("=" * 60)

    try:
        start_date, end_date = _recent_window(90)
        days = get_trading_days(start_date=start_date, end_date=end_date)
        print(f"查询区间: {start_date} ~ {end_date}")
        print(f"交易日数量: {len(days)}")
        if days:
            print(f"第一个交易日: {days[0]}")
            print(f"最后一个交易日: {days[-1]}")
        else:
            print("（无交易日数据）")
    except Exception as e:
        print(f"获取交易日失败: {e}")


def example_specific_month():
    """获取特定月份的交易日"""
    print("\n" + "=" * 60)
    print("示例4: 获取最近自然月的交易日")
    print("=" * 60)

    try:
        # 使用近 45 天窗口，尽量覆盖最近一个自然月
        start_date, end_date = _recent_window(45)
        days = get_trading_days(start_date=start_date, end_date=end_date)
        print(f"查询区间: {start_date} ~ {end_date}")
        print(f"交易日数量: {len(days)}")
        if days:
            print(f"交易日列表: {days}")
        else:
            print("（无交易日数据）")
    except Exception as e:
        print(f"获取交易日失败: {e}")


def example_check_if_trading_day():
    """实用场景：判断某天是否为交易日"""
    print("\n" + "=" * 60)
    print("示例5: 判断指定日期是否为交易日")
    print("=" * 60)

    try:
        start_date, end_date = _recent_window(60)
        days = get_trading_days(start_date=start_date, end_date=end_date)

        if not days:
            print("（无交易日数据，无法判断）")
            return

        # 将日期转换为字符串格式以便比较
        import datetime

        str_days = []
        for d in days:
            if isinstance(d, datetime.date):
                str_days.append(d.strftime("%Y-%m-%d"))
            else:
                str_days.append(str(d))

        # 判断几个日期是否为交易日：最近交易日、最近周末、窗口起点
        test_dates = [
            str_days[-1],
            (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
            (date.today() - timedelta(days=2)).strftime("%Y-%m-%d"),
            start_date,
        ]
        for test_date in test_dates:
            is_trading = test_date in str_days
            print(f"{test_date} {'是' if is_trading else '不是'}交易日")
    except Exception as e:
        print(f"获取交易日失败: {e}")


def example_count_trading_days():
    """实用场景：统计两个日期之间的交易日数量"""
    print("\n" + "=" * 60)
    print("示例6: 统计两个日期之间的交易日数量")
    print("=" * 60)

    try:
        start_180, end_180 = _recent_window(180)
        days = get_trading_days(start_date=start_180, end_date=end_180)
        print(f"最近180天交易日数量: {len(days)}")

        start_365, end_365 = _recent_window(365)
        days_full = get_trading_days(start_date=start_365, end_date=end_365)
        print(f"最近365天交易日数量: {len(days_full)}")
    except Exception as e:
        print(f"获取交易日失败: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_no_parameters()
    example_single_date_range()
    example_specific_month()
    example_check_if_trading_day()
    example_count_trading_days()
