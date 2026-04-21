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

from akshare_data import get_trading_days


def example_basic_usage():
    """基础用法：获取最近的交易日列表"""
    print("=" * 60)
    print("示例1: 获取2024年1月的交易日")
    print("=" * 60)

    try:
        # 获取2024年1月的所有交易日
        days = get_trading_days(start_date="2024-01-01", end_date="2024-01-31")
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
    print("示例3: 获取2024年第一季度的交易日")
    print("=" * 60)

    try:
        # 获取2024年Q1（1-3月）的交易日
        days = get_trading_days(start_date="2024-01-01", end_date="2024-03-31")
        print(f"2024年Q1交易日数量: {len(days)}")
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
    print("示例4: 获取2024年6月的交易日")
    print("=" * 60)

    try:
        # 获取2024年6月的交易日
        days = get_trading_days(start_date="2024-06-01", end_date="2024-06-30")
        print(f"2024年6月交易日数量: {len(days)}")
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
        # 获取某个月的交易日
        days = get_trading_days(start_date="2024-02-01", end_date="2024-02-29")

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

        # 判断几个日期是否为交易日
        test_dates = ["2024-02-01", "2024-02-04", "2024-02-10", "2024-02-29"]
        for date in test_dates:
            is_trading = date in str_days
            print(f"{date} {'是' if is_trading else '不是'}交易日")
    except Exception as e:
        print(f"获取交易日失败: {e}")


def example_count_trading_days():
    """实用场景：统计两个日期之间的交易日数量"""
    print("\n" + "=" * 60)
    print("示例6: 统计两个日期之间的交易日数量")
    print("=" * 60)

    try:
        # 统计2024年上半年的交易日
        days = get_trading_days(start_date="2024-01-01", end_date="2024-06-30")
        print(f"2024年上半年交易日数量: {len(days)}")

        # 统计2024年全年的交易日
        days_full = get_trading_days(start_date="2024-01-01", end_date="2024-12-31")
        print(f"2024年全年交易日数量: {len(days_full)}")
    except Exception as e:
        print(f"获取交易日失败: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_no_parameters()
    example_single_date_range()
    example_specific_month()
    example_check_if_trading_day()
    example_count_trading_days()
