"""
龙虎榜接口使用示例

本示例展示如何通过 DataService 的 akshare adapter 获取龙虎榜数据。
包含两个接口：
  - get_dragon_tiger_list(date): 获取指定日期的龙虎榜个股数据
  - get_dragon_tiger_summary(start_date, end_date): 获取日期范围内的龙虎榜汇总数据

参数说明：
    date: 必填，查询日期，格式 "YYYY-MM-DD"（内部会自动转换为 YYYYMMDD）
    start_date: 可选，起始日期，格式 "YYYY-MM-DD"
    end_date: 可选，结束日期，格式 "YYYY-MM-DD"

返回：
    get_dragon_tiger_list: pd.DataFrame，包含个股龙虎榜明细
    get_dragon_tiger_summary: pd.DataFrame，包含龙虎榜汇总统计
"""

from akshare_data import get_service


def example_basic_dragon_tiger_list():
    """基础用法：获取指定日期的龙虎榜个股数据"""
    print("=" * 60)
    print("示例1: 获取指定日期的龙虎榜个股数据")
    print("=" * 60)

    service = get_service()

    try:
        # 获取2024年6月28日的龙虎榜数据
        # 底层 AkShare 函数 stock_lhb_detail_em 需要 start_date/end_date 范围
        # date 参数会自动转换为相同起止日期
        df = service.akshare.get_dragon_tiger_list(date="2024-06-28")

        # 打印数据基本信息
        print(f"数据形状: {df.shape}")
        if not df.empty:
            print(f"列名: {df.columns.tolist()}")
            print(f"\n前5行数据:")
            print(df.head())
        else:
            print("该日期无龙虎榜数据（可能是非交易日或数据源无数据）")
    except Exception as e:
        print(f"获取龙虎榜数据失败: {e}")


def example_dragon_tiger_list_recent():
    """获取最近交易日的龙虎榜数据"""
    print("\n" + "=" * 60)
    print("示例2: 获取最近交易日的龙虎榜数据")
    print("=" * 60)

    service = get_service()

    try:
        # 获取2024年6月27日的龙虎榜数据
        df = service.akshare.get_dragon_tiger_list(date="2024-06-27")

        if df.empty:
            print("该日期无龙虎榜数据（可能是非交易日）")
        else:
            print(f"共 {df.shape[0]} 条龙虎榜记录")
            print(f"\n数据列:")
            for col in df.columns:
                print(f"  - {col}")
            print(f"\n前10行数据:")
            print(df.head(10))
    except Exception as e:
        print(f"获取龙虎榜数据失败: {e}")


def example_dragon_tiger_summary_basic():
    """基础用法：获取龙虎榜汇总数据"""
    print("\n" + "=" * 60)
    print("示例3: 获取龙虎榜汇总数据（日期范围）")
    print("=" * 60)

    service = get_service()

    try:
        # 获取2024年6月1日至6月30日的龙虎榜汇总数据
        # start_date/end_date 定义查询的日期范围
        df = service.akshare.get_dragon_tiger_summary(
            start_date="2024-06-01",
            end_date="2024-06-30",
        )

        print(f"数据形状: {df.shape}")
        print(f"列名: {df.columns.tolist()}")
        print(f"\n前5行数据:")
        print(df.head())
    except Exception as e:
        print(f"获取龙虎榜汇总数据失败: {e}")


def example_dragon_tiger_summary_full_month():
    """获取整月龙虎榜汇总并做简单分析"""
    print("\n" + "=" * 60)
    print("示例4: 获取整月龙虎榜汇总并分析")
    print("=" * 60)

    service = get_service()

    try:
        # 获取2024年5月的龙虎榜汇总数据
        df = service.akshare.get_dragon_tiger_summary(
            start_date="2024-05-01",
            end_date="2024-05-31",
        )

        if df.empty:
            print("该日期范围内无龙虎榜汇总数据")
        else:
            print(f"共 {df.shape[0]} 条汇总记录")
            print(
                f"日期范围: {df.iloc[0].get('日期', df.iloc[0].get('date', 'N/A'))} ~ "
                f"{df.iloc[-1].get('日期', df.iloc[-1].get('date', 'N/A'))}"
            )

            # 打印完整的列名
            print(f"\n数据列:")
            for col in df.columns:
                print(f"  - {col}")

            # 查看前几行和后几行数据
            print(f"\n前5行数据:")
            print(df.head())
            print(f"\n后5行数据:")
            print(df.tail())
    except Exception as e:
        print(f"获取龙虎榜汇总数据失败: {e}")


def example_dragon_tiger_multiple_dates():
    """对比多个日期的龙虎榜数据"""
    print("\n" + "=" * 60)
    print("示例5: 对比多个日期的龙虎榜数据量")
    print("=" * 60)

    service = get_service()

    # 测试多个交易日
    dates = [
        "2024-06-24",
        "2024-06-25",
        "2024-06-26",
        "2024-06-27",
        "2024-06-28",
    ]

    for date in dates:
        try:
            df = service.akshare.get_dragon_tiger_list(date=date)
            if df.empty:
                print(f"{date}: 无龙虎榜数据")
            else:
                print(f"{date}: {df.shape[0]} 条记录")
        except Exception as e:
            print(f"{date}: 获取失败 - {e}")


def example_dragon_tiger_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 60)
    print("示例6: 错误处理示例")
    print("=" * 60)

    service = get_service()

    # 测试非交易日
    try:
        df = service.akshare.get_dragon_tiger_list(date="2024-02-10")  # 春节假期
        if df.empty:
            print("非交易日返回空DataFrame")
        else:
            print(f"获取到 {df.shape[0]} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")

    # 测试无效的日期格式
    try:
        df = service.akshare.get_dragon_tiger_list(date="invalid-date")
        print(f"获取到 {df.shape[0]} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")

    # 测试汇总接口日期范围错误
    try:
        df = service.akshare.get_dragon_tiger_summary(
            start_date="2024-12-31",
            end_date="2024-01-01",  # 结束日期早于开始日期
        )
        if df.empty:
            print("日期范围无效，返回空DataFrame")
        else:
            print(f"获取到 {df.shape[0]} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic_dragon_tiger_list()
    example_dragon_tiger_list_recent()
    example_dragon_tiger_summary_basic()
    example_dragon_tiger_summary_full_month()
    example_dragon_tiger_multiple_dates()
    example_dragon_tiger_error_handling()
