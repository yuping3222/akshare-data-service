"""
融资融券数据接口示例

演示如何使用 service.akshare 获取融资融券数据。

接口说明:
- get_margin_data(date): 获取指定日期的融资融券明细数据
- get_margin_summary(start_date, end_date): 获取指定日期范围的融资融券汇总数据

使用方式:
    from akshare_data import get_service
    service = get_service()
    # 通过 service.akshare 访问 AkShareAdapter
    df = service.akshare.get_margin_data(date="2024-01-15")
    df = service.akshare.get_margin_summary(start_date="2024-01-01", end_date="2024-01-31")
"""

from datetime import date
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定日期的融资融券明细数据
# ============================================================
def example_margin_data_basic():
    """基本用法: 获取单只股票指定日期的融资融券明细"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取融资融券明细数据")
    print("=" * 60)

    service = get_service()

    try:
        # date: 指定日期，格式 "YYYY-MM-DD" 或 date 对象
        # 返回 DataFrame，包含融资融券明细数据
        df = service.akshare.get_margin_data(date="2024-01-15")

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        if df.empty:
            print("该日期无融资融券数据")
            return
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取融资融券明细数据失败: {e}")


# ============================================================
# 示例 2: 使用 date 对象作为参数
# ============================================================
def example_margin_data_with_date_object():
    """使用 Python date 对象作为日期参数"""
    print("\n" + "=" * 60)
    print("示例 2: 使用 date 对象获取融资融券明细")
    print("=" * 60)

    service = get_service()

    try:
        # date 参数也支持 Python date 对象
        target_date = date(2024, 3, 15)
        df = service.akshare.get_margin_data(date=target_date)

        print(f"查询日期: {target_date}")
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        if not df.empty:
            print("\n前5行数据:")
            print(df.head())
        else:
            print("该日期无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 获取融资融券汇总数据 (基本用法)
# ============================================================
def example_margin_summary_basic():
    """基本用法: 获取指定日期范围的融资融券汇总数据"""
    print("\n" + "=" * 60)
    print("示例 3: 基本用法 - 获取融资融券汇总数据")
    print("=" * 60)

    service = get_service()

    try:
        # start_date: 起始日期，格式 "YYYY-MM-DD"
        # end_date: 结束日期，格式 "YYYY-MM-DD"
        # 返回 DataFrame，包含融资融券汇总数据
        df = service.akshare.get_margin_summary(
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        if df.empty:
            print("该日期范围内无融资融券汇总数据")
            return
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取融资融券汇总数据失败: {e}")


# ============================================================
# 示例 4: 获取不同日期范围的汇总数据
# ============================================================
def example_margin_summary_different_ranges():
    """演示获取不同日期范围的融资融券汇总数据"""
    print("\n" + "=" * 60)
    print("示例 4: 不同日期范围的汇总数据")
    print("=" * 60)

    service = get_service()

    # 不同时间范围
    ranges = [
        ("2024-01-01", "2024-01-07", "一周"),
        ("2024-01-01", "2024-01-31", "一月"),
        ("2024-01-01", "2024-03-31", "一季度"),
    ]

    for start, end, label in ranges:
        try:
            df = service.akshare.get_margin_summary(
                start_date=start,
                end_date=end,
            )
            if not df.empty:
                print(f"\n时间范围: {label} ({start} ~ {end})")
                print(f"  数据行数: {len(df)}")
                print(
                    f"  日期范围: {df.iloc[0].get('date', 'N/A')} ~ {df.iloc[-1].get('date', 'N/A')}"
                )
            else:
                print(f"\n时间范围: {label} ({start} ~ {end}) - 无数据")
        except Exception as e:
            print(f"\n时间范围: {label} ({start} ~ {end}) - 获取失败: {e}")


# ============================================================
# 示例 5: 数据分析 - 融资融券趋势分析
# ============================================================
def example_margin_analysis():
    """演示获取汇总数据后进行简单趋势分析"""
    print("\n" + "=" * 60)
    print("示例 5: 数据分析 - 融资融券趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        # 获取一个月的汇总数据
        df = service.akshare.get_margin_summary(
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        if df.empty:
            print("无数据")
            return

        print(f"融资融券汇总数据 ({len(df)}个交易日)")
        print(f"数据形状: {df.shape}")

        # 打印基本统计信息 (如果存在相关数值列)
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        # 打印最新5天数据
        print("\n最新5天数据:")
        print(df.tail(5).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效日期、无数据等情况"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效日期格式
    print("\n测试 1: 无效日期格式")
    try:
        df = service.akshare.get_margin_data(date="invalid-date")
        print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试未来日期 (可能无数据)
    print("\n测试 2: 未来日期")
    try:
        df = service.akshare.get_margin_data(date="2099-12-31")
        if df.empty:
            print("  结果: 无数据 (空 DataFrame)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试日期范围倒置
    print("\n测试 3: 日期范围倒置")
    try:
        df = service.akshare.get_margin_summary(
            start_date="2024-12-31",
            end_date="2024-01-01",
        )
        if df.empty:
            print("  结果: 无数据 (空 DataFrame)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_margin_data_basic()
    example_margin_data_with_date_object()
    example_margin_summary_basic()
    example_margin_summary_different_ranges()
    example_margin_analysis()
    example_error_handling()
