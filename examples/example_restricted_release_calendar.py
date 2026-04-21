"""
get_restricted_release_calendar() 接口示例

演示如何使用 akshare_data.get_restricted_release_calendar() 获取限售股解禁日历数据。

接口说明:
- 获取限售股解禁的日历视图数据
- start_date: 起始日期（可选）
- end_date: 结束日期（可选）
- 返回字段包含: 日期、当日解禁股票数量、当日解禁市值等汇总信息

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_restricted_release_calendar(start_date="2024-01-01", end_date="2024-12-31")
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定日期范围的解禁日历
# ============================================================
def example_basic():
    """基本用法: 获取限售股解禁日历"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取解禁日历")
    print("=" * 60)

    service = get_service()

    try:
        # start_date: 起始日期，格式 "YYYY-MM-DD"
        # end_date: 结束日期，格式 "YYYY-MM-DD"
        df = service.get_restricted_release_calendar(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )

        if df is None or df.empty:
            print("\n无数据（接口暂无可用数据源）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        if not df.empty:
            print("\n前5行数据:")
            print(df.head())
        else:
            print("\n无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不指定日期 - 获取全部解禁日历
# ============================================================
def example_all():
    """不指定日期，获取全部解禁日历"""
    print("\n" + "=" * 60)
    print("示例 2: 不指定日期 - 获取全部解禁日历")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release_calendar()

        if df is None:
            print("无数据（接口暂无可用数据源）")
            return

        if not df.empty:
            print(f"数据形状: {df.shape}")
            print(f"字段列表: {list(df.columns)}")
            print(f"\n前5行数据:")
            print(df.head())
        else:
            print("无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 按月份查看解禁日历
# ============================================================
def example_by_month():
    """按月份查看解禁情况"""
    print("\n" + "=" * 60)
    print("示例 3: 按月份查看解禁日历")
    print("=" * 60)

    service = get_service()

    months = [
        ("2024-01-01", "2024-01-31", "2024年1月"),
        ("2024-02-01", "2024-02-29", "2024年2月"),
        ("2024-03-01", "2024-03-31", "2024年3月"),
    ]

    for start, end, label in months:
        try:
            df = service.get_restricted_release_calendar(
                start_date=start,
                end_date=end,
            )

            if df is None:
                print(f"\n{label}: 接口暂无可用数据源")
                continue

            print(f"\n{label}: {len(df)} 条记录")
            if not df.empty:
                print(df.head(5).to_string(index=False))

        except Exception as e:
            print(f"\n{label}: 获取失败 - {e}")


# ============================================================
# 示例 4: 解禁高峰分析
# ============================================================
def example_peak_analysis():
    """分析解禁高峰日期"""
    print("\n" + "=" * 60)
    print("示例 4: 解禁高峰分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release_calendar(
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        if df is None or df.empty:
            print("无数据（接口暂无可用数据源）")
            return

        print(f"全年解禁日历: {len(df)} 天")
        print(f"字段列表: {list(df.columns)}")

        # 查找数值列并排序
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            col = numeric_cols[0]
            print(f"\n按 {col} 排序的前10天:")
            top_days = df.nlargest(10, col)
            print(top_days.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    try:
        print("\n测试 1: 日期范围倒置")
        df = service.get_restricted_release_calendar(
            start_date="2024-12-31",
            end_date="2024-01-01",
        )
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 正常调用")
        df = service.get_restricted_release_calendar(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_all()
    example_by_month()
    example_peak_analysis()
    example_error_handling()
