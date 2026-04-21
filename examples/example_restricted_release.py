"""
get_restricted_release() 接口示例

演示如何使用 akshare_data.get_restricted_release() 获取限售股解禁数据。

接口说明:
- 获取限售股解禁明细数据
- start_date: 起始日期（必填），格式 "YYYY-MM-DD" 或 "YYYYMMDD"
- end_date: 结束日期（必填），格式 "YYYY-MM-DD" 或 "YYYYMMDD"
- 注意: 该接口不支持按个股筛选，返回指定日期范围内的全市场解禁数据
- 返回字段包含: 股票代码、解禁日期、解禁数量、解禁市值等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_restricted_release(start_date="2024-01-01", end_date="2024-12-31")

如需获取单只股票的解禁数据，可先获取全市场数据后自行筛选。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票解禁数据
# ============================================================
def example_basic():
    """基本用法: 获取指定日期范围的限售股解禁数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取解禁数据")
    print("=" * 60)

    service = get_service()

    try:
        # start_date: 起始日期，格式 "YYYY-MM-DD" 或 "YYYYMMDD"
        # end_date: 结束日期，格式 "YYYY-MM-DD" 或 "YYYYMMDD"
        df = service.get_restricted_release(
            start_date="2024-01-01",
            end_date="2024-03-31",
        )

        if df is None or df.empty:
            print("\n无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 全市场解禁数据
# ============================================================
def example_all_market():
    """获取全市场解禁数据"""
    print("\n" + "=" * 60)
    print("示例 2: 全市场解禁数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release(
            start_date="2024-01-01",
            end_date="2024-03-31",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 多只股票的解禁数据对比
# ============================================================
def example_compare_periods():
    """对比不同日期范围的解禁情况"""
    print("\n" + "=" * 60)
    print("示例 3: 不同日期范围解禁数据对比")
    print("=" * 60)

    service = get_service()

    ranges = [
        ("2024-01-01", "2024-03-31", "2024年Q1"),
        ("2024-04-01", "2024-06-30", "2024年Q2"),
        ("2024-07-01", "2024-09-30", "2024年Q3"),
    ]

    for start, end, label in ranges:
        try:
            df = service.get_restricted_release(
                start_date=start,
                end_date=end,
            )

            if df is None or df.empty:
                print(f"\n{label}: 无解禁数据")
            else:
                print(f"\n{label}: {len(df)} 条解禁记录")
                print(df.head(3).to_string(index=False))

        except Exception as e:
            print(f"\n{label}: 获取失败 - {e}")


# ============================================================
# 示例 4: 按日期范围筛选
# ============================================================
def example_date_range():
    """演示不同日期范围的解禁数据"""
    print("\n" + "=" * 60)
    print("示例 4: 不同日期范围的解禁数据")
    print("=" * 60)

    service = get_service()

    date_ranges = [
        ("2024-01-01", "2024-03-31"),
        ("2024-04-01", "2024-06-30"),
        ("2024-07-01", "2024-09-30"),
    ]

    for start, end in date_ranges:
        try:
            df = service.get_restricted_release(
                start_date=start,
                end_date=end,
            )

            if df is None or df.empty:
                print(f"\n{start} ~ {end}: 无解禁数据")
            else:
                print(f"\n{start} ~ {end}: {len(df)} 条记录")
                print(df.head(3).to_string(index=False))

        except Exception as e:
            print(f"\n{start} ~ {end}: 获取失败 - {e}")


# ============================================================
# 示例 5: 解禁数据统计分析
# ============================================================
def example_analysis():
    """对解禁数据进行简单统计分析"""
    print("\n" + "=" * 60)
    print("示例 5: 解禁数据统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release(
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"全市场解禁数据: {len(df)} 条")
        print(f"字段列表: {list(df.columns)}")

        # 统计数值列
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_all_market()
    example_compare_periods()
    example_date_range()
    example_analysis()
