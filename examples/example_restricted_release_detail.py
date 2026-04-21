"""
get_restricted_release_detail() 接口示例

演示如何使用 akshare_data.get_restricted_release_detail() 获取限售股解禁详细数据。

接口说明:
- 获取全市场限售股解禁的详细明细数据
- start_date: 起始日期（可选）
- end_date: 结束日期（可选）
- 返回字段包含: 股票代码、解禁日期、解禁股份类型、解禁数量、占总股本比例等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_restricted_release_detail(start_date="2024-01-01", end_date="2024-12-31")
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定日期范围的解禁明细
# ============================================================
def example_basic():
    """基本用法: 获取限售股解禁详细数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取解禁明细数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release_detail(
            start_date="2024-01-01",
            end_date="2024-03-31",
        )

        if df is None or df.empty:
            print("\n无数据（接口暂无可用数据源）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        if df is None or df.empty:
            print("\n无数据（接口暂无可用数据源）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不指定日期 - 获取全部解禁明细
# ============================================================
def example_no_date():
    """不指定日期，获取全部解禁明细"""
    print("\n" + "=" * 60)
    print("示例 2: 不指定日期 - 获取全部解禁明细")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release_detail()

        if df is None:
            print("无数据（接口暂无可用数据源）")
            return

        if df.empty:
            print(f"数据形状: {df.shape}")
            print(f"字段列表: {list(df.columns)}")
            print(f"\n前5行数据:")
            print(df.head())
        else:
            print("无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 按季度获取解禁明细
# ============================================================
def example_quarterly():
    """按季度获取解禁明细并对比"""
    print("\n" + "=" * 60)
    print("示例 3: 按季度获取解禁明细")
    print("=" * 60)

    service = get_service()

    quarters = [
        ("2024-01-01", "2024-03-31", "2024年Q1"),
        ("2024-04-01", "2024-06-30", "2024年Q2"),
    ]

    for start, end, label in quarters:
        try:
            df = service.get_restricted_release_detail(
                start_date=start,
                end_date=end,
            )

            if df is None:
                print(f"\n{label}: 接口暂无可用数据源")
                continue

            print(f"\n{label}: {len(df)} 条记录")
            if not df.empty:
                print(df.head(3).to_string(index=False))

        except Exception as e:
            print(f"\n{label}: 获取失败 - {e}")


# ============================================================
# 示例 4: 数据预览与字段分析
# ============================================================
def example_field_analysis():
    """分析解禁明细数据的字段"""
    print("\n" + "=" * 60)
    print("示例 4: 数据字段分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_restricted_release_detail(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )

        if df is None or df.empty:
            print("无数据（接口暂无可用数据源）")
            return

        print(f"总记录数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段: {numeric_cols}")
            print(df[numeric_cols].describe())

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
        print("\n测试 1: 无效日期格式")
        df = service.get_restricted_release_detail(
            start_date="invalid",
            end_date="2024-12-31",
        )
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 正常调用")
        df = service.get_restricted_release_detail(
            start_date="2024-01-01",
            end_date="2024-03-31",
        )
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_no_date()
    example_quarterly()
    example_field_analysis()
    example_error_handling()
