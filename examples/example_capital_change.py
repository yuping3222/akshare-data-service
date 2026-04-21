"""
股本变动接口示例 (get_capital_change)

注意: 当前 akshare 数据源中 capital_change 接口映射到 stock_changes_em，
该函数实际获取的是"盘口异动"数据（按异动类型分类，如火箭发射、大笔买入等），
而非个股的股本变动（如增发、配股、解禁等）。

stock_changes_em 的 symbol 参数接受的是异动类型，而非股票代码，
有效值包括: 火箭发射, 快速反弹, 大笔买入, 大笔卖出, 加速下跌,
高台跳水, 封涨停板, 打开涨停板, 打开跌停板, 涨速, 跌速 等。

如需获取真实的股本变动数据，建议:
1. 使用 lixinger 数据源（需配置 LIXINGER_TOKEN）
2. 或等待 akshare 数据源更新更合适的股本变动接口
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取盘口异动数据
# ============================================================
def example_basic():
    """基本用法: 获取大笔买入异动数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取盘口异动数据")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: 此处的 symbol 是异动类型，不是股票代码
        # akshare 的 stock_changes_em 按异动类型返回数据
        print("注意: capital_change 接口当前映射到 akshare 的 stock_changes_em，")
        print("      获取的是盘口异动数据，非个股股本变动数据。")
        print()

        df = service.get_capital_change(symbol="大笔买入")

        if df is None or df.empty:
            print("无数据（数据源不可用或无异动记录）")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取不同异动类型数据
# ============================================================
def example_sz_stock():
    """获取不同类型的异动数据"""
    print("\n" + "=" * 60)
    print("示例 2: 获取不同异动类型数据")
    print("=" * 60)

    service = get_service()

    change_types = ["火箭发射", "大笔买入", "大笔卖出", "加速下跌"]

    for change_type in change_types:
        try:
            df = service.get_capital_change(symbol=change_type)

            if df is None or df.empty:
                print(f"{change_type}: 无数据")
            else:
                print(f"{change_type}: 共 {len(df)} 条记录")

        except Exception as e:
            print(f"{change_type}: 获取失败 - {e}")


# ============================================================
# 示例 3: 批量获取多种异动类型
# ============================================================
def example_multiple_stocks():
    """批量获取多种异动类型数据"""
    print("\n" + "=" * 60)
    print("示例 3: 批量获取多种异动类型")
    print("=" * 60)

    service = get_service()

    change_types = ["火箭发射", "大笔买入", "封涨停板"]

    for change_type in change_types:
        try:
            df = service.get_capital_change(symbol=change_type)
            if df is None or df.empty:
                print(f"\n{change_type}: 无数据")
            else:
                print(f"\n{change_type}: 共 {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{change_type}: 获取失败 - {e}")


# ============================================================
# 示例 4: 数据分析 - 异动数据概览
# ============================================================
def example_analysis():
    """演示获取数据后进行异动数据分析"""
    print("\n" + "=" * 60)
    print("示例 4: 数据分析 - 异动数据概览")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_capital_change(symbol="火箭发射")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"火箭发射异动数据 ({len(df)}条)")
        print(f"数据形状: {df.shape}")

        # 打印基本统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        print("\n完整数据:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效异动类型等情况"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试有效异动类型
    print("\n测试 1: 有效异动类型")
    try:
        df = service.get_capital_change(symbol="火箭发射")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试无效异动类型
    print("\n测试 2: 无效异动类型")
    try:
        df = service.get_capital_change(symbol="INVALID_TYPE")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_sz_stock()
    example_multiple_stocks()
    example_analysis()
    example_error_handling()
