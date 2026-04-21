"""
get_equity_pledge() 接口示例

演示如何使用 akshare_data.get_equity_pledge() 获取股票股权质押数据。

参数说明:
    symbol:     证券代码，支持多种格式 (如 "000001", "sh600000")
    start_date: 起始日期，格式 "YYYY-MM-DD"
    end_date:   结束日期，格式 "YYYY-MM-DD"

返回字段: 包含质押方、质权方、质押股数、质押比例等信息
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票股权质押数据
# ============================================================
def example_basic():
    """基本用法: 获取平安银行股权质押数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取平安银行股权质押数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_equity_pledge(
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        if df is None:
            print("（无数据 - 所有数据源均不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        if not df.empty:
            print("\n前5行数据:")
            print(df.head())
            print("\n后5行数据:")
            print(df.tail())
        else:
            print("（无股权质押数据）")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取沪市股票股权质押数据
# ============================================================
def example_sh_stock():
    """获取沪市股票股权质押数据"""
    print("\n" + "=" * 60)
    print("示例 2: 获取贵州茅台股权质押数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_equity_pledge(
            symbol="600519",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        print(f"数据形状: {df.shape}")

        if not df.empty:
            print("\n前5行数据:")
            print(df.head())
        else:
            print("（无股权质押数据）")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 不同日期范围对比
# ============================================================
def example_date_ranges():
    """对比不同日期范围的股权质押数据"""
    print("\n" + "=" * 60)
    print("示例 3: 不同日期范围对比")
    print("=" * 60)

    service = get_service()
    symbol = "000001"

    date_ranges = [
        ("2024-01-01", "2024-03-31"),
        ("2024-04-01", "2024-06-30"),
        ("2024-07-01", "2024-09-30"),
    ]

    for start, end in date_ranges:
        try:
            df = service.get_equity_pledge(symbol, start, end)
            print(f"\n{start} ~ {end}: {len(df)} 条记录")
            if not df.empty:
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{start} ~ {end}: 获取失败 - {e}")


# ============================================================
# 示例 4: 股权质押统计分析
# ============================================================
def example_analysis():
    """对股权质押数据进行简单统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 股权质押统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_equity_pledge(
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"总质押记录数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 如果有数值列，打印统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效代码
    print("\n测试 1: 无效证券代码")
    try:
        df = service.get_equity_pledge(
            symbol="999999",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        print(f"  结果: {len(df)} 条记录")
        if df is None or df.empty:
            print("  （无数据）")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_sh_stock()
    example_date_ranges()
    example_analysis()
    example_error_handling()
