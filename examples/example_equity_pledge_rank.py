"""
get_equity_pledge_rank() 接口示例

演示如何使用 akshare_data.get_equity_pledge_rank() 获取股权质押比例排名数据。

参数说明:
    symbol: 查询类型，默认 "全部"，可选 "股东增持"、"股东减持"
    top_n:  返回前 N 条记录，默认 50

返回字段: 包含股票代码、质押比例、质押股数、总市值等信息
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取最新股权质押比例排名
# ============================================================
def example_basic():
    """基本用法: 获取最新股权质押比例排名 (前20名)"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取最新股权质押比例排名")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_equity_pledge_rank(top_n=20)
        if df is None:
            print("（无数据 - 所有数据源均不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        if not df.empty:
            print("\n前10名:")
            print(df.head(10))
        else:
            print("（无排名数据）")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 指定日期获取排名
# ============================================================
def example_with_type():
    """指定类型获取股权质押比例排名"""
    print("\n" + "=" * 60)
    print("示例 2: 指定类型获取排名")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_equity_pledge_rank(
            symbol="股东增持",
            top_n=30,
        )
        if df is None or df.empty:
            print("无数据")
            return

        print(f"查询类型: 股东增持")
        print(f"数据形状: {df.shape}")

        print("\n前10名:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 获取不同数量的排名
# ============================================================
def example_different_top_n():
    """获取不同数量的排名数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取不同数量的排名")
    print("=" * 60)

    service = get_service()

    for n in [10, 50, 100]:
        try:
            df = service.get_equity_pledge_rank(top_n=n)
            if df is None or df.empty:
                print(f"\nTop {n}: 无数据")
            else:
                print(f"\nTop {n}: {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\nTop {n}: 获取失败 - {e}")


# ============================================================
# 示例 4: 排名数据分析
# ============================================================
def example_analysis():
    """对股权质押排名数据进行简单分析"""
    print("\n" + "=" * 60)
    print("示例 4: 排名数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_equity_pledge_rank(top_n=100)

        if df is None or df.empty:
            print("无数据")
            return

        print(f"获取到 {len(df)} 条排名记录")
        print(f"字段列表: {list(df.columns)}")

        # 打印基本统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 对比不同日期的排名变化
# ============================================================
def example_compare_types():
    """对比不同类型的股权质押排名"""
    print("\n" + "=" * 60)
    print("示例 5: 对比不同类型的排名")
    print("=" * 60)

    service = get_service()
    types = ["全部", "股东增持", "股东减持"]

    for symbol_type in types:
        try:
            df = service.get_equity_pledge_rank(symbol=symbol_type, top_n=10)
            if df is None or df.empty:
                print(f"\n{symbol_type}: 无数据")
            else:
                print(f"\n{symbol_type}: {len(df)} 条记录")
                print(df.head(5))
        except Exception as e:
            print(f"\n{symbol_type}: 获取失败 - {e}")


if __name__ == "__main__":
    example_basic()
    example_with_type()
    example_different_top_n()
    example_analysis()
    example_compare_types()
