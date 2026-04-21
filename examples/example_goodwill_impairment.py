"""
get_goodwill_impairment() 接口示例

演示如何使用 akshare_data.get_goodwill_impairment() 获取商誉减值数据。

参数说明:
    date: 查询日期，格式 "YYYY-MM-DD"，不传则返回最新数据

返回字段: 包含股票代码、商誉减值金额、减值原因、报告期等信息

注意: 当前 akshare 数据源暂无商誉减值数据接口。
      如配置了 Lixinger 等第三方数据源，可正常返回数据。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取最新商誉减值数据
# ============================================================
def example_basic():
    """基本用法: 获取最新商誉减值数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取最新商誉减值数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_impairment()
        if df is None or df.empty:
            print("（无数据 - akshare 暂无商誉减值数据接口，请配置 Lixinger 等数据源）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("提示: akshare 暂无商誉减值数据接口")


# ============================================================
# 示例 2: 指定日期获取商誉减值数据
# ============================================================
def example_with_date():
    """指定日期获取商誉减值数据"""
    print("\n" + "=" * 60)
    print("示例 2: 指定日期获取商誉减值数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_impairment(date="2024-06-30")
        if df is None or df.empty:
            print("（无数据 - akshare 暂无商誉减值数据接口）")
            return

        print(f"查询日期: 2024-06-30")
        print(f"数据形状: {df.shape}")

        print("\n前10行数据:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 不同报告期对比
# ============================================================
def example_compare_periods():
    """对比不同报告期的商誉减值数据"""
    print("\n" + "=" * 60)
    print("示例 3: 对比不同报告期的商誉减值数据")
    print("=" * 60)

    service = get_service()
    dates = ["2023-12-31", "2024-03-31", "2024-06-30"]

    for date in dates:
        try:
            df = service.get_goodwill_impairment(date=date)
            if df is None or df.empty:
                print(f"\n{date}: 无数据")
            else:
                print(f"\n{date}: {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{date}: 获取失败 - {e}")


# ============================================================
# 示例 4: 商誉减值统计分析
# ============================================================
def example_analysis():
    """对商誉减值数据进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 商誉减值统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_impairment()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"总减值记录数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 打印基本统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 筛选大额减值
# ============================================================
def example_filter_large():
    """筛选大额商誉减值记录"""
    print("\n" + "=" * 60)
    print("示例 5: 筛选大额商誉减值")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_impairment()

        if df is None or df.empty:
            print("无数据")
            return

        # 尝试筛选大额减值
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            amount_col = numeric_cols[0]
            large = df[df[amount_col] > df[amount_col].median()]
            print(f"大额减值记录数: {len(large)} / {len(df)}")
            if not large.empty:
                print("\n前5条大额减值:")
                print(large.head())
        else:
            print("无数值字段可用于筛选")

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_with_date()
    example_compare_periods()
    example_analysis()
    example_filter_large()
