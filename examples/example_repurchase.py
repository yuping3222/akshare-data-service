"""
get_repurchase_data() 接口示例

演示如何使用 akshare_data.get_repurchase_data() 获取股票回购数据。

注意: 底层 akshare 接口 stock_repurchase_em 不接受任何参数，
返回全市场回购数据。symbol/start_date/end_date 参数当前被忽略。

返回字段: 包含股票代码、回购价格、回购数量、回购金额等
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票回购数据
# ============================================================
def example_basic():
    """基本用法: 获取全市场回购数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取回购数据")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: 底层接口不接受参数，返回全市场数据
        df = service.get_repurchase_data()
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n回购数据 (前10行):")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 对比多只股票回购数据
# ============================================================
def example_compare_stocks():
    """获取回购数据概览"""
    print("\n" + "=" * 60)
    print("示例 2: 回购数据概览")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_repurchase_data()
        if df is None or df.empty:
            print("无数据")
        else:
            print(f"共 {len(df)} 条记录")
            print(df.head(3))
    except Exception as e:
        print(f"获取失败 - {e}")


# ============================================================
# 示例 3: 不同时间区间
# ============================================================
def example_date_ranges():
    """获取回购数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取回购数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_repurchase_data()
        if df is None or df.empty:
            print("无数据")
        else:
            print(f"共 {len(df)} 条回购记录")
    except Exception as e:
        print(f"获取失败 - {e}")


# ============================================================
# 示例 4: 统计分析
# ============================================================
def example_statistics():
    """对回购数据进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 回购数据统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_repurchase_data()
        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 条回购记录")
        print(f"字段列表: {list(df.columns)}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_stocks()
    example_date_ranges()
    example_statistics()
