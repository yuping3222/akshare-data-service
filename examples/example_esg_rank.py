"""
get_esg_rank() 接口示例

演示如何使用 akshare_data.get_esg_rank() 获取 ESG 评级排名数据。

注意: 底层 akshare 接口 stock_esg_rate_sina 不接受任何参数，
返回全市场 ESG 排名数据。date/top_n 参数当前被忽略。

返回字段: 包含股票代码、ESG 评分、排名、行业等信息
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取最新 ESG 排名
# ============================================================
def example_basic():
    """基本用法: 获取最新 ESG 排名"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取最新 ESG 排名")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: 底层接口不接受参数，返回全市场数据
        df = service.get_esg_rank()
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
def example_with_date():
    """获取 ESG 排名数据"""
    print("\n" + "=" * 60)
    print("示例 2: 获取 ESG 排名数据")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: date/top_n 参数被底层接口忽略
        df = service.get_esg_rank()
        if df is None:
            print("（无数据 - 所有数据源均不可用）")
            return

        print(f"数据形状: {df.shape}")

        if not df.empty:
            print("\n前10名:")
            print(df.head(10))
        else:
            print("（无排名数据）")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 获取不同数量的排名
# ============================================================
def example_different_top_n():
    """获取 ESG 排名数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取 ESG 排名")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_esg_rank()
        if df is None:
            print("无数据 - 所有数据源均不可用")
        else:
            print(f"共 {len(df)} 条记录")
            if not df.empty:
                print(f"  字段: {list(df.columns)}")
    except Exception as e:
        print(f"获取失败 - {e}")


# ============================================================
# 示例 4: 排名数据分析
# ============================================================
def example_analysis():
    """对 ESG 排名数据进行简单分析"""
    print("\n" + "=" * 60)
    print("示例 4: 排名数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_esg_rank()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"获取到 {len(df)} 条记录")
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
def example_compare_dates():
    """获取 ESG 排名数据"""
    print("\n" + "=" * 60)
    print("示例 5: ESG 排名数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_esg_rank()
        if df is None:
            print("无数据 - 所有数据源均不可用")
        else:
            print(f"共 {len(df)} 条记录")
            if not df.empty:
                print(df.head(5))
    except Exception as e:
        print(f"获取失败 - {e}")


if __name__ == "__main__":
    example_basic()
    example_with_date()
    example_different_top_n()
    example_analysis()
    example_compare_dates()
