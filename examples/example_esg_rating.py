"""
get_esg_rating() 接口示例

演示如何使用 akshare_data.get_esg_rating() 获取股票 ESG 评级数据。

注意: 底层 akshare 接口 stock_esg_msci_sina 不接受任何参数，
返回全市场 ESG 评级数据。symbol/start_date/end_date 参数当前被忽略。

返回字段: 包含股票代码、ESG 综合评分、环境(E)、社会(S)、治理(G)分项评分等信息
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票 ESG 评级
# ============================================================
def example_basic():
    """基本用法: 获取全市场 ESG 评级数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取 ESG 评级数据")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: 底层接口不接受参数，返回全市场数据
        # symbol/start_date/end_date 参数当前被忽略
        df = service.get_esg_rating()
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
            print("（无 ESG 评级数据）")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取沪市股票 ESG 评级
# ============================================================
def example_sh_stock():
    """获取 ESG 评级数据"""
    print("\n" + "=" * 60)
    print("示例 2: 获取 ESG 评级数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_esg_rating()

        if df is None:
            print("（无数据）")
            return

        print(f"数据形状: {df.shape}")

        if not df.empty:
            print("\n前5行数据:")
            print(df.head())
        else:
            print("（无 ESG 评级数据）")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 对比不同股票的 ESG 评级
# ============================================================
def example_compare_stocks():
    """获取 ESG 评级数据概览"""
    print("\n" + "=" * 60)
    print("示例 3: ESG 评级数据概览")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_esg_rating()
        if df is None:
            print("（无数据）")
            return

        print(f"共 {len(df)} 条记录")
        if not df.empty:
            print(f"  字段: {list(df.columns)}")
    except Exception as e:
        print(f"获取失败 - {e}")


# ============================================================
# 示例 4: ESG 评级趋势分析
# ============================================================
def example_trend_analysis():
    """ESG 评级数据统计"""
    print("\n" + "=" * 60)
    print("示例 4: ESG 评级数据统计")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_esg_rating()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"总记录数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 打印基本统计
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

    print("\n测试 1: 正常调用")
    try:
        df = service.get_esg_rating()
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: {len(df)} 条记录")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_sh_stock()
    example_compare_stocks()
    example_trend_analysis()
    example_error_handling()
