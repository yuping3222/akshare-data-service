"""
get_earnings_forecast() 接口示例

演示如何使用 akshare_data.get_earnings_forecast() 获取盈利预测数据。

接口说明:
- 获取指定行业板块的机构盈利预测数据
- symbol: 行业板块名称（如 "船舶制造"、"银行" 等），不传或传空字符串返回全市场
- 返回字段包含: 股票代码、股票名称、预测年度、预测每股收益、预测净利润等
- 行业板块列表可通过 ak.stock_board_industry_name_em() 获取

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_earnings_forecast()  # 全市场
    df = service.get_earnings_forecast(symbol="船舶制造")  # 指定行业
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取全市场盈利预测
# ============================================================
def example_basic():
    """基本用法: 获取全市场盈利预测"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取全市场盈利预测")
    print("=" * 60)

    service = get_service()

    try:
        # 不传 symbol 返回全市场盈利预测
        df = service.get_earnings_forecast()

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
# 示例 2: 指定行业板块获取盈利预测
# ============================================================
def example_by_industry():
    """获取指定行业的盈利预测"""
    print("\n" + "=" * 60)
    print("示例 2: 按行业板块获取盈利预测")
    print("=" * 60)

    service = get_service()

    # 几个代表性行业
    industries = ["船舶制造", "银行", "酿酒行业"]

    for industry in industries:
        try:
            df = service.get_earnings_forecast(symbol=industry)

            if df is None:
                print(f"\n{industry}: 接口暂无可用数据源")
                continue

            if not df.empty:
                print(f"\n{industry}: {len(df)} 条预测")
                print(df.head(3).to_string(index=False))
            else:
                print(f"\n{industry}: 无盈利预测数据")

        except Exception as e:
            print(f"\n{industry}: 获取失败 - {e}")


# ============================================================
# 示例 3: 盈利预测趋势分析
# ============================================================
def example_trend():
    """分析盈利预测的趋势"""
    print("\n" + "=" * 60)
    print("示例 3: 银行业盈利预测分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_earnings_forecast(symbol="银行")

        if df is None:
            print("无数据（接口暂无可用数据源）")
            return

        if df.empty:
            print("无数据")
            return

        print(f"银行业盈利预测: {len(df)} 条")
        print(f"字段列表: {list(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n预测数值统计:")
            print(df[numeric_cols].describe())

        print("\n最新预测数据:")
        print(df.head(5).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 4: 错误处理")
    print("=" * 60)

    service = get_service()

    try:
        print("\n测试 1: 全市场获取")
        df = service.get_earnings_forecast()
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 指定行业获取")
        df = service.get_earnings_forecast(symbol="银行")
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_by_industry()
    example_trend()
    example_error_handling()
