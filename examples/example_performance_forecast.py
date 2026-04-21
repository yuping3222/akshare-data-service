"""
get_performance_forecast() 接口示例

演示如何使用 akshare_data.get_performance_forecast() 获取业绩预报数据。

接口说明:
- 获取指定报告期的全市场业绩预报数据
- date: 报告期日期（格式 YYYYMMDD），如 "20240331"、"20240630" 等
  可选值从 20100331 开始的各季度末日期
- 返回字段包含: 股票代码、预告类型、预告净利润变动幅度、预告日期等
- akshare 函数: stock_yjbb_em(date="YYYYMMDD")

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_performance_forecast(date="20240331")
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定报告期业绩预告
# ============================================================
def example_basic():
    """基本用法: 获取2024年Q1报告期业绩预告"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取2024年Q1业绩预告")
    print("=" * 60)

    service = get_service()

    try:
        # date: 报告期日期，格式 "YYYYMMDD"
        # 常见报告期: "20240331"(Q1), "20240630"(中报), "20240930"(Q3), "20241231"(年报)
        df = service.get_performance_forecast(date="20240331")

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
# 示例 2: 不同报告期对比
# ============================================================
def example_all_market():
    """获取多个报告期的业绩预告进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 多报告期业绩预告对比")
    print("=" * 60)

    service = get_service()

    dates = {
        "20231231": "2023年年报",
        "20240331": "2024年Q1",
        "20240630": "2024年中报",
    }

    for date, label in dates.items():
        try:
            df = service.get_performance_forecast(date=date)

            if df is None:
                print(f"\n{label}: 无数据（接口暂无可用数据源）")
                continue

            if not df.empty:
                print(f"\n{label}: 共 {len(df)} 条业绩预告")
                print(f"字段列表: {list(df.columns)}")
                print(df.head(3).to_string(index=False))
            else:
                print(f"\n{label}: 无数据")

        except Exception as e:
            print(f"\n{label}: 获取失败 - {e}")


# ============================================================
# 示例 3: 业绩预告类型统计
# ============================================================
def example_type_statistics():
    """统计业绩预告的类型分布"""
    print("\n" + "=" * 60)
    print("示例 3: 业绩预告类型统计")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_performance_forecast(date="20240331")

        if df is None or df.empty:
            print("无数据（接口暂无可用数据源）")
            return

        print(f"全市场业绩预告: {len(df)} 条")

        # 查找预告类型字段
        type_col = None
        for col in df.columns:
            if "类型" in col or "type" in col.lower() or "预告" in col:
                type_col = col
                break

        if type_col:
            print(f"\n预告类型分布:")
            print(df[type_col].value_counts().head(10))
        else:
            print(f"\n字段列表: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 高增长股票筛选
# ============================================================
def example_high_growth():
    """筛选业绩高增长的股票"""
    print("\n" + "=" * 60)
    print("示例 4: 高增长股票筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_performance_forecast(date="20240331")

        if df is None or df.empty:
            print("无数据（接口暂无可用数据源）")
            return

        # 查找变动幅度字段
        change_col = None
        for col in df.columns:
            if "变动" in col or "增幅" in col or "增长" in col or "change" in col.lower():
                change_col = col
                break

        if change_col:
            # 筛选增长超过100%的
            df_high = df[df[change_col] > 100]
            print(f"业绩增长超过100%的股票: {len(df_high)} 只")
            if not df_high.empty:
                print(df_high.head(10).to_string(index=False))
        else:
            print(f"字段列表: {list(df.columns)}")
            print("无法识别变动幅度字段")

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
        print("\n测试 1: 正常报告期")
        df = service.get_performance_forecast(date="20240331")
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 不同报告期")
        df = service.get_performance_forecast(date="20231231")
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_all_market()
    example_type_statistics()
    example_high_growth()
    example_error_handling()
