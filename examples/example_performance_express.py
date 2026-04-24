"""
get_performance_express() 接口示例

演示如何使用 akshare_data.get_performance_express() 获取业绩快报数据。

接口说明:
- 获取全市场或指定股票的业绩快报数据
- symbol: 股票代码（可选，不指定则返回全市场）
- start_date: 起始日期（可选）
- end_date: 结束日期（可选）
- 返回字段包含: 股票代码、报告期、营业收入、净利润、同比增长率等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_performance_express(symbol="000001", start_date="2024-01-01", end_date="2024-12-31")
"""

from akshare_data import get_service
from _example_utils import call_with_date_range_fallback


# ============================================================
# 示例 1: 基本用法 - 获取单只股票业绩快报
# ============================================================
def example_basic():
    """基本用法: 获取平安银行业绩快报"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取平安银行业绩快报")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 股票代码
        # start_date: 起始日期，格式 "YYYY-MM-DD"
        # end_date: 结束日期，格式 "YYYY-MM-DD"
        df, used_end = call_with_date_range_fallback(
            service,
            service.get_performance_express,
            symbol="000001",
            max_backtrack=10,
            window_days=365,
        )

        if df is None or df.empty:
            print("\n无数据（接口暂无可用数据源）")
            print("提示: get_performance_express 目前主要由 Lixinger 数据源支持，")
            print("      请确保 LIXINGER_TOKEN 环境变量已配置")
            return

        print(f"数据形状: {df.shape} (回退结束日期: {used_end})")
        print(f"字段列表: {list(df.columns)}")

        if not df.empty:
            print("\n前5行数据:")
            print(df.head())
        else:
            print("\n无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 全市场业绩快报
# ============================================================
def example_all_market():
    """不指定股票，获取全市场业绩快报"""
    print("\n" + "=" * 60)
    print("示例 2: 全市场业绩快报")
    print("=" * 60)

    service = get_service()

    try:
        df, used_end = call_with_date_range_fallback(
            service,
            service.get_performance_express,
            max_backtrack=10,
            window_days=365,
        )

        if df is None:
            print("无数据（接口暂无可用数据源）")
            return

        if not df.empty:
            print(f"数据形状: {df.shape} (回退结束日期: {used_end})")
            print(f"字段列表: {list(df.columns)}")
            print("\n前5行数据:")
            print(df.head())
        else:
            print("无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 多只股票业绩快报对比
# ============================================================
def example_compare():
    """对比多只银行的业绩快报"""
    print("\n" + "=" * 60)
    print("示例 3: 多只股票业绩快报对比")
    print("=" * 60)

    service = get_service()

    symbols = [
        ("000001", "平安银行"),
        ("600000", "浦发银行"),
        ("601398", "工商银行"),
    ]

    for symbol, name in symbols:
        try:
            df, used_end = call_with_date_range_fallback(
                service,
                service.get_performance_express,
                symbol=symbol,
                max_backtrack=10,
                window_days=365,
            )

            if df is None:
                print(f"\n{name} ({symbol}): 接口暂无可用数据源")
                continue

            if not df.empty:
                print(f"\n{name} ({symbol}): {len(df)} 条快报 (回退结束日期: {used_end})")
                print(df.head(3).to_string(index=False))
            else:
                print(f"\n{name} ({symbol}): 无业绩快报数据")

        except Exception as e:
            print(f"\n{name} ({symbol}): 获取失败 - {e}")


# ============================================================
# 示例 4: 业绩快报数据分析
# ============================================================
def example_analysis():
    """对业绩快报进行数据分析"""
    print("\n" + "=" * 60)
    print("示例 4: 业绩快报数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df, used_end = call_with_date_range_fallback(
            service,
            service.get_performance_express,
            max_backtrack=10,
            window_days=365,
        )

        if df is None or df.empty:
            print("无数据（接口暂无可用数据源）")
            return

        print(f"全市场业绩快报: {len(df)} 条 (回退结束日期: {used_end})")
        print(f"字段列表: {list(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
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
        print("\n测试 1: 无效股票代码")
        df, used_end = call_with_date_range_fallback(
            service,
            service.get_performance_express,
            symbol="INVALID",
            max_backtrack=10,
            window_days=365,
        )
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据 (回退结束日期: {used_end})")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 正常调用")
        df, used_end = call_with_date_range_fallback(
            service,
            service.get_performance_express,
            symbol="000001",
            max_backtrack=10,
            window_days=365,
        )
        if df is None:
            print(f"  结果: 无数据（接口暂无可用数据源）")
        else:
            print(f"  结果: {len(df)} 行数据 (回退结束日期: {used_end})")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_all_market()
    example_compare()
    example_analysis()
    example_error_handling()
