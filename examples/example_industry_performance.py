"""
get_industry_performance() 接口示例

演示如何使用 DataService.get_industry_performance() 获取行业板块行情历史数据。

接口说明:
  - symbol: 行业板块名称，如 "半导体"、"银行"、"医药" (必需参数)
  - start_date: 起始日期，格式 "YYYYMMDD" (可选，默认 "20211201")
  - end_date: 结束日期，格式 "YYYYMMDD" (可选，默认 "20220401")
  - period: K线周期，默认 "日k"

返回: pd.DataFrame，包含行业板块的行情历史数据

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_industry_performance(symbol="半导体", start_date="20240101", end_date="20240601")
"""

from datetime import datetime, timedelta
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取行业板块行情
# ============================================================
def example_industry_performance_basic():
    """基本用法: 获取半导体行业行情历史数据"""
    print("=" * 60)
    print("示例 1: 获取半导体行业行情历史")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_industry_performance(
            symbol="半导体",
            start_date="20240101",
            end_date="20240601",
        )

        if df is None or df.empty:
            print("无数据（数据源不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取不同行业的行情对比
# ============================================================
def example_multiple_industries():
    """获取多个行业的行情数据进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 多行业行情对比")
    print("=" * 60)

    service = get_service()

    industries = ["银行", "医药", "半导体", "新能源"]
    start_date = "20240101"
    end_date = "20240301"

    for industry in industries:
        try:
            df = service.get_industry_performance(
                symbol=industry,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"\n{industry}: 无数据")
                continue

            print(f"\n{industry}: {len(df)} 条记录")
            print(f"  字段: {list(df.columns)}")
            print(df.head(3))
        except Exception as e:
            print(f"\n{industry}: 获取失败 - {e}")


# ============================================================
# 示例 3: 不同周期数据
# ============================================================
def example_different_periods():
    """获取不同周期的行业行情"""
    print("\n" + "=" * 60)
    print("示例 3: 不同周期行业行情")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_industry_performance(
            symbol="银行",
            start_date="20240101",
            end_date="20240601",
            period="日k",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"日K线数据: {len(df)} 条记录")
        print(f"字段: {list(df.columns)}")
        print(df.head(5))

    except Exception as e:
        print(f"获取失败 - {e}")


# ============================================================
# 示例 4: 统计分析
# ============================================================
def example_industry_performance_stats():
    """对行业行情数据进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 行业行情统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_industry_performance(
            symbol="半导体",
            start_date="20240101",
            end_date="20240601",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"半导体行业行情 ({len(df)} 条记录)")
        print(f"字段: {list(df.columns)}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效行业名称等"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    # 测试 1: 正常获取
    print("\n测试 1: 正常获取")
    try:
        df = service.get_industry_performance(
            symbol="银行",
            start_date="20240101",
            end_date="20240301",
        )
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 2: 不存在的行业
    print("\n测试 2: 不存在的行业名称")
    try:
        df = service.get_industry_performance(
            symbol="不存在的行业",
            start_date="20240101",
            end_date="20240301",
        )
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_industry_performance_basic()
    example_multiple_industries()
    example_different_periods()
    example_industry_performance_stats()
    example_error_handling()
