"""
get_goodwill_data() 接口示例

演示如何使用 akshare_data.get_goodwill_data() 获取股票商誉数据。

参数说明:
    symbol:     证券代码，支持多种格式 (如 "000001", "sh600000")
    start_date: 起始日期，格式 "YYYY-MM-DD"
    end_date:   结束日期，格式 "YYYY-MM-DD"

返回字段: 包含商誉金额、商誉占总资产比例、商誉占净资产比例等信息

注意: 当前 akshare 数据源暂无商誉数据接口。
      如配置了 Lixinger 等第三方数据源，可正常返回数据。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票商誉数据
# ============================================================
def example_basic():
    """基本用法: 获取平安银行商誉数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取平安银行商誉数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_data(
            symbol="000001",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        if df is None or df.empty:
            print("（无数据 - akshare 暂无商誉数据接口，请配置 Lixinger 等数据源）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("提示: akshare 暂无商誉数据接口，请配置 Lixinger 等第三方数据源")


# ============================================================
# 示例 2: 获取沪市股票商誉数据
# ============================================================
def example_sh_stock():
    """获取沪市股票商誉数据"""
    print("\n" + "=" * 60)
    print("示例 2: 获取贵州茅台商誉数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_data(
            symbol="600519",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        if df is None or df.empty:
            print("（无数据 - akshare 暂无商誉数据接口）")
            return

        print(f"数据形状: {df.shape}")
        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 对比不同股票的商誉
# ============================================================
def example_compare_stocks():
    """对比多只股票的商誉数据"""
    print("\n" + "=" * 60)
    print("示例 3: 对比不同股票的商誉数据")
    print("=" * 60)

    service = get_service()
    symbols = ["000001", "600519", "000858"]

    for symbol in symbols:
        try:
            df = service.get_goodwill_data(
                symbol=symbol,
                start_date="2024-01-01",
                end_date="2024-12-31",
            )
            if df is None or df.empty:
                print(f"\n{symbol}: 无数据")
            else:
                print(f"\n{symbol}: {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{symbol}: 获取失败 - {e}")


# ============================================================
# 示例 4: 商誉数据分析
# ============================================================
def example_analysis():
    """对商誉数据进行简单分析"""
    print("\n" + "=" * 60)
    print("示例 4: 商誉数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_goodwill_data(
            symbol="000001",
            start_date="2023-01-01",
            end_date="2024-12-31",
        )

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

    print("\n测试 1: 无效证券代码")
    try:
        df = service.get_goodwill_data(
            symbol="999999",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        if df is None or df.empty:
            print("  （无数据）")
        else:
            print(f"  结果: {len(df)} 条记录")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_sh_stock()
    example_compare_stocks()
    example_analysis()
    example_error_handling()
