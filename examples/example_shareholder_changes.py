"""
股东持股变动接口示例 (get_shareholder_changes)

演示如何获取股票的股东持股变动数据，包括大股东增减持、
持股比例变化等信息。

接口说明:
- get_shareholder_changes(symbol): 获取指定股票的股东持股变动数据

参数:
  symbol: 股票代码，支持多种格式 (如 "600519", "000001", "sh600519")

返回: DataFrame，包含股东名称、变动日期、变动数量、变动比例等字段

注意: 该接口在 akshare 和 lixinger 数据源均有支持。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的股东持股变动
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的股东持股变动数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台(600519)股东持股变动")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 证券代码
        df = service.get_shareholder_changes(symbol="600519")

        if df is None or df.empty:
            print("无数据（数据源不可用或无变动记录）")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取深市股票的股东持股变动
# ============================================================
def example_sz_stock():
    """获取深市股票的股东持股变动"""
    print("\n" + "=" * 60)
    print("示例 2: 获取平安银行(000001)股东持股变动")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_shareholder_changes(symbol="000001")

        if df is None or df.empty:
            print("无数据（数据源不可用或无变动记录）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n最新5条变动记录:")
        print(df.tail(5))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 批量获取多只股票
# ============================================================
def example_multiple_stocks():
    """批量获取多只股票的股东持股变动"""
    print("\n" + "=" * 60)
    print("示例 3: 批量获取多只股票股东持股变动")
    print("=" * 60)

    service = get_service()

    symbols = ["600519", "000001", "601318"]

    for symbol in symbols:
        try:
            df = service.get_shareholder_changes(symbol=symbol)
            if df is None or df.empty:
                print(f"\n{symbol}: 无数据")
            else:
                print(f"\n{symbol}: 共 {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{symbol}: 获取失败 - {e}")


# ============================================================
# 示例 4: 数据分析 - 统计增减持情况
# ============================================================
def example_analysis():
    """演示获取数据后进行增减持分析"""
    print("\n" + "=" * 60)
    print("示例 4: 数据分析 - 统计股东增减持情况")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_shareholder_changes(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"贵州茅台股东持股变动数据 ({len(df)}条)")
        print(f"数据形状: {df.shape}")

        # 打印基本统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        # 打印最新变动
        print("\n最新10条变动记录:")
        print(df.tail(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效代码等情况"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效代码
    print("\n测试 1: 无效股票代码")
    try:
        df = service.get_shareholder_changes(symbol="999999")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试不存在的代码
    print("\n测试 2: 不存在的股票代码")
    try:
        df = service.get_shareholder_changes(symbol="INVALID")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_sz_stock()
    example_multiple_stocks()
    example_analysis()
    example_error_handling()
