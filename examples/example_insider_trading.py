"""
高管交易/内部人交易接口示例 (get_insider_trading)

注意: 该接口当前未在 akshare 数据源的接口配置中定义。
      调用此接口可能导致错误或返回空数据。

如需要高管/内部人交易相关数据，建议:
1. 使用 lixinger 数据源（需配置 LIXINGER_TOKEN）
2. 或等待 akshare 数据源添加对应接口配置
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的高管交易
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的高管交易"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台(600519)高管交易")
    print("=" * 60)
    print("注意: 此接口当前未在 akshare 配置中定义，可能无法返回数据。")
    print()

    service = get_service()

    try:
        # symbol: 证券代码
        df = service.get_insider_trading(symbol="600519")

        if df is None or df.empty:
            print("无数据（接口未配置或数据源不可用）")
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
        print("提示: 该接口当前未在 akshare 数据源中配置")


# ============================================================
# 示例 2: 获取深市股票的高管交易
# ============================================================
def example_sz_stock():
    """获取深市股票的高管交易"""
    print("\n" + "=" * 60)
    print("示例 2: 获取平安银行(000001)高管交易")
    print("=" * 60)
    print("注意: 此接口当前未在 akshare 配置中定义。")
    print()

    service = get_service()

    try:
        df = service.get_insider_trading(symbol="000001")

        if df is None or df.empty:
            print("无数据（接口未配置或数据源不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n最新10条交易记录:")
        print(df.tail(10))

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("提示: 该接口当前未在 akshare 数据源中配置")


# ============================================================
# 示例 3: 批量获取多只股票
# ============================================================
def example_multiple_stocks():
    """批量获取多只股票的高管交易"""
    print("\n" + "=" * 60)
    print("示例 3: 批量获取多只股票高管交易")
    print("=" * 60)

    service = get_service()

    symbols = ["600519", "000001", "601318"]

    for symbol in symbols:
        try:
            df = service.get_insider_trading(symbol=symbol)
            if df is None or df.empty:
                print(f"\n{symbol}: 无数据")
            else:
                print(f"\n{symbol}: 共 {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{symbol}: 获取失败 - {e}")


# ============================================================
# 示例 4: 数据分析 - 交易方向统计
# ============================================================
def example_analysis():
    """演示获取数据后进行高管交易分析"""
    print("\n" + "=" * 60)
    print("示例 4: 数据分析 - 高管交易方向统计")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_insider_trading(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"贵州茅台高管交易数据 ({len(df)}条)")
        print(f"数据形状: {df.shape}")

        # 打印基本统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        # 打印最新交易
        print("\n最新10条交易记录:")
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
        df = service.get_insider_trading(symbol="999999")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试不存在的代码
    print("\n测试 2: 不存在的股票代码")
    try:
        df = service.get_insider_trading(symbol="INVALID")
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
