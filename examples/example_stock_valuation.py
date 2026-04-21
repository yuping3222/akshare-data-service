"""
get_stock_valuation() 接口示例

演示如何使用 akshare_data.get_stock_valuation() 获取股票估值数据。

接口说明:
  - get_stock_valuation(symbol) -> pd.DataFrame
  - 返回股票的估值指标，如 PE-TTM、PB、PS-TTM、股息率等

常用估值指标:
  - pe_ttm: 市盈率-TTM (Trailing Twelve Months)
  - pb: 市净率
  - ps_ttm: 市销率-TTM
  - dyr: 股息率

注意: 该接口目前主要通过 lixinger 数据源提供支持。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票估值
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的估值数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台估值数据")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 证券代码，支持多种格式
        df = service.get_stock_valuation(symbol="600519")

        if df is None or df.empty:
            print("无数据 (数据源未返回结果)")
            print("提示: get_stock_valuation 目前主要由 Lixinger 数据源支持，")
            print("      请确保 LIXINGER_TOKEN 环境变量已配置")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印全部数据 (通常只有最新一行)
        print("\n估值数据:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取估值数据失败: {e}")


# ============================================================
# 示例 2: 对比多只蓝筹股的估值
# ============================================================
def example_compare_stocks():
    """对比多只蓝筹股的估值水平"""
    print("\n" + "=" * 60)
    print("示例 2: 多股估值对比")
    print("=" * 60)

    service = get_service()

    # 几只代表性股票
    stocks = {
        "600519": "贵州茅台",
        "000001": "平安银行",
        "000002": "万科A",
        "600036": "招商银行",
    }

    results = []
    for code, name in stocks.items():
        try:
            df = service.get_stock_valuation(symbol=code)
            if df is not None and not df.empty:
                row = {"股票代码": code, "股票名称": name}
                # 提取关键估值字段
                for col in ["pe_ttm", "pb", "ps_ttm", "dyr"]:
                    if col in df.columns:
                        row[col] = df[col].iloc[0]
                results.append(row)
                print(f"{name}({code}): 获取成功")
            else:
                print(f"{name}({code}): 无数据")
        except Exception as e:
            print(f"{name}({code}): 获取失败 - {e}")

    if results:
        import pandas as pd

        compare_df = pd.DataFrame(results)
        print("\n估值对比表:")
        print(compare_df.to_string(index=False))


# ============================================================
# 示例 3: 获取深市股票估值
# ============================================================
def example_sz_stock():
    """获取深市股票估值数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取深市股票估值 (平安银行)")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_stock_valuation(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n估值数据:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 使用不同代码格式
# ============================================================
def example_symbol_formats():
    """演示支持的多种证券代码格式"""
    print("\n" + "=" * 60)
    print("示例 4: 不同证券代码格式")
    print("=" * 60)

    service = get_service()

    symbols = [
        "600519",  # 纯数字 (贵州茅台)
        "sh600519",  # 交易所前缀
        "600519.XSHG",  # JoinQuant 格式
    ]

    for sym in symbols:
        try:
            df = service.get_stock_valuation(symbol=sym)
            if df is not None and not df.empty:
                print(f"代码格式: {sym:15s} -> 获取成功, 字段数: {len(df.columns)}")
            else:
                print(f"代码格式: {sym:15s} -> 无数据")
        except Exception as e:
            print(f"代码格式: {sym:15s} -> 获取失败: {e}")


# ============================================================
# 示例 5: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效代码、网络异常等"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试 1: 正常获取
    print("\n测试 1: 正常获取")
    try:
        df = service.get_stock_valuation(symbol="600519")
        if df is None or df.empty:
            print("  结果: 无数据 (空 DataFrame)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 2: 无效代码
    print("\n测试 2: 无效代码")
    try:
        df = service.get_stock_valuation(symbol="999999")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_stocks()
    example_sz_stock()
    example_symbol_formats()
    example_error_handling()
