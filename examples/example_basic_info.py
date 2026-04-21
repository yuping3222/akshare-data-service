"""
get_basic_info() 接口示例

演示如何使用 akshare_data.get_basic_info() 获取股票基本信息。

接口说明:
  - get_basic_info(symbol) -> pd.DataFrame
  - 返回股票的基本信息，如公司名称、所属行业、上市日期、总股本等

返回字段 (可能包含):
  - symbol: 股票代码
  - name: 股票名称
  - industry: 所属行业
  - list_date: 上市日期
  - total_shares: 总股本
  - float_shares: 流通股本
  - market_cap: 总市值
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票基本信息
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台基本信息"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台基本信息")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 证券代码
        df = service.get_basic_info(symbol="600519")

        if df is None or df.empty:
            print("无数据 (数据源未返回结果)")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印全部数据
        print("\n基本信息:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取基本信息失败: {e}")


# ============================================================
# 示例 2: 对比多只股票基本信息
# ============================================================
def example_compare_stocks():
    """对比多只股票的基本信息"""
    print("\n" + "=" * 60)
    print("示例 2: 多股基本信息对比")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "000002", "600036"]

    results = []
    for code in stocks:
        try:
            df = service.get_basic_info(symbol=code)
            if df is not None and not df.empty:
                row = {"股票代码": code}
                # 提取常见字段
                for col in ["name", "industry", "list_date", "total_shares", "market_cap"]:
                    if col in df.columns:
                        row[col] = df[col].iloc[0]
                results.append(row)
                print(f"{code}: 获取成功")
            else:
                print(f"{code}: 无数据")
        except Exception as e:
            print(f"{code}: 获取失败 - {e}")

    if results:
        import pandas as pd

        compare_df = pd.DataFrame(results)
        print("\n基本信息对比表:")
        print(compare_df.to_string(index=False))


# ============================================================
# 示例 3: 获取深市股票基本信息
# ============================================================
def example_sz_stock():
    """获取深市股票基本信息"""
    print("\n" + "=" * 60)
    print("示例 3: 获取深市股票基本信息 (平安银行)")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_basic_info(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n基本信息:")
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
        "600519",  # 纯数字
        "sh600519",  # 交易所前缀
        "600519.XSHG",  # JoinQuant 格式
    ]

    for sym in symbols:
        try:
            df = service.get_basic_info(symbol=sym)
            if df is not None and not df.empty:
                name = df["name"].iloc[0] if "name" in df.columns else "未知"
                print(f"代码格式: {sym:15s} -> {name}, 字段数: {len(df.columns)}")
            else:
                print(f"代码格式: {sym:15s} -> 无数据")
        except Exception as e:
            print(f"代码格式: {sym:15s} -> 获取失败: {e}")


# ============================================================
# 示例 5: 筛选特定信息
# ============================================================
def example_filter_info():
    """演示如何筛选感兴趣的字段"""
    print("\n" + "=" * 60)
    print("示例 5: 筛选特定信息")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "600036"]
    interested_fields = ["symbol", "name", "industry", "list_date"]

    for code in stocks:
        try:
            df = service.get_basic_info(symbol=code)
            if df is None or df.empty:
                continue

            print(f"\n股票代码: {code}")
            # 只显示感兴趣的字段
            available_fields = [f for f in interested_fields if f in df.columns]
            if available_fields:
                print(df[available_fields].to_string(index=False))
            else:
                print("  无匹配字段")

        except Exception as e:
            print(f"{code}: 获取失败 - {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效代码、网络异常等"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试 1: 正常获取
    print("\n测试 1: 正常获取")
    try:
        df = service.get_basic_info(symbol="600519")
        if df is None or df.empty:
            print("  结果: 无数据 (空 DataFrame)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据, {len(df.columns)} 个字段")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 2: 无效代码
    print("\n测试 2: 无效代码")
    try:
        df = service.get_basic_info(symbol="999999")
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
    example_filter_info()
    example_error_handling()
