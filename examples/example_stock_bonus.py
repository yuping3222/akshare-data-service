"""
get_stock_bonus() 接口示例

演示如何使用 akshare_data.get_stock_bonus() 获取个股分红送股数据。

接口说明:
- 获取指定股票的历史分红送股信息
- symbol: 股票代码（必填）
- 返回字段包含: 分红年度、报告期、分红方案、股权登记日、除权除息日等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_stock_bonus(symbol="000001")
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票分红数据
# ============================================================
def example_basic():
    """基本用法: 获取平安银行历史分红数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取平安银行分红数据")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 股票代码，支持多种格式
        df = service.get_stock_bonus(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 多只股票分红对比
# ============================================================
def example_compare():
    """对比多只股票的分红情况"""
    print("\n" + "=" * 60)
    print("示例 2: 多只股票分红对比")
    print("=" * 60)

    service = get_service()

    symbols = [
        ("000001", "平安银行"),
        ("600000", "浦发银行"),
        ("600519", "贵州茅台"),
    ]

    for symbol, name in symbols:
        try:
            df = service.get_stock_bonus(symbol=symbol)

            if df is None or df.empty:
                print(f"\n{name} ({symbol}): 无分红数据")
            else:
                print(f"\n{name} ({symbol}): {len(df)} 次分红")
                print(df.head(3).to_string(index=False))

        except Exception as e:
            print(f"\n{name} ({symbol}): 获取失败 - {e}")


# ============================================================
# 示例 3: 分红趋势分析
# ============================================================
def example_trend():
    """分析单只股票的分红趋势"""
    print("\n" + "=" * 60)
    print("示例 3: 分红趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_stock_bonus(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"贵州茅台历史分红: {len(df)} 次")
        print(f"字段列表: {list(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

        print("\n最新5次分红:")
        print(df.tail(5).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 高股息筛选
# ============================================================
def example_high_dividend():
    """演示如何筛选高股息股票"""
    print("\n" + "=" * 60)
    print("示例 4: 高股息筛选")
    print("=" * 60)

    service = get_service()

    symbols = ["000001", "600000", "601398", "601288"]

    for symbol in symbols:
        try:
            df = service.get_stock_bonus(symbol=symbol)

            if df is None or df.empty:
                continue

            # 查找金额相关字段
            amount_col = None
            for col in df.columns:
                if "派息" in col or "分红" in col or "金额" in col:
                    amount_col = col
                    break

            if amount_col:
                latest = df.iloc[0]
                print(f"\n{symbol}: 最新分红 {latest.get(amount_col, 'N/A')}")
            else:
                print(f"\n{symbol}: {len(df)} 次分红记录")
                print(df.head(1).to_string(index=False))

        except Exception as e:
            print(f"\n{symbol}: 获取失败 - {e}")


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
        df = service.get_stock_bonus(symbol="INVALID")
        print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 正常调用")
        df = service.get_stock_bonus(symbol="000001")
        print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare()
    example_trend()
    example_high_dividend()
    example_error_handling()
