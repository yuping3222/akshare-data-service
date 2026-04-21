"""
get_dividend_by_date() 接口示例

演示如何使用 akshare_data.get_dividend_by_date() 获取指定日期的分红数据。

接口说明:
- 获取指定日期的全市场分红送股数据
- date 参数可选，不指定时返回最新数据
- 返回字段包含: 股票代码、名称、分红方案、股权登记日、除权除息日等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_dividend_by_date(date="2024-06-01")
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定日期的分红数据
# ============================================================
def example_basic():
    """基本用法: 获取指定日期的分红数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取指定日期的分红数据")
    print("=" * 60)

    service = get_service()

    try:
        # date: 日期参数，格式 "YYYY-MM-DD"
        # 获取指定日期的全市场分红数据
        df = service.get_dividend_by_date(date="2024-06-01")

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("\n无数据")
        else:
            print(f"数据形状: {df.shape}")
            print(f"字段列表: {list(df.columns)}")
            print("\n前5行数据:")
            print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不指定日期 - 获取最新分红数据
# ============================================================
def example_latest():
    """不指定日期，获取最新分红数据"""
    print("\n" + "=" * 60)
    print("示例 2: 不指定日期 - 获取最新分红数据")
    print("=" * 60)

    service = get_service()

    try:
        # 不传入 date 参数，获取最新数据
        df = service.get_dividend_by_date()

        if df is not None and not df.empty:
            print(f"数据形状: {df.shape}")
            print(f"字段列表: {list(df.columns)}")
            print("\n前5行数据:")
            print(df.head())
        else:
            print("无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 筛选特定股票的分红数据
# ============================================================
def example_filter_by_symbol():
    """获取数据后筛选特定股票"""
    print("\n" + "=" * 60)
    print("示例 3: 筛选特定股票的分红数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_dividend_by_date(date="2024-06-01")

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        # 假设字段名为 "股票代码" 或 "symbol"
        symbol_col = None
        for col in df.columns:
            if "代码" in col or col.lower() == "symbol":
                symbol_col = col
                break

        if symbol_col:
            # 筛选平安银行
            filtered = df[df[symbol_col].astype(str).str.contains("000001", na=False)]
            if not filtered.empty:
                print(f"平安银行分红数据:")
                print(filtered.to_string(index=False))
            else:
                print("未找到平安银行的分红数据")
        else:
            print(f"字段列表: {list(df.columns)}")
            print("无法识别股票代码字段")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 统计分红信息
# ============================================================
def example_statistics():
    """统计分红数据的基本信息"""
    print("\n" + "=" * 60)
    print("示例 4: 统计分红信息")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_dividend_by_date(date="2024-06-01")

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        print(f"总分红记录数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 打印基本统计
        print("\n数据预览:")
        print(df.head(10).to_string(index=False))

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
        # 测试无效日期格式
        print("\n测试 1: 无效日期格式")
        df = service.get_dividend_by_date(date="invalid-date")
        if df is None:
            print(f"  结果: None (数据不可用)")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        # 测试正常调用
        print("\n测试 2: 正常调用")
        df = service.get_dividend_by_date(date="2024-06-01")
        if df is None:
            print(f"  结果: None (数据不可用)")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_latest()
    example_filter_by_symbol()
    example_statistics()
    example_error_handling()
