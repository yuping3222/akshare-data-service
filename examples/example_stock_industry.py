"""
get_stock_industry() 接口示例

演示如何使用 akshare_data.get_stock_industry() 获取股票所属行业信息。

接口说明:
  - get_stock_industry(symbol) -> pd.DataFrame
  - 底层使用 stock_board_industry_cons_em，返回行业板块成份股列表
  - 注意: 当前 akshare 接口不区分个股 symbol，返回全市场行业数据

返回字段 (可能包含):
  - 行业板块名称、代码、成份股列表等
  - 具体字段以实际返回为准
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票行业信息
# ============================================================
def example_basic():
    """基本用法: 获取行业板块数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取行业板块数据")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: 底层接口返回行业板块数据，不区分个股
        # symbol 参数当前被忽略
        df = service.get_stock_industry(symbol="")

        if df is None or df.empty:
            print("无数据（数据源不可用）")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n行业板块数据 (前10行):")
        print(df.head(10))

    except Exception as e:
        print(f"获取行业信息失败: {e}")


# ============================================================
# 示例 2: 对比多只股票的行业分布
# ============================================================
def example_compare_stocks():
    """查看行业板块数据"""
    print("\n" + "=" * 60)
    print("示例 2: 行业板块数据概览")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_stock_industry(symbol="")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n行业板块数据 (前10行):")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取行业信息失败: {e}")


# ============================================================
# 示例 3: 获取深市股票行业信息
# ============================================================
def example_sz_stock():
    """获取行业板块数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取行业板块数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_stock_industry(symbol="")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n行业板块数据 (前10行):")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 使用不同代码格式
# ============================================================
def example_symbol_formats():
    """演示获取行业数据"""
    print("\n" + "=" * 60)
    print("示例 4: 行业数据统计")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_stock_industry(symbol="")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 条行业数据")
        print(f"字段列表: {list(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())
    except Exception as e:
        print(f"获取失败: {e}")


# ============================================================
# 示例 5: 按行业分组统计
# ============================================================
def example_industry_grouping():
    """演示查看行业数据详情"""
    print("\n" + "=" * 60)
    print("示例 5: 行业数据详情")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_stock_industry(symbol="")
        if df is None or df.empty:
            print("无数据")
            return

        print("\n行业数据详情 (前20行):")
        print(df.head(20).to_string(index=False))
    except Exception as e:
        print(f"获取失败: {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试 1: 正常获取
    print("\n测试 1: 正常获取")
    try:
        df = service.get_stock_industry(symbol="")
        if df is None or df.empty:
            print("  结果: 无数据 (空 DataFrame)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 2: 重复调用 (验证缓存)
    print("\n测试 2: 重复调用")
    try:
        df = service.get_stock_industry(symbol="")
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
    example_industry_grouping()
    example_error_handling()
