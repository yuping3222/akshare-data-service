"""
最新股东户数接口示例 (get_latest_holder_number)

演示如何获取股票的最新股东户数数据，反映股票的筹码集中度。

接口说明:
- get_latest_holder_number(symbol): 获取指定股票的股东户数信息

参数:
  symbol: 股票代码，支持多种格式 (如 "600519", "000001", "sh600519")

返回: DataFrame，包含报告日期、股东户数、户均持股等字段

注意: 该接口当前未在 akshare 数据源的接口配置中定义。
      使用 akshare 数据源时调用会报错。
      可通过 source="lixinger" 强制使用 lixinger 数据源（需配置 LIXINGER_TOKEN）。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的股东户数
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的股东户数"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台(600519)股东户数")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 证券代码
        df = service.get_latest_holder_number(symbol="600519")

        if df is None or df.empty:
            print("无数据（数据源不可用或未配置 Lixinger token）")
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
        print("提示: 该接口需要配置 Lixinger token，或指定 source='lixinger'")


# ============================================================
# 示例 2: 指定数据源为 lixinger
# ============================================================
def example_with_source():
    """显式指定使用 lixinger 数据源"""
    print("\n" + "=" * 60)
    print("示例 2: 指定 lixinger 数据源")
    print("=" * 60)

    service = get_service()

    try:
        # 通过 source 参数强制指定 lixinger 数据源
        df = service.get_latest_holder_number(
            symbol="600519",
            source="lixinger",
        )

        if df is None or df.empty:
            print("无数据（请检查 Lixinger token 是否配置）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n数据预览:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("提示: 请确保已配置 LIXINGER_TOKEN 环境变量或 token.cfg 文件")


# ============================================================
# 示例 3: 获取深市股票的股东户数
# ============================================================
def example_sz_stock():
    """获取深市股票的股东户数"""
    print("\n" + "=" * 60)
    print("示例 3: 获取平安银行(000001)股东户数")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_latest_holder_number(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n数据预览:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 批量获取多只股票
# ============================================================
def example_multiple_stocks():
    """批量获取多只股票的股东户数"""
    print("\n" + "=" * 60)
    print("示例 4: 批量获取多只股票股东户数")
    print("=" * 60)

    service = get_service()

    symbols = ["600519", "000001", "601318"]

    for symbol in symbols:
        try:
            df = service.get_latest_holder_number(symbol=symbol)
            if df is None or df.empty:
                print(f"\n{symbol}: 无数据")
            else:
                print(f"\n{symbol}: 共 {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"\n{symbol}: 获取失败 - {e}")


# ============================================================
# 示例 5: 数据分析 - 股东户数趋势
# ============================================================
def example_analysis():
    """演示获取数据后进行股东户数趋势分析"""
    print("\n" + "=" * 60)
    print("示例 5: 数据分析 - 股东户数趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_latest_holder_number(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"贵州茅台股东户数数据 ({len(df)}条)")
        print(f"数据形状: {df.shape}")

        # 打印基本统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        print("\n完整数据:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_with_source()
    example_sz_stock()
    example_multiple_stocks()
    example_analysis()
