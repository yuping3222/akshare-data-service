"""
实时行情接口示例 (get_realtime_data)

演示如何获取股票的实时行情数据。该接口不缓存数据，每次调用都会
从数据源获取最新的实时行情。

注意: 实时行情仅在交易时段内有意义，非交易时段可能返回空数据或最新快照。
"""

from akshare_data import get_service


def example_basic_usage():
    """基本用法: 获取全市场实时行情并筛选个股"""
    print("=" * 60)
    print("示例1: 获取全市场实时行情并筛选贵州茅台(600519)")
    print("=" * 60)

    try:
        # stock_zh_a_spot_em 不接受任何参数，返回全 A 股市场行情。
        # 先获取全市场数据，再按代码筛选目标个股。
        service = get_service()
        df = service.akshare.get_spot_em()

        if df is None or df.empty:
            print("无数据（数据源不可用或非交易时段）")
            return

        print(f"全市场数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")

        # 筛选目标股票（代码字段可能是 "代码"/"code"/"symbol"）
        code_col = None
        for col in ["代码", "code", "symbol"]:
            if col in df.columns:
                code_col = col
                break

        if code_col:
            target = df[df[code_col] == "600519"]
            if not target.empty:
                print("\n贵州茅台(600519) 实时行情:")
                print(target)
            else:
                print("\n未找到 600519 的数据")
        else:
            print("\n未找到代码列，返回全市场数据:")
            print(df)

    except Exception as e:
        print(f"获取实时行情失败: {e}")


def example_multiple_stocks():
    """批量获取多只股票的实时行情"""
    print("\n" + "=" * 60)
    print("示例2: 批量获取多只股票的实时行情")
    print("=" * 60)

    symbols = [
        "600519",  # 贵州茅台
        "000001",  # 平安银行
        "300750",  # 宁德时代
        "601318",  # 中国平安
    ]

    try:
        service = get_service()
        df = service.akshare.get_spot_em()

        if df is None or df.empty:
            print("全市场行情为空")
            return

        code_col = None
        for col in ["代码", "code", "symbol"]:
            if col in df.columns:
                code_col = col
                break

        if not code_col:
            print("未找到代码列")
            return

        for symbol in symbols:
            row = df[df[code_col] == symbol]
            if row.empty:
                print(f"  {symbol}: 无数据")
            else:
                print(f"  {symbol}:")
                print(row.to_string(index=False))
    except Exception as e:
        print(f"  获取批量行情失败: {e}")


def example_index_realtime():
    """获取指数的实时行情"""
    print("\n" + "=" * 60)
    print("示例3: 获取主要指数的实时行情")
    print("=" * 60)

    indices = [
        "000001",  # 上证指数
        "399001",  # 深证成指
        "399006",  # 创业板指
    ]

    try:
        service = get_service()
        df = service.akshare.get_spot_em()
        if df is None or df.empty:
            print("全市场行情为空")
            return

        code_col = None
        for col in ["代码", "code", "symbol"]:
            if col in df.columns:
                code_col = col
                break

        if not code_col:
            print("未找到代码列")
            return

        for index_code in indices:
            row = df[df[code_col] == index_code]
            if row.empty:
                print(f"  {index_code}: 无数据")
            else:
                print(f"  {index_code}:")
                print(row.to_string(index=False))
    except Exception as e:
        print(f"  获取指数行情失败: {e}")


def example_etf_realtime():
    """获取ETF的实时行情"""
    print("\n" + "=" * 60)
    print("示例4: 获取主要ETF的实时行情")
    print("=" * 60)

    etfs = [
        "510300",  # 沪深300ETF
        "510500",  # 中证500ETF
        "159919",  # 沪深300ETF(深市)
    ]

    try:
        service = get_service()
        df = service.akshare.get_spot_em()
        if df is None or df.empty:
            print("全市场行情为空")
            return

        code_col = None
        for col in ["代码", "code", "symbol"]:
            if col in df.columns:
                code_col = col
                break

        if not code_col:
            print("未找到代码列")
            return

        for etf_code in etfs:
            row = df[df[code_col] == etf_code]
            if row.empty:
                print(f"  {etf_code}: 无数据")
            else:
                print(f"  {etf_code}:")
                print(row.to_string(index=False))
    except Exception as e:
        print(f"  获取ETF行情失败: {e}")


def example_monitor_realtime():
    """监控实时行情: 检查涨跌幅"""
    print("\n" + "=" * 60)
    print("示例5: 监控实时行情并计算涨跌幅")
    print("=" * 60)

    try:
        service = get_service()
        df = service.akshare.get_spot_em()

        if df is None or df.empty:
            print("无数据（数据源不可用或非交易时段）")
            return

        print(f"数据形状: {df.shape}")
        print("\n完整数据:")
        print(df.to_string(index=False))

        # 检查常见行情字段
        common_fields = ["涨跌幅", "成交额", "换手率", "量比"]
        available_fields = [f for f in common_fields if f in df.columns]

        if available_fields:
            print(f"\n可用字段: {available_fields}")
    except Exception as e:
        print(f"获取实时行情失败: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_multiple_stocks()
    example_index_realtime()
    example_etf_realtime()
    example_monitor_realtime()
