"""
get_option_daily() 接口示例

演示如何使用 akshare_data.DataService.get_option_daily() 获取期权日线行情数据。

接口说明:
  - symbol: 期权合约代码

返回字段: symbol, date, open, high, low, close, volume, amount 等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_option_daily(symbol="10000001")

注意: 期权代码需要先通过 get_option_list() 获取。
      底层 option_sse_daily_sina 仅支持上交所期权。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单个期权合约日线数据
# ============================================================
def example_option_daily_basic():
    """基本用法: 获取单个期权合约的日线行情数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取期权日线数据")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 期权合约代码
        # 注意: 期权代码需要先通过 get_option_list() 获取
        df = service.get_option_daily(symbol="10000001")

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
        print(f"获取期权日线数据失败: {e}")
        _show_mock_option_daily()


def _show_mock_option_daily():
    """展示期权日线数据的期望输出格式"""
    print("\n  --- 期望输出格式示例 ---")
    data = {
        "date": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
        "open": [0.1523, 0.1530, 0.1515, 0.1540, 0.1555],
        "high": [0.1550, 0.1560, 0.1545, 0.1570, 0.1580],
        "low": [0.1500, 0.1510, 0.1495, 0.1520, 0.1535],
        "close": [0.1530, 0.1515, 0.1540, 0.1555, 0.1565],
        "volume": [1200, 1500, 980, 2100, 1800],
        "amount": [184000, 228000, 149000, 325000, 282000],
    }
    df = pd.DataFrame(data)
    print(df.to_string(index=False))


# ============================================================
# 示例 2: 获取多个期权合约的日线数据
# ============================================================
def example_option_daily_multiple():
    """获取多个期权合约的日线数据进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 多个期权合约日线数据对比")
    print("=" * 60)

    service = get_service()

    # 示例期权合约代码列表
    symbols = [
        "10000001",
        "10000002",
        "10000003",
    ]

    for symbol in symbols:
        try:
            df = service.get_option_daily(symbol=symbol)
            if not df.empty:
                print(f"\n期权合约 {symbol}:")
                print(f"  数据行数: {len(df)}")
                if "close" in df.columns:
                    print(f"  最新收盘价: {df['close'].iloc[-1]:.4f}")
                if "volume" in df.columns:
                    print(f"  最新成交量: {df['volume'].iloc[-1]}")
            else:
                print(f"\n期权合约 {symbol}: 无数据")
        except Exception as e:
            print(f"\n期权合约 {symbol}: 获取失败 - {e}")


# ============================================================
# 示例 3: 结合 get_option_list 获取合约并查询日线
# ============================================================
def example_option_daily_with_list():
    """先获取期权列表，再查询前几个合约的日线数据"""
    print("\n" + "=" * 60)
    print("示例 3: 结合期权列表获取日线数据")
    print("=" * 60)

    service = get_service()

    try:
        # 第一步: 获取期权合约列表
        option_list = service.get_option_list()

        if option_list.empty:
            print("期权列表为空，无法查询日线数据")
            return

        print(f"获取到 {len(option_list)} 个期权合约")

        # 找到合约代码列
        symbol_col = None
        for col in ["symbol", "合约代码", "code", "期权代码"]:
            if col in option_list.columns:
                symbol_col = col
                break

        if symbol_col is None:
            print(f"未找到合约代码列，可用列: {list(option_list.columns)}")
            return

        # 第二步: 取前3个合约查询日线数据
        symbols_to_query = option_list[symbol_col].head(3).tolist()
        print(f"\n查询前3个合约的日线数据: {symbols_to_query}")

        for sym in symbols_to_query:
            try:
                df = service.get_option_daily(symbol=str(sym))
                if not df.empty:
                    print(f"\n合约 {sym}:")
                    print(f"  数据行数: {len(df)}")
                    if "close" in df.columns:
                        print(f"  收盘价范围: {df['close'].min():.4f} ~ {df['close'].max():.4f}")
                else:
                    print(f"\n合约 {sym}: 无日线数据")
            except Exception as e:
                print(f"\n合约 {sym}: 获取失败 - {e}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 期权数据分析 - 计算波动率指标
# ============================================================
def example_option_daily_analysis():
    """获取期权日线数据后计算简单技术指标"""
    print("\n" + "=" * 60)
    print("示例 4: 期权数据分析 - 计算价格统计指标")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_option_daily(symbol="10000001")

        if df.empty:
            print("无数据")
            return

        print(f"期权合约 10000001 日线数据统计")
        print(f"数据形状: {df.shape}")

        # 基本统计
        if "close" in df.columns:
            print(f"\n收盘价统计:")
            print(f"  最高价: {df['high'].max():.4f}")
            print(f"  最低价: {df['low'].min():.4f}")
            print(f"  平均收盘价: {df['close'].mean():.4f}")
            print(f"  收盘价标准差: {df['close'].std():.4f}")

        if "volume" in df.columns:
            print(f"\n成交量统计:")
            print(f"  最大成交量: {df['volume'].max()}")
            print(f"  平均成交量: {df['volume'].mean():.0f}")

        # 计算日收益率
        if "close" in df.columns and len(df) > 1:
            df["daily_return"] = df["close"].pct_change() * 100
            print(f"\n日收益率统计:")
            print(f"  最大涨幅: {df['daily_return'].max():.2f}%")
            print(f"  最大跌幅: {df['daily_return'].min():.2f}%")
            print(f"  平均日收益率: {df['daily_return'].mean():.2f}%")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 期权数据分析
# ============================================================
def example_option_daily_cross_market():
    """期权日线数据分析"""
    print("\n" + "=" * 60)
    print("示例 5: 期权日线数据分析")
    print("=" * 60)

    service = get_service()

    try:
        # 获取上交所期权列表（底层 option_current_day_sse 仅支持上交所）
        option_list = service.get_option_list()

        if option_list.empty:
            print("无期权合约")
            return

        # 找到合约代码列
        symbol_col = None
        for col in ["symbol", "合约代码", "code", "期权代码"]:
            if col in option_list.columns:
                symbol_col = col
                break

        if symbol_col is None:
            print(f"未找到合约代码列，可用列: {list(option_list.columns)}")
            return

        # 取第一个合约查询日线
        first_symbol = option_list[symbol_col].iloc[0]
        df = service.get_option_daily(symbol=str(first_symbol))

        if not df.empty:
            print(f"示例合约 {first_symbol}:")
            print(f"  数据行数: {len(df)}")
            if "close" in df.columns:
                print(f"  最新收盘价: {df['close'].iloc[-1]:.4f}")
        else:
            print("无日线数据")

    except Exception as e:
        print(f"获取失败: {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_option_daily_error_handling():
    """演示错误处理 - 无效合约代码等情况"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效合约代码
    print("\n测试 1: 无效合约代码")
    try:
        df = service.get_option_daily(symbol="INVALID")
        if df.empty:
            print("  结果: 返回空 DataFrame")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试正常调用
    print("\n测试 2: 正常调用")
    try:
        df = service.get_option_daily(symbol="10000001")
        print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_option_daily_basic()
    example_option_daily_multiple()
    example_option_daily_with_list()
    example_option_daily_analysis()
    example_option_daily_cross_market()
    example_option_daily_error_handling()
