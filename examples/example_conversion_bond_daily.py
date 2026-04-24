"""
get_conversion_bond_daily() 接口示例

演示如何使用 DataService 获取可转债历史日线数据。

接口说明:
- get_conversion_bond_daily(symbol) - 获取可转债日线数据
  - symbol: 可转债代码 (必需) - 如 "127045" 或 "sh127045"
  - 返回: DataFrame - 包含指定可转债的历史日线数据

注意:
- 该接口使用缓存机制，相同参数的请求会优先从缓存获取
- 底层 bond_zh_cov_daily 仅接受 symbol 参数，不支持日期过滤
- 日线数据包含 OHLCV (开高低收量)

典型返回字段:
- date: 交易日期
- open: 开盘价
- high: 最高价
- low: 最低价
- close: 收盘价
- volume: 成交量
- amount: 成交额
"""

import pandas as pd
from akshare_data import get_service
from _example_utils import (
    fetch_with_retry,
    normalize_symbol_input,
    print_df_brief,
    stable_df,
)


def _mock_daily_data(symbol="127045"):
    """返回模拟的可转债日线数据用于演示"""
    dates = pd.date_range(start="2024-01-01", end="2024-03-31", freq="B")
    n = len(dates)

    # 生成模拟数据
    base_price = 120.0
    prices = base_price + (pd.Series(range(n)) * 0.1) + (pd.Series(range(n)).apply(lambda x: (x % 5 - 2) * 0.5))

    return pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "open": prices - 0.2,
        "high": prices + 0.5,
        "low": prices - 0.5,
        "close": prices,
        "volume": [50000 + (i % 10) * 5000 for i in range(n)],
        "amount": [6000000 + (i % 10) * 600000 for i in range(n)],
    })


# ============================================================
# 示例 1: 基本用法 - 获取单只可转债日线数据
# ============================================================
def example_basic():
    """基本用法: 获取单只可转债的历史日线数据"""
    print("=" * 60)
    print("示例 1: 获取可转债日线数据")
    print("=" * 60)

    service = get_service()

    try:
        # 获取牧原转债的日线数据
        # 注意: 该接口仅接受 symbol 参数，返回全部历史数据
        symbol = normalize_symbol_input("127045")

        print(f"\n查询参数:")
        print(f"  代码: {symbol}")

        df = fetch_with_retry(lambda: service.get_conversion_bond_daily(symbol=symbol), retries=2)

        if df is None or df.empty:
            print("\n[数据源不可用，使用演示数据]")
            df = _mock_daily_data(symbol)

        print_df_brief(stable_df(df), rows=5)

    except Exception as e:
        print(f"\n获取数据失败: {e}")
        print("使用演示数据:")
        df = _mock_daily_data()
        print(df.head(5).to_string(index=False))


# ============================================================
# 示例 2: 不同代码格式测试
# ============================================================
def example_symbol_formats():
    """测试不同可转债代码格式"""
    print("\n" + "=" * 60)
    print("示例 2: 不同代码格式测试")
    print("=" * 60)

    service = get_service()

    # 测试不同的代码格式
    symbols = [
        ("127045", "纯数字"),
        ("sh127045", "带交易所前缀"),
    ]

    for symbol, desc in symbols:
        try:
            print(f"\n测试代码格式: {symbol} ({desc})")
            df = fetch_with_retry(
                lambda: service.get_conversion_bond_daily(symbol=normalize_symbol_input(symbol)),
                retries=2,
            )

            if df.empty:
                print(f"  结果: 无数据")
            else:
                print(f"  结果: 获取到 {len(df)} 条记录")
                if "date" in df.columns:
                    print(f"  日期范围: {df['date'].iloc[0]} 至 {df['date'].iloc[-1]}")

        except Exception as e:
            print(f"  错误: {e}")


# ============================================================
# 示例 3: 计算技术指标
# ============================================================
def example_technical_analysis():
    """基于日线数据计算简单技术指标"""
    print("\n" + "=" * 60)
    print("示例 3: 技术指标分析")
    print("=" * 60)

    service = get_service()

    try:
        df = fetch_with_retry(
            lambda: service.get_conversion_bond_daily(symbol=normalize_symbol_input("127045")),
            retries=2,
        )

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_daily_data()

        # 确保数据类型正确
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        print(f"\n基础统计:")
        print(f"  交易日数: {len(df)}")
        print(f"  价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
        print(f"  平均收盘价: {df['close'].mean():.2f}")

        # 计算移动平均线
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()

        print(f"\n最新5日均线: {df['ma5'].iloc[-1]:.2f}")
        print(f"最新20日均线: {df['ma20'].iloc[-1]:.2f}")

        # 计算波动率
        df["daily_return"] = df["close"].pct_change()
        volatility = df["daily_return"].std() * (252 ** 0.5) * 100
        print(f"年化波动率: {volatility:.2f}%")

        # 显示最新10天数据
        print(f"\n最新10个交易日:")
        display_cols = ["date", "close", "ma5", "ma20", "volume"]
        available_cols = [c for c in display_cols if c in df.columns]
        print(df[available_cols].tail(10).to_string(index=False))

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 4: 对比多只可转债走势
# ============================================================
def example_compare_bonds():
    """对比多只可转债的价格走势"""
    print("\n" + "=" * 60)
    print("示例 4: 多只可转债对比")
    print("=" * 60)

    service = get_service()

    symbols = [
        ("127045", "牧原转债"),
        ("110059", "南航转债"),
        ("113050", "南银转债"),
    ]

    print(f"\n对比 {len(symbols)} 只可转债:")

    results = []
    for symbol, name in symbols:
        try:
            df = fetch_with_retry(
                lambda: service.get_conversion_bond_daily(symbol=normalize_symbol_input(symbol)),
                retries=2,
            )

            if df.empty:
                print(f"  {symbol} ({name}): 无数据")
                continue

            # 转换为数值
            df["close"] = pd.to_numeric(df["close"], errors="coerce")

            start_price = df["close"].iloc[0]
            end_price = df["close"].iloc[-1]
            change_pct = (end_price / start_price - 1) * 100
            max_price = df["close"].max()
            min_price = df["close"].min()

            results.append({
                "code": symbol,
                "name": name,
                "start_price": start_price,
                "end_price": end_price,
                "change_pct": change_pct,
                "max_price": max_price,
                "min_price": min_price,
                "trading_days": len(df),
            })

        except Exception as e:
            print(f"  {symbol} ({name}): 获取失败 - {e}")

    if results:
        results_df = pd.DataFrame(results)
        print("\n对比结果:")
        print(results_df.to_string(index=False))

        # 排序
        print("\n按涨跌幅排序:")
        sorted_df = results_df.sort_values("change_pct", ascending=False)
        print(sorted_df[["code", "name", "change_pct"]].to_string(index=False))


# ============================================================
# 示例 5: 成交量分析
# ============================================================
def example_volume_analysis():
    """分析可转债成交量变化"""
    print("\n" + "=" * 60)
    print("示例 5: 成交量分析")
    print("=" * 60)

    service = get_service()

    try:
        df = fetch_with_retry(
            lambda: service.get_conversion_bond_daily(symbol=normalize_symbol_input("127045")),
            retries=2,
        )

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_daily_data()

        # 确保成交量为数值
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

            print(f"\n成交量统计:")
            print(f"  平均成交量: {df['volume'].mean():.0f}")
            print(f"  最大成交量: {df['volume'].max():.0f}")
            print(f"  最小成交量: {df['volume'].min():.0f}")

            # 计算成交量移动平均
            df["volume_ma5"] = df["volume"].rolling(window=5).mean()

            # 找出放量交易日 (成交量 > 5日均量 * 1.5)
            high_volume_days = df[df["volume"] > df["volume_ma5"] * 1.5]
            print(f"\n放量交易日 (>5日均量1.5倍): {len(high_volume_days)} 天")

            if not high_volume_days.empty:
                print("\n前5个放量交易日:")
                display_cols = ["date", "close", "volume", "volume_ma5"]
                available_cols = [c for c in display_cols if c in high_volume_days.columns]
                print(high_volume_days[available_cols].head(5).to_string(index=False))
        else:
            print("数据中无成交量字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效代码等情况"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效代码
    print("\n测试 1: 无效可转债代码")
    try:
        df = fetch_with_retry(
            lambda: service.get_conversion_bond_daily(symbol=normalize_symbol_input("INVALID")),
            retries=1,
        )
        if df.empty:
            print("  结果: 返回空 DataFrame")
        else:
            print(f"  结果: 获取到 {len(df)} 条记录")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试正常调用
    print("\n测试 2: 正常调用")
    try:
        df = fetch_with_retry(
            lambda: service.get_conversion_bond_daily(symbol=normalize_symbol_input("127045")),
            retries=2,
        )
        print(f"  结果: 获取到 {len(df)} 条记录")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_symbol_formats()
    example_technical_analysis()
    example_compare_bonds()
    example_volume_analysis()
    example_error_handling()
