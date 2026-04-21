"""
get_futures_daily() 接口示例 (DataService 高层接口)

演示如何使用 akshare_data.DataService.get_futures_daily() 获取期货日线行情数据。

这是 DataService 提供的高层统一接口，与直接使用 service.akshare.get_futures_daily()
不同，该接口具有缓存优先策略:
  1. 先检查本地缓存
  2. 缓存未命中则从数据源获取
  3. 将结果写入缓存供后续使用

接口说明:
  - symbol: 期货合约代码，如 "RB0" (螺纹钢主力连续), "CU0" (沪铜主力连续)

返回字段: symbol, date, open, high, low, close, volume, amount, open_interest 等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_futures_daily(symbol="RB0")

注意: 底层 futures_zh_daily_sina 接口仅接受 symbol 参数，
      不支持 start_date/end_date。DataService 层在获取全量数据后
      可以在内存中进行日期过滤。
本文件是对 example_futures.py 的补充，演示 DataService 高层接口的使用方式。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取期货日线数据 (带缓存)
# ============================================================
def example_futures_daily_basic():
    """基本用法: 获取单个期货合约的日线数据 (缓存优先)"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取螺纹钢主力连续日线数据")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 期货合约代码，如 "RB0" (螺纹钢主力连续)
        # 注意: 底层接口仅接受 symbol 参数，返回全部历史数据
        df = service.get_futures_daily(symbol="RB0")

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
        print(f"获取期货日线数据失败: {e}")
        _show_mock_futures_daily()


def _show_mock_futures_daily():
    """展示期货日线数据的期望输出格式"""
    print("\n  --- 期望输出格式示例 ---")
    data = {
        "date": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
        "open": [3850.0, 3860.0, 3880.0, 3870.0, 3890.0],
        "high": [3880.0, 3890.0, 3900.0, 3890.0, 3920.0],
        "low": [3840.0, 3850.0, 3870.0, 3860.0, 3880.0],
        "close": [3860.0, 3880.0, 3870.0, 3890.0, 3910.0],
        "volume": [150000, 160000, 145000, 155000, 170000],
        "open_interest": [200000, 205000, 210000, 208000, 215000],
    }
    df = pd.DataFrame(data)
    print(df.to_string(index=False))


# ============================================================
# 示例 2: 不同品种期货日线数据对比
# ============================================================
def example_futures_daily_varieties():
    """获取不同品种期货的日线数据进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 不同品种期货日线数据对比")
    print("=" * 60)

    service = get_service()

    # 期货合约列表: (合约代码, 品种名称)
    contracts = [
        ("RB0", "螺纹钢"),
        ("HC0", "热卷"),
        ("I0", "铁矿石"),
        ("J0", "焦炭"),
        ("CU0", "沪铜"),
        ("AU0", "沪金"),
    ]

    for symbol, name in contracts:
        try:
            df = service.get_futures_daily(symbol=symbol)

            if not df.empty:
                print(f"\n{name} ({symbol}):")
                print(f"  数据行数: {len(df)}")
                if "close" in df.columns:
                    print(
                        f"  收盘价范围: {df['close'].min():.2f} ~ {df['close'].max():.2f}"
                    )
                if "volume" in df.columns:
                    print(f"  总成交量: {df['volume'].sum():,.0f}")
            else:
                print(f"\n{name} ({symbol}) - 无数据")
        except Exception as e:
            print(f"\n{name} ({symbol}) - 获取失败: {e}")


# ============================================================
# 示例 3: 数据分析 - 价格统计
# ============================================================
def example_futures_daily_analysis():
    """获取期货日线数据后进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 3: 期货日线数据统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_futures_daily(symbol="RB0")

        if df.empty:
            print("无数据")
            return

        print(f"螺纹钢主力连续日线数据统计")
        print(f"数据形状: {df.shape}")
        print(f"交易日数: {len(df)}")

        if "date" in df.columns:
            print(f"\n日期范围:")
            print(f"  起始日期: {df['date'].iloc[0]}")
            print(f"  结束日期: {df['date'].iloc[-1]}")

        if "close" in df.columns:
            print(f"\n价格统计:")
            print(f"  最高价: {df['high'].max():.2f}")
            print(f"  最低价: {df['low'].min():.2f}")
            print(f"  最新收盘价: {df.iloc[-1]['close']:.2f}")
            print(f"  平均收盘价: {df['close'].mean():.2f}")

            if len(df) > 1:
                change_pct = (
                    (df.iloc[-1]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"] * 100
                )
                print(f"  区间涨跌幅: {change_pct:.2f}%")

        if "volume" in df.columns:
            print(f"\n成交量统计:")
            print(f"  平均日成交量: {df['volume'].mean():,.0f}")
            print(f"  最大日成交量: {df['volume'].max():,.0f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 缓存效果演示 - 重复查询同一合约
# ============================================================
def example_futures_daily_cache():
    """演示缓存效果: 首次查询后缓存命中"""
    print("\n" + "=" * 60)
    print("示例 4: 缓存效果演示")
    print("=" * 60)

    service = get_service()
    symbol = "RB0"

    try:
        import time

        # 第一次查询
        print("\n第一次查询:")
        t1 = time.time()
        df1 = service.get_futures_daily(symbol=symbol)
        t2 = time.time()
        print(f"  耗时: {(t2 - t1) * 1000:.2f} ms")
        print(f"  数据行数: {len(df1)}")

        # 第二次查询 (应该从缓存读取)
        print("\n第二次查询 (缓存命中):")
        t3 = time.time()
        df2 = service.get_futures_daily(symbol=symbol)
        t4 = time.time()
        print(f"  耗时: {(t4 - t3) * 1000:.2f} ms")
        print(f"  数据行数: {len(df2)}")

        # 验证两次数据一致
        if not df1.empty and not df2.empty:
            print(f"\n数据一致性检查: {'通过' if df1.equals(df2) else '不一致'}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 技术分析 - 计算移动平均线
# ============================================================
def example_futures_daily_technical_analysis():
    """获取期货日线数据后计算技术指标"""
    print("\n" + "=" * 60)
    print("示例 5: 期货日线数据技术分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_futures_daily(symbol="RB0")

        if df.empty:
            print("无数据")
            return

        print(f"螺纹钢主力连续技术分析")
        print(f"数据形状: {df.shape}")

        if "close" in df.columns:
            # 计算5日、10日、20日移动平均线
            df["ma5"] = df["close"].rolling(window=5).mean()
            df["ma10"] = df["close"].rolling(window=10).mean()
            df["ma20"] = df["close"].rolling(window=20).mean()

            print("\n最新10个交易日数据 (含均线):")
            display_cols = ["date", "close", "ma5", "ma10", "ma20", "volume"]
            available_cols = [col for col in display_cols if col in df.columns]
            print(df[available_cols].tail(10).to_string(index=False))

            # 均线趋势判断
            latest = df.iloc[-1]
            if pd.notna(latest.get("ma5")) and pd.notna(latest.get("ma20")):
                trend = "多头" if latest["ma5"] > latest["ma20"] else "空头"
                print(f"\n均线趋势判断: {trend} (MA5 vs MA20)")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 多合约数据汇总分析
# ============================================================
def example_futures_daily_portfolio():
    """获取多个期货合约数据，构建简单的组合分析"""
    print("\n" + "=" * 60)
    print("示例 6: 多合约组合分析")
    print("=" * 60)

    service = get_service()

    contracts = [
        ("RB0", "螺纹钢"),
        ("I0", "铁矿石"),
        ("J0", "焦炭"),
    ]

    results = []
    for symbol, name in contracts:
        try:
            df = service.get_futures_daily(symbol=symbol)

            if not df.empty and "close" in df.columns:
                start_price = df.iloc[0]["close"]
                end_price = df.iloc[-1]["close"]
                change_pct = (end_price - start_price) / start_price * 100
                max_price = df["high"].max()
                min_price = df["low"].min()
                avg_volume = df["volume"].mean() if "volume" in df.columns else 0

                results.append({
                    "品种": name,
                    "代码": symbol,
                    "期初价格": start_price,
                    "期末价格": end_price,
                    "涨跌幅%": change_pct,
                    "最高价": max_price,
                    "最低价": min_price,
                    "平均成交量": avg_volume,
                })
        except Exception as e:
            print(f"{name} ({symbol}): 获取失败 - {e}")

    if results:
        summary_df = pd.DataFrame(results)
        print("\n黑色系期货表现汇总:")
        print(summary_df.to_string(index=False))
    else:
        print("\n无可用数据")


# ============================================================
# 示例 7: 错误处理演示
# ============================================================
def example_futures_daily_error_handling():
    """演示错误处理 - 无效合约代码等情况"""
    print("\n" + "=" * 60)
    print("示例 7: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效合约代码
    print("\n测试 1: 无效合约代码")
    try:
        df = service.get_futures_daily(symbol="INVALID")
        if df.empty:
            print("  结果: 返回空 DataFrame")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试正常调用
    print("\n测试 2: 正常调用")
    try:
        df = service.get_futures_daily(symbol="RB0")
        print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_futures_daily_basic()
    example_futures_daily_varieties()
    example_futures_daily_analysis()
    example_futures_daily_cache()
    example_futures_daily_technical_analysis()
    example_futures_daily_portfolio()
    example_futures_daily_error_handling()
