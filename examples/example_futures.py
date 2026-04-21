"""
期货相关接口示例

演示如何使用 DataService.akshare 获取期货数据:
  - get_futures_daily: 期货日线数据 (仅支持 symbol 参数)
  - get_futures_spot: 期货实时行情数据
  - get_futures_main_contracts: 期货主力合约列表

使用方式:
  from akshare_data import get_service
  service = get_service()
  # 通过 service.akshare 访问 AkShareAdapter

常见期货合约代码 (主力连续):
  - rb0: 螺纹钢主力连续
  - hc0: 热卷主力连续
  - i0: 铁矿石主力连续
  - j0: 焦炭主力连续
  - cu0: 沪铜主力连续
  - au0: 沪金主力连续

注意: get_futures_daily 底层使用 futures_zh_daily_sina，
      该接口仅接受 symbol 参数，不支持 start_date/end_date。
      如需指定日期范围，请使用 DataService 层的 get_futures_daily()。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 获取期货日线数据 (基本用法)
# ============================================================
def example_futures_daily_basic():
    """基本用法: 获取单个期货合约的历史K线数据"""
    print("=" * 60)
    print("示例 1: 获取期货日线数据 - 螺纹钢主力连续 (rb0)")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 期货合约代码，如 "rb0" (螺纹钢主力连续)
        # 注意: 该接口仅接受 symbol 参数，不支持 start_date/end_date
        # 底层使用 futures_zh_daily_sina，返回该合约的全部历史日线数据
        df = service.akshare.get_futures_daily(symbol="rb0")

        if df is None or df.empty:
            print("无数据")
            return

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


# ============================================================
# 示例 2: 获取不同品种期货的日线数据
# ============================================================
def example_futures_daily_varieties():
    """演示获取不同品种期货的日线数据"""
    print("\n" + "=" * 60)
    print("示例 2: 不同品种期货的日线数据")
    print("=" * 60)

    service = get_service()

    # 期货合约列表: (合约代码, 品种名称)
    contracts = [
        ("rb0", "螺纹钢"),
        ("hc0", "热卷"),
        ("i0", "铁矿石"),
        ("j0", "焦炭"),
        ("cu0", "沪铜"),
        ("au0", "沪金"),
    ]

    for symbol, name in contracts:
        try:
            df = service.akshare.get_futures_daily(symbol=symbol)

            if not df.empty:
                print(f"\n{name} ({symbol})")
                print(f"  数据行数: {len(df)}")
                # 打印收盘价范围
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
# 示例 3: 获取期货实时行情数据
# ============================================================
def example_futures_spot_basic():
    """基本用法: 获取期货实时行情数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取期货实时行情数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_futures_spot()

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取期货实时数据失败: {e}")
        print("(实时行情接口需要额外的配置支持)")


# ============================================================
# 示例 4: 获取主力合约列表
# ============================================================
def example_futures_main_contracts_basic():
    """基本用法: 获取期货主力合约列表"""
    print("\n" + "=" * 60)
    print("示例 4: 获取期货主力合约列表")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_futures_main_contracts()

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print(f"\n主力合约总数: {len(df)}")
        print("\n前10个主力合约:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取主力合约列表失败: {e}")
        print("(主力合约列表接口需要额外的配置支持)")


# ============================================================
# 示例 5: 日线数据分析 - 价格统计
# ============================================================
def example_futures_daily_analysis():
    """获取期货日线数据后进行价格统计分析"""
    print("\n" + "=" * 60)
    print("示例 5: 期货日线数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_futures_daily(symbol="rb0")

        if df.empty:
            print("无数据")
            return

        print(f"螺纹钢主力连续日线数据统计")
        print(f"数据形状: {df.shape}")
        print(f"交易日数: {len(df)}")

        if "close" in df.columns:
            print(f"\n价格统计:")
            print(f"  最高价: {df['high'].max():.2f}")
            print(f"  最低价: {df['low'].min():.2f}")
            print(f"  最新收盘价: {df.iloc[-1]['close']:.2f}")
            print(f"  平均收盘价: {df['close'].mean():.2f}")

            # 计算涨跌幅
            if len(df) > 1:
                first_close = df.iloc[0]["close"]
                last_close = df.iloc[-1]["close"]
                change_pct = (last_close - first_close) / first_close * 100
                print(f"  区间涨跌幅: {change_pct:.2f}%")

        if "volume" in df.columns:
            print(f"\n成交量统计:")
            print(f"  平均日成交量: {df['volume'].mean():,.0f}")
            print(f"  最大日成交量: {df['volume'].max():,.0f}")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_futures_error_handling():
    """演示错误处理 - 无效合约代码等情况"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效合约代码
    print("\n测试 1: 无效合约代码")
    try:
        df = service.akshare.get_futures_daily(symbol="INVALID")
        if df.empty:
            print("  结果: 返回空 DataFrame")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试正常调用
    print("\n测试 2: 正常调用")
    try:
        df = service.akshare.get_futures_daily(symbol="rb0")
        print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_futures_daily_basic()
    example_futures_daily_varieties()
    example_futures_spot_basic()
    example_futures_main_contracts_basic()
    example_futures_daily_analysis()
    example_futures_error_handling()
