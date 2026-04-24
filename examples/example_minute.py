"""
get_minute() 接口示例

演示如何使用 akshare_data.get_minute() 获取股票分钟级行情数据。

支持频率:
  - "1min": 1分钟线 (默认)
  - "5min": 5分钟线
  - "15min": 15分钟线
  - "30min": 30分钟线
  - "60min": 60分钟线

日期参数 (start_date/end_date) 为可选:
  - 不传: 返回缓存中的全部分钟数据
  - 传入: 获取指定日期范围的数据

返回字段: symbol, datetime, open, high, low, close, volume, amount
"""

import pandas as pd
from datetime import date, timedelta
from akshare_data import get_minute


def _last_trading_day(anchor: date | None = None) -> date:
    d = min(anchor or date.today(), date.today())
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _date_range(days: int = 3) -> tuple[str, str]:
    end = _last_trading_day()
    start = end - timedelta(days=max(days * 2, 3))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _candidate_fallback_dates(count: int = 5) -> list[str]:
    d = _last_trading_day()
    out: list[str] = []
    while len(out) < count:
        if d.weekday() < 5:
            out.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return out


def _get_minute(symbol, freq="1min", start_date=None, end_date=None):
    """Get minute data with graceful empty-data handling."""
    df = get_minute(symbol, freq=freq, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        print(f"  [无数据] {symbol} 在指定范围内无分钟数据")
        print(f"  候选回退日期: {', '.join(_candidate_fallback_dates())}")
        return pd.DataFrame()
    return df


# ============================================================
# 示例 1: 基本用法 - 获取1分钟线数据
# ============================================================
def example_basic():
    """基本用法: 获取指定日期范围的1分钟线数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取1分钟线数据")
    print("=" * 60)

    try:
        start, end = _date_range(5)
        df = _get_minute(symbol="000001", freq="1min", start_date=start, end_date=end)

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前几行
        if not df.empty:
            print("\n前5行数据:")
            print(df.head())

            # 打印时间范围
            print(f"\n时间范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
        else:
            print("\n无数据 (该日期范围可能无分钟数据)")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不同频率对比
# ============================================================
def example_freq_types():
    """演示不同频率的分钟线数据"""
    print("\n" + "=" * 60)
    print("示例 2: 不同频率对比")
    print("=" * 60)

    symbol = "600519"  # 贵州茅台
    start, end = _date_range(3)

    freqs = ["1min", "5min", "15min", "30min", "60min"]

    for freq in freqs:
        try:
            df = _get_minute(symbol, freq=freq, start_date=start, end_date=end)
            if not df.empty:
                print(
                    f"频率: {freq:8s} -> 数据行数: {len(df):5d}, 时间范围: {df['datetime'].min()} ~ {df['datetime'].max()}"
                )
            else:
                print(f"频率: {freq:8s} -> 无数据")
        except Exception as e:
            print(f"频率: {freq:8s} -> 获取失败: {e}")


# ============================================================
# 示例 3: 不指定日期范围 (获取缓存数据)
# ============================================================
def example_no_dates():
    """不指定日期范围，返回缓存中该股票的全部分钟数据"""
    print("\n" + "=" * 60)
    print("示例 3: 不指定日期范围")
    print("=" * 60)

    try:
        # 不传 start_date 和 end_date，返回缓存中的全部数据
        start, end = _date_range(5)
        df = get_minute("000001", freq="5min", start_date=start, end_date=end)

        if df is not None and not df.empty:
            print(f"数据形状: {df.shape}")
            print(f"时间范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
            print(f"\n前5行:")
            print(df.head())
        else:
            print("无数据（指定范围内未返回分钟线）")
            print(f"候选回退日期: {', '.join(_candidate_fallback_dates())}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 深市股票分钟线
# ============================================================
def example_sz_stock():
    """获取深市股票的分钟线数据"""
    print("\n" + "=" * 60)
    print("示例 4: 深市股票分钟线 (万科A)")
    print("=" * 60)

    try:
        start, end = _date_range(8)
        df = _get_minute(symbol="sz000002", freq="15min", start_date=start, end_date=end)

        if not df.empty:
            print(f"数据形状: {df.shape}")
            print(f"\n基本统计信息:")
            print(df[["open", "high", "low", "close", "volume"]].describe())
        else:
            print("无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 数据分析 - 日内成交量分布
# ============================================================
def example_analysis():
    """演示获取分钟数据后进行日内分析"""
    print("\n" + "=" * 60)
    print("示例 5: 数据分析 - 日内成交量分布")
    print("=" * 60)

    try:
        start, end = _date_range(1)
        df = _get_minute(symbol="600036", freq="5min", start_date=start, end_date=end)

        if df.empty:
            print("无数据")
            return

        # 提取时间部分
        df["time"] = pd.to_datetime(df["datetime"]).dt.time

        # 按时间统计平均成交量
        time_volume = df.groupby("time")["volume"].mean().sort_index()

        print(f"招商银行 2024-06-03 5分钟线")
        print(f"数据行数: {len(df)}")
        print(f"\n各时段平均成交量:")
        print(time_volume.to_string())

        # 找出成交量最大的时段
        max_time = time_volume.idxmax()
        max_vol = time_volume.max()
        print(f"\n成交量最大时段: {max_time}, 平均成交量: {max_vol:.0f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 多种代码格式
# ============================================================
def example_symbol_formats():
    """演示分钟线支持的不同代码格式"""
    print("\n" + "=" * 60)
    print("示例 6: 不同证券代码格式")
    print("=" * 60)

    symbols = [
        "600000",
        "sh600000",
        "600000.XSHG",
    ]

    for sym in symbols:
        try:
            start, end = _date_range(1)
            df = _get_minute(sym, freq="30min", start_date=start, end_date=end)
            if not df.empty:
                print(
                    f"代码格式: {sym:15s} -> 标准化后: {df['symbol'].iloc[0]:10s}, 行数: {len(df)}"
                )
            else:
                print(f"代码格式: {sym:15s} -> 无数据")
        except Exception as e:
            print(f"代码格式: {sym:15s} -> 获取失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_freq_types()
    example_no_dates()
    example_sz_stock()
    example_analysis()
    example_symbol_formats()
