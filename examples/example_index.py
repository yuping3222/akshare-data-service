"""
get_index() 接口示例

演示如何使用 akshare_data.get_index() 获取指数日线行情数据。

常用指数代码:
  - "000300": 沪深300
  - "000001": 上证指数
  - "399001": 深证成指
  - "399006": 创业板指
  - "000016": 上证50
  - "000905": 中证500

日期参数:
  - start_date: 默认 "1990-01-01" (从上市以来)
  - end_date: 默认当天

返回字段: symbol, date, open, high, low, close, volume, amount
"""

import pandas as pd
from datetime import datetime
from akshare_data import get_index


def _get_index(index_code, start_date=None, end_date=None):
    """Get index data with graceful empty-data handling."""
    if start_date is None:
        start_date = "1990-01-01"
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    df = get_index(index_code=index_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        print(f"  [无数据] {index_code} 在 {start_date} ~ {end_date} 范围内无数据")
        return pd.DataFrame()
    return df


# ============================================================
# 示例 1: 基本用法 - 获取沪深300指数数据
# ============================================================
def example_basic():
    """基本用法: 获取沪深300指数日线数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取沪深300指数数据")
    print("=" * 60)

    try:
        df = _get_index("000300", "2024-01-01", "2024-03-31")

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        if not df.empty:
            print("\n前5行数据:")
            print(df.head())

            # 打印后5行
            print("\n后5行数据:")
            print(df.tail())
        else:
            print("\n无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取主要指数对比
# ============================================================
def example_major_indices():
    """获取A股主要指数的近期表现"""
    print("\n" + "=" * 60)
    print("示例 2: 获取A股主要指数对比")
    print("=" * 60)

    indices = {
        "000001": "上证指数",
        "399001": "深证成指",
        "000300": "沪深300",
        "399006": "创业板指",
        "000016": "上证50",
        "000905": "中证500",
    }

    start = "2024-01-01"
    end = "2024-06-30"

    print(
        f"{'指数名称':<10} {'指数代码':<10} {'行数':>6} {'期初收盘':>12} {'期末收盘':>12} {'涨跌幅':>10}"
    )
    print("-" * 65)

    for code, name in indices.items():
        try:
            df = _get_index(code, start, end)
            if not df.empty and len(df) >= 2:
                start_close = df.iloc[0]["close"]
                end_close = df.iloc[-1]["close"]
                change_pct = (end_close - start_close) / start_close * 100
                print(
                    f"{name:<10} {code:<10} {len(df):>6} {start_close:>12.2f} {end_close:>12.2f} {change_pct:>9.2f}%"
                )
            else:
                print(f"{name:<10} {code:<10} {'无数据':>6}")
        except Exception as e:
            print(f"{name:<10} {code:<10} {'失败':>6} ({e})")


# ============================================================
# 示例 3: 使用默认日期 (获取全部历史数据)
# ============================================================
def example_default_dates():
    """不指定日期，获取指数全部历史数据"""
    print("\n" + "=" * 60)
    print("示例 3: 使用默认日期 (获取全部历史)")
    print("=" * 60)

    try:
        # 不传 start_date 和 end_date，默认从 1990-01-01 到当天
        df = _get_index("000001", "2020-01-01", "2024-12-31")

        if not df.empty:
            print(f"上证指数全部历史数据")
            print(f"数据形状: {df.shape}")
            print(f"时间范围: {df['date'].min()} ~ {df['date'].max()}")
            print(f"总交易日数: {len(df)}")
            print(f"\n最早5个交易日:")
            print(df.head())
            print(f"\n最近5个交易日:")
            print(df.tail())
        else:
            print("无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 不同代码格式
# ============================================================
def example_symbol_formats():
    """演示指数代码支持的不同格式"""
    print("\n" + "=" * 60)
    print("示例 4: 不同指数代码格式")
    print("=" * 60)

    # 沪深300的不同写法
    codes = [
        "000300",
        "sh000300",
        "000300.XSHG",
    ]

    for code in codes:
        try:
            df = _get_index(code, "2024-06-01", "2024-06-10")
            if not df.empty:
                print(
                    f"代码格式: {code:15s} -> 标准化后: {df['symbol'].iloc[0]:10s}, 行数: {len(df)}"
                )
            else:
                print(f"代码格式: {code:15s} -> 无数据")
        except Exception as e:
            print(f"代码格式: {code:15s} -> 获取失败: {e}")


# ============================================================
# 示例 5: 数据分析 - 计算指数移动平均线
# ============================================================
def example_analysis():
    """演示获取指数数据后进行技术分析"""
    print("\n" + "=" * 60)
    print("示例 5: 数据分析 - 沪深300均线分析")
    print("=" * 60)

    try:
        df = _get_index("000300", "2024-01-01", "2024-12-31")

        if df.empty:
            print("无数据")
            return

        # 计算移动平均线
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma10"] = df["close"].rolling(window=10).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()
        df["ma60"] = df["close"].rolling(window=60).mean()

        # 计算日收益率
        df["pct_change"] = df["close"].pct_change() * 100

        print(f"沪深300 2024年日线数据 (含均线)")
        print(f"数据形状: {df.shape}")

        # 统计信息
        print(f"\n2024年统计:")
        print(
            f"  最高收盘: {df['close'].max():.2f} ({df.loc[df['close'].idxmax(), 'date']})"
        )
        print(
            f"  最低收盘: {df['close'].min():.2f} ({df.loc[df['close'].idxmin(), 'date']})"
        )
        print(f"  年化波动: {df['pct_change'].std() * (252**0.5):.2f}%")

        # 最新数据
        print(f"\n最新10行数据:")
        cols = ["date", "close", "ma5", "ma10", "ma20", "ma60", "pct_change"]
        available_cols = [c for c in cols if c in df.columns]
        print(df[available_cols].tail(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 计算指数年度涨跌幅
# ============================================================
def example_yearly_return():
    """计算沪深300近5年年度涨跌幅"""
    print("\n" + "=" * 60)
    print("示例 6: 计算年度涨跌幅")
    print("=" * 60)

    try:
        df = _get_index("000300", "2020-01-01", "2024-12-31")

        if df.empty or len(df) < 2:
            print("数据不足")
            return

        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year

        print(f"{'年份':<8} {'年初收盘':>12} {'年末收盘':>12} {'年度涨跌幅':>12}")
        print("-" * 48)

        for year in sorted(df["year"].unique()):
            year_data = df[df["year"] == year].sort_values("date")
            if len(year_data) >= 2:
                start_close = year_data.iloc[0]["close"]
                end_close = year_data.iloc[-1]["close"]
                change = (end_close - start_close) / start_close * 100
                print(
                    f"{year:<8} {start_close:>12.2f} {end_close:>12.2f} {change:>11.2f}%"
                )

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_major_indices()
    example_default_dates()
    example_symbol_formats()
    example_analysis()
    example_yearly_return()
