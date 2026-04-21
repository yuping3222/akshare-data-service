"""
实时行情接口示例 (get_realtime_data)

演示如何获取A股市场的实时行情快照数据，包括价格、成交量、涨跌幅等。
可用于盘中监控、市场广度分析、涨跌停筛选等场景。

返回字段: symbol, name, price, change, pct_change, volume, amount, open, high, low, prev_close

导入方式: from akshare_data import get_realtime_data
"""

import logging
logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_realtime_data


def example_basic_usage():
    """基本用法: 获取全市场实时行情快照"""
    print("=" * 60)
    print("示例1: 获取A股全市场实时行情")
    print("=" * 60)

    try:
        df = get_realtime_data()

        if df is None or df.empty:
            print("无数据（可能是非交易时间）")
            return

        print(f"数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10))

    except Exception as e:
        print(f"获取实时行情失败: {e}")


def example_filter_by_symbol():
    """过滤: 按股票代码获取单只或多只股票的实时数据"""
    print("\n" + "=" * 60)
    print("示例2: 获取单只股票的实时行情")
    print("=" * 60)

    try:
        df = get_realtime_data()

        if df is None or df.empty:
            print("无数据")
            return

        symbols = ["600519", "000001", "300750"]  # 贵州茅台、平安银行、宁德时代
        subset = df[df["symbol"].isin(symbols)]

        if subset.empty:
            print("未找到匹配的股票")
            return

        print(f"找到 {len(subset)} 只股票:")
        print(subset.to_string(index=False))

    except Exception as e:
        print(f"获取实时行情失败: {e}")


def example_top_gainers():
    """分析: 找出涨幅最大的前10只股票"""
    print("\n" + "=" * 60)
    print("示例3: 涨幅榜TOP 10")
    print("=" * 60)

    try:
        df = get_realtime_data()

        if df is None or df.empty:
            print("无数据")
            return

        if "pct_change" in df.columns:
            top = df.nlargest(10, "pct_change")[["symbol", "name", "price", "pct_change", "volume"]]
            print(top.to_string(index=False))

    except Exception as e:
        print(f"获取实时行情失败: {e}")


def example_volume_analysis():
    """分析: 按成交量排序，找出最活跃的股票"""
    print("\n" + "=" * 60)
    print("示例4: 成交量TOP 10")
    print("=" * 60)

    try:
        df = get_realtime_data()

        if df is None or df.empty:
            print("无数据")
            return

        if "volume" in df.columns:
            top_volume = df.nlargest(10, "volume")[["symbol", "name", "price", "pct_change", "volume", "amount"]]
            print(top_volume.to_string(index=False))

    except Exception as e:
        print(f"获取实时行情失败: {e}")


def example_market_breadth():
    """分析: 市场广度统计"""
    print("\n" + "=" * 60)
    print("示例5: 市场广度分析")
    print("=" * 60)

    try:
        df = get_realtime_data()

        if df is None or df.empty:
            print("无数据")
            return

        total = len(df)
        print(f"全市场股票数: {total}")

        if "pct_change" in df.columns:
            up = (df["pct_change"] > 0).sum()
            down = (df["pct_change"] < 0).sum()
            flat = (df["pct_change"] == 0).sum()

            print(f"上涨: {up} ({up/total*100:.1f}%)")
            print(f"下跌: {down} ({down/total*100:.1f}%)")
            print(f"平盘: {flat} ({flat/total*100:.1f}%)")

        if "price" in df.columns:
            avg_price = df["price"].mean()
            median_price = df["price"].median()
            print(f"\n均价: {avg_price:.2f}")
            print(f"中位数价: {median_price:.2f}")

        if "amount" in df.columns:
            total_amount = df["amount"].sum()
            print(f"全市场总成交额: {total_amount/1e8:.2f} 亿")

    except Exception as e:
        print(f"获取实时行情失败: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_filter_by_symbol()
    example_top_gainers()
    example_volume_analysis()
    example_market_breadth()
