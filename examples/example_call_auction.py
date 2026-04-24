"""
集合竞价接口示例 (get_call_auction)

演示如何获取A股集合竞价阶段的逐笔成交数据。
可用于分析开盘集合竞价的量价特征、主力动向等场景。

导入方式: from akshare_data import get_call_auction
"""

import logging
from datetime import date, timedelta

logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_call_auction


def _candidate_fallback_dates(count: int = 5) -> list[str]:
    today = date.today()
    d = today if today.weekday() < 5 else today - timedelta(days=today.weekday() - 4)
    out: list[str] = []
    while len(out) < count:
        if d.weekday() < 5 and d <= today:
            out.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return out


def _fetch_call_auction_with_fallback(symbol: str, target_date: str | None = None):
    dates = [target_date] if target_date else _candidate_fallback_dates()
    for d in dates:
        df = get_call_auction(symbol=symbol, date=d)
        if df is not None and not df.empty:
            return df, d
    return None, dates


def example_basic_usage():
    """基本用法: 获取单只股票的集合竞价数据"""
    print("=" * 60)
    print("示例1: 获取单只股票集合竞价数据")
    print("=" * 60)

    try:
        df, used_date = _fetch_call_auction_with_fallback(symbol="600519")

        if df is None or df.empty:
            print("无数据（可能是非交易时间或数据不可用）")
            print(f"候选回退日期: {', '.join(_candidate_fallback_dates())}")
            return
        print(f"使用日期: {used_date}")

        print(f"数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10))

    except Exception as e:
        print(f"获取集合竞价数据失败: {e}")


def example_multiple_symbols():
    """多只股票: 获取多只股票的集合竞价数据"""
    print("\n" + "=" * 60)
    print("示例2: 获取多只股票集合竞价数据")
    print("=" * 60)

    symbols = ["600519", "000001", "300750"]  # 贵州茅台、平安银行、宁德时代

    for sym in symbols:
        try:
            df, used_date = _fetch_call_auction_with_fallback(symbol=sym)
            if df is not None and not df.empty:
                print(f"\n{sym}: {df.shape[0]} 条记录 (日期: {used_date})")
                print(df.head(3))
            else:
                print(f"\n{sym}: 无数据")
                print(f"候选回退日期: {', '.join(_candidate_fallback_dates())}")
        except Exception as e:
            print(f"\n{sym}: 获取失败 - {e}")


def example_with_date():
    """指定日期: 获取特定日期的集合竞价数据"""
    print("\n" + "=" * 60)
    print("示例3: 指定日期的集合竞价数据")
    print("=" * 60)

    try:
        target_date = _candidate_fallback_dates(count=1)[0]
        df, used_date = _fetch_call_auction_with_fallback(symbol="600519", target_date=target_date)

        if df is None or df.empty:
            print("无数据（指定日期可能无交易）")
            print(f"候选回退日期: {', '.join(_candidate_fallback_dates())}")
            return

        print(f"使用日期: {used_date}")
        print(f"数据形状: {df.shape}")
        print("\n数据预览:")
        print(df.head(10))

    except Exception as e:
        print(f"获取集合竞价数据失败: {e}")


def example_analysis():
    """分析: 集合竞价量价分析"""
    print("\n" + "=" * 60)
    print("示例4: 集合竞价量价分析")
    print("=" * 60)

    try:
        df, used_date = _fetch_call_auction_with_fallback(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            print(f"候选回退日期: {', '.join(_candidate_fallback_dates())}")
            return

        print(f"使用日期: {used_date}")
        print(f"总记录数: {len(df)}")

        # 统计成交量/额字段
        vol_cols = [c for c in df.columns if 'vol' in c.lower() or '量' in c]
        price_cols = [c for c in df.columns if 'price' in c.lower() or '价' in c or 'close' in c.lower()]
        amount_cols = [c for c in df.columns if 'amount' in c.lower() or '额' in c]

        if vol_cols:
            print(f"\n成交量列: {vol_cols}")
        if price_cols:
            print(f"价格列: {price_cols}")
        if amount_cols:
            print(f"成交额列: {amount_cols}")

        print("\n描述统计:")
        numeric_cols = df.select_dtypes(include='number').columns
        if len(numeric_cols) > 0:
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"分析失败: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_multiple_symbols()
    example_with_date()
    example_analysis()
