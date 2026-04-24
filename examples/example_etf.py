"""ETF 日线数据获取示例

演示如何使用 get_etf() 接口获取ETF的日线行情数据。

get_etf(symbol, start_date, end_date) 参数说明:
    - symbol: ETF代码，如 "510300" (沪深300ETF)、"510050" (上证50ETF)
    - start_date: 起始日期，格式 "YYYY-MM-DD"
    - end_date: 结束日期，格式 "YYYY-MM-DD"

返回: pd.DataFrame，包含字段:
    - symbol: ETF代码
    - date: 交易日期
    - open: 开盘价
    - high: 最高价
    - low: 最低价
    - close: 收盘价
    - volume: 成交量
    - amount: 成交额

注意:
    - 数据采用 Cache-First 策略，首次获取后会缓存到本地
    - 后续相同范围的请求会直接返回缓存数据，无需重复请求
    - 支持带前缀的代码格式 (如 "sh510300")，系统会自动规范化
"""

import warnings
from datetime import date, timedelta

from akshare_data import get_etf

warnings.filterwarnings("ignore", category=DeprecationWarning)


def _last_trading_day(anchor: date | None = None) -> date:
    d = min(anchor or date.today(), date.today())
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _date_range(days: int) -> tuple[str, str]:
    end = _last_trading_day()
    start = end - timedelta(days=max(days * 2, 10))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _candidate_fallback_dates(count: int = 5) -> list[str]:
    d = _last_trading_day()
    out: list[str] = []
    while len(out) < count:
        out.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return out


def _print_empty_or_data(label, df):
    """Helper: print a clear message if data is empty, otherwise print it."""
    if df is None or df.empty:
        print(f"  {label}: 无数据 (数据源未返回结果，可能是网络问题或尚未缓存)")
        print(f"  候选回退日期: {', '.join(_candidate_fallback_dates())}")
        return False
    return True


def example_basic_etf():
    """示例1: 获取单只ETF的基本日线数据"""
    print("=" * 60)
    print("示例1: 获取沪深300ETF (510300) 的日线数据")
    print("=" * 60)

    try:
        # 获取 2024年1月 到 2024年3月 的数据
        start, end = _date_range(60)
        df = get_etf("510300", start, end)

        if not _print_empty_or_data("沪深300ETF(510300)", df):
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"  - 行数 (交易日数): {df.shape[0]}")
        print(f"  - 列数 (字段数): {df.shape[1]}")

        # 打印列名
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行数据
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行数据
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取ETF数据失败: {e}")


def example_multiple_etfs():
    """示例2: 获取多只ETF的数据并对比"""
    print("\n" + "=" * 60)
    print("示例2: 获取多只主流ETF的数据")
    print("=" * 60)

    # 定义要查询的ETF列表
    etfs = {
        "510300": "沪深300ETF",
        "510050": "上证50ETF",
        "159919": "沪深300ETF(深市)",
        "510500": "中证500ETF",
    }

    for symbol, name in etfs.items():
        try:
            print(f"\n--- {name} ({symbol}) ---")
            start, end = _date_range(22)
            df = get_etf(symbol, start, end)

            if df.empty:
                print("  无数据 (数据源未返回结果)")
            else:
                print(f"  数据行数: {len(df)}")
                if "date" in df.columns:
                    print(f"  日期范围: {df['date'].min()} ~ {df['date'].max()}")
                if "close" in df.columns:
                    print(f"  最新收盘价: {df['close'].iloc[-1]}")
                if "high" in df.columns:
                    print(f"  期间最高价: {df['high'].max()}")
                if "low" in df.columns:
                    print(f"  期间最低价: {df['low'].min()}")

        except Exception as e:
            print(f"  获取 {symbol} 数据失败: {e}")


def example_with_prefix():
    """示例3: 使用带市场前缀的代码格式"""
    print("\n" + "=" * 60)
    print("示例3: 使用带前缀的代码格式 (sh510300)")
    print("=" * 60)

    try:
        # 系统会自动将 "sh510300" 规范化为 "510300"
        start, end = _date_range(22)
        df = get_etf("sh510300", start, end)

        if not _print_empty_or_data("sh510300", df):
            return

        print(f"数据形状: {df.shape}")
        print("\n前3行数据:")
        print(df.head(3))

    except Exception as e:
        print(f"获取ETF数据失败: {e}")


def example_data_analysis():
    """示例4: 对ETF数据进行简单统计分析"""
    print("\n" + "=" * 60)
    print("示例4: ETF数据简单统计分析")
    print("=" * 60)

    try:
        # 获取较长时间跨度的数据用于分析
        start, end = _date_range(260)
        df = get_etf("510300", start, end)

        if df.empty:
            print("无数据 (数据源未返回结果)")
            return

        print("\n沪深300ETF (510300) 2024年度统计:")
        print(f"  交易日总数: {len(df)}")
        if "date" in df.columns:
            print(f"  日期范围: {df['date'].min()} ~ {df['date'].max()}")

        # 价格统计
        if "close" in df.columns:
            print("\n  收盘价统计:")
            print(f"    最高收盘价: {df['close'].max():.4f}")
            print(f"    最低收盘价: {df['close'].min():.4f}")
            print(f"    平均收盘价: {df['close'].mean():.4f}")

        # 成交量统计
        if "volume" in df.columns:
            print("\n  成交量统计:")
            print(f"    总成交量: {df['volume'].sum():,.0f}")
            print(f"    日均成交量: {df['volume'].mean():,.0f}")

        # 成交额统计
        if "amount" in df.columns:
            print("\n  成交额统计:")
            print(f"    总成交额: {df['amount'].sum():,.0f}")
            print(f"    日均成交额: {df['amount'].mean():,.0f}")

        # 计算涨跌幅
        if "close" in df.columns:
            df["pct_change"] = df["close"].pct_change() * 100
            print("\n  涨跌幅统计:")
            print(f"    最大单日涨幅: {df['pct_change'].max():.2f}%")
            print(f"    最大单日跌幅: {df['pct_change'].min():.2f}%")
            print(f"    平均日涨跌幅: {df['pct_change'].mean():.2f}%")

    except Exception as e:
        print(f"数据分析失败: {e}")


def example_short_period():
    """示例5: 获取短期数据 (最近一个月)"""
    print("\n" + "=" * 60)
    print("示例5: 获取短期数据")
    print("=" * 60)

    try:
        # 获取单只ETF的短期数据
        start, end = _date_range(22)
        df = get_etf("510050", start, end)

        if not _print_empty_or_data("上证50ETF(510050)", df):
            return

        print("上证50ETF (510050) 2024年11月数据:")
        print(f"数据形状: {df.shape}")
        print("\n完整数据:")
        print(df.to_string())

    except Exception as e:
        print(f"获取ETF数据失败: {e}")


if __name__ == "__main__":
    # 运行所有示例
    example_basic_etf()
    example_multiple_etfs()
    example_with_prefix()
    example_data_analysis()
    example_short_period()
