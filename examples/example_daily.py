"""
get_daily() 接口示例

演示如何使用 akshare_data.get_daily() 获取股票日线行情数据。

支持多种证券代码格式:
  - 纯数字: "000001", "600000"
  - 交易所前缀: "sh600000", "sz000001"
  - JoinQuant格式: "600000.XSHG", "000001.XSHE"

复权类型:
  - "qfq": 前复权 (默认)
  - "hfq": 后复权
  - "none": 不复权

返回字段: symbol, date, open, high, low, close, volume, amount, adjust
"""

from datetime import date, timedelta

from akshare_data import get_daily


def _last_trading_day(anchor: date | None = None) -> date:
    """回退到最近工作日（避免周末和未来日）"""
    d = min(anchor or date.today(), date.today())
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _date_range(trading_days: int) -> tuple[str, str]:
    end = _last_trading_day()
    start = end - timedelta(days=max(trading_days * 2, 7))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _candidate_fallback_dates(count: int = 5) -> list[str]:
    d = _last_trading_day()
    out: list[str] = []
    while len(out) < count:
        if d.weekday() < 5:
            out.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return out


def _print_empty_hint(symbol: str, start_date: str, end_date: str) -> None:
    print(f"{symbol} 在 {start_date} ~ {end_date} 无数据。")
    print(f"候选回退日期: {', '.join(_candidate_fallback_dates())}")


# ============================================================
# 示例 1: 基本用法 - 获取平安银行前复权日线数据
# ============================================================
def example_basic():
    """基本用法: 获取单只股票的日线数据 (默认前复权)"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取平安银行前复权日线数据")
    print("=" * 60)

    try:
        # symbol: 证券代码，支持多种格式
        # start_date: 起始日期，格式 "YYYY-MM-DD"
        # end_date: 结束日期，格式 "YYYY-MM-DD"
        # adjust: 复权类型，默认 "qfq" (前复权)
        start, end = _date_range(60)
        df = get_daily(symbol="000001", start_date=start, end_date=end, adjust="qfq")
        if df is None or df.empty:
            _print_empty_hint("000001", start, end)
            return

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
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不同复权类型对比
# ============================================================
def example_adjust_types():
    """演示三种复权类型的区别"""
    print("\n" + "=" * 60)
    print("示例 2: 不同复权类型对比")
    print("=" * 60)

    symbol = "600519"  # 贵州茅台
    start, end = _date_range(25)

    adjust_types = ["qfq", "hfq", "none"]

    for adj in adjust_types:
        try:
            df = get_daily(symbol=symbol, start_date=start, end_date=end, adjust=adj)
            if df is not None and not df.empty:
                print(f"\n复权类型: {adj}")
                print(f"  数据行数: {len(df)}")
                print(
                    f"  收盘价范围: {df['close'].min():.2f} ~ {df['close'].max():.2f}"
                )
                print(f"  前3行收盘价: {df['close'].head(3).tolist()}")
            else:
                _print_empty_hint(symbol, start, end)
        except Exception as e:
            print(f"\n复权类型: {adj} - 获取失败: {e}")


# ============================================================
# 示例 3: 不同证券代码格式
# ============================================================
def example_symbol_formats():
    """演示支持的多种证券代码格式"""
    print("\n" + "=" * 60)
    print("示例 3: 不同证券代码格式")
    print("=" * 60)

    # 同一只股票的不同写法
    symbols = [
        "600000",  # 纯数字 (浦发银行)
        "sh600000",  # 交易所前缀
        "600000.XSHG",  # JoinQuant 格式
    ]

    for sym in symbols:
        try:
            start, end = _date_range(10)
            df = get_daily(symbol=sym, start_date=start, end_date=end)
            if df is not None and not df.empty:
                print(
                    f"代码格式: {sym:15s} -> 标准化后: {df['symbol'].iloc[0]:10s}, 行数: {len(df)}"
                )
            else:
                _print_empty_hint(sym, start, end)
        except Exception as e:
            print(f"代码格式: {sym:15s} -> 获取失败: {e}")


# ============================================================
# 示例 4: 获取深市股票
# ============================================================
def example_sz_stock():
    """获取深市股票数据"""
    print("\n" + "=" * 60)
    print("示例 4: 获取深市股票 (万科A)")
    print("=" * 60)

    try:
        start, end = _date_range(40)
        df = get_daily(symbol="sz000002", start_date=start, end_date=end, adjust="qfq")

        if df is None or df.empty:
            _print_empty_hint("sz000002", start, end)
            return

        print(f"数据形状: {df.shape}")
        print(f"\n基本统计信息:")
        print(df[["open", "high", "low", "close", "volume"]].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 数据分析 - 计算简单指标
# ============================================================
def example_analysis():
    """演示获取数据后进行简单技术分析"""
    print("\n" + "=" * 60)
    print("示例 5: 数据分析 - 计算移动平均线")
    print("=" * 60)

    try:
        start, end = _date_range(130)
        df = get_daily(symbol="600036", start_date=start, end_date=end, adjust="qfq")

        if df is None or df.empty:
            _print_empty_hint("600036", start, end)
            return

        # Ensure date column is tz-naive for consistent comparisons
        if "date" in df.columns:
            if hasattr(df["date"].dtype, "tz") and df["date"].dtype.tz is not None:
                df["date"] = df["date"].dt.tz_localize(None)

        # 计算5日和20日移动平均线
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()

        print(f"招商银行 2024上半年日线数据 (含MA5/MA20)")
        print(f"数据形状: {df.shape}")
        print("\n最新10行数据:")
        print(
            df[["date", "close", "ma5", "ma20", "volume"]]
            .tail(10)
            .to_string(index=False)
        )

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 不复权数据 (用于研究原始价格)
# ============================================================
def example_no_adjust():
    """获取不复权数据，用于研究原始价格走势"""
    print("\n" + "=" * 60)
    print("示例 6: 不复权数据")
    print("=" * 60)

    try:
        start, end = _date_range(250)
        df = get_daily(symbol="000001", start_date=start, end_date=end, adjust="none")

        if df is None or df.empty:
            _print_empty_hint("000001", start, end)
            return

        print(f"平安银行 2023年不复权日线数据")
        print(f"数据形状: {df.shape}")
        print(f"全年交易日数: {len(df)}")
        print(f"年初收盘价: {df.iloc[0]['close']:.2f}")
        print(f"年末收盘价: {df.iloc[-1]['close']:.2f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_adjust_types()
    example_symbol_formats()
    example_sz_stock()
    example_analysis()
    example_no_adjust()
