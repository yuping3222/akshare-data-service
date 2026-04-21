"""
财务指标接口示例 (get_finance_indicator)

演示如何获取股票的财务指标数据，包括市盈率(PE)、市净率(PB)、
市销率(PS)、净资产收益率(ROE)、净利润和营业收入等。

返回字段: symbol, report_date, pe, pb, ps, roe, net_profit, revenue

导入方式: from akshare_data import get_finance_indicator
"""

import pandas as pd


def example_basic_usage():
    """基本用法: 获取单只股票的全部财务指标"""
    print("=" * 60)
    print("示例1: 获取贵州茅台(600519)的全部财务指标")
    print("=" * 60)

    from akshare_data import get_finance_indicator

    df = get_finance_indicator(symbol="600519")
    if df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print(f"列名: {list(df.columns)}")
    print("\n前5行数据:")
    print(df.head())


def example_with_date_range():
    """指定年份: 获取特定年份的财务指标"""
    print("\n" + "=" * 60)
    print("示例2: 获取比亚迪(002594) 2023年的财务指标")
    print("=" * 60)

    from akshare_data import get_finance_indicator

    df = get_finance_indicator(
        symbol="002594",
        start_year="2023",
    )
    if df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print("\n数据内容:")
    print(df)


def example_recent_quarters():
    """获取最近几个季度的财务指标"""
    print("\n" + "=" * 60)
    print("示例3: 获取宁德时代(300750)最近两年的财务指标")
    print("=" * 60)

    from akshare_data import get_finance_indicator

    df = get_finance_indicator(
        symbol="300750",
        start_year="2024",
    )
    if df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print("\n数据内容:")
    print(df)


def example_multiple_stocks():
    """批量获取多只股票的财务指标"""
    print("\n" + "=" * 60)
    print("示例4: 批量获取多只银行股的财务指标")
    print("=" * 60)

    from akshare_data import get_finance_indicator

    symbols = ["600036", "601166", "600000"]

    for symbol in symbols:
        print(f"\n--- 获取 {symbol} 的财务指标 ---")
        df = get_finance_indicator(
            symbol=symbol,
            start_year="2024",
        )
        if df.empty:
            print(f"  {symbol}: 无数据")
        else:
            print(f"  数据形状: {df.shape}")
            print(f"  最新数据:")
            print(df.tail(1).to_string(index=False))


def example_analyze_metrics():
    """分析财务指标: 计算PE/PB变化趋势"""
    print("\n" + "=" * 60)
    print("示例5: 分析平安银行(000001)的PE/PB变化趋势")
    print("=" * 60)

    from akshare_data import get_finance_indicator

    df = get_finance_indicator(
        symbol="000001",
        start_year="2020",
    )
    if df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print("\n所有数据:")
    print(df.to_string(index=False))

    # 检查是否包含关键指标列
    pe_col = "pe" if "pe" in df.columns else ("pe_ttm" if "pe_ttm" in df.columns else None)
    pb_col = "pb" if "pb" in df.columns else None

    if pe_col and pb_col:
        latest = df.iloc[-1]
        print(f"\n最新一期 ({latest.get('date', latest.get('report_date', 'N/A'))}):")
        print(f"  PE (市盈率): {latest[pe_col]}")
        print(f"  PB (市净率): {latest[pb_col]}")


if __name__ == "__main__":
    example_basic_usage()
    example_with_date_range()
    example_recent_quarters()
    example_multiple_stocks()
    example_analyze_metrics()
