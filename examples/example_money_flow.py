"""
get_money_flow() 接口使用示例

本示例展示如何使用 akshare 获取个股的资金流向数据。
akshare.stock_individual_fund_flow 返回指定股票近期的资金流向明细。

注意:
- akshare 的 stock_individual_fund_flow 接口不支持日期范围参数，
  返回的是该股票近期所有可用的资金流向数据。
- 需要通过 market 参数指定交易所: "sh"(沪市), "sz"(深市), "bj"(北交所)

返回 pd.DataFrame，典型字段:
    - 日期: 交易日期
    - 收盘价: 当日收盘价
    - 涨跌幅: 当日涨跌幅
    - 主力净流入-净额: 主力净流入金额（元）
    - 主力净流入-净占比: 主力净流入占比（%）
    - 超大单/大单/中单/小单 净流入-净额 和 净占比

注意: 接口依赖外部数据源，网络不可用时会自动跳过。
"""

import akshare as ak
import pandas as pd
from datetime import date, timedelta


def _candidate_fallback_dates(count: int = 5) -> list[str]:
    today = date.today()
    d = today if today.weekday() < 5 else today - timedelta(days=today.weekday() - 4)
    out: list[str] = []
    while len(out) < count:
        if d.weekday() < 5 and d <= today:
            out.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return out


def _print_empty_hint(symbol: str) -> None:
    print(f"{symbol}: 无数据 (网络不可用或接口返回为空)")
    print("  说明: stock_individual_fund_flow 不接受日期范围参数，只返回近期可用数据。")
    print(f"  候选回退日期: {', '.join(_candidate_fallback_dates())}")


def _get_money_flow(symbol, market=None):
    """获取个股资金流向，网络失败时返回 None"""
    if market is None:
        # 根据代码前缀自动判断市场
        if symbol.startswith(("6", "9")):
            market = "sh"
        elif symbol.startswith(("3", "0")):
            market = "sz"
        elif symbol.startswith(("4", "8")):
            market = "bj"
        else:
            market = "sz"  # 默认深市
    try:
        return ak.stock_individual_fund_flow(stock=symbol, market=market)
    except Exception as e:
        print(f"  (获取 {symbol} 失败: {e})")
        return None


def example_basic_usage():
    """基础用法：获取单只股票近期的资金流向"""
    print("=" * 60)
    print("示例1: 获取平安银行(000001)近期资金流向")
    print("=" * 60)

    df = _get_money_flow("000001", market="sz")
    if df is None or df.empty:
        _print_empty_hint("000001")
        return

    print(f"数据形状: {df.shape}")
    print(f"列名: {df.columns.tolist()}")
    print(f"\n前5行数据:")
    print(df.head())


def example_latest_data():
    """获取最新交易日资金流向"""
    print("\n" + "=" * 60)
    print("示例2: 获取贵州茅台(600519)最新资金流向")
    print("=" * 60)

    df = _get_money_flow("600519", market="sh")
    if df is None or df.empty:
        _print_empty_hint("600519")
        return

    print(f"数据形状: {df.shape}")
    if not df.empty:
        print(f"最早日期: {df['日期'].iloc[0]}")
        print(f"最晚日期: {df['日期'].iloc[-1]}")
        print(f"\n前5行数据:")
        print(df.head())
        print(f"\n后5行数据:")
        print(df.tail())


def example_different_symbols():
    """不同股票代码格式"""
    print("\n" + "=" * 60)
    print("示例3: 不同股票代码的用法")
    print("=" * 60)

    symbols = [
        ("000001", "sz"),  # 深市主板
        ("600000", "sh"),  # 沪市主板
        ("300750", "sz"),  # 创业板
        ("688981", "sh"),  # 科创板
    ]

    for symbol, market in symbols:
        df = _get_money_flow(symbol, market=market)
        if df is not None and not df.empty:
            print(f"股票 {symbol} ({market}): {df.shape[0]} 条记录")
        else:
            print(f"股票 {symbol} ({market}): 无数据")
            print(f"  候选回退日期: {', '.join(_candidate_fallback_dates())}")


def example_analyze_net_inflow():
    """实用场景：分析主力资金净流入情况"""
    print("\n" + "=" * 60)
    print("示例4: 分析主力资金净流入情况")
    print("=" * 60)

    df = _get_money_flow("300750", market="sz")
    if df is None or df.empty:
        _print_empty_hint("300750")
        return

    # 计算主力净流入的统计信息
    col = "主力净流入-净额"
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"\n主力净流入统计（单位：元）:")
        print(f"  平均值: {df[col].mean():.2f}")
        print(f"  最大值: {df[col].max():.2f}")
        print(f"  最小值: {df[col].min():.2f}")
        print(f"  总和: {df[col].sum():.2f}")

        # 统计净流入为正/负的天数
        positive_days = (df[col] > 0).sum()
        negative_days = (df[col] < 0).sum()
        print(f"\n主力净流入为正的天数: {positive_days}")
        print(f"主力净流入为负的天数: {negative_days}")

        # 找出主力净流入最大的那一天
        max_idx = df[col].idxmax()
        print(f"\n主力净流入最大的日期: {df.loc[max_idx, '日期']}")
        print(f"净流入金额: {df.loc[max_idx, col]:.2f}")
    else:
        print(f"未找到 '{col}' 列，可用列: {df.columns.tolist()}")


def example_compare_flow_sizes():
    """实用场景：对比不同规模资金的流向"""
    print("\n" + "=" * 60)
    print("示例5: 对比不同规模资金的流向")
    print("=" * 60)

    df = _get_money_flow("002594", market="sz")
    if df is None or df.empty:
        _print_empty_hint("002594")
        return

    # 计算各类资金净流入的总和
    flow_columns = [
        "超大单净流入-净额",
        "大单净流入-净额",
        "中单净流入-净额",
        "小单净流入-净额",
    ]
    # 只保留存在的列
    flow_columns = [c for c in flow_columns if c in df.columns]

    if flow_columns:
        for col in flow_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        print("\n各类资金净流入总和（单位：元）:")
        for col in flow_columns:
            total = df[col].sum()
            print(f"  {col}: {total:,.2f}")

        print(f"\n前5行详细数据:")
        print(df.head())
    else:
        print(f"未找到资金流向列，可用列: {df.columns.tolist()}")


def example_with_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 60)
    print("示例6: 错误处理示例")
    print("=" * 60)

    # 测试无效股票代码
    try:
        df = ak.stock_individual_fund_flow(stock="INVALID", market="sz")
        if df.empty:
            print("无效股票代码，返回空DataFrame")
        else:
            print(f"获取到 {df.shape[0]} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_latest_data()
    example_different_symbols()
    example_analyze_net_inflow()
    example_compare_flow_sizes()
    example_with_error_handling()
