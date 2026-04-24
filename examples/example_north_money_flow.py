"""
get_north_money_flow() 接口使用示例

本示例展示如何使用 akshare 获取北向资金（沪深港通）的资金流向汇总数据。
akshare.stock_hsgt_fund_flow_summary_em 返回北向资金流向汇总。

注意:
- stock_hsgt_fund_flow_summary_em 接口不接受任何参数。
- 返回的是汇总数据，包含北向、沪股通、深股通的当日资金流向。

返回 pd.DataFrame，典型字段:
    - 序号/名称: 资金通道名称
    - 当日资金流入/当日资金流: 当日净流入（亿元）
    - 当日买入成交额 / 当日卖出成交额
    - 当日余额: 当日额度余额
    - 历史累计净买额: 累计净买入金额

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


def _print_empty_hint() -> None:
    print("无数据 (网络不可用或接口返回为空)")
    print("  说明: stock_hsgt_fund_flow_summary_em 不接受日期参数，仅返回最新可用汇总。")
    print(f"  候选回退日期: {', '.join(_candidate_fallback_dates())}")


def _get_north_flow_summary():
    """获取北向资金流向汇总，网络失败时返回 None"""
    try:
        return ak.stock_hsgt_fund_flow_summary_em()
    except Exception as e:
        print(f"  (获取北向资金汇总失败: {e})")
        return None


def example_basic_usage():
    """基础用法：获取北向资金流向汇总"""
    print("=" * 60)
    print("示例1: 获取北向资金流向汇总")
    print("=" * 60)

    df = _get_north_flow_summary()
    if df is None or df.empty:
        _print_empty_hint()
        return

    print(f"数据形状: {df.shape}")
    print(f"列名: {df.columns.tolist()}")
    print(f"\n完整数据:")
    print(df)


def example_analyze_net_flow():
    """实用场景：分析北向资金净流入情况"""
    print("\n" + "=" * 60)
    print("示例2: 分析北向资金净流入情况")
    print("=" * 60)

    df = _get_north_flow_summary()
    if df is None or df.empty:
        _print_empty_hint()
        return

    # 尝试找到净流入相关列
    net_flow_col = None
    for col in df.columns:
        if (
            "当日资金流入" in str(col)
            or "当日资金流" in str(col)
            or "当日净" in str(col)
            or "资金净流入" in str(col)
            or "净流入" in str(col)
        ):
            net_flow_col = col
            break

    if net_flow_col:
        df[net_flow_col] = pd.to_numeric(df[net_flow_col], errors="coerce")
        print(f"\n各通道当日净流入统计（单位：亿元）:")
        name_col = df.columns[0] if len(df.columns) > 0 else "名称"
        if name_col not in df.columns:
            name_col = df.columns[0]

        for _, row in df.iterrows():
            name = row.get(name_col, "未知")
            flow = row[net_flow_col]
            direction = "流入" if flow > 0 else "流出"
            print(f"  {name}: {abs(flow):.2f} 亿元 ({direction})")

        total = df[net_flow_col].sum()
        print(f"\n  合计: {total:.2f} 亿元")
    else:
        print(f"未找到净流入列，可用列: {df.columns.tolist()}")


def example_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 60)
    print("示例3: 错误处理示例")
    print("=" * 60)

    try:
        df = ak.stock_hsgt_fund_flow_summary_em()
        if df.empty:
            print("返回空DataFrame")
        else:
            print(f"获取到 {df.shape[0]} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic_usage()
    example_analyze_net_flow()
    example_error_handling()
