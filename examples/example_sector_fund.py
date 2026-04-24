"""
板块资金流向接口示例

演示如何使用 akshare 获取板块资金流向数据。

包含接口:
1. stock_fund_flow_industry(symbol) - 行业资金流向
   - symbol: "即时", "3日", "5日", "10日"

返回数据为 pandas DataFrame。

注意: 部分 akshare 接口依赖外部数据源，网络不可用时会自动跳过。
"""

import akshare as ak
import pandas as pd


def _mock_industry_fund_flow(symbol="即时"):
    return pd.DataFrame(
        {
            "行业": ["半导体", "银行", "食品饮料", "医药生物", "新能源车"],
            "主力净流入": [12.5, -8.2, 5.1, 3.8, 9.4],
            "阶段": [symbol] * 5,
        }
    )


def _call_industry_fund_flow(symbol="即时"):
    """调用 stock_fund_flow_industry，网络失败时返回 None"""
    try:
        df = ak.stock_fund_flow_industry(symbol=symbol)
        if df is None or df.empty:
            print("  (接口返回空数据，使用演示数据)")
            return _mock_industry_fund_flow(symbol=symbol)
        return df
    except Exception as e:
        print(f"  (网络请求失败: {e}，使用演示数据)")
        return _mock_industry_fund_flow(symbol=symbol)


# ============================================================
# 示例 1: 获取行业板块资金流向排名（即时）
# ============================================================
def example_industry_fund_flow():
    """获取行业板块资金流向排名数据（即时）"""
    print("=" * 60)
    print("示例 1: 行业板块资金流向排名（即时）")
    print("=" * 60)

    df = _call_industry_fund_flow(symbol="即时")
    if df is None or df.empty:
        print("无数据 (网络不可用或接口返回为空)")
        return

    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")
    print("\n前10行数据:")
    print(df.head(10).to_string(index=False))


# ============================================================
# 示例 2: 不同时间窗口的行业资金流向排名
# ============================================================
def example_fund_flow_with_period():
    """获取不同时间窗口的行业资金流向排名"""
    print("\n" + "=" * 60)
    print("示例 2: 不同时间窗口的行业资金流向排名")
    print("=" * 60)

    for symbol in ["即时", "3日", "5日", "10日"]:
        df = _call_industry_fund_flow(symbol=symbol)
        if df is not None and not df.empty:
            print(f"\n{symbol}排名:")
            print(f"  数据行数: {len(df)}")
            name_col = df.columns[0] if len(df.columns) > 0 else None
            if name_col:
                print(f"  前3名: {df[name_col].head(3).tolist()}")
        else:
            print(f"\n{symbol}: 无数据")


# ============================================================
# 示例 3: 数据分析 - 资金净流入排序
# ============================================================
def example_fund_flow_analysis():
    """对资金流向数据进行简单分析"""
    print("\n" + "=" * 60)
    print("示例 3: 行业板块资金流向分析")
    print("=" * 60)

    df = _call_industry_fund_flow(symbol="即时")
    if df is None or df.empty:
        print("无数据 (网络不可用或接口返回为空)")
        return

    # 尝试找到净流入相关列进行排序
    net_flow_col = None
    for col in df.columns:
        if "净流入" in str(col) or "net" in str(col).lower():
            net_flow_col = col
            break

    if net_flow_col:
        df[net_flow_col] = pd.to_numeric(df[net_flow_col], errors="coerce")
        top5 = df.nlargest(5, net_flow_col)
        print(f"行业板块资金净流入 Top5 (按 {net_flow_col} 排序):")
        print(top5.to_string(index=False))
    else:
        print("未找到净流入列，显示原始数据前5行:")
        print(df.head(5).to_string(index=False))


# ============================================================
# 示例 4: 错误处理示例
# ============================================================
def example_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 60)
    print("示例 4: 错误处理示例")
    print("=" * 60)

    # 测试正常调用
    try:
        df = ak.stock_fund_flow_industry(symbol="即时")
        if df.empty:
            print("返回空DataFrame")
        else:
            print(f"获取到 {df.shape[0]} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_industry_fund_flow()
    example_fund_flow_with_period()
    example_fund_flow_analysis()
    example_error_handling()
