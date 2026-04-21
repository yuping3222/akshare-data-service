"""
现金流量表接口示例 (get_cash_flow)

演示如何使用 get_cash_flow() 获取上市公司的现金流量表数据。

现金流量表反映企业在一定会计期间的现金及现金等价物流入和流出，包括：
- 经营活动现金流
- 投资活动现金流
- 筹资活动现金流
- 现金及现金等价物净增加额

使用方法:
    from akshare_data import get_service
    service = get_service()
    df = service.get_cash_flow(date="20240331")

注意: 现金流量表按指定报告期返回全市场数据，akshare 函数 stock_xjll_em
接受 date 参数（格式 YYYYMMDD），如 "20240331"、"20240630" 等。
返回的 DataFrame 包含该报告期所有上市公司的现金流量表数据，
可通过股票代码字段筛选指定个股。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定报告期的现金流量表
# ============================================================
def example_basic():
    """基本用法: 获取2024年Q1报告期的现金流量表（全市场）"""
    print("=" * 60)
    print("示例 1: 获取2024年Q1报告期的现金流量表（全市场）")
    print("=" * 60)

    service = get_service()

    try:
        # date: 报告期日期，格式 "YYYYMMDD"
        # 常见报告期: "20240331"(Q1), "20240630"(中报), "20240930"(Q3), "20241231"(年报)
        df = service.get_cash_flow(date="20240331")

        if df is None or df.empty:
            print("无数据 (数据源未返回结果)")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 筛选指定股票的现金流数据
# ============================================================
def example_filter_by_stock():
    """从全市场数据中筛选指定股票的现金流"""
    print("\n" + "=" * 60)
    print("示例 2: 从2024年中报中筛选银行股现金流数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_cash_flow(date="20240630")

        if df is None or df.empty:
            print("无数据")
            return

        # 查找股票代码字段
        symbol_col = None
        for col in df.columns:
            if any(kw in col for kw in ["代码", "symbol", "stock"]):
                symbol_col = col
                break

        if not symbol_col:
            print("未找到股票代码字段")
            print(f"可用字段: {list(df.columns)}")
            return

        # 筛选银行股
        stocks = {"600036": "招商银行", "000001": "平安银行", "601166": "兴业银行"}
        for code, name in stocks.items():
            stock_df = df[df[symbol_col].astype(str).str.contains(code, na=False)]
            if not stock_df.empty:
                print(f"\n{name} ({code}):")
                print(stock_df.to_string(index=False))
            else:
                print(f"\n{name} ({code}): 未找到数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 现金流结构分析
# ============================================================
def example_cash_flow_structure():
    """分析现金流结构"""
    print("\n" + "=" * 60)
    print("示例 3: 分析报告期现金流结构")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_cash_flow(date="20240331")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"报告期公司数量: {len(df)}")

        # 查找三类现金流字段
        operating_col = None
        investing_col = None
        financing_col = None

        for col in df.columns:
            if not operating_col and any(kw in col for kw in ["经营", "operating"]):
                operating_col = col
            if not investing_col and any(kw in col for kw in ["投资", "investing"]):
                investing_col = col
            if not financing_col and any(kw in col for kw in ["筹资", "融资", "financing"]):
                financing_col = col

        print("\n识别的现金流字段:")
        print(f"  经营活动: {operating_col or '未找到'}")
        print(f"  投资活动: {investing_col or '未找到'}")
        print(f"  筹资活动: {financing_col or '未找到'}")

        # 统计经营现金流为正的公司数量
        if operating_col:
            positive_count = (df[operating_col] > 0).sum()
            print(f"\n经营现金流为正的公司: {positive_count}/{len(df)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 不同报告期对比
# ============================================================
def example_compare_periods():
    """对比不同报告期的现金流变化"""
    print("\n" + "=" * 60)
    print("示例 4: 对比2023年年报与2024年中报")
    print("=" * 60)

    service = get_service()

    dates = {
        "20231231": "2023年年报",
        "20240630": "2024年中报",
    }

    for date, label in dates.items():
        try:
            df = service.get_cash_flow(date=date)

            if df is None or df.empty:
                print(f"\n{label}: 无数据")
                continue

            print(f"\n{label}: 共 {len(df)} 条记录")
            print(f"字段数量: {len(df.columns)}")

        except Exception as e:
            print(f"\n{label}: 获取失败 - {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试正常报告期
    print("\n测试 1: 正常报告期")
    try:
        df = service.get_cash_flow(date="20240331")
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试不同报告期
    print("\n测试 2: 不同报告期")
    for date in ["20231231", "20220630"]:
        try:
            df = service.get_cash_flow(date=date)
            if df is None or df.empty:
                print(f"  {date}: 返回空数据")
            else:
                print(f"  {date}: 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"  {date}: 捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_filter_by_stock()
    example_cash_flow_structure()
    example_compare_periods()
    example_error_handling()
