"""
利润表接口示例 (get_income_statement)

演示如何使用 get_income_statement() 获取上市公司的利润表数据。

利润表反映企业在一定会计期间的经营成果，包括：
- 营业收入
- 营业成本
- 营业利润
- 净利润
- 每股收益等

使用方法:
    from akshare_data import get_service
    service = get_service()
    df = service.get_income_statement(date="20240331")

注意: 利润表按指定报告期返回全市场数据，akshare 函数 stock_lrb_em
接受 date 参数（格式 YYYYMMDD），如 "20240331"、"20240630" 等。
返回的 DataFrame 包含该报告期所有上市公司的利润表数据，
可通过股票代码字段筛选指定个股。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取指定报告期的利润表
# ============================================================
def example_basic():
    """基本用法: 获取2024年Q1报告期的利润表（全市场）"""
    print("=" * 60)
    print("示例 1: 获取2024年Q1报告期的利润表（全市场）")
    print("=" * 60)

    service = get_service()

    try:
        # date: 报告期日期，格式 "YYYYMMDD"
        # 常见报告期: "20240331"(Q1), "20240630"(中报), "20240930"(Q3), "20241231"(年报)
        df = service.get_income_statement(date="20240331")

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
# 示例 2: 筛选指定股票的利润表
# ============================================================
def example_filter_by_stock():
    """从全市场数据中筛选指定股票的利润表"""
    print("\n" + "=" * 60)
    print("示例 2: 从2024年中报中筛选白酒股利润数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_income_statement(date="20240630")

        if df is None or df.empty:
            print("无数据")
            return

        # 查找股票代码字段
        symbol_col = None
        for col in df.columns:
            if any(kw in col for kw in ["代码", "symbol", "代码", "stock"]):
                symbol_col = col
                break

        if not symbol_col:
            print("未找到股票代码字段")
            print(f"可用字段: {list(df.columns)}")
            return

        # 筛选白酒股
        stocks = {"600519": "贵州茅台", "000858": "五粮液", "000568": "泸州老窖"}
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
# 示例 3: 不同报告期对比
# ============================================================
def example_compare_periods():
    """对比不同报告期的利润表数据"""
    print("\n" + "=" * 60)
    print("示例 3: 对比2023年年报与2024年中报")
    print("=" * 60)

    service = get_service()

    dates = {
        "20231231": "2023年年报",
        "20240630": "2024年中报",
    }

    for date, label in dates.items():
        try:
            df = service.get_income_statement(date=date)

            if df is None or df.empty:
                print(f"\n{label}: 无数据")
                continue

            print(f"\n{label}: 共 {len(df)} 条记录")
            print(f"字段数量: {len(df.columns)}")

        except Exception as e:
            print(f"\n{label}: 获取失败 - {e}")


# ============================================================
# 示例 4: 利润率分析
# ============================================================
def example_profit_margin():
    """计算利润率等指标"""
    print("\n" + "=" * 60)
    print("示例 4: 分析报告期利润数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_income_statement(date="20240331")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")

        # 查找关键字段
        revenue_col = None
        profit_col = None
        symbol_col = None

        for col in df.columns:
            if not revenue_col and any(kw in col for kw in ["营业总收入", "营业收入", "利息收入"]):
                revenue_col = col
            if not profit_col and any(kw in col for kw in ["净利润", "归属于母公司"]):
                profit_col = col
            if not symbol_col and any(kw in col for kw in ["代码", "symbol"]):
                symbol_col = col

        print(f"\n识别字段:")
        print(f"  收入字段: {revenue_col or '未找到'}")
        print(f"  利润字段: {profit_col or '未找到'}")
        print(f"  代码字段: {symbol_col or '未找到'}")

        if revenue_col and profit_col:
            # 筛选收入前10的公司
            df_valid = df[df[revenue_col].notna() & df[profit_col].notna()].copy()
            if not df_valid.empty:
                df_valid["net_margin"] = df_valid[profit_col] / df_valid[revenue_col] * 100
                top10 = df_valid.nlargest(10, revenue_col)
                print("\n营业收入前10家公司:")
                for _, row in top10.iterrows():
                    code = row[symbol_col] if symbol_col else "N/A"
                    margin = row["net_margin"]
                    rev = row[revenue_col]
                    profit = row[profit_col]
                    print(f"  {code}: 收入={rev:,.0f}, 净利润={profit:,.0f}, 净利率={margin:.2f}%")

    except Exception as e:
        print(f"获取数据失败: {e}")


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
        df = service.get_income_statement(date="20240331")
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试不同报告期格式
    print("\n测试 2: 不同日期格式")
    for date in ["20231231", "20220630"]:
        try:
            df = service.get_income_statement(date=date)
            if df is None or df.empty:
                print(f"  {date}: 返回空数据")
            else:
                print(f"  {date}: 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"  {date}: 捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_filter_by_stock()
    example_compare_periods()
    example_profit_margin()
    example_error_handling()
