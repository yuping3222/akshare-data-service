"""
财务指标接口示例 (get_financial_metrics)

演示如何使用 get_financial_metrics() 获取上市公司的综合财务指标数据。

财务指标包括：
- 估值指标：PE（市盈率）、PB（市净率）、PS（市销率）
- 盈利能力：ROE（净资产收益率）、ROA（总资产收益率）
- 财务健康度：资产负债率、流动比率等
- 成长性指标：收入增长率、利润增长率等

使用方法:
    from akshare_data import get_service
    service = get_service()
    df = service.get_financial_metrics(symbol="600519")

注意: 财务指标通常按报告期计算，不同数据源提供的指标可能略有差异。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的财务指标
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的财务指标"""
    print("=" * 60)
    print("示例 1: 获取贵州茅台(600519)的财务指标")
    print("=" * 60)

    service = get_service()

    try:
        # 获取财务指标数据
        df = service.get_financial_metrics(symbol="600519")

        if df is None or df.empty:
            print("无数据 (数据源未返回结果或该股票暂无数据)")
            print("提示: get_financial_metrics 需要 LIXINGER_TOKEN 环境变量已配置")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据 (最新):")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 估值指标分析
# ============================================================
def example_valuation_metrics():
    """分析估值指标：PE、PB、PS"""
    print("\n" + "=" * 60)
    print("示例 2: 分析多只股票的估值指标")
    print("=" * 60)

    service = get_service()

    # 选择几只不同行业的股票
    stocks = [
        ("600519", "贵州茅台"),
        ("000858", "五粮液"),
        ("600036", "招商银行"),
    ]

    for code, name in stocks:
        try:
            df = service.get_financial_metrics(symbol=code)

            if df is None or df.empty:
                print(f"\n{name} ({code}): 无数据")
                continue

            print(f"\n{name} ({code}):")
            print(f"  数据形状: {df.shape}")

            # 查找估值指标字段
            latest = df.iloc[-1]

            # PE
            pe_col = None
            for col in df.columns:
                if any(kw in col.lower() for kw in ["pe", "市盈率"]):
                    pe_col = col
                    break

            # PB
            pb_col = None
            for col in df.columns:
                if any(kw in col.lower() for kw in ["pb", "市净率"]):
                    pb_col = col
                    break

            # ROE
            roe_col = None
            for col in df.columns:
                if any(kw in col.lower() for kw in ["roe", "净资产收益率"]):
                    roe_col = col
                    break

            print("  最新估值指标:")
            if pe_col and pe_col in df.columns:
                pe = latest[pe_col]
                if pd.notna(pe):
                    print(f"    PE (市盈率): {pe:.2f}")

            if pb_col and pb_col in df.columns:
                pb = latest[pb_col]
                if pd.notna(pb):
                    print(f"    PB (市净率): {pb:.2f}")

            if roe_col and roe_col in df.columns:
                roe = latest[roe_col]
                if pd.notna(roe):
                    print(f"    ROE: {roe:.2f}%")

        except Exception as e:
            print(f"\n{name} ({code}): 获取失败 - {e}")


# ============================================================
# 示例 3: 盈利能力分析
# ============================================================
def example_profitability():
    """分析盈利能力指标"""
    print("\n" + "=" * 60)
    print("示例 3: 分析比亚迪的盈利能力指标")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_financial_metrics(symbol="002594")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 查找盈利能力相关字段
        profit_cols = [col for col in df.columns if any(
            kw in col.lower() for kw in ["roe", "roa", "profit", "margin", "盈利"]
        )]

        print(f"\n盈利能力相关字段 ({len(profit_cols)}个):")
        for col in profit_cols:
            print(f"  - {col}")

        # 显示最新数据
        if profit_cols:
            latest = df.iloc[-1]
            print("\n最新报告期盈利能力指标:")
            for col in profit_cols[:5]:
                if col in df.columns:
                    value = latest[col]
                    if pd.notna(value):
                        print(f"  {col}: {value:.2f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 财务健康度分析
# ============================================================
def example_financial_health():
    """分析财务健康度指标"""
    print("\n" + "=" * 60)
    print("示例 4: 分析宁德时代的财务健康度")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_financial_metrics(symbol="300750")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")

        # 查找财务健康相关字段
        health_cols = [col for col in df.columns if any(
            kw in col.lower() for kw in ["debt", "ratio", "rate", "资产", "负债", "比率"]
        )]

        print(f"\n财务健康相关字段 ({len(health_cols)}个):")
        for col in health_cols[:10]:
            print(f"  - {col}")

        # 显示最新数据
        if health_cols:
            latest = df.iloc[-1]
            print("\n最新报告期财务健康指标:")
            for col in health_cols[:5]:
                if col in df.columns:
                    value = latest[col]
                    if pd.notna(value):
                        if isinstance(value, (int, float)):
                            print(f"  {col}: {value:.2f}")
                        else:
                            print(f"  {col}: {value}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 多期趋势分析
# ============================================================
def example_trend_analysis():
    """分析财务指标的多期趋势"""
    print("\n" + "=" * 60)
    print("示例 5: 分析平安银行的ROE趋势")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_financial_metrics(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"报告期数量: {len(df)}")

        # 查找ROE字段
        roe_col = None
        for col in df.columns:
            if "roe" in col.lower() or "净资产收益率" in col:
                roe_col = col
                break

        if not roe_col:
            print("未找到ROE字段")
            return

        print(f"\n使用字段: {roe_col}")
        print("\nROE历史趋势 (最近6个报告期):")

        # 显示最近6个报告期
        recent_df = df.tail(6)
        for idx, row in recent_df.iterrows():
            roe = row[roe_col]
            if pd.notna(roe):
                print(f"  第{idx+1}期: {roe:.2f}%")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 同行业对比
# ============================================================
def example_industry_comparison():
    """同行业公司财务指标对比"""
    print("\n" + "=" * 60)
    print("示例 6: 银行业财务指标对比")
    print("=" * 60)

    service = get_service()

    banks = [
        ("600036", "招商银行"),
        ("601166", "兴业银行"),
        ("000001", "平安银行"),
        ("600000", "浦发银行"),
    ]

    print("\n银行 | PE | PB | ROE")
    print("-" * 50)

    for code, name in banks:
        try:
            df = service.get_financial_metrics(symbol=code)

            if df is None or df.empty:
                print(f"{name:8s} | 无数据")
                continue

            latest = df.iloc[-1]

            # 查找指标
            pe = "N/A"
            pb = "N/A"
            roe = "N/A"

            for col in df.columns:
                if pe == "N/A" and any(kw in col.lower() for kw in ["pe", "市盈率"]):
                    val = latest[col]
                    if pd.notna(val):
                        pe = f"{val:.2f}"

                if pb == "N/A" and any(kw in col.lower() for kw in ["pb", "市净率"]):
                    val = latest[col]
                    if pd.notna(val):
                        pb = f"{val:.2f}"

                if roe == "N/A" and any(kw in col.lower() for kw in ["roe", "净资产收益率"]):
                    val = latest[col]
                    if pd.notna(val):
                        roe = f"{val:.2f}%"

            print(f"{name:8s} | {pe:6s} | {pb:6s} | {roe:8s}")

        except Exception as e:
            print(f"{name:8s} | 获取失败: {e}")


# ============================================================
# 示例 7: 综合展示所有字段
# ============================================================
def example_all_fields():
    """展示所有可用的财务指标字段"""
    print("\n" + "=" * 60)
    print("示例 7: 贵州茅台所有财务指标字段")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_financial_metrics(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"\n所有字段 ({len(df.columns)}个):")

        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")

        # 显示最新一期的完整数据
        print("\n最新报告期所有指标:")
        latest = df.iloc[-1]
        for col in df.columns:
            value = latest[col]
            if pd.notna(value):
                if isinstance(value, (int, float)):
                    print(f"  {col}: {value:.4f}")
                else:
                    print(f"  {col}: {value}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 8: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 8: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效代码
    print("\n测试 1: 无效股票代码")
    try:
        df = service.get_financial_metrics(symbol="999999")
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试带前缀的代码
    print("\n测试 2: 带前缀的股票代码")
    try:
        df = service.get_financial_metrics(symbol="sh600519")
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_valuation_metrics()
    example_profitability()
    example_financial_health()
    example_trend_analysis()
    example_industry_comparison()
    example_all_fields()
    example_error_handling()
