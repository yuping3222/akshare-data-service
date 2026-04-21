"""
资产负债表接口示例 (get_balance_sheet)

演示如何使用 get_balance_sheet() 获取上市公司的资产负债表数据。

资产负债表反映企业在特定日期的财务状况，包括：
- 资产：流动资产、非流动资产
- 负债：流动负债、非流动负债
- 所有者权益：股本、资本公积、未分配利润等

使用方法:
    from akshare_data import get_service
    service = get_service()
    df = service.get_balance_sheet(symbol="600519")

注意: 资产负债表通常按报告期发布（季报、半年报、年报）。
"""

import pandas as pd
from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的资产负债表
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的资产负债表"""
    print("=" * 60)
    print("示例 1: 获取贵州茅台(600519)的资产负债表")
    print("=" * 60)

    service = get_service()

    try:
        # 获取资产负债表数据
        df = service.get_balance_sheet(symbol="600519")

        if df is None or df.empty:
            print("无数据 (数据源未返回结果或该股票暂无财报)")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据 (最新报告期):")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不同股票对比
# ============================================================
def example_multiple_stocks():
    """获取多只股票的资产负债表进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 获取多只银行股的资产负债表")
    print("=" * 60)

    service = get_service()

    # 银行股代码
    stocks = [
        ("600036", "招商银行"),
        ("601166", "兴业银行"),
        ("000001", "平安银行"),
    ]

    for code, name in stocks:
        try:
            df = service.get_balance_sheet(symbol=code)

            if df is None or df.empty:
                print(f"\n{name} ({code}): 无数据")
                continue

            print(f"\n{name} ({code}):")
            print(f"  数据形状: {df.shape}")
            print(f"  报告期数量: {len(df)}")

            # 显示最新一期的关键字段（如果存在）
            latest = df.iloc[-1] if not df.empty else None
            if latest is not None:
                # 尝试显示总资产
                total_assets_col = None
                for col in df.columns:
                    if "总资产" in col or "total_assets" in col.lower():
                        total_assets_col = col
                        break

                if total_assets_col:
                    assets = latest[total_assets_col]
                    print(f"  最新总资产: {assets:,.0f} 元")

        except Exception as e:
            print(f"\n{name} ({code}): 获取失败 - {e}")


# ============================================================
# 示例 3: 资产负债表分析 - 资产结构
# ============================================================
def example_asset_structure():
    """分析资产结构"""
    print("\n" + "=" * 60)
    print("示例 3: 分析贵州茅台的资产结构")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_balance_sheet(symbol="600519")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")

        # 查找可能的资产字段
        asset_cols = [col for col in df.columns if any(
            keyword in col for keyword in ["资产", "asset", "流动", "非流动"]
        )]

        print(f"\n资产相关字段 ({len(asset_cols)}个):")
        for col in asset_cols[:10]:  # 只显示前10个
            print(f"  - {col}")

        if len(asset_cols) > 10:
            print(f"  ... 还有 {len(asset_cols) - 10} 个字段")

        # 显示最新一期的数据
        print("\n最新报告期数据:")
        latest = df.iloc[-1]
        for col in asset_cols[:5]:
            if col in df.columns:
                value = latest[col]
                if pd.notna(value):
                    print(f"  {col}: {value:,.0f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 资产负债表分析 - 负债与权益
# ============================================================
def example_liability_equity():
    """分析负债和所有者权益"""
    print("\n" + "=" * 60)
    print("示例 4: 分析平安银行的负债与权益结构")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_balance_sheet(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")

        # 查找负债和权益字段
        liability_cols = [col for col in df.columns if any(
            keyword in col for keyword in ["负债", "liability", "借款", "应付"]
        )]
        equity_cols = [col for col in df.columns if any(
            keyword in col for keyword in ["权益", "equity", "股本", "公积", "利润"]
        )]

        print(f"\n负债相关字段 ({len(liability_cols)}个):")
        for col in liability_cols[:5]:
            print(f"  - {col}")

        print(f"\n权益相关字段 ({len(equity_cols)}个):")
        for col in equity_cols[:5]:
            print(f"  - {col}")

        # 显示最新数据
        latest = df.iloc[-1]
        print("\n最新报告期关键数据:")
        for col in liability_cols[:3]:
            if col in df.columns:
                value = latest[col]
                if pd.notna(value):
                    print(f"  {col}: {value:,.0f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 跨期比较分析
# ============================================================
def example_cross_period_analysis():
    """比较不同报告期的资产负债表变化"""
    print("\n" + "=" * 60)
    print("示例 5: 跨期比较 - 比亚迪资产负债表变化")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_balance_sheet(symbol="002594")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"报告期数量: {len(df)}")

        if len(df) < 2:
            print("数据不足，无法进行比较")
            return

        # 查找总资产字段
        total_assets_col = None
        for col in df.columns:
            if "总资产" in col or "total_assets" in col.lower():
                total_assets_col = col
                break

        if total_assets_col:
            print(f"\n使用字段: {total_assets_col}")
            print("\n总资产变化趋势:")

            # 显示最近5个报告期的数据
            recent_df = df.tail(5)
            for idx, row in recent_df.iterrows():
                assets = row[total_assets_col]
                # 尝试获取报告期日期
                date_col = None
                for dcol in ["报告期", "报告日期", "date", "period"]:
                    if dcol in df.columns:
                        date_col = dcol
                        break

                date_str = row[date_col] if date_col and date_col in df.columns else f"第{idx+1}期"
                print(f"  {date_str}: {assets:,.0f} 元")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 不同数据源对比
# ============================================================
def example_data_source_comparison():
    """比较不同数据源获取的资产负债表数据"""
    print("\n" + "=" * 60)
    print("示例 6: 不同数据源对比 - 宁德时代")
    print("=" * 60)

    service = get_service()

    # 默认数据源 (通常是 akshare)
    try:
        df = service.get_balance_sheet(symbol="300750")
        print(f"默认数据源:")
        print(f"  数据形状: {df.shape}")
        print(f"  字段数量: {len(df.columns)}")
        if df is not None and not df.empty:
            print(f"  最新报告期字段示例: {list(df.columns)[:5]}")
    except Exception as e:
        print(f"默认数据源获取失败: {e}")


# ============================================================
# 示例 7: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 7: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效代码
    print("\n测试 1: 无效股票代码")
    try:
        df = service.get_balance_sheet(symbol="999999")  # 不存在的代码
        if df is None or df.empty:
            print("  结果: 返回空数据 (代码可能无效)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试特殊格式代码
    print("\n测试 2: 带前缀的股票代码")
    try:
        df = service.get_balance_sheet(symbol="sh600519")
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_stocks()
    example_asset_structure()
    example_liability_equity()
    example_cross_period_analysis()
    example_data_source_comparison()
    example_error_handling()
