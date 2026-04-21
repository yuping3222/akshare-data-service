"""
get_concept_performance() 接口示例

演示如何使用 akshare_data.get_concept_performance() 获取概念板块行情历史数据。

参数说明:
    symbol: 概念板块名称，如 "人工智能"、"芯片"、"新能源" (必需参数)
    start_date: 起始日期，格式 "YYYYMMDD" (可选，默认 "20220101")
    end_date: 结束日期，格式 "YYYYMMDD" (可选，默认 "20221128")
    period: K线周期，默认 "daily"

返回: pd.DataFrame，包含概念板块的行情历史数据
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取概念板块行情
# ============================================================
def example_basic():
    """基本用法: 获取人工智能概念板块行情"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取人工智能概念行情")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_performance(
            symbol="人工智能",
            start_date="20240101",
            end_date="20240601",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n概念板块行情:")
        print(df.head(20))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不同概念板块对比
# ============================================================
def example_compare_concepts():
    """对比不同概念板块的行情"""
    print("\n" + "=" * 60)
    print("示例 2: 不同概念板块行情对比")
    print("=" * 60)

    service = get_service()

    concepts = ["人工智能", "芯片", "新能源"]
    start_date = "20240101"
    end_date = "20240301"

    for concept in concepts:
        try:
            df = service.get_concept_performance(
                symbol=concept,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                print(f"\n{concept}: 无数据")
            else:
                print(f"\n{concept}: {len(df)} 条记录")
                print(df.head(5))
        except Exception as e:
            print(f"\n{concept}: 获取失败 - {e}")


# ============================================================
# 示例 3: 涨跌幅分析
# ============================================================
def example_change_analysis():
    """分析概念板块的涨跌幅"""
    print("\n" + "=" * 60)
    print("示例 3: 涨跌幅分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_performance(
            symbol="人工智能",
            start_date="20240101",
            end_date="20240601",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"字段列表: {list(df.columns)}")

        # 查找涨跌幅字段
        change_col = None
        for col in ["change_pct", "涨跌幅", "pct_change", "change"]:
            if col in df.columns:
                change_col = col
                break

        if change_col:
            top5 = df.nlargest(5, change_col)
            print(f"\n涨幅前5:")
            print(top5)

            bottom5 = df.nsmallest(5, change_col)
            print(f"\n跌幅前5:")
            print(bottom5)

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 统计分析
# ============================================================
def example_statistics():
    """对概念行情进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 概念行情统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_performance(
            symbol="芯片",
            start_date="20240101",
            end_date="20240601",
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 条记录")
        print(f"字段列表: {list(df.columns)}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_concepts()
    example_change_analysis()
    example_statistics()
