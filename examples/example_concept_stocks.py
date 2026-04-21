"""
get_concept_stocks() 接口示例

演示如何使用 akshare_data.get_concept_stocks() 获取概念板块成份股列表。

参数说明:
    concept_code: 概念板块名称，如 "人工智能"、"芯片"、"新能源"

返回字段: 包含股票代码、名称、所属概念等
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取概念股列表
# ============================================================
def example_basic():
    """基本用法: 获取人工智能概念股列表"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取人工智能概念股列表")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_stocks(concept_code="人工智能")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print(f"\n概念股数量: {len(df)}")
        print("\n前20只概念股:")
        print(df.head(20))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 多个概念对比
# ============================================================
def example_compare_concepts():
    """对比多个概念的成分股"""
    print("\n" + "=" * 60)
    print("示例 2: 多概念成分股对比")
    print("=" * 60)

    service = get_service()

    concepts = ["人工智能", "芯片", "新能源"]

    for concept in concepts:
        try:
            df = service.get_concept_stocks(concept_code=concept)
            if df is None or df.empty:
                print(f"\n{concept}: 无数据")
            else:
                print(f"\n{concept}: {len(df)} 只概念股")
                print(df.head(5))
        except Exception as e:
            print(f"\n{concept}: 获取失败 - {e}")


# ============================================================
# 示例 3: 概念股详情
# ============================================================
def example_details():
    """查看概念股详细信息"""
    print("\n" + "=" * 60)
    print("示例 3: 概念股详细信息")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_stocks(concept_code="芯片")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"字段列表: {list(df.columns)}")
        print(f"总概念股数: {len(df)}")

        if "symbol" in df.columns:
            print(f"\n股票代码示例: {df['symbol'].head(10).tolist()}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 筛选特定股票
# ============================================================
def example_filter_stock():
    """从概念股中筛选特定股票"""
    print("\n" + "=" * 60)
    print("示例 4: 筛选特定股票")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_stocks(concept_code="人工智能")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"人工智能概念股总数: {len(df)}")

        if "symbol" in df.columns:
            target = "600519"
            matches = df[df["symbol"].str.contains(target, na=False)]
            if not matches.empty:
                print(f"找到 {target}:")
                print(matches)

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_concepts()
    example_details()
    example_filter_stock()
