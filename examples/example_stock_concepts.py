"""
get_stock_concepts() 接口示例

演示如何获取个股所属概念板块。

注意: 当前配置中 get_stock_concepts 使用 stock_board_concept_cons_em 接口，
该接口实际上是通过概念名称获取成份股。要获取个股所属概念，建议先获取概念列表，
再通过 get_concept_stocks() 遍历各概念的成份股来构建反向映射。

替代方案:
    1. 使用 get_concept_list() 获取概念板块列表
    2. 使用 get_concept_stocks(concept_code) 获取某概念下的成份股
    3. 通过遍历概念列表并检查股票是否在其中，构建反向映射
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 获取概念板块列表
# ============================================================
def example_concept_list():
    """基本用法: 获取概念板块列表"""
    print("=" * 60)
    print("示例 1: 获取概念板块列表")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_list()
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print(f"\n概念板块数量: {len(df)}")
        print("\n前20个概念:")
        print(df.head(20))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 查找股票所属概念
# ============================================================
def example_find_stock_concepts():
    """通过遍历概念列表查找股票所属概念"""
    print("\n" + "=" * 60)
    print("示例 2: 查找股票所属概念")
    print("=" * 60)

    service = get_service()

    target_stock = "600519"
    matched_concepts = []

    try:
        # 获取概念列表
        concept_df = service.get_concept_list()
        if concept_df is None or concept_df.empty:
            print("概念列表为空")
            return

        # 确定概念名称字段
        name_col = None
        for col in ["name", "concept_name", "概念名称", "板块名称", "板块名称", "名称"]:
            if col in concept_df.columns:
                name_col = col
                break

        if name_col is None:
            print(f"无法确定概念名称字段，可用字段: {list(concept_df.columns)}")
            return

        print(f"正在检查 {len(concept_df)} 个概念板块...")
        print(f"目标股票: {target_stock}")

        # 遍历概念，检查目标股票是否在成份股中
        checked = 0
        for _, row in concept_df.iterrows():
            concept_name = str(row[name_col])
            try:
                stocks_df = service.get_concept_stocks(concept_code=concept_name)
                if stocks_df is not None and not stocks_df.empty:
                    if "symbol" in stocks_df.columns:
                        if (stocks_df["symbol"] == target_stock).any():
                            matched_concepts.append(concept_name)
                            print(f"  匹配: {concept_name}")
            except Exception:
                pass

            checked += 1
            if checked % 10 == 0:
                print(f"  已检查 {checked}/{len(concept_df)} 个概念...")

        if matched_concepts:
            print(f"\n{target_stock} 所属概念 ({len(matched_concepts)} 个):")
            for c in matched_concepts:
                print(f"  - {c}")
        else:
            print(f"\n{target_stock} 未在任何概念板块中找到")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 查看热门概念成份股
# ============================================================
def example_popular_concepts():
    """查看几个热门概念的成份股"""
    print("\n" + "=" * 60)
    print("示例 3: 热门概念成份股")
    print("=" * 60)

    service = get_service()

    popular_concepts = ["人工智能", "芯片", "新能源", "半导体", "医药"]

    for concept in popular_concepts:
        try:
            df = service.get_concept_stocks(concept_code=concept)
            if df is None or df.empty:
                print(f"\n{concept}: 无数据")
            else:
                print(f"\n{concept}: {len(df)} 只成份股")
                if "symbol" in df.columns:
                    print(f"  前5只: {df['symbol'].head(5).tolist()}")
        except Exception as e:
            print(f"\n{concept}: 获取失败 - {e}")


if __name__ == "__main__":
    example_concept_list()
    example_find_stock_concepts()
    example_popular_concepts()
