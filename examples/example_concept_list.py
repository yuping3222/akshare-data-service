"""
get_concept_list() 接口示例

演示如何使用 DataService.get_concept_list() 获取A股概念板块列表。

接口说明:
  - source: 数据源名称 (可选，默认自动选择)

返回: pd.DataFrame，包含概念代码、名称等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_concept_list()
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取概念板块列表
# ============================================================
def example_concept_list_basic():
    """基本用法: 获取全部概念板块列表"""
    print("=" * 60)
    print("示例 1: 获取概念板块列表")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据（数据源不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前15行数据:")
        print(df.head(15).to_string(index=False))

        print(f"\n共获取到 {len(df)} 个概念板块")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 搜索特定概念
# ============================================================
def example_concept_list_search():
    """在概念列表中搜索特定关键词"""
    print("\n" + "=" * 60)
    print("示例 2: 搜索特定概念")
    print("=" * 60)

    service = get_service()

    keywords = ["人工智能", "新能源", "芯片", "医药", "消费"]

    try:
        df = service.get_concept_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        # 查找概念名称字段
        name_col = None
        for col in ["name", "concept_name", "概念名称", "concept"]:
            if col in df.columns:
                name_col = col
                break

        if name_col is None:
            print(f"无法确定概念名称字段，可用字段: {list(df.columns)}")
            return

        for keyword in keywords:
            matched = df[df[name_col].astype(str).str.contains(keyword, na=False)]
            if not matched.empty:
                print(f"\n关键词 '{keyword}': 找到 {len(matched)} 个概念")
                print(matched.head().to_string(index=False))
            else:
                print(f"\n关键词 '{keyword}': 未找到匹配")

    except Exception as e:
        print(f"搜索失败: {e}")


# ============================================================
# 示例 3: 概念板块分类统计
# ============================================================
def example_concept_list_stats():
    """对概念板块列表进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 3: 概念板块统计")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        print(f"概念板块总数: {len(df)}")
        print(f"数据字段: {list(df.columns)}")

        # 尝试显示所有概念名称
        name_col = None
        for col in ["name", "concept_name", "概念名称", "concept"]:
            if col in df.columns:
                name_col = col
                break

        if name_col:
            print(f"\n全部概念板块 (前30个):")
            for idx, name in enumerate(df[name_col].head(30).tolist(), 1):
                print(f"  {idx:2d}. {name}")

            if len(df) > 30:
                print(f"  ... 还有 {len(df) - 30} 个概念")

    except Exception as e:
        print(f"统计失败: {e}")


# ============================================================
# 示例 4: 获取概念代码映射
# ============================================================
def example_concept_list_mapping():
    """建立概念名称到代码的映射"""
    print("\n" + "=" * 60)
    print("示例 4: 概念名称到代码映射")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        # 查找字段
        code_col = None
        name_col = None
        for col in df.columns:
            if code_col is None and col.lower() in ["code", "concept_code", "概念代码"]:
                code_col = col
            if name_col is None and col.lower() in ["name", "concept_name", "概念名称", "concept"]:
                name_col = col

        if code_col and name_col:
            mapping = dict(zip(df[name_col], df[code_col]))
            print(f"建立了 {len(mapping)} 个概念的名称->代码映射")

            # 展示前10个
            print("\n前10个映射:")
            for idx, (name, code) in enumerate(list(mapping.items())[:10], 1):
                print(f"  {idx:2d}. {name} -> {code}")
        else:
            print(f"未找到 code/name 字段，可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"建立映射失败: {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_concept_list()
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("概念列表为空 (数据源可能不可用)")
        else:
            print(f"成功获取 {len(df)} 个概念板块")
    except Exception as e:
        print(f"获取概念列表失败: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_concept_list_basic()
    example_concept_list_search()
    example_concept_list_stats()
    example_concept_list_mapping()
    example_error_handling()
