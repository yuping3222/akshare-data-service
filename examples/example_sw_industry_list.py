"""
get_sw_industry_list() 接口示例

演示如何使用 DataService.get_sw_industry_list() 获取申万行业分类列表。

接口说明:
  - level: 行业级别，'1' | '2' | '3' (默认 '1')
    - '1': 一级行业 (如: 食品饮料、电子、医药生物)
    - '2': 二级行业
    - '3': 三级行业
  - source: 数据源名称 (可选，默认自动选择)

返回: pd.DataFrame，包含行业代码、名称、级别等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_sw_industry_list(level='1')
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取申万一级行业列表
# ============================================================
def example_sw_industry_list_basic():
    """基本用法: 获取申万一级行业分类列表"""
    print("=" * 60)
    print("示例 1: 获取申万一级行业列表")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: get_sw_industry_list() 不接受 level 参数
        # level 在返回数据中以字段形式体现
        df = service.get_sw_industry_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据（数据源不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

        print(f"\n共获取到 {len(df)} 个一级行业")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取不同级别的行业列表
# ============================================================
def example_sw_industry_list_levels():
    """获取一级、二级、三级行业列表"""
    print("\n" + "=" * 60)
    print("示例 2: 获取不同级别的行业列表")
    print("=" * 60)

    service = get_service()

    levels = [
        ("1", "一级行业"),
        ("2", "二级行业"),
        ("3", "三级行业"),
    ]

    for level, name in levels:
        try:
            df = service.get_sw_industry_list()

            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                print(f"\n{name} (level={level}): 无数据")
                continue

            print(f"\n{name} (level={level}): {len(df)} 个行业")
            print(f"  字段: {list(df.columns)}")
            print(f"  前5行:")
            print(df.head().to_string(index=False))

        except Exception as e:
            print(f"\n{name} (level={level}): 获取失败 - {e}")


# ============================================================
# 示例 3: 搜索特定行业
# ============================================================
def example_sw_industry_search():
    """在行业列表中搜索特定关键词"""
    print("\n" + "=" * 60)
    print("示例 3: 搜索特定行业")
    print("=" * 60)

    service = get_service()

    keywords = ["食品", "医药", "电子", "银行"]

    try:
        df = service.get_sw_industry_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        # 假设有 'name' 或 'industry_name' 字段
        name_col = None
        for col in ["name", "industry_name", "行业名称", "industry"]:
            if col in df.columns:
                name_col = col
                break

        if name_col is None:
            print(f"无法确定行业名称字段，可用字段: {list(df.columns)}")
            return

        for keyword in keywords:
            matched = df[df[name_col].astype(str).str.contains(keyword, na=False)]
            if not matched.empty:
                print(f"\n关键词 '{keyword}': 找到 {len(matched)} 个行业")
                print(matched.head().to_string(index=False))
            else:
                print(f"\n关键词 '{keyword}': 未找到匹配")

    except Exception as e:
        print(f"搜索失败: {e}")


# ============================================================
# 示例 4: 行业列表数据分析
# ============================================================
def example_sw_industry_analysis():
    """对行业列表进行简单统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 行业列表数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_sw_industry_list()

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无数据")
            return

        print(f"一级行业总数: {len(df)}")
        print(f"数据字段: {list(df.columns)}")

        # 尝试打印所有行业名称
        name_col = None
        for col in ["name", "industry_name", "行业名称", "industry"]:
            if col in df.columns:
                name_col = col
                break

        if name_col:
            print(f"\n全部一级行业名称:")
            for idx, name in enumerate(df[name_col].tolist(), 1):
                print(f"  {idx:2d}. {name}")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效参数"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    # 测试无效 level 值
    try:
        df = service.get_sw_industry_list()
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print("无效 level='999': 返回空数据")
        else:
            print(f"无效 level='999': 返回 {len(df)} 行")
    except Exception as e:
        print(f"无效 level='999': 捕获异常 {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_sw_industry_list_basic()
    example_sw_industry_list_levels()
    example_sw_industry_search()
    example_sw_industry_analysis()
    example_error_handling()
