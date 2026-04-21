"""
get_fund_open_daily() 接口示例

演示如何使用 DataService.get_fund_open_daily() 获取所有开放式基金每日净值列表。

接口说明:
- get_fund_open_daily(): 获取全部开放式基金的当日净值列表
- 无必需参数
- 返回: pd.DataFrame，包含基金代码、名称、净值等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_fund_open_daily()

注意:
- 该接口返回全市场开放式基金的最新净值快照
- 数据量较大，首次获取可能需要一些时间
- 采用 Cache-First 策略，后续请求会直接返回缓存数据
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取全部开放式基金每日净值列表
# ============================================================
def example_basic():
    """基本用法: 获取全部开放式基金每日净值列表"""
    print("=" * 60)
    print("示例 1: 获取全部开放式基金每日净值列表")
    print("=" * 60)

    service = get_service()

    try:
        # 获取全部开放式基金最新净值列表
        df = service.get_fund_open_daily()

        if df is None or df.empty:
            print("无数据 (数据源未返回结果，可能是网络问题或非交易日)")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"基金数量: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail().to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 筛选特定类型的基金
# ============================================================
def example_filter_by_type():
    """获取基金列表后按类型筛选"""
    print("\n" + "=" * 60)
    print("示例 2: 按基金类型筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_fund_open_daily()

        if df is None or df.empty:
            print("无数据")
            return

        # 尝试按常见字段筛选 (字段名可能因数据源而异)
        print(f"总基金数量: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 如果存在基金类型列，进行筛选演示
        type_col = None
        for col in df.columns:
            if "类型" in str(col) or "type" in str(col).lower() or "类别" in str(col):
                type_col = col
                break

        if type_col:
            print(f"\n基金类型分布:")
            print(df[type_col].value_counts().head(10))
        else:
            print("\n当前数据源返回的字段中未发现基金类型列")
            print("可尝试按其他字段筛选，如净值范围等")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 查找特定基金
# ============================================================
def example_find_specific_fund():
    """在基金列表中查找特定基金"""
    print("\n" + "=" * 60)
    print("示例 3: 查找特定基金")
    print("=" * 60)

    service = get_service()

    # 要查找的基金代码
    target_code = "110011"

    try:
        df = service.get_fund_open_daily()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"在 {len(df)} 只基金中查找代码: {target_code}")

        # 尝试在不同列中查找基金代码
        code_col = None
        for col in df.columns:
            if "代码" in str(col) or "code" in str(col).lower() or "symbol" in str(col).lower():
                code_col = col
                break

        if code_col:
            matched = df[df[code_col].astype(str).str.contains(target_code, na=False)]
            if not matched.empty:
                print(f"\n找到匹配基金:")
                print(matched.to_string(index=False))
            else:
                print(f"\n未找到代码为 {target_code} 的基金")
        else:
            print("当前数据未包含基金代码字段，无法按代码筛选")
            print("\n显示前5行数据供参考:")
            print(df.head().to_string(index=False))

    except Exception as e:
        print(f"查找失败: {e}")


# ============================================================
# 示例 4: 净值统计分析
# ============================================================
def example_nav_statistics():
    """对基金净值进行简单统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 净值统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_fund_open_daily()

        if df is None or df.empty:
            print("无数据")
            return

        # 查找净值相关列
        nav_col = None
        for col in df.columns:
            if "净值" in str(col) or "nav" in str(col).lower() or "value" in str(col).lower():
                if df[col].dtype in ["float64", "int64"]:
                    nav_col = col
                    break

        if nav_col:
            print(f"使用净值列: {nav_col}")
            print(f"\n净值统计:")
            print(f"  平均值: {df[nav_col].mean():.4f}")
            print(f"  中位数: {df[nav_col].median():.4f}")
            print(f"  最大值: {df[nav_col].max():.4f}")
            print(f"  最小值: {df[nav_col].min():.4f}")

            # 净值分布
            print(f"\n净值分布 (分位数):")
            print(df[nav_col].quantile([0.1, 0.25, 0.5, 0.75, 0.9]).to_string())
        else:
            print("当前数据中未发现数值型净值字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"统计分析失败: {e}")


# ============================================================
# 示例 5: 获取多只基金的净值对比
# ============================================================
def example_multiple_funds_comparison():
    """对比多只基金的净值数据"""
    print("\n" + "=" * 60)
    print("示例 5: 多只基金净值对比")
    print("=" * 60)

    service = get_service()

    # 目标基金代码列表
    target_codes = ["110011", "000001", "161725"]

    try:
        df = service.get_fund_open_daily()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"基金代码列表: {target_codes}")
        print(f"总基金数量: {len(df)}")

        # 查找代码列
        code_col = None
        for col in df.columns:
            if "代码" in str(col) or "code" in str(col).lower():
                code_col = col
                break

        if code_col:
            for code in target_codes:
                matched = df[df[code_col].astype(str) == code]
                if not matched.empty:
                    print(f"\n基金 {code}:")
                    print(matched.to_string(index=False))
                else:
                    print(f"\n基金 {code}: 未找到")
        else:
            print("未找到基金代码字段，显示前5行:")
            print(df.head().to_string(index=False))

    except Exception as e:
        print(f"对比分析失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_filter_by_type()
    example_find_specific_fund()
    example_nav_statistics()
    example_multiple_funds_comparison()
