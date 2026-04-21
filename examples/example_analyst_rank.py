"""
get_analyst_rank() 接口示例

演示如何使用 akshare_data.get_analyst_rank() 获取分析师排名数据。

参数说明:
    start_date: 开始日期，格式 "YYYY-MM-DD"
    end_date:   结束日期，格式 "YYYY-MM-DD"

返回字段: 包含分析师姓名、所属机构、评级次数、评级准确率等
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取分析师排名
# ============================================================
def example_basic():
    """基本用法: 获取分析师排名数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取分析师排名数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_analyst_rank(
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n前10名分析师:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不同时间区间
# ============================================================
def example_date_ranges():
    """获取不同时间区间的分析师排名"""
    print("\n" + "=" * 60)
    print("示例 2: 不同时间区间分析师排名")
    print("=" * 60)

    service = get_service()

    ranges = [
        ("2024-01-01", "2024-06-30"),
        ("2024-07-01", "2024-12-31"),
    ]

    for start, end in ranges:
        try:
            df = service.get_analyst_rank(start_date=start, end_date=end)
            if df is None or df.empty:
                print(f"\n{start} ~ {end}: 无数据")
            else:
                print(f"\n{start} ~ {end}: {len(df)} 位分析师")
                print(df.head(5))
        except Exception as e:
            print(f"\n{start} ~ {end}: 获取失败 - {e}")


# ============================================================
# 示例 3: 筛选特定机构分析师
# ============================================================
def example_filter_institution():
    """筛选特定机构的分析师"""
    print("\n" + "=" * 60)
    print("示例 3: 筛选特定机构分析师")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_analyst_rank(
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        if df is None or df.empty:
            print("无数据")
            return

        print(f"总分析师数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        if "institution" in df.columns:
            institutions = df["institution"].unique()
            print(f"\n机构数量: {len(institutions)}")
            print(f"前5个机构: {institutions[:5]}")
        elif "所属机构" in df.columns:
            institutions = df["所属机构"].unique()
            print(f"\n机构数量: {len(institutions)}")
            print(f"前5个机构: {institutions[:5]}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 分析师排名统计分析
# ============================================================
def example_statistics():
    """对分析师排名进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 分析师排名统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_analyst_rank(
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 位分析师")
        print(f"字段列表: {list(df.columns)}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_date_ranges()
    example_filter_institution()
    example_statistics()
