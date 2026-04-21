"""
get_research_report() 接口示例

演示如何使用 akshare_data.get_research_report() 获取个股研报数据。

参数说明:
    symbol:      股票代码
    start_date:  开始日期，格式 "YYYY-MM-DD"
    end_date:    结束日期，格式 "YYYY-MM-DD"

返回字段: 包含研报标题、机构名称、评级、发布日期等
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票研报
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台研报数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台研报数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_research_report(
            symbol="600519",
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n前10条研报:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 对比多只股票研报
# ============================================================
def example_compare_stocks():
    """对比多只股票的研报数据"""
    print("\n" + "=" * 60)
    print("示例 2: 多股研报数据对比")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "600036"]

    for code in stocks:
        try:
            df = service.get_research_report(
                symbol=code,
                start_date="2024-01-01",
                end_date="2024-12-31"
            )
            if df is None or df.empty:
                print(f"\n{code}: 无数据")
            else:
                print(f"\n{code}: {len(df)} 条研报")
                print(df.head(3))
        except Exception as e:
            print(f"\n{code}: 获取失败 - {e}")


# ============================================================
# 示例 3: 不同时间区间
# ============================================================
def example_date_ranges():
    """获取不同时间区间的研报数据"""
    print("\n" + "=" * 60)
    print("示例 3: 不同时间区间研报数据")
    print("=" * 60)

    service = get_service()

    ranges = [
        ("2024-01-01", "2024-03-31"),
        ("2024-04-01", "2024-06-30"),
        ("2024-07-01", "2024-09-30"),
        ("2024-10-01", "2024-12-31"),
    ]

    for start, end in ranges:
        try:
            df = service.get_research_report(
                symbol="600519",
                start_date=start,
                end_date=end
            )
            if df is None or df.empty:
                print(f"{start} ~ {end}: 无数据")
            else:
                print(f"{start} ~ {end}: {len(df)} 条研报")
        except Exception as e:
            print(f"{start} ~ {end}: 获取失败 - {e}")


# ============================================================
# 示例 4: 研报数据统计分析
# ============================================================
def example_statistics():
    """对研报数据进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 研报数据统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_research_report(
            symbol="600036",
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 条研报")
        print(f"字段列表: {list(df.columns)}")

        if "institution" in df.columns or "机构" in df.columns:
            col = "institution" if "institution" in df.columns else "机构"
            print(f"\n机构数: {df[col].nunique()}")

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_stocks()
    example_date_ranges()
    example_statistics()
