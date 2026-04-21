"""
get_hot_rank() 接口示例

演示如何使用 akshare_data.get_hot_rank() 获取股票热度排名数据。

返回字段: 包含股票代码、名称、热度排名、热度指数等
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取热度排名
# ============================================================
def example_basic():
    """基本用法: 获取股票热度排名"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取股票热度排名")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_hot_rank()
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n热度排名Top20:")
        print(df.head(20))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 筛选特定排名范围
# ============================================================
def example_filter_rank():
    """筛选特定排名范围内的股票"""
    print("\n" + "=" * 60)
    print("示例 2: 筛选特定排名范围")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_hot_rank()
        if df is None or df.empty:
            print("无数据")
            return

        print(f"总记录数: {len(df)}")

        if "rank" in df.columns or "排名" in df.columns:
            col = "rank" if "rank" in df.columns else "排名"
            top10 = df[df[col] <= 10]
            print(f"\nTop10股票 ({len(top10)} 只):")
            print(top10)

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 热度统计分析
# ============================================================
def example_statistics():
    """对热度排名进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 3: 热度排名统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_hot_rank()
        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 只股票")
        print(f"字段列表: {list(df.columns)}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 获取高热度股票
# ============================================================
def example_high_rank():
    """获取高热度股票"""
    print("\n" + "=" * 60)
    print("示例 4: 获取高热度股票")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_hot_rank()
        if df is None or df.empty:
            print("无数据")
            return

        if "hot" in df.columns or "热度" in df.columns:
            col = "hot" if "hot" in df.columns else "热度"
            top_hot = df.nlargest(10, col)
            print(f"\n热度最高的10只股票:")
            print(top_hot)
        else:
            print(f"字段: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_filter_rank()
    example_statistics()
    example_high_rank()
