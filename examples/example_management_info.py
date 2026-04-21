"""
get_management_info() 接口示例

演示如何使用 akshare_data.get_management_info() 获取管理层信息。

注意: 该接口底层调用 akshare 的 stock_hold_management_detail_cninfo，
其 symbol 参数不是股票代码，而是变动方向:
    symbol="增持" - 获取高管增持记录
    symbol="减持" - 获取高管减持记录

返回字段: 包含证券代码、证券名称、变动截止日期、公告日期、高管姓名、
          职务、变动数量、变动比例、变动后持股数等

如需获取特定股票的管理层详细信息，建议使用:
    service.cn.stock.equity.management_info(symbol="增持")
然后对返回结果按股票代码进行筛选
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取高管增持信息
# ============================================================
def example_basic():
    """基本用法: 获取高管增持信息"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取高管增持信息")
    print("=" * 60)

    service = get_service()

    try:
        # symbol 是变动方向，不是股票代码
        df = service.get_management_info(symbol="增持")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n管理层增持信息:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取高管减持信息
# ============================================================
def example_compare_stocks():
    """获取高管减持信息"""
    print("\n" + "=" * 60)
    print("示例 2: 获取高管减持信息")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_management_info(symbol="减持")
        if df is None or df.empty:
            print("\n无数据")
        else:
            print(f"\n高管减持: {len(df)} 条记录")
            print(df.head(5))
    except Exception as e:
        print(f"\n获取失败 - {e}")


# ============================================================
# 示例 3: 对比增减持情况
# ============================================================
def example_filter_position():
    """对比高管增减持情况"""
    print("\n" + "=" * 60)
    print("示例 3: 对比高管增减持情况")
    print("=" * 60)

    service = get_service()

    for change_type in ["增持", "减持"]:
        try:
            df = service.get_management_info(symbol=change_type)
            if df is None or df.empty:
                print(f"{change_type}: 无数据")
            else:
                print(f"\n{change_type}: {len(df)} 条记录")
                print(f"  字段: {list(df.columns)}")
        except Exception as e:
            print(f"{change_type}: 获取失败 - {e}")


# ============================================================
# 示例 4: 统计分析
# ============================================================
def example_statistics():
    """对管理层增减持信息进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 管理层增持信息统计")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_management_info(symbol="增持")
        if df is None or df.empty:
            print("无数据")
            return

        print(f"共 {len(df)} 条增持记录")
        print(f"字段列表: {list(df.columns)}")

        # 打印基本统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_stocks()
    example_filter_position()
    example_statistics()
