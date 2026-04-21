"""
get_macro_exchange_rate() 接口示例

演示如何使用 akshare_data.get_macro_exchange_rate() 获取宏观经济汇率数据。

注意: 底层 akshare 函数 forex_spot_em 不接受参数，返回当前汇率快照。
      如需筛选历史日期范围，获取后可在 DataFrame 上自行筛选。

返回字段包括: 货币对、汇率等。

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_macro_exchange_rate()
"""

from akshare_data import get_service


def example_basic():
    """基本用法: 获取汇率数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取汇率数据")
    print("=" * 60)

    service = get_service()

    try:
        # 注意: 该接口不接受 start_date/end_date 参数，返回当前汇率快照
        df = service.get_macro_exchange_rate()

        if df is None or df.empty:
            print("无数据（数据源不可用）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_filtered():
    """获取汇率数据后按条件筛选"""
    print("\n" + "=" * 60)
    print("示例 2: 汇率数据筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_macro_exchange_rate()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"汇率数据: {len(df)} 条")

        # 按货币对筛选（示例）
        name_col = None
        for col in df.columns:
            if "货币" in col or "name" in col.lower() or "pair" in col.lower():
                name_col = col
                break

        if name_col:
            unique_pairs = df[name_col].unique()[:5]
            print(f"\n前5个货币对: {list(unique_pairs)}")

        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_analysis():
    """汇率数据分析"""
    print("\n" + "=" * 60)
    print("示例 3: 汇率数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_macro_exchange_rate()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        print("\n最新数据:")
        print(df.tail(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_latest_data():
    """获取汇率数据"""
    print("\n" + "=" * 60)
    print("示例 4: 获取汇率数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_macro_exchange_rate()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"共有 {len(df)} 条汇率记录")
        print("\n最新的5条数据:")
        print(df.tail(5).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_date_range()
    example_analysis()
    example_latest_data()
