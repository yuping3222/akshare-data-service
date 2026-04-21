"""
get_us_stocks() 接口示例

演示如何使用 akshare_data.get_us_stocks() 获取美股实时行情数据。

该接口返回美股实时行情数据。

返回字段包括: 代码、名称、涨跌幅、成交额、换手率等。

注意: 该接口不支持日期参数，返回当前实时行情。
"""

from akshare_data import get_service


def example_basic():
    """基本用法: 获取美股实时行情"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取美股实时行情")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_us_stocks()

        if df is None or df.empty:
            print("无数据（数据源不可用或非交易时段）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_top_stocks():
    """获取美股成交额前10"""
    print("\n" + "=" * 60)
    print("示例 2: 美股成交额前10")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_us_stocks()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"总股票数量: {len(df)}")

        if "成交额" in df.columns:
            df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce")
            top10 = df.nlargest(10, "成交额")
            print("\n成交额前10名:")
            print(top10[["代码", "名称", "成交额"]].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_filter_by_change():
    """筛选涨跌幅较大的美股"""
    print("\n" + "=" * 60)
    print("示例 3: 筛选涨跌幅较大的美股")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_us_stocks()

        if df is None or df.empty:
            print("无数据")
            return

        if "涨跌幅" in df.columns:
            df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")

            rising = df[df["涨跌幅"] > 5].sort_values("涨跌幅", ascending=False)
            falling = df[df["涨跌幅"] < -5].sort_values("涨跌幅")

            print(f"涨幅超过5%的美股: {len(rising)} 只")
            if not rising.empty:
                print(rising.head(10)[["代码", "名称", "涨跌幅"]].to_string(index=False))

            print(f"\n跌幅超过5%的美股: {len(falling)} 只")
            if not falling.empty:
                print(falling.head(10)[["代码", "名称", "涨跌幅"]].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_statistics():
    """美股市场统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 美股市场统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_us_stocks()

        if df is None or df.empty:
            print("无数据")
            return

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计信息:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    import pandas as pd

    example_basic()
    example_top_stocks()
    example_filter_by_change()
    example_statistics()
