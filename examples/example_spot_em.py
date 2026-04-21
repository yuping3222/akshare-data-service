"""
get_spot_em() 接口示例

演示如何使用 akshare_data.get_spot_em() 获取东方财富实时行情数据。

该接口返回A股全市场实时行情，包括沪市、深市、北交所的股票数据。

返回字段包括: 代码、名称、涨跌幅、成交额、量比等。

注意: 该接口不支持日期参数，返回当前实时行情。
"""

import pandas as pd
from akshare_data import get_service


def example_basic():
    """基本用法: 获取实时行情数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取实时行情数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_spot_em()

        if df is None or df.empty:
            print("无数据（数据源不可用或非交易时段）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_market_overview():
    """获取市场概览数据"""
    print("\n" + "=" * 60)
    print("示例 2: 市场概览数据")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_spot_em()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"总股票数量: {len(df)}")

        common_fields = ["涨跌幅", "成交额", "换手率", "量比"]
        available_fields = [f for f in common_fields if f in df.columns]

        if available_fields:
            print(f"可用字段: {available_fields}")
            for field in available_fields[:4]:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors="coerce")

            print(f"\n成交额最大的5只股票:")
            if "成交额" in df.columns:
                top5 = df.nlargest(5, "成交额")
                print(top5[["代码", "名称", "成交额"]].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_filter_by_change():
    """筛选涨跌幅较大的股票"""
    print("\n" + "=" * 60)
    print("示例 3: 筛选涨跌幅较大的股票")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_spot_em()

        if df is None or df.empty:
            print("无数据")
            return

        if "涨跌幅" in df.columns:
            df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")

            rising = df[df["涨跌幅"] > 5].sort_values("涨跌幅", ascending=False)
            falling = df[df["涨跌幅"] < -5].sort_values("涨跌幅")

            print(f"涨幅超过5%的股票: {len(rising)} 只")
            if not rising.empty:
                print(rising.head(10)[["代码", "名称", "涨跌幅"]].to_string(index=False))

            print(f"\n跌幅超过5%的股票: {len(falling)} 只")
            if not falling.empty:
                print(falling.head(10)[["代码", "名称", "涨跌幅"]].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_statistics():
    """市场数据统计分析"""
    print("\n" + "=" * 60)
    print("示例 4: 市场数据统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.akshare.get_spot_em()

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
    example_basic()
    example_market_overview()
    example_filter_by_change()
    example_statistics()
