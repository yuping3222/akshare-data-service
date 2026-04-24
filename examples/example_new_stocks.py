"""
get_new_stocks() 接口示例

演示如何使用 akshare_data.get_new_stocks() 获取新股数据。

接口说明:
- 获取全市场新股申购/上市数据
- 无需参数
- 返回字段包含: 股票代码、股票名称、申购日期、上市日期、发行价格等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_new_stocks()
"""

import pandas as pd

from akshare_data import get_service


def _as_dataframe(data, label: str) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        print(f"{label}: 返回类型异常，期望 DataFrame，实际 {type(data).__name__}")
        return pd.DataFrame()
    if data.empty:
        print(f"{label}: 返回空数据")
    return data


# ============================================================
# 示例 1: 基本用法 - 获取新股数据
# ============================================================
def example_basic():
    """基本用法: 获取新股数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取新股数据")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_new_stocks(), "示例1")
        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 数据概览与统计
# ============================================================
def example_overview():
    """对新数据进行概览统计"""
    print("\n" + "=" * 60)
    print("示例 2: 新股数据概览")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_new_stocks(), "示例2")
        if df.empty:
            return

        print(f"新股数据总数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 近期新股筛选
# ============================================================
def example_recent():
    """筛选近期上市的新股"""
    print("\n" + "=" * 60)
    print("示例 3: 近期新股筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_new_stocks(), "示例3")
        if df.empty:
            return

        # 查找日期相关字段
        date_col = None
        for col in df.columns:
            if "上市" in col or "日期" in col or "date" in col.lower():
                date_col = col
                break

        if date_col:
            # 显示最新的10只新股
            print(f"最新的10只新股:")
            print(df.head(10).to_string(index=False))
        else:
            print(f"字段列表: {list(df.columns)}")
            print("\n全部新股数据 (前20条):")
            print(df.head(20).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 发行价格分析
# ============================================================
def example_price_analysis():
    """分析新股发行价格"""
    print("\n" + "=" * 60)
    print("示例 4: 发行价格分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_new_stocks(), "示例4")
        if df.empty:
            return

        # 查找价格相关字段
        price_col = None
        for col in df.columns:
            if "价格" in col or "发行价" in col or "price" in col.lower():
                price_col = col
                break

        if price_col:
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
            print(f"发行价格统计 ({price_col}):")
            print(df[price_col].describe())

            # 最贵的新股
            print("\n发行价格最高的5只新股:")
            top_price = df.nlargest(5, price_col)
            print(top_price.to_string(index=False))
        else:
            print(f"字段列表: {list(df.columns)}")
            print("无法识别发行价格字段")

    except Exception as e:
        print(f"获取数据失败: {e}")


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
        print("\n测试 1: 正常调用")
        df = _as_dataframe(service.get_new_stocks(), "示例5-测试1")
        if df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 再次调用（验证缓存）")
        df = _as_dataframe(service.get_new_stocks(), "示例5-测试2")
        if df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_overview()
    example_recent()
    example_price_analysis()
    example_error_handling()
