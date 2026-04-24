"""
get_ipo_info() 接口示例

演示如何使用 akshare_data.get_ipo_info() 获取新股IPO信息。

注意: ipo_info 接口当前未在 equity.yaml 中配置，此功能可能不可用。
如需新股数据，请使用 get_new_stocks() 接口 (对应 stock_xgsglb_em)。

返回字段: 包含股票代码、名称、发行价、市盈率、申购日期等信息
"""

import pandas as pd
from akshare_data import get_service


def _fetch_ipo_df(service):
    """优先 IPO 表，空数据时回退新股申购表。"""
    for func in (service.get_ipo_info, service.get_new_stocks):
        try:
            df = func()
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _show_sample_ipo():
    sample = pd.DataFrame(
        [
            {"symbol": "301000", "name": "示例科技", "issue_price": 18.5, "pe_ratio": 22.3, "申购日期": "2024-06-10"},
            {"symbol": "603000", "name": "示例制造", "issue_price": 25.8, "pe_ratio": 19.7, "申购日期": "2024-07-03"},
        ]
    )
    print("使用本地样本数据回退:")
    print(sample.to_string(index=False))


# ============================================================
# 示例 1: 基本用法 - 获取最新IPO信息
# ============================================================
def example_basic():
    """基本用法: 获取最新新股IPO信息"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取最新新股IPO信息")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_ipo_df(service)
        if df is None or df.empty:
            print("无数据，切换样本回退")
            _show_sample_ipo()
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前10条IPO信息:")
        print(df.head(10))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 筛选特定字段
# ============================================================
def example_filter_fields():
    """演示如何筛选感兴趣的字段"""
    print("\n" + "=" * 60)
    print("示例 2: 筛选特定字段")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_ipo_df(service)
        if df is None or df.empty:
            print("无数据，切换样本回退")
            _show_sample_ipo()
            return

        interest_cols = ["symbol", "name", "issue_price", "pe_ratio", "申购日期"]
        available = [c for c in interest_cols if c in df.columns]

        if available:
            print(f"可用字段: {available}")
            print(df[available].head(10))
        else:
            print(f"所有字段: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 统计IPO数据
# ============================================================
def example_statistics():
    """对新股IPO数据进行统计分析"""
    print("\n" + "=" * 60)
    print("示例 3: IPO数据统计分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_ipo_df(service)
        if df is None or df.empty:
            print("无数据，切换样本回退")
            _show_sample_ipo()
            return

        print(f"共 {len(df)} 只新股")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_filter_fields()
    example_statistics()
