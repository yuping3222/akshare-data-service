"""
get_convert_bond_premium() 接口示例

演示如何使用 DataService 获取可转债溢价率数据。

接口说明:
- get_convert_bond_premium() - 获取可转债溢价率数据
  - 无必需参数
  - 返回: DataFrame - 包含所有可转债的溢价率信息

该接口目前仅在 LixingerAdapter 中实现，如需使用需配置 LIXINGER_TOKEN。
若数据源不可用，示例中会展示期望的数据格式。

典型返回字段:
- bond_code: 可转债代码
- bond_name: 可转债名称
- stock_code: 正股代码
- stock_name: 正股名称
- premium_rate: 转股溢价率 (%)
- conversion_value: 转股价值
- bond_price: 可转债价格
- stock_price: 正股价格
"""

import pandas as pd
from akshare_data import get_service
from _example_utils import fetch_with_retry, normalize_symbol_input, print_df_brief, stable_df


def _mock_premium_data():
    """返回模拟的可转债溢价率数据用于演示"""
    return pd.DataFrame({
        "bond_code": ["127045", "110059", "123107", "113050", "128143"],
        "bond_name": ["牧原转债", "南航转债", "蓝盾转债", "南银转债", "锋龙转债"],
        "stock_code": ["002714", "601111", "300297", "601009", "002931"],
        "stock_name": ["牧原股份", "中国国航", "蓝盾股份", "南京银行", "锋龙股份"],
        "premium_rate": [15.2, -2.5, 35.8, 8.3, 22.1],
        "conversion_value": [105.3, 120.7, 78.5, 92.8, 89.2],
        "bond_price": [120.5, 117.8, 106.2, 100.5, 109.1],
        "stock_price": [42.5, 6.8, 3.2, 9.5, 12.8],
    })


# ============================================================
# 示例 1: 基本用法 - 获取可转债溢价率数据
# ============================================================
def example_basic():
    """基本用法: 获取所有可转债的溢价率数据"""
    print("=" * 60)
    print("示例 1: 获取可转债溢价率数据")
    print("=" * 60)

    service = get_service()

    try:
        # 获取可转债溢价率数据
        # 该接口返回全市场可转债的溢价率信息
        df = fetch_with_retry(lambda: service.get_convert_bond_premium(), retries=2)

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_premium_data()

        print_df_brief(stable_df(df), rows=10)

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("使用演示数据:")
        df = _mock_premium_data()
        print(df.head(10).to_string(index=False))


# ============================================================
# 示例 2: 溢价率分析 - 筛选低溢价可转债
# ============================================================
def example_low_premium():
    """筛选低溢价的可转债（潜在套利机会）"""
    print("\n" + "=" * 60)
    print("示例 2: 筛选低溢价可转债")
    print("=" * 60)

    service = get_service()

    try:
        df = fetch_with_retry(lambda: service.get_convert_bond_premium(), retries=2)

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_premium_data()

        # 确保 premium_rate 是数值类型
        if "premium_rate" in df.columns:
            df["premium_rate"] = pd.to_numeric(df["premium_rate"], errors="coerce")

            # 筛选溢价率低于 5% 的可转债
            low_premium = df[df["premium_rate"] < 5].copy()
            low_premium = low_premium.sort_values("premium_rate")

            print(f"\n溢价率低于 5% 的可转债: {len(low_premium)} 只")
            if not low_premium.empty:
                display_cols = ["bond_code", "bond_name", "premium_rate", "conversion_value"]
                available_cols = [c for c in display_cols if c in low_premium.columns]
                print(low_premium[available_cols].head(10).to_string(index=False))

                # 特别标注负溢价（套利机会）
                negative_premium = low_premium[low_premium["premium_rate"] < 0]
                if not negative_premium.empty:
                    print(f"\n负溢价可转债（潜在套利机会）: {len(negative_premium)} 只")
                    print(negative_premium[available_cols].to_string(index=False))
            else:
                print("暂无低溢价可转债")
        else:
            print("数据中无溢价率字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 3: 溢价率分布统计
# ============================================================
def example_premium_statistics():
    """统计可转债溢价率分布"""
    print("\n" + "=" * 60)
    print("示例 3: 溢价率分布统计")
    print("=" * 60)

    service = get_service()

    try:
        df = fetch_with_retry(lambda: service.get_convert_bond_premium(), retries=2)

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_premium_data()

        if "premium_rate" in df.columns:
            df["premium_rate"] = pd.to_numeric(df["premium_rate"], errors="coerce")

            print(f"\n全市场可转债数量: {len(df)}")
            print("\n溢价率统计信息:")
            print(df["premium_rate"].describe())

            # 分区间统计
            bins = [-float("inf"), 0, 10, 20, 30, 50, float("inf")]
            labels = ["<0% (折价)", "0-10%", "10-20%", "20-30%", "30-50%", ">50%"]
            df["premium_range"] = pd.cut(df["premium_rate"], bins=bins, labels=labels)

            print("\n溢价率分布:")
            distribution = df["premium_range"].value_counts().sort_index()
            for range_label, count in distribution.items():
                print(f"  {range_label}: {count} 只 ({count/len(df)*100:.1f}%)")
        else:
            print("数据中无溢价率字段")

    except Exception as e:
        print(f"统计失败: {e}")


# ============================================================
# 示例 4: 双低策略筛选（低价格 + 低溢价）
# ============================================================
def example_double_low_strategy():
    """双低策略: 筛选价格低且溢价率低的可转债"""
    print("\n" + "=" * 60)
    print("示例 4: 双低策略筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = fetch_with_retry(lambda: service.get_convert_bond_premium(), retries=2)

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_premium_data()

        # 确保数值类型
        numeric_cols = ["premium_rate", "bond_price", "conversion_value"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 双低策略: 价格 < 120 且 溢价率 < 20%
        if "bond_price" in df.columns and "premium_rate" in df.columns:
            double_low = df[
                (df["bond_price"] < 120) & (df["premium_rate"] < 20)
            ].copy()

            # 计算双低值 = 价格 + 溢价率
            double_low["double_low_value"] = double_low["bond_price"] + double_low["premium_rate"]
            double_low = double_low.sort_values("double_low_value")

            print(f"\n双低策略筛选结果（价格<120, 溢价率<20%）: {len(double_low)} 只")

            if not double_low.empty:
                display_cols = ["bond_code", "bond_name", "bond_price", "premium_rate", "double_low_value"]
                available_cols = [c for c in display_cols if c in double_low.columns]
                print(double_low[available_cols].head(15).to_string(index=False))
            else:
                print("暂无符合条件的可转债")
        else:
            print("数据中缺少价格或溢价率字段")

    except Exception as e:
        print(f"筛选失败: {e}")


# ============================================================
# 示例 5: 按正股筛选可转债
# ============================================================
def example_filter_by_stock():
    """根据正股代码筛选可转债"""
    print("\n" + "=" * 60)
    print("示例 5: 按正股筛选可转债")
    print("=" * 60)

    service = get_service()

    try:
        df = fetch_with_retry(lambda: service.get_convert_bond_premium(), retries=2)

        if df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_premium_data()

        # 示例: 查找牧原股份的可转债
        stock_code = normalize_symbol_input("002714")
        stock_name = "牧原股份"

        if "stock_code" in df.columns:
            bond = df[df["stock_code"] == stock_code]
            if bond.empty:
                # 尝试按名称匹配
                if "stock_name" in df.columns:
                    bond = df[df["stock_name"] == stock_name]

            print(f"\n正股 {stock_code} ({stock_name}) 对应的可转债:")
            if not bond.empty:
                print(bond.to_string(index=False))
            else:
                print("  未找到对应可转债")
        else:
            print("数据中无正股代码字段")

    except Exception as e:
        print(f"筛选失败: {e}")


# ============================================================
# 示例 6: 数据源对比
# ============================================================
def example_source_comparison():
    """对比不同数据源的可转债溢价数据"""
    print("\n" + "=" * 60)
    print("示例 6: 数据源对比")
    print("=" * 60)

    service = get_service()

    try:
        # 尝试从默认源获取
        print("\n从默认源获取数据...")
        df_default = fetch_with_retry(lambda: service.get_convert_bond_premium(), retries=2)
        print(f"  默认源: {len(df_default)} 条记录")

        # 尝试从 lixinger 获取
        print("\n从 lixinger 源获取数据...")
        df_lixinger = fetch_with_retry(
            lambda: service.get_convert_bond_premium(source="lixinger"),
            retries=1,
        )
        print(f"  lixinger: {len(df_lixinger)} 条记录")

        # 尝试从 akshare 获取
        print("\n从 akshare 源获取数据...")
        df_akshare = fetch_with_retry(
            lambda: service.get_convert_bond_premium(source="akshare"),
            retries=1,
        )
        print(f"  akshare: {len(df_akshare)} 条记录")

    except Exception as e:
        print(f"对比失败: {e}")
        print("\n注意: get_convert_bond_premium 接口仅在 LixingerAdapter 中实现")


if __name__ == "__main__":
    example_basic()
    example_low_premium()
    example_premium_statistics()
    example_double_low_strategy()
    example_filter_by_stock()
    example_source_comparison()
