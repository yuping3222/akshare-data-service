"""
get_convert_bond_spot() 接口示例

演示如何使用 DataService 获取可转债实时行情数据。

接口说明:
- get_convert_bond_spot() - 获取可转债实时行情
  - 无必需参数
  - 返回: DataFrame - 包含所有可转债的实时行情数据

该接口目前仅在 LixingerAdapter 中实现，如需使用需配置 LIXINGER_TOKEN。
若数据源不可用，示例中会展示期望的数据格式。

注意: 该接口返回实时行情数据，不支持缓存，每次调用都会从数据源获取最新数据。

典型返回字段:
- bond_code: 可转债代码
- bond_name: 可转债名称
- current_price: 当前价格
- change_percent: 涨跌幅 (%)
- volume: 成交量
- amount: 成交额
- premium_rate: 溢价率
"""

import pandas as pd
from akshare_data import get_service


def _mock_spot_data():
    """返回模拟的可转债实时行情数据用于演示"""
    return pd.DataFrame({
        "bond_code": ["127045", "110059", "123107", "113050", "128143", "113052"],
        "bond_name": ["牧原转债", "南航转债", "蓝盾转债", "南银转债", "锋龙转债", "兴业转债"],
        "current_price": [120.50, 117.80, 106.20, 100.50, 109.10, 112.30],
        "change_percent": [1.25, -0.85, 2.15, 0.35, -1.20, 0.75],
        "volume": [52000, 125000, 89000, 45000, 32000, 67000],
        "amount": [6266000, 14725000, 9451800, 4522500, 3491200, 7524100],
        "premium_rate": [15.2, -2.5, 35.8, 8.3, 22.1, 18.5],
    })


# ============================================================
# 示例 1: 基本用法 - 获取可转债实时行情
# ============================================================
def example_basic():
    """基本用法: 获取可转债实时行情数据"""
    print("=" * 60)
    print("示例 1: 获取可转债实时行情")
    print("=" * 60)

    service = get_service()

    try:
        # 获取可转债实时行情
        # 注意: 该接口返回实时数据，不经过缓存
        df = service.get_convert_bond_spot()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_spot_data()

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

        # 显示统计信息
        print(f"\n共获取 {len(df)} 只可转债的实时行情")

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("使用演示数据:")
        df = _mock_spot_data()
        print(df.head(10).to_string(index=False))


# ============================================================
# 示例 2: 涨幅榜和跌幅榜
# ============================================================
def example_top_movers():
    """查看可转债涨跌幅排行"""
    print("\n" + "=" * 60)
    print("示例 2: 可转债涨跌幅排行")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_convert_bond_spot()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_spot_data()

        # 确保涨跌幅为数值类型
        if "change_percent" in df.columns:
            df["change_percent"] = pd.to_numeric(df["change_percent"], errors="coerce")

            # 涨幅榜 TOP 5
            print("\n涨幅榜 TOP 5:")
            top_gainers = df.nlargest(5, "change_percent")
            display_cols = ["bond_code", "bond_name", "current_price", "change_percent"]
            available_cols = [c for c in display_cols if c in top_gainers.columns]
            print(top_gainers[available_cols].to_string(index=False))

            # 跌幅榜 TOP 5
            print("\n跌幅榜 TOP 5:")
            top_losers = df.nsmallest(5, "change_percent")
            print(top_losers[available_cols].to_string(index=False))
        else:
            print("数据中无涨跌幅字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 3: 筛选活跃可转债
# ============================================================
def example_active_bonds():
    """筛选成交量活跃的可转债"""
    print("\n" + "=" * 60)
    print("示例 3: 筛选活跃可转债")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_convert_bond_spot()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_spot_data()

        # 确保成交量为数值类型
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

            # 筛选成交量 > 50000 的可转债
            active_threshold = 50000
            active_bonds = df[df["volume"] > active_threshold].copy()
            active_bonds = active_bonds.sort_values("volume", ascending=False)

            print(f"\n成交量超过 {active_threshold} 的可转债: {len(active_bonds)} 只")

            if not active_bonds.empty:
                display_cols = ["bond_code", "bond_name", "current_price", "volume", "amount", "change_percent"]
                available_cols = [c for c in display_cols if c in active_bonds.columns]
                print(active_bonds[available_cols].head(10).to_string(index=False))
            else:
                print("暂无高成交量可转债")
        else:
            print("数据中无成交量字段")

    except Exception as e:
        print(f"筛选失败: {e}")


# ============================================================
# 示例 4: 价格区间分析
# ============================================================
def example_price_range():
    """按价格区间统计可转债分布"""
    print("\n" + "=" * 60)
    print("示例 4: 价格区间分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_convert_bond_spot()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_spot_data()

        if "current_price" in df.columns:
            df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")

            # 定义价格区间
            bins = [0, 100, 110, 120, 130, 150, float("inf")]
            labels = ["<100", "100-110", "110-120", "120-130", "130-150", ">150"]
            df["price_range"] = pd.cut(df["current_price"], bins=bins, labels=labels)

            print(f"\n全市场可转债价格分布:")
            distribution = df["price_range"].value_counts().sort_index()
            for range_label, count in distribution.items():
                percentage = count / len(df) * 100
                bar = "█" * int(percentage / 2)
                print(f"  {range_label:8s}: {count:4d} 只 ({percentage:5.1f}%) {bar}")

            # 统计信息
            print(f"\n价格统计:")
            print(f"  平均价格: {df['current_price'].mean():.2f}")
            print(f"  中位数: {df['current_price'].median():.2f}")
            print(f"  最低: {df['current_price'].min():.2f}")
            print(f"  最高: {df['current_price'].max():.2f}")
        else:
            print("数据中无当前价格字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 查找特定可转债行情
# ============================================================
def example_find_bond():
    """查找特定可转债的实时行情"""
    print("\n" + "=" * 60)
    print("示例 5: 查找特定可转债行情")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_convert_bond_spot()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_spot_data()

        # 示例: 查找牧原转债
        bond_code = "127045"
        bond_name = "牧原转债"

        print(f"\n查找可转债: {bond_code} ({bond_name})")

        if "bond_code" in df.columns:
            bond = df[df["bond_code"] == bond_code]
            if bond.empty and "bond_name" in df.columns:
                bond = df[df["bond_name"] == bond_name]

            if not bond.empty:
                print("\n行情数据:")
                print(bond.to_string(index=False))

                # 显示详细分析
                if "change_percent" in bond.columns and "premium_rate" in bond.columns:
                    change = float(bond["change_percent"].iloc[0])
                    premium = float(bond["premium_rate"].iloc[0])

                    print(f"\n分析:")
                    print(f"  今日涨跌: {change:+.2f}%")
                    print(f"  当前溢价率: {premium:.2f}%")

                    if change > 3:
                        print("  状态: 今日涨幅较大，关注是否追高")
                    elif change < -3:
                        print("  状态: 今日跌幅较大，可能存在机会")

                    if premium < 0:
                        print("  状态: 负溢价，存在转股套利空间")
                    elif premium < 10:
                        print("  状态: 低溢价，股性较强")
                    else:
                        print("  状态: 高溢价，债性较强")
            else:
                print(f"  未找到代码为 {bond_code} 的可转债")
        else:
            print("数据中无可转债代码字段")

    except Exception as e:
        print(f"查询失败: {e}")


# ============================================================
# 示例 6: 综合筛选 - 活跃且低估的可转债
# ============================================================
def example_comprehensive_filter():
    """综合筛选: 寻找活跃且低估的可转债"""
    print("\n" + "=" * 60)
    print("示例 6: 综合筛选 - 活跃且低估的可转债")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_convert_bond_spot()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_spot_data()

        # 转换为数值类型
        numeric_cols = ["current_price", "volume", "change_percent", "premium_rate"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 综合条件:
        # 1. 价格在 100-120 之间 (相对安全)
        # 2. 成交量 > 40000 (活跃)
        # 3. 溢价率 < 20 (股性较好)
        conditions = (
            (df["current_price"] >= 100) &
            (df["current_price"] <= 120) &
            (df["volume"] >= 40000) &
            (df["premium_rate"] < 20)
        )

        candidates = df[conditions].copy()

        # 按成交量排序
        candidates = candidates.sort_values("volume", ascending=False)

        print(f"\n筛选条件: 价格100-120, 成交量>40000, 溢价率<20%")
        print(f"符合条件可转债: {len(candidates)} 只")

        if not candidates.empty:
            display_cols = ["bond_code", "bond_name", "current_price", "volume", "premium_rate", "change_percent"]
            available_cols = [c for c in display_cols if c in candidates.columns]
            print(candidates[available_cols].head(10).to_string(index=False))
        else:
            print("暂无符合条件的可转债")

    except Exception as e:
        print(f"筛选失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_top_movers()
    example_active_bonds()
    example_price_range()
    example_find_bond()
    example_comprehensive_filter()
