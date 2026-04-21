"""
get_conversion_bond_list() 接口示例

演示如何使用 DataService 获取可转债列表数据。

接口说明:
- get_conversion_bond_list() - 获取可转债列表
  - 无必需参数
  - 返回: DataFrame - 包含所有可转债的基本信息

该接口在 JSLAdapter 中实现，返回全市场可转债的基础数据。
若数据源不可用，示例中会展示期望的数据格式。

典型返回字段:
- bond_code: 可转债代码
- bond_name: 可转债名称
- stock_code: 正股代码
- stock_name: 正股名称
- list_date: 上市日期
- maturity_date: 到期日期
- face_value: 面值
- issue_size: 发行规模
- credit_rating: 信用评级
"""

import pandas as pd
from akshare_data import get_service


def _mock_bond_list():
    """返回模拟的可转债列表数据用于演示"""
    return pd.DataFrame({
        "bond_code": ["127045", "110059", "123107", "113050", "128143", "113052", "127046"],
        "bond_name": ["牧原转债", "南航转债", "蓝盾转债", "南银转债", "锋龙转债", "兴业转债", "中装转债"],
        "stock_code": ["002714", "601111", "300297", "601009", "002931", "601166", "002822"],
        "stock_name": ["牧原股份", "中国国航", "蓝盾股份", "南京银行", "锋龙股份", "兴业银行", "中装建设"],
        "list_date": ["2021-09-06", "2021-09-13", "2020-12-15", "2021-03-24", "2021-03-30", "2022-01-18", "2021-05-07"],
        "maturity_date": ["2027-09-06", "2027-09-13", "2026-12-15", "2027-03-24", "2027-03-30", "2028-01-18", "2027-05-07"],
        "face_value": [100.0] * 7,
        "issue_size": [90.0, 100.0, 4.0, 200.0, 2.45, 500.0, 11.6],
        "credit_rating": ["AA+", "AAA", "A+", "AAA", "A+", "AAA", "AA"],
    })


# ============================================================
# 示例 1: 基本用法 - 获取可转债列表
# ============================================================
def example_basic():
    """基本用法: 获取所有可转债的列表"""
    print("=" * 60)
    print("示例 1: 获取可转债列表")
    print("=" * 60)

    service = get_service()

    try:
        # 获取可转债列表
        df = service.get_conversion_bond_list()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_bond_list()

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print(f"\n全市场可转债数量: {len(df)}")

        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")
        print("使用演示数据:")
        df = _mock_bond_list()
        print(df.head(10).to_string(index=False))


# ============================================================
# 示例 2: 按发行规模筛选
# ============================================================
def example_filter_by_size():
    """按发行规模筛选可转债"""
    print("\n" + "=" * 60)
    print("示例 2: 按发行规模筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_conversion_bond_list()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_bond_list()

        if "issue_size" in df.columns:
            df["issue_size"] = pd.to_numeric(df["issue_size"], errors="coerce")

            # 大规模发行 (>50亿)
            large = df[df["issue_size"] > 50].sort_values("issue_size", ascending=False)
            print(f"\n大规模发行 (>50亿): {len(large)} 只")
            if not large.empty:
                cols = ["bond_code", "bond_name", "issue_size", "credit_rating"]
                available_cols = [c for c in cols if c in large.columns]
                print(large[available_cols].head(10).to_string(index=False))

            # 小规模发行 (<10亿)
            small = df[df["issue_size"] < 10].sort_values("issue_size")
            print(f"\n小规模发行 (<10亿): {len(small)} 只")
            if not small.empty:
                print(small[available_cols].head(10).to_string(index=False))

            # 发行规模统计
            print(f"\n发行规模统计:")
            print(f"  总规模: {df['issue_size'].sum():.2f} 亿元")
            print(f"  平均规模: {df['issue_size'].mean():.2f} 亿元")
            print(f"  最大规模: {df['issue_size'].max():.2f} 亿元")
            print(f"  最小规模: {df['issue_size'].min():.2f} 亿元")
        else:
            print("数据中无发行规模字段")

    except Exception as e:
        print(f"筛选失败: {e}")


# ============================================================
# 示例 3: 按信用评级分析
# ============================================================
def example_credit_rating_analysis():
    """按信用评级统计分析"""
    print("\n" + "=" * 60)
    print("示例 3: 信用评级分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_conversion_bond_list()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_bond_list()

        if "credit_rating" in df.columns:
            print("\n信用评级分布:")
            rating_counts = df["credit_rating"].value_counts().sort_index(ascending=False)

            for rating, count in rating_counts.items():
                percentage = count / len(df) * 100
                bar = "█" * int(percentage / 2)
                print(f"  {rating:6s}: {count:4d} 只 ({percentage:5.1f}%) {bar}")

            # 高评级可转债 (AAA)
            high_rating = df[df["credit_rating"] == "AAA"]
            print(f"\nAAA级可转债: {len(high_rating)} 只")
            if not high_rating.empty and "issue_size" in high_rating.columns:
                print(f"  占市场总规模比例: {high_rating['issue_size'].sum() / df['issue_size'].sum() * 100:.1f}%")
        else:
            print("数据中无信用评级字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 4: 上市日期分析
# ============================================================
def example_listing_date_analysis():
    """分析可转债上市时间分布"""
    print("\n" + "=" * 60)
    print("示例 4: 上市日期分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_conversion_bond_list()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_bond_list()

        if "list_date" in df.columns:
            # 转换日期格式
            df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")
            df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")

            # 计算剩余期限
            if "maturity_date" in df.columns:
                today = pd.Timestamp.now()
                df["remaining_years"] = (df["maturity_date"] - today).dt.days / 365.25

                print("\n剩余期限分布:")
                bins = [0, 1, 2, 3, 5, float("inf")]
                labels = ["<1年", "1-2年", "2-3年", "3-5年", ">5年"]
                df["remaining_range"] = pd.cut(df["remaining_years"], bins=bins, labels=labels)

                distribution = df["remaining_range"].value_counts().sort_index()
                for range_label, count in distribution.items():
                    percentage = count / len(df) * 100
                    print(f"  {range_label}: {count} 只 ({percentage:.1f}%)")

            # 按年份统计新上市可转债
            df["list_year"] = df["list_date"].dt.year
            print("\n各年度上市可转债数量:")
            yearly_counts = df["list_year"].value_counts().sort_index(ascending=False)
            for year, count in yearly_counts.head(5).items():
                print(f"  {year}: {count} 只")
        else:
            print("数据中无上市日期字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 搜索特定可转债
# ============================================================
def example_search_bond():
    """搜索特定可转债信息"""
    print("\n" + "=" * 60)
    print("示例 5: 搜索特定可转债")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_conversion_bond_list()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_bond_list()

        # 示例 1: 按代码搜索
        bond_code = "127045"
        print(f"\n按代码搜索: {bond_code}")
        if "bond_code" in df.columns:
            bond = df[df["bond_code"] == bond_code]
            if not bond.empty:
                print(bond.to_string(index=False))
            else:
                print(f"  未找到代码为 {bond_code} 的可转债")

        # 示例 2: 按名称搜索
        bond_name = "牧原"
        print(f"\n按名称搜索: {bond_name}")
        if "bond_name" in df.columns:
            matches = df[df["bond_name"].str.contains(bond_name, na=False)]
            if not matches.empty:
                print(matches.to_string(index=False))
            else:
                print(f"  未找到名称包含 '{bond_name}' 的可转债")

        # 示例 3: 按正股搜索
        stock_code = "002714"
        print(f"\n按正股代码搜索: {stock_code}")
        if "stock_code" in df.columns:
            matches = df[df["stock_code"] == stock_code]
            if not matches.empty:
                print("对应的可转债:")
                print(matches.to_string(index=False))
            else:
                print(f"  未找到正股代码为 {stock_code} 的可转债")

    except Exception as e:
        print(f"搜索失败: {e}")


# ============================================================
# 示例 6: 综合统计报告
# ============================================================
def example_comprehensive_report():
    """生成可转债市场综合统计报告"""
    print("\n" + "=" * 60)
    print("示例 6: 可转债市场综合统计报告")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_conversion_bond_list()

        if df is None or df.empty:
            print("[数据源不可用，使用演示数据]")
            df = _mock_bond_list()

        print("\n" + "=" * 50)
        print("可转债市场统计报告")
        print("=" * 50)

        # 基础统计
        print(f"\n1. 市场规模")
        print(f"   可转债总数: {len(df)} 只")

        if "issue_size" in df.columns:
            df["issue_size"] = pd.to_numeric(df["issue_size"], errors="coerce")
            print(f"   总发行规模: {df['issue_size'].sum():.2f} 亿元")
            print(f"   平均发行规模: {df['issue_size'].mean():.2f} 亿元")

        # 评级分布
        if "credit_rating" in df.columns:
            print(f"\n2. 信用评级分布")
            rating_counts = df["credit_rating"].value_counts().sort_index(ascending=False)
            for rating, count in rating_counts.head(5).items():
                print(f"   {rating}: {count} 只")

        # 上市日期统计
        if "list_date" in df.columns:
            df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")
            recent = df[df["list_date"] >= "2024-01-01"]
            print(f"\n3. 今年新上市: {len(recent)} 只")

        # 到期统计
        if "maturity_date" in df.columns:
            df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")
            today = pd.Timestamp.now()
            expiring_soon = df[df["maturity_date"] <= today + pd.DateOffset(years=1)]
            print(f"\n4. 一年内到期: {len(expiring_soon)} 只")

        print("\n" + "=" * 50)

    except Exception as e:
        print(f"生成报告失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_filter_by_size()
    example_credit_rating_analysis()
    example_listing_date_analysis()
    example_search_bond()
    example_comprehensive_report()
