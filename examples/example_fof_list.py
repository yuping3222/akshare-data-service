"""
get_fof_list() 接口示例

演示如何使用 DataService.get_fof_list() 获取FOF基金列表。

接口说明:
- get_fof_list(): 获取全部FOF(Fund of Funds)基金列表
- 无必需参数
- 返回: pd.DataFrame，包含FOF基金代码、名称、类型等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_fof_list()

注意:
- FOF基金是投资于其他基金的基金
- 该接口返回全市场FOF基金的基本信息
- 采用 Cache-First 策略
"""

import logging
import warnings
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_service


def _as_dataframe(data, label: str) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        print(f"{label}: 返回类型异常，期望 DataFrame，实际 {type(data).__name__}")
        return pd.DataFrame()
    if data.empty:
        print(f"{label}: 返回空数据")
    return data


# ============================================================
# 示例 1: 基本用法 - 获取全部FOF基金列表
# ============================================================
def example_basic():
    """基本用法: 获取全部FOF基金列表"""
    print("=" * 60)
    print("示例 1: 获取全部FOF基金列表")
    print("=" * 60)

    service = get_service()

    try:
        # 获取全部FOF基金列表
        df = _as_dataframe(service.get_fof_list(), "示例1")
        if df.empty:
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"FOF基金数量: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail().to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: FOF基金类型分析
# ============================================================
def example_fof_type_analysis():
    """分析FOF基金的类型分布"""
    print("\n" + "=" * 60)
    print("示例 2: FOF基金类型分布")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_fof_list(), "示例2")
        if df.empty:
            return

        print(f"FOF基金总数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 查找类型相关列
        type_col = None
        for col in df.columns:
            if "类型" in str(col) or "type" in str(col).lower() or "类别" in str(col):
                type_col = col
                break

        if type_col:
            print("\nFOF基金类型分布:")
            print(df[type_col].value_counts().to_string())
        else:
            print("\n当前数据中未发现基金类型字段")

        # 查找策略相关列
        strategy_col = None
        for col in df.columns:
            if "策略" in str(col) or "strategy" in str(col).lower():
                strategy_col = col
                break

        if strategy_col:
            print("\nFOF策略分布:")
            print(df[strategy_col].value_counts().head(10).to_string())

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 3: 筛选特定FOF基金
# ============================================================
def example_filter_fof():
    """在FOF列表中查找特定基金"""
    print("\n" + "=" * 60)
    print("示例 3: 查找特定FOF基金")
    print("=" * 60)

    service = get_service()

    # 示例FOF基金代码 (实际代码请查询)
    target_keywords = ["养老", "配置", "稳健"]

    try:
        df = _as_dataframe(service.get_fof_list(), "示例3")
        if df.empty:
            return

        print(f"FOF基金总数: {len(df)}")

        # 查找名称列
        name_col = None
        for col in df.columns:
            if "名称" in str(col) or "name" in str(col).lower():
                name_col = col
                break

        if name_col:
            for keyword in target_keywords:
                matched = df[df[name_col].astype(str).str.contains(keyword, na=False)]
                print(f"\n包含'{keyword}'的FOF基金: {len(matched)} 只")
                if not matched.empty:
                    print(matched.head(5).to_string(index=False))
        else:
            print("未找到基金名称字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"查找失败: {e}")


# ============================================================
# 示例 4: FOF基金统计信息
# ============================================================
def example_fof_statistics():
    """获取FOF基金的统计信息"""
    print("\n" + "=" * 60)
    print("示例 4: FOF基金统计信息")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_fof_list(), "示例4")
        if df.empty:
            return

        print(f"FOF基金总数: {len(df)}")
        print(f"数据字段: {list(df.columns)}")

        # 查找数值型字段进行统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

        if numeric_cols:
            for col in numeric_cols[:3]:  # 只显示前3个数值列
                print(f"\n{col} 统计:")
                print(f"  平均值: {df[col].mean():.4f}")
                print(f"  中位数: {df[col].median():.4f}")
                print(f"  最大值: {df[col].max():.4f}")
                print(f"  最小值: {df[col].min():.4f}")
        else:
            print("\n当前数据中无数值型字段")

    except Exception as e:
        print(f"统计失败: {e}")


# ============================================================
# 示例 5: 获取前N只FOF基金
# ============================================================
def example_top_fofs():
    """获取FOF基金列表并显示前N只"""
    print("\n" + "=" * 60)
    print("示例 5: 查看前N只FOF基金")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_fof_list(), "示例5")
        if df.empty:
            return

        print(f"FOF基金总数: {len(df)}")
        print("\n前20只FOF基金:")
        print(df.head(20).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_fof_type_analysis()
    example_filter_fof()
    example_fof_statistics()
    example_top_fofs()
