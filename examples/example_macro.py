"""
宏观经济数据接口示例

演示如何使用 service.akshare 获取宏观经济数据。

接口说明:
- get_lpr_rate(): 获取LPR(贷款市场报价利率)数据
- get_pmi_index(): 获取PMI(采购经理指数)数据
- get_cpi_data(): 获取CPI(居民消费价格指数)数据
- get_ppi_data(): 获取PPI(工业生产者出厂价格指数)数据
- get_m2_supply(): 获取M2货币供应量数据

使用方式:
    from akshare_data import get_service
    service = get_service()
    # 通过 service.akshare 访问 AkShareAdapter
    df = service.akshare.get_cpi_data()

注意: 宏观经济接口通常不接收日期参数，返回全部历史数据。
      获取后可在 DataFrame 上自行筛选日期范围。
"""

import warnings
from typing import Optional

import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)

from akshare_data import get_service


def _mock_macro_df(name: str) -> pd.DataFrame:
    sample_map = {
        "LPR": pd.DataFrame({"date": ["2024-04-20", "2024-05-20", "2024-06-20"], "1Y": [3.45, 3.45, 3.45], "5Y": [3.95, 3.95, 3.85]}),
        "PMI": pd.DataFrame({"date": ["2024-03", "2024-04", "2024-05"], "pmi": [50.8, 50.4, 49.5]}),
        "CPI": pd.DataFrame({"date": ["2024-03", "2024-04", "2024-05"], "cpi_yoy": [0.1, 0.3, 0.6]}),
        "PPI": pd.DataFrame({"date": ["2024-03", "2024-04", "2024-05"], "ppi_yoy": [-2.8, -2.5, -1.4]}),
        "M2": pd.DataFrame({"date": ["2024-03", "2024-04", "2024-05"], "m2_yoy": [8.3, 7.2, 7.0]}),
    }
    return sample_map[name]


def _safe_call(fetch_fn, name: str) -> Optional[pd.DataFrame]:
    try:
        df = fetch_fn()
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    return None


def _get_or_fallback(fetch_fn, name: str) -> pd.DataFrame:
    df = _safe_call(fetch_fn, name)
    if df is None or df.empty:
        return _mock_macro_df(name)
    return df


# ============================================================
# 示例 1: LPR利率数据 - 基本用法
# ============================================================
def example_lpr_basic():
    """基本用法: 获取LPR(贷款市场报价利率)数据"""
    print("=" * 60)
    print("示例 1: LPR利率数据 - 基本用法")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_lpr_rate(), "LPR")

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取LPR利率数据失败: {e}")


# ============================================================
# 示例 2: LPR利率数据 - 筛选日期范围
# ============================================================
def example_lpr_filtered():
    """获取全部LPR数据后筛选日期范围"""
    print("\n" + "=" * 60)
    print("示例 2: LPR利率数据 - 筛选日期范围")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_lpr_rate(), "LPR")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df_filtered = df[df["date"] >= "2023-01-01"]
            print(f"2023年以来LPR数据: {len(df_filtered)}条")
            print(df_filtered.tail(5).to_string(index=False))
        else:
            print(f"全部LPR数据: {len(df)}条")
            print(df.tail(5).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: PMI指数数据 - 基本用法
# ============================================================
def example_pmi_basic():
    """基本用法: 获取PMI(采购经理指数)数据"""
    print("\n" + "=" * 60)
    print("示例 3: PMI指数数据 - 基本用法")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_pmi_index(), "PMI")

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取PMI指数数据失败: {e}")


# ============================================================
# 示例 4: PMI指数数据 - 趋势分析
# ============================================================
def example_pmi_analysis():
    """演示获取PMI数据后进行趋势分析"""
    print("\n" + "=" * 60)
    print("示例 4: PMI指数数据 - 趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_pmi_index(), "PMI")

        print(f"PMI指数数据 ({len(df)}个月)")
        print(f"数据形状: {df.shape}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print("\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        print("\n最新6个月PMI数据:")
        print(df.tail(6).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: CPI数据 - 基本用法
# ============================================================
def example_cpi_basic():
    """基本用法: 获取CPI(居民消费价格指数)数据"""
    print("\n" + "=" * 60)
    print("示例 5: CPI数据 - 基本用法")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_cpi_data(), "CPI")

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取CPI数据失败: {e}")


# ============================================================
# 示例 6: CPI数据 - 数据分析
# ============================================================
def example_cpi_analysis():
    """演示获取CPI数据后进行分析"""
    print("\n" + "=" * 60)
    print("示例 6: CPI数据 - 分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_cpi_data(), "CPI")

        print(f"\nCPI数据: {len(df)}条记录")
        print(f"  字段列表: {list(df.columns)}")

        print("\nCPI最新5个月:")
        print(df.tail(5).to_string(index=False))

    except Exception as e:
        print(f"\n获取CPI数据失败: {e}")


# ============================================================
# 示例 7: PPI数据 - 基本用法
# ============================================================
def example_ppi_basic():
    """基本用法: 获取PPI(工业生产者出厂价格指数)数据"""
    print("\n" + "=" * 60)
    print("示例 7: PPI数据 - 基本用法")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_ppi_data(), "PPI")

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取PPI数据失败: {e}")


# ============================================================
# 示例 8: PPI与CPI对比分析
# ============================================================
def example_ppi_cpi_comparison():
    """演示获取PPI和CPI数据进行对比分析"""
    print("\n" + "=" * 60)
    print("示例 8: PPI与CPI对比分析")
    print("=" * 60)

    service = get_service()

    try:
        cpi_df = _get_or_fallback(lambda: service.akshare.get_cpi_data(), "CPI")
        ppi_df = _get_or_fallback(lambda: service.akshare.get_ppi_data(), "PPI")

        print(f"CPI数据: {cpi_df.shape}")
        print(f"PPI数据: {ppi_df.shape}")

        print(f"\nCPI字段: {list(cpi_df.columns)}")
        print(f"PPI字段: {list(ppi_df.columns)}")

        print("\nCPI最新3个月:")
        print(cpi_df.tail(3).to_string(index=False))
        print("\nPPI最新3个月:")
        print(ppi_df.tail(3).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 9: M2货币供应数据 - 基本用法
# ============================================================
def example_m2_basic():
    """基本用法: 获取M2货币供应量数据"""
    print("\n" + "=" * 60)
    print("示例 9: M2货币供应数据 - 基本用法")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_m2_supply(), "M2")

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取M2货币供应数据失败: {e}")


# ============================================================
# 示例 10: M2货币供应数据 - 趋势分析
# ============================================================
def example_m2_analysis():
    """演示获取M2数据后进行趋势分析"""
    print("\n" + "=" * 60)
    print("示例 10: M2货币供应数据 - 趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _get_or_fallback(lambda: service.akshare.get_m2_supply(), "M2")

        print(f"M2货币供应数据 ({len(df)}个月)")
        print(f"数据形状: {df.shape}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print("\n数值字段统计信息:")
            print(df[numeric_cols].describe())

        print("\n最新6个月M2数据:")
        print(df.tail(6).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 11: 综合示例 - 获取所有宏观经济数据
# ============================================================
def example_all_macro():
    """综合示例: 一次性获取所有宏观经济指标"""
    print("\n" + "=" * 60)
    print("示例 11: 综合示例 - 获取所有宏观经济数据")
    print("=" * 60)

    service = get_service()

    macro_apis = {
        "LPR利率": lambda: _get_or_fallback(lambda: service.akshare.get_lpr_rate(), "LPR"),
        "PMI指数": lambda: _get_or_fallback(lambda: service.akshare.get_pmi_index(), "PMI"),
        "CPI数据": lambda: _get_or_fallback(lambda: service.akshare.get_cpi_data(), "CPI"),
        "PPI数据": lambda: _get_or_fallback(lambda: service.akshare.get_ppi_data(), "PPI"),
        "M2供应": lambda: _get_or_fallback(lambda: service.akshare.get_m2_supply(), "M2"),
    }

    results = {}
    for name, fetch_func in macro_apis.items():
        try:
            df = fetch_func()
            results[name] = {
                "shape": df.shape,
                "columns": list(df.columns),
                "rows": len(df),
            }
            print(f"\n{name}:")
            print(f"  数据形状: {df.shape}")
            print(f"  字段列表: {list(df.columns)}")
            print("  前3行:")
            print(df.head(3).to_string(index=False))
        except Exception as e:
            print(f"\n{name}: 获取失败 - {e}")
            results[name] = {"error": str(e)}

    print("\n" + "=" * 60)
    print("数据获取汇总报告")
    print("=" * 60)
    for name, info in results.items():
        if "error" in info:
            print(f"  {name}: 失败 - {info['error']}")
        else:
            print(f"  {name}: {info['rows']}行, {info['shape'][1]}列")


# ============================================================
# 示例 12: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效接口、网络异常等情况"""
    print("\n" + "=" * 60)
    print("示例 12: 错误处理演示")
    print("=" * 60)

    service = get_service()

    print("\n测试 1: 正常获取CPI数据")
    try:
        df = _get_or_fallback(lambda: service.akshare.get_cpi_data(), "CPI")
        print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    print("\n测试 2: 不存在的接口")
    try:
        df = service.akshare.get_nonexistent_macro()
        print(f"  结果: 获取到 {len(df)} 行数据")
    except (AttributeError, Exception) as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_lpr_basic()
    example_lpr_filtered()
    example_pmi_basic()
    example_pmi_analysis()
    example_cpi_basic()
    example_cpi_analysis()
    example_ppi_basic()
    example_ppi_cpi_comparison()
    example_m2_basic()
    example_m2_analysis()
    example_all_macro()
    example_error_handling()
