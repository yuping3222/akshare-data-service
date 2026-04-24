"""
get_index_valuation() 接口示例

演示如何使用 akshare_data.get_index_valuation() 获取指数估值数据。

接口说明:
  - get_index_valuation(index_code) -> pd.DataFrame
  - 返回指数的估值指标，如 PE-TTM、PB 等

常用指数代码:
  - "000001": 上证指数
  - "000016": 上证50
  - "000300": 沪深300
  - "000905": 中证500
  - "399001": 深证成指
  - "399006": 创业板指

常用估值指标:
  - pe_ttm: 指数市盈率-TTM
  - pb: 指数市净率
"""

from akshare_data import get_service
def _first_non_empty_index(service, codes):
    for code in codes:
        try:
            df = service.get_index_valuation(index_code=code)
            if df is not None and not df.empty:
                return df, code
        except Exception:
            continue
    return None, None




# ============================================================
# 示例 1: 基本用法 - 获取上证指数估值
# ============================================================
def example_basic():
    """基本用法: 获取上证指数估值数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取上证指数估值")
    print("=" * 60)

    service = get_service()

    try:
        # index_code: 指数代码
        df, used_code = _first_non_empty_index(service, ["000001", "000300", "399001"])

        if df is None or df.empty:
            print("无数据")
            print("提示: get_index_valuation 目前主要由 Lixinger 数据源支持，")
            print("      请确保 LIXINGER_TOKEN 环境变量已配置")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"回退命中指数: {used_code}")
        print(f"字段列表: {list(df.columns)}")

        # 打印全部数据
        print("\n估值数据:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取估值数据失败: {e}")


# ============================================================
# 示例 2: 对比多个宽基指数估值
# ============================================================
def example_compare_indices():
    """对比多个宽基指数的估值水平"""
    print("\n" + "=" * 60)
    print("示例 2: 宽基指数估值对比")
    print("=" * 60)

    service = get_service()

    # 主要宽基指数
    indices = {
        "000001": "上证指数",
        "000016": "上证50",
        "000300": "沪深300",
        "000905": "中证500",
        "399001": "深证成指",
        "399006": "创业板指",
    }

    results = []
    for code, name in indices.items():
        try:
            df = service.get_index_valuation(index_code=code)
            if df is not None and not df.empty:
                row = {"指数代码": code, "指数名称": name}
                for col in ["pe_ttm", "pb"]:
                    if col in df.columns:
                        row[col] = df[col].iloc[0]
                results.append(row)
                print(f"{name}({code}): 获取成功")
            else:
                print(f"{name}({code}): 无数据")
        except Exception as e:
            print(f"{name}({code}): 获取失败 - {e}")

    if results:
        import pandas as pd

        compare_df = pd.DataFrame(results)
        print("\n指数估值对比表:")
        print(compare_df.to_string(index=False))


# ============================================================
# 示例 3: 获取深证指数估值
# ============================================================
def example_sz_index():
    """获取深证成指估值数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取深证成指估值")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_index_valuation(index_code="399001")

        if df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n估值数据:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 估值水平分析
# ============================================================
def example_valuation_analysis():
    """演示获取指数估值后进行简单分析"""
    print("\n" + "=" * 60)
    print("示例 4: 估值水平分析")
    print("=" * 60)

    service = get_service()

    # 关注几只核心指数
    core_indices = {
        "000300": "沪深300",
        "000905": "中证500",
        "399006": "创业板指",
    }

    for code, name in core_indices.items():
        try:
            df = service.get_index_valuation(index_code=code)
            if df.empty:
                continue

            print(f"\n{name}({code}):")
            if "pe_ttm" in df.columns:
                pe = df["pe_ttm"].iloc[0]
                print(f"  市盈率(PE-TTM): {pe}")
                # 简单判断估值分位 (仅为示例，非真实历史分位)
                if pe < 15:
                    print(f"  估值判断: 偏低")
                elif pe < 25:
                    print(f"  估值判断: 适中")
                else:
                    print(f"  估值判断: 偏高")

            if "pb" in df.columns:
                pb = df["pb"].iloc[0]
                print(f"  市净率(PB): {pb}")

        except Exception as e:
            print(f"{name}({code}): 分析失败 - {e}")


# ============================================================
# 示例 5: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效指数代码等"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试 1: 正常获取
    print("\n测试 1: 正常获取")
    try:
        df, used_code = _first_non_empty_index(service, ["000001", "000300", "399001"])
        if df is None or df.empty:
            print("  结果: 无数据 (空 DataFrame)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据 (回退指数: {used_code})")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 2: 无效指数代码
    print("\n测试 2: 无效指数代码")
    try:
        df = service.get_index_valuation(index_code="999999")
        if df is None or df.empty:
            print("  结果: 无数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_indices()
    example_sz_index()
    example_valuation_analysis()
    example_error_handling()
