"""
get_chip_distribution() 接口示例

演示如何使用 akshare_data.get_chip_distribution() 获取筹码分布数据。

参数说明:
    symbol: 股票代码

返回字段: 包含价格区间、筹码数量、持仓成本等
"""

from datetime import date, timedelta

from akshare_data import get_service


def _candidate_fallback_dates(count: int = 5) -> list[str]:
    """返回最近若干个可用回退日期（仅工作日，不含未来）"""
    today = date.today()
    cursor = today if today.weekday() < 5 else today - timedelta(days=today.weekday() - 4)
    candidates: list[str] = []
    while len(candidates) < count:
        if cursor.weekday() < 5 and cursor <= today:
            candidates.append(cursor.strftime("%Y-%m-%d"))
        cursor -= timedelta(days=1)
    return candidates


def _print_empty_hint(symbol: str) -> None:
    dates = ", ".join(_candidate_fallback_dates())
    print(f"{symbol}: 无数据。")
    print("  说明: get_chip_distribution 当前示例按股票维度查询，不直接接收 date 参数。")
    print(f"  建议: 稍后重试，或先检查最近交易日缓存是否已回补。候选回退日期: {dates}")


# ============================================================
# 示例 1: 基本用法 - 获取筹码分布
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台筹码分布"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台筹码分布")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_chip_distribution(symbol="600519")
        if df is None or df.empty:
            _print_empty_hint("600519")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n筹码分布:")
        print(df)

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 多只股票筹码分布对比
# ============================================================
def example_compare_stocks():
    """对比多只股票的筹码分布"""
    print("\n" + "=" * 60)
    print("示例 2: 多股筹码分布对比")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "600036"]

    for code in stocks:
        try:
            df = service.get_chip_distribution(symbol=code)
            if df is None or df.empty:
                print(f"\n{code}: 无数据")
                print(f"  候选回退日期: {', '.join(_candidate_fallback_dates())}")
            else:
                print(f"\n{code}: {len(df)} 条记录")
                print(df.head(5))
        except Exception as e:
            print(f"\n{code}: 获取失败 - {e}")


# ============================================================
# 示例 3: 筹码集中度分析
# ============================================================
def example_concentration():
    """分析筹码集中度"""
    print("\n" + "=" * 60)
    print("示例 3: 筹码集中度分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_chip_distribution(symbol="600519")
        if df is None or df.empty:
            _print_empty_hint("600519")
            return

        print(f"字段列表: {list(df.columns)}")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 价格区间分析
# ============================================================
def example_price_range():
    """分析筹码在不同价格区间的分布"""
    print("\n" + "=" * 60)
    print("示例 4: 价格区间分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_chip_distribution(symbol="600519")
        if df is None or df.empty:
            _print_empty_hint("600519")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段: {list(df.columns)}")

        if "price" in df.columns or "价格" in df.columns:
            col = "price" if "price" in df.columns else "价格"
            print(f"\n价格范围: {df[col].min():.2f} ~ {df[col].max():.2f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_stocks()
    example_concentration()
    example_price_range()
