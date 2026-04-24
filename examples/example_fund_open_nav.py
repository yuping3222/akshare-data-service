"""
get_fund_open_nav() 接口示例

演示如何使用 DataService.get_fund_open_nav() 获取开放式基金历史净值数据。

接口说明:
- get_fund_open_nav(fund_code, start_date, end_date): 获取指定基金的历史净值
  - fund_code: 基金代码，如 "110011"
  - start_date: 起始日期，格式 "YYYY-MM-DD"
  - end_date: 结束日期，格式 "YYYY-MM-DD"
- 返回: pd.DataFrame，包含日期、单位净值、累计净值等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_fund_open_nav("110011", "2024-01-01", "2024-03-31")

注意:
- 该接口需要指定具体的基金代码
- 日期范围建议不超过一年，以控制数据量
- 采用 Cache-First 策略，支持增量更新
"""

from datetime import date, timedelta

import pandas as pd

from akshare_data import get_service


def _as_dataframe(data, label: str) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        print(f"{label}: 返回类型异常，期望 DataFrame，实际 {type(data).__name__}")
        return pd.DataFrame()
    if data.empty:
        print(f"{label}: 返回空数据")
    return data


def _to_numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def _recent_date_range(days: int = 180) -> tuple[str, str]:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days)
    return start.isoformat(), end.isoformat()


# ============================================================
# 示例 1: 基本用法 - 获取单只基金历史净值
# ============================================================
def example_basic():
    """基本用法: 获取易方达蓝筹精选的历史净值"""
    print("=" * 60)
    print("示例 1: 获取基金历史净值 - 易方达蓝筹精选 (110011)")
    print("=" * 60)

    service = get_service()

    try:
        # fund_code: 基金代码
        # start_date: 起始日期
        # end_date: 结束日期
        start_date, end_date = _recent_date_range(days=120)
        df = _as_dataframe(
            service.get_fund_open_nav(
            fund_code="110011",
            start_date=start_date,
            end_date=end_date,
            ),
            "示例1",
        )

        if df.empty:
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head().to_string(index=False))

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail().to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 不同基金对比
# ============================================================
def example_multiple_funds():
    """对比多只基金的历史净值表现"""
    print("\n" + "=" * 60)
    print("示例 2: 多只基金净值对比")
    print("=" * 60)

    service = get_service()

    funds = [
        ("110011", "易方达蓝筹精选"),
        ("000001", "华夏成长"),
        ("161725", "招商中证白酒指数"),
    ]

    for code, name in funds:
        try:
            start_date, end_date = _recent_date_range(days=30)
            df = _as_dataframe(
                service.get_fund_open_nav(
                fund_code=code,
                start_date=start_date,
                end_date=end_date,
                ),
                f"示例2-{code}",
            )

            if not df.empty:
                print(f"\n{name} ({code}):")
                print(f"  数据行数: {len(df)}")

                # 查找净值列
                nav_col = None
                for col in df.columns:
                    if "净值" in str(col) or "nav" in str(col).lower():
                        nav_col = col
                        break

                if nav_col:
                    nav_series = _to_numeric_series(df, nav_col).dropna()
                    if not nav_series.empty:
                        print(f"  {nav_col}范围: {nav_series.min():.4f} ~ {nav_series.max():.4f}")
                    else:
                        print(f"  {nav_col} 列无法转换为有效数值")
                else:
                    print(f"  字段: {list(df.columns)}")
            else:
                print(f"\n{name} ({code}) - 无数据")
        except Exception as e:
            print(f"\n{name} ({code}) - 获取失败: {e}")


# ============================================================
# 示例 3: 长期净值趋势分析
# ============================================================
def example_long_term_trend():
    """获取较长时间跨度的净值数据进行分析"""
    print("\n" + "=" * 60)
    print("示例 3: 长期净值趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(days=365)
        df = _as_dataframe(
            service.get_fund_open_nav(
            fund_code="110011",
            start_date=start_date,
            end_date=end_date,
            ),
            "示例3",
        )

        if df.empty:
            return

        print(f"易方达蓝筹精选 最近一年净值数据")
        print(f"数据形状: {df.shape}")
        print(f"全年净值更新天数: {len(df)}")

        # 查找净值列
        nav_col = None
        for col in df.columns:
            if "单位净值" in str(col) or "nav" in str(col).lower():
                nav_col = col
                break

        if nav_col and nav_col in df.columns:
            nav_series = _to_numeric_series(df, nav_col).dropna()
            if len(nav_series) < 2:
                print(f"\n{nav_col} 有效数值不足，无法计算区间收益")
                return
            print(f"\n区间起点{nav_col}: {nav_series.iloc[0]:.4f}")
            print(f"区间终点{nav_col}: {nav_series.iloc[-1]:.4f}")
            yearly_return = (nav_series.iloc[-1] - nav_series.iloc[0]) / nav_series.iloc[0] * 100
            print(f"年度收益率: {yearly_return:.2f}%")
        else:
            print(f"\n可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 净值波动分析
# ============================================================
def example_volatility_analysis():
    """计算基金净值的波动情况"""
    print("\n" + "=" * 60)
    print("示例 4: 净值波动分析")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(days=180)
        df = _as_dataframe(
            service.get_fund_open_nav(
            fund_code="110011",
            start_date=start_date,
            end_date=end_date,
            ),
            "示例4",
        )

        if df.empty:
            return

        print(f"数据形状: {df.shape}")

        # 查找净值列并计算日涨跌幅
        nav_col = None
        for col in df.columns:
            if "单位净值" in str(col) or ("nav" in str(col).lower() and "累计" not in str(col)):
                nav_col = col
                break

        if nav_col and nav_col in df.columns:
            nav_series = _to_numeric_series(df, nav_col)
            df["daily_return"] = nav_series.pct_change() * 100

            print(f"\n日收益率统计:")
            print(f"  平均日收益率: {df['daily_return'].mean():.4f}%")
            print(f"  日收益率标准差: {df['daily_return'].std():.4f}%")
            print(f"  最大单日涨幅: {df['daily_return'].max():.2f}%")
            print(f"  最大单日跌幅: {df['daily_return'].min():.2f}%")
        else:
            print("未找到单位净值字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 错误处理 - 无效基金代码
# ============================================================
def example_error_handling():
    """演示错误处理 - 使用无效基金代码"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理 - 无效基金代码")
    print("=" * 60)

    service = get_service()

    invalid_codes = ["999999", "ABCDEF", ""]

    for code in invalid_codes:
        try:
            start_date, end_date = _recent_date_range(days=30)
            df = _as_dataframe(
                service.get_fund_open_nav(
                fund_code=code,
                start_date=start_date,
                end_date=end_date,
                ),
                f"示例5-{code}",
            )
            if df.empty:
                print(f"基金代码 '{code}': 无数据 (空 DataFrame)")
            else:
                print(f"基金代码 '{code}': 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"基金代码 '{code}': 捕获异常 - {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_funds()
    example_long_term_trend()
    example_volatility_analysis()
    example_error_handling()
