"""
get_fof_nav() 接口示例

演示如何使用 DataService.get_fof_nav() 获取FOF基金历史净值数据。

接口说明:
- get_fof_nav(fund_code, start_date, end_date): 获取指定FOF基金的历史净值
  - fund_code: FOF基金代码，如 "005156"
  - start_date: 起始日期，格式 "YYYY-MM-DD"
  - end_date: 结束日期，格式 "YYYY-MM-DD"
- 返回: pd.DataFrame，包含日期、单位净值、累计净值等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_fof_nav("005156", "2024-01-01", "2024-03-31")

注意:
- FOF基金净值更新频率通常较低(周度或月度)
- 该接口需要指定具体的FOF基金代码
- 采用 Cache-First 策略
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


def _recent_date_range(days: int) -> tuple[str, str]:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days)
    return start.isoformat(), end.isoformat()


# ============================================================
# 示例 1: 基本用法 - 获取单只FOF基金净值
# ============================================================
def example_basic():
    """基本用法: 获取FOF基金历史净值"""
    print("=" * 60)
    print("示例 1: 获取FOF基金历史净值")
    print("=" * 60)

    service = get_service()

    try:
        # fund_code: FOF基金代码
        # start_date: 起始日期
        # end_date: 结束日期
        start_date, end_date = _recent_date_range(120)
        df = _as_dataframe(service.get_fof_nav(
            fund_code="005156",
            start_date=start_date,
            end_date=end_date,
        ), "示例1")

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
# 示例 2: 对比多只FOF基金
# ============================================================
def example_multiple_fofs():
    """对比多只FOF基金的净值表现"""
    print("\n" + "=" * 60)
    print("示例 2: 多只FOF基金净值对比")
    print("=" * 60)

    service = get_service()

    # 示例FOF基金代码
    fof_codes = [
        ("005156", "某养老目标日期FOF"),
        ("006321", "某稳健配置FOF"),
    ]

    for code, name in fof_codes:
        try:
            start_date, end_date = _recent_date_range(30)
            df = _as_dataframe(service.get_fof_nav(
                fund_code=code,
                start_date=start_date,
                end_date=end_date,
            ), f"示例2-{code}")

            if not df.empty:
                print(f"\n{name} ({code}):")
                print(f"  数据行数: {len(df)}")

                # 查找净值列
                nav_col = None
                for col in df.columns:
                    if "净值" in str(col) or "nav" in str(col).lower():
                        nav_col = col
                        break

                if nav_col and nav_col in df.columns:
                    nav = pd.to_numeric(df[nav_col], errors="coerce").dropna()
                    if not nav.empty:
                        print(f"  {nav_col}范围: {nav.min():.4f} ~ {nav.max():.4f}")
                    else:
                        print(f"  {nav_col} 无有效数值")
                else:
                    print(f"  字段: {list(df.columns)}")
            else:
                print(f"\n{name} ({code}) - 无数据")
        except Exception as e:
            print(f"\n{name} ({code}) - 获取失败: {e}")


# ============================================================
# 示例 3: 长期净值走势
# ============================================================
def example_long_term():
    """获取FOF基金长期净值数据"""
    print("\n" + "=" * 60)
    print("示例 3: FOF基金长期净值走势")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(365)
        df = _as_dataframe(service.get_fof_nav(
            fund_code="005156",
            start_date=start_date,
            end_date=end_date,
        ), "示例3")

        if df.empty:
            return

        print("FOF基金最近一年净值数据")
        print(f"数据形状: {df.shape}")
        print(f"净值更新次数: {len(df)}")

        # 查找净值列
        nav_col = None
        for col in df.columns:
            if "单位净值" in str(col) or ("nav" in str(col).lower() and "累计" not in str(col)):
                nav_col = col
                break

        if nav_col and nav_col in df.columns:
            nav = pd.to_numeric(df[nav_col], errors="coerce").dropna()
            if len(nav) < 2:
                print(f"\n{nav_col} 有效数据不足")
                return
            print(f"\n区间起点{nav_col}: {nav.iloc[0]:.4f}")
            print(f"区间终点{nav_col}: {nav.iloc[-1]:.4f}")
            yearly_return = (nav.iloc[-1] - nav.iloc[0]) / nav.iloc[0] * 100
            print(f"年度收益率: {yearly_return:.2f}%")
        else:
            print(f"\n可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 净值波动分析
# ============================================================
def example_volatility():
    """分析FOF基金净值波动"""
    print("\n" + "=" * 60)
    print("示例 4: FOF基金净值波动分析")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(180)
        df = _as_dataframe(service.get_fof_nav(
            fund_code="005156",
            start_date=start_date,
            end_date=end_date,
        ), "示例4")

        if df.empty:
            return

        print(f"数据形状: {df.shape}")

        # 查找净值列
        nav_col = None
        for col in df.columns:
            if "单位净值" in str(col) or ("nav" in str(col).lower() and "累计" not in str(col)):
                nav_col = col
                break

        if nav_col and nav_col in df.columns:
            df["daily_return"] = pd.to_numeric(df[nav_col], errors="coerce").pct_change() * 100

            print(f"\n日收益率统计:")
            print(f"  平均日收益率: {df['daily_return'].mean():.4f}%")
            print(f"  收益率标准差: {df['daily_return'].std():.4f}%")
            print(f"  最大单日涨幅: {df['daily_return'].max():.2f}%")
            print(f"  最大单日跌幅: {df['daily_return'].min():.2f}%")
        else:
            print("未找到单位净值字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    test_cases = [
        ("999999", "不存在的基金代码"),
        ("", "空字符串"),
        ("ABC", "非法格式"),
    ]

    for code, desc in test_cases:
        try:
            start_date, end_date = _recent_date_range(30)
            df = _as_dataframe(service.get_fof_nav(
                fund_code=code,
                start_date=start_date,
                end_date=end_date,
            ), f"示例5-{code}")
            if df.empty:
                print(f"{desc} ('{code}'): 无数据")
            else:
                print(f"{desc} ('{code}'): 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"{desc} ('{code}'): 异常 - {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_fofs()
    example_long_term()
    example_volatility()
    example_error_handling()
