"""
get_lof_daily() 接口示例

演示如何使用 DataService.get_lof_daily() 获取LOF基金历史日线数据。

接口说明:
- get_lof_daily(symbol, start_date, end_date): 获取指定LOF基金的历史日线行情
  - symbol: LOF基金代码，如 "162605"
  - start_date: 起始日期，格式 "YYYY-MM-DD"
  - end_date: 结束日期，格式 "YYYY-MM-DD"
- 返回: pd.DataFrame，包含日期、开盘、收盘、最高、最低、成交量等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_lof_daily("162605", "2024-01-01", "2024-03-31")

注意:
- 该接口获取的是LOF基金在场内交易的历史日线数据
- 与 get_lof_nav() 获取的场外净值不同
- 采用 Cache-First 策略，支持增量更新
- 数据包含 K 线信息 (open/high/low/close/volume)
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
# 示例 1: 基本用法 - 获取单只LOF日线数据
# ============================================================
def example_basic():
    """基本用法: 获取景顺长城鼎益的日线数据"""
    print("=" * 60)
    print("示例 1: 获取LOF基金日线数据 - 景顺长城鼎益 (162605)")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: LOF基金代码
        # start_date: 起始日期
        # end_date: 结束日期
        start_date, end_date = _recent_date_range(120)
        df = _as_dataframe(service.get_lof_daily(
            symbol="162605",
            start_date=start_date,
            end_date=end_date,
        ), "示例1")

        if df.empty:
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"交易日数: {len(df)}")
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
# 示例 2: 对比多只LOF基金的日线表现
# ============================================================
def example_multiple_lofs():
    """对比多只LOF基金在同一时期的日线表现"""
    print("\n" + "=" * 60)
    print("示例 2: 多只LOF基金日线对比")
    print("=" * 60)

    service = get_service()

    lofs = [
        ("162605", "景顺长城鼎益"),
        ("163402", "兴全趋势投资"),
        ("161005", "富国天惠成长"),
    ]

    for code, name in lofs:
        try:
            start_date, end_date = _recent_date_range(30)
            df = _as_dataframe(service.get_lof_daily(
                symbol=code,
                start_date=start_date,
                end_date=end_date,
            ), f"示例2-{code}")

            if not df.empty:
                print(f"\n{name} ({code}):")
                print(f"  数据行数: {len(df)}")

                if "close" in df.columns:
                    print(f"  收盘价范围: {df['close'].min():.3f} ~ {df['close'].max():.3f}")
                    print(f"  最新收盘价: {df['close'].iloc[-1]:.3f}")
                elif "收盘" in df.columns:
                    print(f"  收盘价范围: {df['收盘'].min():.3f} ~ {df['收盘'].max():.3f}")

                if "volume" in df.columns:
                    print(f"  总成交量: {df['volume'].sum():,.0f}")
            else:
                print(f"\n{name} ({code}) - 无数据")
        except Exception as e:
            print(f"\n{name} ({code}) - 获取失败: {e}")


# ============================================================
# 示例 3: 长期趋势分析
# ============================================================
def example_long_term_trend():
    """获取LOF基金长期日线数据进行趋势分析"""
    print("\n" + "=" * 60)
    print("示例 3: LOF基金长期趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(365)
        df = _as_dataframe(service.get_lof_daily(
            symbol="162605",
            start_date=start_date,
            end_date=end_date,
        ), "示例3")

        if df.empty:
            return

        print("景顺长城鼎益 (162605) 最近一年日线数据")
        print(f"数据形状: {df.shape}")
        print(f"全年交易日数: {len(df)}")

        # 查找收盘价列
        close_col = None
        for col in df.columns:
            if "close" in str(col).lower() or "收盘" in str(col):
                close_col = col
                break

        if close_col and close_col in df.columns:
            print(f"\n价格统计:")
            print(f"  年初收盘价: {df.iloc[0][close_col]:.3f}")
            print(f"  年末收盘价: {df.iloc[-1][close_col]:.3f}")
            yearly_return = (df.iloc[-1][close_col] - df.iloc[0][close_col]) / df.iloc[0][close_col] * 100
            print(f"  年度涨跌幅: {yearly_return:.2f}%")
            print(f"  最高价: {df[close_col].max():.3f}")
            print(f"  最低价: {df[close_col].min():.3f}")
        else:
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 技术指标计算
# ============================================================
def example_technical_indicators():
    """基于LOF日线数据计算简单技术指标"""
    print("\n" + "=" * 60)
    print("示例 4: 技术指标计算 - 移动平均线")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(180)
        df = _as_dataframe(service.get_lof_daily(
            symbol="162605",
            start_date=start_date,
            end_date=end_date,
        ), "示例4")

        if df.empty:
            return

        print(f"数据形状: {df.shape}")

        # 查找收盘价列
        close_col = None
        for col in df.columns:
            if "close" in str(col).lower() or "收盘" in str(col):
                close_col = col
                break

        if close_col and close_col in df.columns:
            # 计算5日、10日、20日移动平均线
            close_series = pd.to_numeric(df[close_col], errors="coerce")
            df["ma5"] = close_series.rolling(window=5).mean()
            df["ma10"] = close_series.rolling(window=10).mean()
            df["ma20"] = close_series.rolling(window=20).mean()

            print(f"\n移动平均线分析 (基于 {close_col}):")
            print("\n最新10个交易日数据:")
            display_cols = [col for col in ["date", "日期", close_col, "ma5", "ma10", "ma20"] if col in df.columns]
            if display_cols:
                print(df[display_cols].tail(10).to_string(index=False))
            else:
                print(df.tail(10).to_string(index=False))

            # 判断趋势
            latest = df.iloc[-1]
            if "ma5" in df.columns and "ma20" in df.columns:
                if latest["ma5"] > latest["ma20"]:
                    print("\n趋势判断: 短期均线在长期均线上方，短期趋势偏强")
                else:
                    print("\n趋势判断: 短期均线在长期均线下方，短期趋势偏弱")
        else:
            print("未找到收盘价字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"计算失败: {e}")


# ============================================================
# 示例 5: 成交量分析
# ============================================================
def example_volume_analysis():
    """分析LOF基金的成交量变化"""
    print("\n" + "=" * 60)
    print("示例 5: 成交量分析")
    print("=" * 60)

    service = get_service()

    try:
        start_date, end_date = _recent_date_range(120)
        df = _as_dataframe(service.get_lof_daily(
            symbol="162605",
            start_date=start_date,
            end_date=end_date,
        ), "示例5")

        if df.empty:
            return

        print(f"数据形状: {df.shape}")

        # 查找成交量列
        volume_col = None
        for col in df.columns:
            if "volume" in str(col).lower() or "成交量" in str(col) or "vol" in str(col).lower():
                volume_col = col
                break

        if volume_col and volume_col in df.columns:
            df[volume_col] = pd.to_numeric(df[volume_col], errors="coerce")

            print(f"\n成交量统计 ({volume_col}):")
            print(f"  总成交量: {df[volume_col].sum():,.0f}")
            print(f"  日均成交量: {df[volume_col].mean():,.0f}")
            print(f"  最大单日成交量: {df[volume_col].max():,.0f}")
            print(f"  最小单日成交量: {df[volume_col].min():,.0f}")

            # 成交量最大的5天
            top_vol_days = df.nlargest(5, volume_col)
            print(f"\n成交量最大的5个交易日:")
            display_cols = [col for col in ["date", "日期", volume_col] if col in df.columns]
            if display_cols:
                print(top_vol_days[display_cols].to_string(index=False))
        else:
            print("未找到成交量字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 6: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理")
    print("=" * 60)

    service = get_service()

    test_cases = [
        ("999999", "不存在的基金代码"),
        ("", "空字符串"),
        ("162605", "近未来区间"),
    ]

    for code, desc in test_cases:
        try:
            if desc == "近未来区间":
                start = (date.today() + timedelta(days=1)).isoformat()
                end = (date.today() + timedelta(days=30)).isoformat()
            else:
                start, end = _recent_date_range(30)
            df = service.get_lof_daily(symbol=code, start_date=start, end_date=end)
            df = _as_dataframe(df, f"示例6-{code or 'EMPTY'}")
            if df.empty:
                print(f"{desc}: 无数据")
            else:
                print(f"{desc}: 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"{desc}: 异常 - {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_lofs()
    example_long_term_trend()
    example_technical_indicators()
    example_volume_analysis()
    example_error_handling()
