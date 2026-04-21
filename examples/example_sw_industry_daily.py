"""
get_sw_industry_daily() 接口示例

演示如何使用 DataService.get_sw_industry_daily() 获取申万行业指数日线行情数据。

接口说明:
  - industry_code: 申万行业指数代码 (如 "801120" 食品饮料)
  - start_date: 起始日期，格式 "YYYY-MM-DD" (可选)
  - end_date: 结束日期，格式 "YYYY-MM-DD" (可选)
  - source: 数据源名称 (可选)

返回: pd.DataFrame，包含 open, high, low, close, volume 等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_sw_industry_daily("801120")

常用申万行业指数代码:
  - "801120": 食品饮料    "801080": 电子
  - "801150": 医药生物    "801750": 计算机
  - "801780": 银行        "801880": 汽车
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单个行业指数日线
# ============================================================
def example_sw_industry_daily_basic():
    """基本用法: 获取食品饮料行业指数日线数据"""
    print("=" * 60)
    print("示例 1: 获取食品饮料行业指数 (801120) 日线")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_sw_industry_daily(
            index_code="801120",
            start_date="2024-01-01",
            end_date="2024-03-31",
        )

        if df.empty:
            print("无数据（数据源不可用或非交易日）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 获取多个行业指数对比
# ============================================================
def example_sw_industry_daily_compare():
    """获取多个行业指数的近期表现进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 多个行业指数对比")
    print("=" * 60)

    service = get_service()

    industries = {
        "801120": "食品饮料",
        "801080": "电子",
        "801150": "医药生物",
        "801750": "计算机",
        "801780": "银行",
    }

    start_date = "2024-01-01"
    end_date = "2024-06-30"

    print(f"\n{'行业名称':<10} {'指数代码':<10} {'行数':>6} {'期初收盘':>12} {'期末收盘':>12} {'涨跌幅':>10}")
    print("-" * 65)

    for code, name in industries.items():
        try:
            df = service.get_sw_industry_daily(
                index_code=code,
                start_date=start_date,
                end_date=end_date,
            )

            if df.empty or len(df) < 2:
                print(f"{name:<10} {code:<10} {'无数据':>6}")
                continue

            # 假设收盘价字段为 'close'
            close_col = "close" if "close" in df.columns else df.columns[-1]
            start_close = df.iloc[0][close_col]
            end_close = df.iloc[-1][close_col]
            change_pct = (end_close - start_close) / start_close * 100

            print(
                f"{name:<10} {code:<10} {len(df):>6} {start_close:>12.2f} {end_close:>12.2f} {change_pct:>9.2f}%"
            )

        except Exception as e:
            print(f"{name:<10} {code:<10} {'失败':>6} ({e})")


# ============================================================
# 示例 3: 不指定日期范围 (获取全部历史)
# ============================================================
def example_sw_industry_daily_full_history():
    """不指定日期，获取行业指数全部历史数据"""
    print("\n" + "=" * 60)
    print("示例 3: 获取全部历史数据")
    print("=" * 60)

    service = get_service()

    try:
        # 不传入 start_date 和 end_date
        df = service.get_sw_industry_daily(index_code="801120")

        if df.empty:
            print("无数据")
            return

        print(f"食品饮料行业指数全部历史")
        print(f"数据形状: {df.shape}")

        date_col = None
        for col in ["date", "trade_date", "日期"]:
            if col in df.columns:
                date_col = col
                break

        if date_col:
            print(f"时间范围: {df[date_col].min()} ~ {df[date_col].max()}")

        print(f"\n前5行:")
        print(df.head())
        print(f"\n后5行:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 计算行业指数移动平均线
# ============================================================
def example_sw_industry_daily_ma():
    """演示获取行业指数数据后计算移动平均线"""
    print("\n" + "=" * 60)
    print("示例 4: 计算移动平均线")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_sw_industry_daily(
            index_code="801080",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        if df.empty:
            print("无数据")
            return

        close_col = "close" if "close" in df.columns else None
        if close_col is None:
            print(f"未找到收盘价字段，可用字段: {list(df.columns)}")
            return

        df["ma5"] = df[close_col].rolling(window=5).mean()
        df["ma20"] = df[close_col].rolling(window=20).mean()

        print(f"电子行业指数 (801080) 2024年日线 + 均线")
        print(f"数据行数: {len(df)}")

        print(f"\n最新10行:")
        display_cols = [col for col in ["date", close_col, "ma5", "ma20"] if col in df.columns]
        print(df[display_cols].tail(10).to_string(index=False))

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效行业代码"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    # 测试无效行业代码
    try:
        df = service.get_sw_industry_daily(
            index_code="999999",
            start_date="2024-01-01",
            end_date="2024-03-31",
        )
        if df.empty:
            print("无效行业代码 '999999': 返回空数据")
        else:
            print(f"无效行业代码 '999999': 返回 {len(df)} 行")
    except Exception as e:
        print(f"无效行业代码 '999999': 捕获异常 {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_sw_industry_daily_basic()
    example_sw_industry_daily_compare()
    example_sw_industry_daily_full_history()
    example_sw_industry_daily_ma()
    example_error_handling()
