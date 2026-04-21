"""
get_lof_nav() 接口示例

演示如何使用 DataService.get_lof_nav() 获取LOF基金净值数据。

接口说明:
- get_lof_nav(fund_code): 获取指定LOF基金的最新净值
  - fund_code: LOF基金代码，如 "162605"
- 返回: pd.DataFrame，包含净值日期、单位净值、累计净值等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_lof_nav("162605")

注意:
- LOF基金既有场内交易价格，也有场外净值
- 该接口获取的是LOF基金的场外净值数据
- 采用 Cache-First 策略
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只LOF基金净值
# ============================================================
def example_basic():
    """基本用法: 获取LOF基金净值"""
    print("=" * 60)
    print("示例 1: 获取LOF基金净值")
    print("=" * 60)

    service = get_service()

    try:
        # fund_code: LOF基金代码
        df = service.get_lof_nav(fund_code="162605")

        if df is None or df.empty:
            print("无数据 (数据源未返回结果，可能是网络问题或基金代码不存在)")
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
# 示例 2: 对比多只LOF基金净值
# ============================================================
def example_multiple_lofs():
    """对比多只LOF基金的净值"""
    print("\n" + "=" * 60)
    print("示例 2: 多只LOF基金净值对比")
    print("=" * 60)

    service = get_service()

    lofs = [
        ("162605", "景顺长城鼎益"),
        ("163402", "兴全趋势投资"),
        ("161005", "富国天惠成长"),
    ]

    for code, name in lofs:
        try:
            df = service.get_lof_nav(fund_code=code)

            if df is not None and not df.empty:
                print(f"\n{name} ({code}):")
                print(f"  数据行数: {len(df)}")

                # 查找净值列
                nav_col = None
                for col in df.columns:
                    if "净值" in str(col) or "nav" in str(col).lower():
                        nav_col = col
                        break

                if nav_col and nav_col in df.columns and df[nav_col].dtype in ["float64", "int64"]:
                    print(f"  最新{nav_col}: {df.iloc[-1][nav_col]:.4f}")
                else:
                    print(f"  字段: {list(df.columns)}")
            else:
                print(f"\n{name} ({code}) - 无数据")
        except Exception as e:
            print(f"\n{name} ({code}) - 获取失败: {e}")


# ============================================================
# 示例 3: 净值趋势分析
# ============================================================
def example_nav_trend():
    """分析LOF基金净值趋势"""
    print("\n" + "=" * 60)
    print("示例 3: LOF基金净值趋势")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_lof_nav(fund_code="162605")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"景顺长城鼎益 (162605) 净值数据")
        print(f"数据形状: {df.shape}")

        # 查找净值列和日期列
        nav_col = None
        date_col = None

        for col in df.columns:
            if "净值" in str(col) or "nav" in str(col).lower():
                nav_col = col
            if "日期" in str(col) or "date" in str(col).lower() or "时间" in str(col):
                date_col = col

        if nav_col and nav_col in df.columns and df[nav_col].dtype in ["float64", "int64"]:
            print(f"\n{nav_col}统计:")
            print(f"  最新: {df.iloc[-1][nav_col]:.4f}")
            print(f"  最高: {df[nav_col].max():.4f}")
            print(f"  最低: {df[nav_col].min():.4f}")
            print(f"  平均: {df[nav_col].mean():.4f}")

            if len(df) > 1:
                total_return = (df.iloc[-1][nav_col] - df.iloc[0][nav_col]) / df.iloc[0][nav_col] * 100
                print(f"  区间总收益: {total_return:.2f}%")
        else:
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 4: 净值与场内价格对比概念
# ============================================================
def example_premium_discount():
    """演示LOF折溢价概念 (需要结合 get_lof_spot 数据)"""
    print("\n" + "=" * 60)
    print("示例 4: LOF折溢价概念")
    print("=" * 60)

    service = get_service()

    try:
        # 获取LOF净值
        nav_df = service.get_lof_nav(fund_code="162605")

        if nav_df is None or nav_df.empty:
            print("净值数据: 无数据")
            return

        print("LOF基金同时具有:")
        print("  1. 场外净值 (通过 get_lof_nav 获取)")
        print("  2. 场内交易价格 (通过 get_lof_spot 获取)")
        print("\n当交易价格 > 净值时，称为溢价")
        print("当交易价格 < 净值时，称为折价")
        print("\n景顺长城鼎益 (162605) 最新净值数据:")
        print(nav_df.tail(3).to_string(index=False))

        # 查找最新净值
        nav_col = None
        for col in nav_df.columns:
            if "净值" in str(col) or "nav" in str(col).lower():
                nav_col = col
                break

        if nav_col and nav_col in nav_df.columns:
            latest_nav = nav_df.iloc[-1][nav_col]
            print(f"\n最新净值: {latest_nav}")
            print("\n可通过 get_lof_spot() 获取场内价格后计算折溢价率:")
            print("  折溢价率 = (场内价格 - 净值) / 净值 * 100%")

    except Exception as e:
        print(f"获取数据失败: {e}")


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
            df = service.get_lof_nav(fund_code=code)
            if df is None or df.empty:
                print(f"{desc} ('{code}'): 无数据")
            else:
                print(f"{desc} ('{code}'): 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"{desc} ('{code}'): 异常 - {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_lofs()
    example_nav_trend()
    example_premium_discount()
    example_error_handling()
