"""
可转债相关接口示例

演示如何使用 DataService 获取可转债数据。

包含三个接口:
1. get_conversion_bond_list() - 获取可转债列表
   - 无必需参数
   - 返回: DataFrame - 包含所有可转债的基本信息

2. get_conversion_bond_daily(symbol, start_date, end_date) - 获取可转债日线数据
   - symbol: 可转债代码 (如 "127045" 或 "sh127045")
   - start_date/end_date: 可选，格式 "YYYY-MM-DD"
   - 返回: DataFrame - 指定可转债的历史日线数据

3. calculate_conversion_value() - 计算转股价值
   - bond_price: 可转债价格
   - conversion_ratio: 转股比例
   - stock_price: 正股价格
   - 返回: dict - 包含转股价值和溢价率

注意: 接口通过 service.akshare 访问 AkShareAdapter。
若接口不可用，会打印错误信息并跳过。
"""

import pandas as pd


def calculate_conversion_value(
    bond_price: float,
    conversion_ratio: float,
    stock_price: float,
) -> dict:
    """本地计算转股价值与溢价率（避免依赖 adapter 私有方法）。"""
    conversion_value = conversion_ratio * stock_price
    premium_rate = ((bond_price - conversion_value) / conversion_value) * 100
    return {
        "bond_price": bond_price,
        "conversion_ratio": conversion_ratio,
        "stock_price": stock_price,
        "conversion_value": conversion_value,
        "premium_rate": premium_rate,
    }


# ============================================================
# 示例 1: 获取可转债列表
# ============================================================
def example_convert_bond_list():
    """获取可转债列表"""
    print("=" * 60)
    print("示例 1: 获取可转债列表")
    print("=" * 60)

    from akshare_data import get_service

    service = get_service()
    try:
        df = service.get_conversion_bond_list()
    except Exception as e:
        print(f"获取数据失败: {e}")
        return

    if df is None or df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")
    print("\n前10行数据:")
    print(df.head(10).to_string(index=False))


# ============================================================
# 示例 2: 获取可转债日线数据
# ============================================================
def example_convert_bond_daily():
    """获取单只可转债的日线数据"""
    print("\n" + "=" * 60)
    print("示例 2: 获取可转债日线数据")
    print("=" * 60)

    from akshare_data import get_service

    service = get_service()
    try:
        df = service.get_conversion_bond_daily(
            symbol="127045",
            start_date="2024-01-01",
            end_date="2024-03-31",
        )
    except Exception as e:
        print(f"获取数据失败: {e}")
        return

    if df is None or df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")
    print("\n数据内容:")
    print(df.to_string(index=False))


# ============================================================
# 示例 3: 可转债日线数据 (带交易所前缀)
# ============================================================
def example_convert_bond_daily_with_prefix():
    """带交易所前缀的可转债代码"""
    print("\n" + "=" * 60)
    print("示例 3: 带交易所前缀的可转债代码")
    print("=" * 60)

    from akshare_data import get_service

    service = get_service()
    try:
        df = service.get_conversion_bond_daily(
            symbol="sh110059",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
    except Exception as e:
        print(f"获取数据失败: {e}")
        return

    if df is None or df.empty:
        print("无数据")
        return

    print(f"数据形状: {df.shape}")
    print(f"字段列表: {list(df.columns)}")
    print("\n数据内容:")
    print(df.to_string(index=False))


# ============================================================
# 示例 4: 计算转股价值
# ============================================================
def example_conversion_value():
    """计算可转债的转股价值和溢价率"""
    print("\n" + "=" * 60)
    print("示例 4: 计算转股价值")
    print("=" * 60)

    result = calculate_conversion_value(
        bond_price=120.5,
        conversion_ratio=8.5,
        stock_price=14.2,
    )

    print("计算结果:")
    print(f"  可转债价格: {result['bond_price']:.2f} 元")
    print(f"  转股比例: {result['conversion_ratio']:.2f}")
    print(f"  正股价格: {result['stock_price']:.2f} 元")
    print(f"  转股价值: {result['conversion_value']:.2f} 元")
    print(f"  转股溢价率: {result['premium_rate']:.2f}%")

    if result["premium_rate"] < 0:
        print("\n  转股溢价率为负，存在套利机会!")
    else:
        print("\n  转股溢价率为正，暂无套利机会")


# ============================================================
# 示例 5: 可转债列表筛选分析
# ============================================================
def example_convert_bond_analysis():
    """对可转债列表进行简单筛选分析"""
    print("\n" + "=" * 60)
    print("示例 5: 可转债列表筛选分析")
    print("=" * 60)

    from akshare_data import get_service

    service = get_service()
    try:
        df = service.get_conversion_bond_list()
    except Exception as e:
        print(f"获取数据失败: {e}")
        return

    if df is None or df.empty:
        print("无数据")
        return

    print(f"可转债总数: {len(df)}")
    print(f"字段列表: {list(df.columns)}")

    price_col = None
    for col in df.columns:
        if "价格" in str(col) or "price" in str(col).lower():
            price_col = col
            break

    if price_col:
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        low_price = df[df[price_col] < 100]
        print(f"\n价格低于100元的可转债: {len(low_price)} 只")
        if not low_price.empty:
            print(low_price.head(5).to_string(index=False))
    else:
        print("\n无价格列，显示前5行数据:")
        print(df.head(5).to_string(index=False))


# ============================================================
# 示例 6: 批量获取可转债日线数据
# ============================================================
def example_batch_convert_bond_daily():
    """批量获取多只可转债的日线数据"""
    print("\n" + "=" * 60)
    print("示例 6: 批量获取可转债日线数据")
    print("=" * 60)

    from akshare_data import get_service

    bond_codes = ["127045", "110059", "123107"]

    service = get_service()
    for code in bond_codes:
        try:
            df = service.get_conversion_bond_daily(
                symbol=code,
                start_date="2024-01-01",
                end_date="2024-01-31",
            )
        except Exception as e:
            print(f"\n可转债 {code}: 获取失败 - {e}")
            continue
        if df is None or df.empty:
            print(f"\n可转债 {code}: 无数据")
            continue
        print(f"\n可转债 {code}:")
        print(f"  数据行数: {len(df)}")
        if "close" in df.columns:
            print(f"  收盘价范围: {df['close'].min():.2f} ~ {df['close'].max():.2f}")
        else:
            print("  无收盘价数据")


if __name__ == "__main__":
    example_convert_bond_list()
    example_convert_bond_daily()
    example_convert_bond_daily_with_prefix()
    example_conversion_value()
    example_convert_bond_analysis()
    example_batch_convert_bond_daily()
