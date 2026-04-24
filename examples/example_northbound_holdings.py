"""
北向持股 (Northbound Holdings) 接口示例

演示如何使用 akshare_data.get_northbound_holdings() 获取北向资金持股数据。

北向资金指通过沪股通、深股通投资A股的香港及境外资金。

接口说明:
    get_northbound_holdings(symbol, start_date, end_date)
    
    参数:
        symbol: 证券代码，支持多种格式 (如 "000001", "sh600519")
        start_date: 起始日期，格式 "YYYY-MM-DD"
        end_date: 结束日期，格式 "YYYY-MM-DD"
    
    返回:
        pd.DataFrame，包含以下字段:
        - date: 日期
        - symbol: 证券代码
        - name: 股票名称
        - hold_volume: 持股数量
        - hold_ratio: 持股占流通股比例
        - market_value: 持股市值
        - 其他相关字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_northbound_holdings("600519", "2024-01-01", "2024-06-30")
    
    或:
    df = service.cn.stock.capital.northbound_holdings("600519", "2024-01-01", "2024-06-30")

注意:
- 北向持股数据通常有1天延迟
- 该接口依赖 akshare 数据源，可通过 source 参数指定
"""

import re
import time
from typing import Callable, Optional

import pandas as pd
from akshare_data import get_service


def _normalize_symbol(symbol: str) -> str:
    m = re.search(r"(\d{6})", symbol)
    return m.group(1) if m else symbol.strip()


def _fetch_with_retry(fetcher: Callable[[], pd.DataFrame], desc: str) -> Optional[pd.DataFrame]:
    for i in range(3):
        try:
            df = fetcher()
            if df is not None and not df.empty:
                return df
            print(f"{desc}: 第 {i + 1}/3 次返回空结果")
        except Exception as e:  # noqa: BLE001
            print(f"{desc}: 第 {i + 1}/3 次失败 -> {e}")
        time.sleep(1)
    return None


# ============================================================
# 示例 1: 基本用法 - 获取单只股票的北向持股数据
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的北向持股数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台北向持股数据")
    print("=" * 60)

    service = get_service()

    try:
        # 获取贵州茅台2024年第一季度的北向持股数据
        df = _fetch_with_retry(
            lambda: service.get_northbound_holdings(
            symbol=_normalize_symbol("600519"),
            start_date="2024-01-01",
            end_date="2024-03-31",
            ),
            "get_northbound_holdings(600519)",
        )

        if df is None:
            print("无数据（数据源不可用或非交易日）")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取北向持股数据失败: {e}")


# ============================================================
# 示例 2: 获取多只股票的北向持股数据进行对比
# ============================================================
def example_multiple_stocks():
    """获取多只股票，对比北向资金持股情况"""
    print("\n" + "=" * 60)
    print("示例 2: 多只股票北向持股对比")
    print("=" * 60)

    service = get_service()

    # 选取几只不同行业的股票
    stocks = [
        ("600519", "贵州茅台"),  # 白酒
        ("000001", "平安银行"),  # 银行
        ("600036", "招商银行"),  # 银行
        ("000002", "万科A"),     # 房地产
    ]

    results = []

    for code, name in stocks:
        try:
            df = _fetch_with_retry(
                lambda s=code: service.get_northbound_holdings(
                symbol=_normalize_symbol(s),
                start_date="2024-06-01",
                end_date="2024-06-30",
                ),
                f"get_northbound_holdings({code})",
            )

            if df is not None:
                # 获取最后一天的数据
                latest = df.iloc[-1].to_dict()
                latest["name"] = name
                latest["symbol"] = code
                results.append(latest)

        except Exception as e:
            print(f"获取 {name}({code}) 数据失败: {e}")

    if results:
        print(f"获取到 {len(results)} 只股票的数据:")
        for item in results:
            print(f"\n{item['name']} ({item['symbol']}):")
            for key, value in item.items():
                if key not in ["name", "symbol"]:
                    print(f"  {key}: {value}")
    else:
        print("未获取到任何股票数据")


# ============================================================
# 示例 3: 北向持股趋势分析
# ============================================================
def example_trend_analysis():
    """分析北向资金的持股变化趋势"""
    print("\n" + "=" * 60)
    print("示例 3: 北向持股趋势分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_with_retry(
            lambda: service.get_northbound_holdings(
            symbol=_normalize_symbol("600519"),
            start_date="2024-01-01",
            end_date="2024-06-30",
            ),
            "trend get_northbound_holdings",
        )

        if df is None:
            print("无数据")
            return

        print(f"贵州茅台 2024上半年北向持股数据 ({len(df)} 个交易日)")
        print(f"数据形状: {df.shape}")

        # 找出数值列
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

        # 显示首尾对比
        print("\n月初 vs 月末持股数据:")
        print(f"  年初第一天:")
        first_row = df.iloc[0]
        for col in numeric_cols[:4]:  # 只显示前4个数值列
            print(f"    {col}: {first_row[col]}")

        print(f"  年中最后一天:")
        last_row = df.iloc[-1]
        for col in numeric_cols[:4]:
            print(f"    {col}: {last_row[col]}")

        if "hold_volume" in df.columns:
            change = last_row["hold_volume"] - first_row["hold_volume"]
            print(f"\n  持股数量变化: {change:,.0f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 使用不同代码格式
# ============================================================
def example_symbol_formats():
    """演示支持的多种证券代码格式"""
    print("\n" + "=" * 60)
    print("示例 4: 不同证券代码格式")
    print("=" * 60)

    service = get_service()

    symbols = [
        "600519",        # 纯数字
        "sh600519",      # 交易所前缀
        "600519.XSHG",   # JoinQuant格式
    ]

    for sym in symbols:
        try:
            df = _fetch_with_retry(
                lambda x=sym: service.get_northbound_holdings(
                symbol=_normalize_symbol(x),
                start_date="2024-06-01",
                end_date="2024-06-10",
                ),
                f"symbol_format {sym}",
            )

            if df is None:
                print(f"代码格式 {sym:20s}: 无数据")
            else:
                print(f"代码格式 {sym:20s}: 获取到 {len(df)} 条数据")

        except Exception as e:
            print(f"代码格式 {sym:20s}: 获取失败 - {e}")


# ============================================================
# 示例 5: 批量获取深证股票北向持股
# ============================================================
def example_sz_stocks():
    """获取深市股票的北向持股数据"""
    print("\n" + "=" * 60)
    print("示例 5: 深市股票北向持股")
    print("=" * 60)

    service = get_service()

    try:
        # 获取平安银行的北向持股数据
        df = _fetch_with_retry(
            lambda: service.get_northbound_holdings(
            symbol=_normalize_symbol("sz000001"),
            start_date="2024-05-01",
            end_date="2024-05-31",
            ),
            "sz stock northbound",
        )

        if df is None:
            print("无数据")
            return

        print(f"平安银行 2024年5月北向持股数据")
        print(f"数据形状: {df.shape}")

        # 计算月度变化
        if len(df) >= 2:
            first = df.iloc[0]
            last = df.iloc[-1]
            print(f"\n月度变化:")
            numeric_cols = df.select_dtypes(include=["number"]).columns[:3]
            for col in numeric_cols:
                change = last[col] - first[col]
                change_pct = (change / first[col] * 100) if first[col] != 0 else 0
                print(f"  {col}: {change:,.2f} ({change_pct:+.2f}%)")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 通过命名空间调用
# ============================================================
def example_namespace_call():
    """使用命名空间方式调用接口"""
    print("\n" + "=" * 60)
    print("示例 6: 通过命名空间调用")
    print("=" * 60)

    service = get_service()

    try:
        # 通过 cn.stock.capital.northbound_holdings 调用
        df = _fetch_with_retry(
            lambda: service.cn.stock.capital.northbound_holdings(
            symbol=_normalize_symbol("600036"),
            start_date="2024-04-01",
            end_date="2024-04-30",
            ),
            "namespace northbound",
        )

        if df is None:
            print("无数据")
            return

        print(f"招商银行 (600036) 2024年4月北向持股")
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 7: 数据筛选与导出
# ============================================================
def example_filter_and_export():
    """演示如何筛选和导出北向持股数据"""
    print("\n" + "=" * 60)
    print("示例 7: 数据筛选与导出")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_with_retry(
            lambda: service.get_northbound_holdings(
            symbol=_normalize_symbol("600519"),
            start_date="2024-01-01",
            end_date="2024-03-31",
            ),
            "filter/export northbound",
        )

        if df is None:
            print("无数据")
            return

        print(f"原始数据: {len(df)} 行")

        # 显示所有字段
        print(f"\n数据字段: {list(df.columns)}")

        # 筛选特定列（如果存在）
        selected_cols = ["date", "symbol", "hold_volume", "hold_ratio"]
        available_cols = [col for col in selected_cols if col in df.columns]
        
        if available_cols:
            filtered_df = df[available_cols]
            print(f"\n筛选后的数据 (前5行):")
            print(filtered_df.head())

            # 保存到CSV示例（注释掉，实际使用时可取消）
            # filtered_df.to_csv("northbound_holdings.csv", index=False)
            # print("\n数据已保存到 northbound_holdings.csv")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 8: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 8: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试无效日期格式
    print("\n测试1: 无效日期格式")
    try:
        df = service.get_northbound_holdings(
            symbol=_normalize_symbol("600519"),
            start_date="invalid",
            end_date="2024-06-30",
        )
        print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试无效股票代码
    print("\n测试2: 无效股票代码")
    try:
        df = service.get_northbound_holdings(
            symbol=_normalize_symbol("invalid_code"),
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        if df.empty:
            print("  结果: 返回空DataFrame")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试日期范围无效
    print("\n测试3: 日期范围无效（结束早于开始）")
    try:
        df = service.get_northbound_holdings(
            symbol=_normalize_symbol("600519"),
            start_date="2024-12-31",
            end_date="2024-01-01",
        )
        if df.empty:
            print("  结果: 返回空DataFrame")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_stocks()
    example_trend_analysis()
    example_symbol_formats()
    example_sz_stocks()
    example_namespace_call()
    example_filter_and_export()
    example_error_handling()
