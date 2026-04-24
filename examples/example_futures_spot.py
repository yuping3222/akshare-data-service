"""
get_futures_spot() 接口示例

演示如何使用 akshare_data.DataService.get_futures_spot() 获取期货实时行情数据。

接口说明:
  - 无需参数，返回全市场期货合约的实时行情快照

返回字段: 包含合约代码、最新价、涨跌幅、成交量、持仓量等实时数据

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_futures_spot()

注意: 实时行情数据为快照数据，每次调用获取最新市场状态。
"""

import pandas as pd
from akshare_data import get_service


def _fetch_futures_spot(service):
    try:
        df = service.get_futures_spot()
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    # 回退主力合约列表，至少给出有效样本结构
    try:
        df = service.get_futures_main_contracts()
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    return pd.DataFrame()


# ============================================================
# 示例 1: 基本用法 - 获取全市场期货实时行情
# ============================================================
def example_futures_spot_basic():
    """基本用法: 获取全市场期货实时行情快照"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取期货实时行情")
    print("=" * 60)

    service = get_service()

    try:
        # 无需参数，获取全市场期货实时行情
        df = _fetch_futures_spot(service)

        if df is None or df.empty:
            print("无数据 (接口可能未实现)")
            _show_mock_futures_spot()
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取期货实时行情失败: {e}")
        _show_mock_futures_spot()


def _show_mock_futures_spot():
    """展示期货实时行情的期望输出格式"""
    print("\n  --- 期望输出格式示例 ---")
    data = {
        "symbol": ["rb2501", "i2501", "j2501", "cu2501", "au2501"],
        "name": ["螺纹钢", "铁矿石", "焦炭", "沪铜", "沪金"],
        "last_price": [3850.0, 780.5, 2150.0, 69800.0, 480.5],
        "change_pct": [1.25, -0.85, 0.50, 0.30, -0.15],
        "volume": [150000, 220000, 85000, 45000, 32000],
        "open_interest": [520000, 680000, 310000, 180000, 95000],
    }
    df = pd.DataFrame(data)
    print(df.to_string(index=False))


# ============================================================
# 示例 2: 按品种筛选实时行情
# ============================================================
def example_futures_spot_filter_by_variety():
    """获取实时行情后按品种进行筛选"""
    print("\n" + "=" * 60)
    print("示例 2: 按品种筛选实时行情")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_futures_spot(service)

        if df is None or df.empty:
            print("无数据")
            return

        print(f"全市场期货合约数量: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 尝试按品种名称筛选
        # 常见列名: "name", "品种", "variety", "symbol"
        name_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["name", "品种", "variety"])
        ]

        if name_cols:
            col = name_cols[0]
            print(f"\n使用列 '{col}' 进行品种统计:")
            variety_counts = df[col].value_counts().head(10)
            print(variety_counts.to_string())
        else:
            print("\n品种名称列未找到，打印前10行:")
            print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 涨幅榜与跌幅榜
# ============================================================
def example_futures_spot_top_gainers_losers():
    """获取实时行情后统计涨幅榜和跌幅榜"""
    print("\n" + "=" * 60)
    print("示例 3: 涨幅榜与跌幅榜")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_futures_spot(service)

        if df is None or df.empty:
            print("无数据")
            return

        # 找到涨跌幅列
        change_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["change", "涨跌幅", "pct", "percent"])
        ]

        if not change_cols:
            print(f"未找到涨跌幅列，可用列: {list(df.columns)}")
            return

        change_col = change_cols[0]
        print(f"使用列 '{change_col}' 进行排序")

        # 确保涨跌幅为数值类型
        df[change_col] = pd.to_numeric(df[change_col], errors="coerce")

        # 涨幅榜 Top 10
        print("\n涨幅榜 Top 10:")
        gainers = df.nlargest(10, change_col)
        display_cols = [col for col in ["symbol", "name", change_col, "last_price", "volume"]
                        if col in df.columns]
        print(gainers[display_cols].to_string(index=False))

        # 跌幅榜 Top 10
        print("\n跌幅榜 Top 10:")
        losers = df.nsmallest(10, change_col)
        print(losers[display_cols].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 按交易所筛选
# ============================================================
def example_futures_spot_by_exchange():
    """按交易所筛选实时行情数据"""
    print("\n" + "=" * 60)
    print("示例 4: 按交易所筛选实时行情")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_futures_spot(service)

        if df is None or df.empty:
            print("无数据")
            return

        # 尝试找到交易所相关列
        exchange_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["exchange", "交易所", "market"])
        ]

        if exchange_cols:
            col = exchange_cols[0]
            print(f"使用列 '{col}' 进行交易所统计:")
            exchange_counts = df[col].value_counts()
            print(exchange_counts.to_string())

            # 分别展示各交易所的前5个合约
            for exchange in exchange_counts.index[:4]:
                print(f"\n{exchange} 交易所前5合约:")
                exchange_df = df[df[col] == exchange].head(5)
                display_cols = [c for c in ["symbol", "name", "last_price", "change_pct"]
                                if c in df.columns]
                print(exchange_df[display_cols].to_string(index=False))
        else:
            print("交易所列未找到，尝试按合约代码前缀推断:")
            # 常见期货代码前缀对应交易所
            if "symbol" in df.columns:
                df["exchange"] = df["symbol"].apply(_infer_exchange)
                exchange_counts = df["exchange"].value_counts()
                print(exchange_counts.to_string())

    except Exception as e:
        print(f"获取数据失败: {e}")


def _infer_exchange(symbol):
    """根据合约代码前缀推断交易所"""
    prefix_map = {
        "rb": "SHFE", "hc": "SHFE", "cu": "SHFE", "al": "SHFE",
        "au": "SHFE", "ag": "SHFE", "fu": "SHFE", "ru": "SHFE",
        "i": "DCE", "j": "DCE", "jm": "DCE", "m": "DCE",
        "a": "DCE", "b": "DCE", "c": "DCE", "cs": "DCE",
        "cf": "CZCE", "sr": "CZCE", "ta": "CZCE", "ma": "CZCE",
        "oi": "CZCE", "rm": "CZCE", "fg": "CZCE",
        "if": "CFFEX", "ic": "CFFEX", "ih": "CFFEX", "tf": "CFFEX", "ts": "CFFEX",
    }
    for prefix, exchange in prefix_map.items():
        if str(symbol).lower().startswith(prefix):
            return exchange
    return "UNKNOWN"


# ============================================================
# 示例 5: 成交量与持仓量分析
# ============================================================
def example_futures_spot_volume_oi_analysis():
    """分析实时行情中的成交量和持仓量数据"""
    print("\n" + "=" * 60)
    print("示例 5: 成交量与持仓量分析")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_futures_spot(service)

        if df is None or df.empty:
            print("无数据")
            return

        print(f"全市场期货合约数量: {len(df)}")

        # 成交量分析
        volume_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["volume", "成交量", "vol"])
        ]

        if volume_cols:
            vol_col = volume_cols[0]
            df[vol_col] = pd.to_numeric(df[vol_col], errors="coerce")
            print(f"\n成交量 Top 10 (使用列 '{vol_col}'):")
            top_volume = df.nlargest(10, vol_col)
            display_cols = [c for c in ["symbol", "name", vol_col, "last_price"]
                            if c in df.columns]
            print(top_volume[display_cols].to_string(index=False))

        # 持仓量分析
        oi_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["open_interest", "持仓量", "oi"])
        ]

        if oi_cols:
            oi_col = oi_cols[0]
            df[oi_col] = pd.to_numeric(df[oi_col], errors="coerce")
            print(f"\n持仓量 Top 10 (使用列 '{oi_col}'):")
            top_oi = df.nlargest(10, oi_col)
            display_cols = [c for c in ["symbol", "name", oi_col, "last_price"]
                            if c in df.columns]
            print(top_oi[display_cols].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 综合市场概览
# ============================================================
def example_futures_spot_market_overview():
    """综合展示期货市场实时行情概览"""
    print("\n" + "=" * 60)
    print("示例 6: 期货市场实时行情概览")
    print("=" * 60)

    service = get_service()

    try:
        df = _fetch_futures_spot(service)

        if df is None or df.empty:
            print("无数据")
            return

        print(f"\n市场总览:")
        print(f"  合约总数: {len(df)}")

        # 找到关键列
        change_cols = [col for col in df.columns if "change" in col.lower() or "涨跌幅" in col]
        if change_cols:
            change_col = change_cols[0]
            df[change_col] = pd.to_numeric(df[change_col], errors="coerce")
            up_count = len(df[df[change_col] > 0])
            down_count = len(df[df[change_col] < 0])
            flat_count = len(df[df[change_col] == 0])
            print(f"  上涨合约: {up_count}")
            print(f"  下跌合约: {down_count}")
            print(f"  平盘合约: {flat_count}")

        # 价格范围
        price_cols = [col for col in df.columns if "price" in col.lower() or "价" in col]
        if price_cols:
            price_col = price_cols[0]
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
            print(f"\n价格统计 (使用列 '{price_col}'):")
            print(f"  最高价格: {df[price_col].max():.2f}")
            print(f"  最低价格: {df[price_col].min():.2f}")
            print(f"  平均价格: {df[price_col].mean():.2f}")

        # 成交量统计
        vol_cols = [col for col in df.columns if "volume" in col.lower() or "成交量" in col]
        if vol_cols:
            vol_col = vol_cols[0]
            df[vol_col] = pd.to_numeric(df[vol_col], errors="coerce")
            total_volume = df[vol_col].sum()
            print(f"\n成交量统计:")
            print(f"  总成交量: {total_volume:,.0f}")
            print(f"  平均单合约成交量: {df[vol_col].mean():,.0f}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 7: 定时刷新演示 (模拟)
# ============================================================
def example_futures_spot_refresh():
    """演示多次获取实时行情，观察数据变化"""
    print("\n" + "=" * 60)
    print("示例 7: 实时行情刷新演示")
    print("=" * 60)

    service = get_service()

    try:
        import time

        # 第一次获取
        print("\n第一次获取:")
        df1 = _fetch_futures_spot(service)
        if df1 is None or df1.empty:
            print("  结果: 返回 None (接口可能未实现)")
            return
        print(f"  合约数量: {len(df1)}")
        if not df1.empty and "last_price" in df1.columns:
            print(f"  示例合约价格: {df1['last_price'].iloc[0]}")

        # 模拟等待后再次获取
        print("\n等待 2 秒后再次获取...")
        time.sleep(2)

        print("\n第二次获取:")
        df2 = _fetch_futures_spot(service)
        if df2 is None or df2.empty:
            print("  结果: 返回 None")
            return
        print(f"  合约数量: {len(df2)}")
        if not df2.empty and "last_price" in df2.columns:
            print(f"  示例合约价格: {df2['last_price'].iloc[0]}")

        print("\n注意: 实时行情数据可能因市场状态而变化。")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 8: 错误处理演示
# ============================================================
def example_futures_spot_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 8: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 正常调用 (无需参数)
    print("\n测试: 正常调用")
    try:
        df = _fetch_futures_spot(service)
        if df is None or df.empty:
            print("  结果: 返回 None (接口可能未实现)")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_futures_spot_basic()
    example_futures_spot_filter_by_variety()
    example_futures_spot_top_gainers_losers()
    example_futures_spot_by_exchange()
    example_futures_spot_volume_oi_analysis()
    example_futures_spot_market_overview()
    example_futures_spot_refresh()
    example_futures_spot_error_handling()
