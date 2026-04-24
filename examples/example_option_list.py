"""
get_option_list() 接口示例

演示如何使用 akshare_data.DataService.get_option_list() 获取期权合约列表。

接口说明:
  - 无必需参数
  - 底层使用 option_current_day_sse，返回上交所期权当日合约列表
  - market 参数会被忽略（底层接口不支持）

返回字段: 包含期权合约代码、名称、标的资产、到期日等信息

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_option_list()
"""

import pandas as pd
from akshare_data import get_service


def _fetch_option_list(service):
    for market in ("sse", "szse", "cffex"):
        try:
            df = service.get_option_list(market=market)
            if df is not None and not df.empty:
                return market, df
        except Exception:
            continue
    return "sse", pd.DataFrame()


def _show_option_list_sample():
    sample = pd.DataFrame(
        [
            {"symbol": "10000001", "name": "50ETF购2406A", "market": "sse", "underlying": "510050"},
            {"symbol": "10000002", "name": "50ETF沽2406A", "market": "sse", "underlying": "510050"},
        ]
    )
    print("使用本地样本数据回退:")
    print(sample.to_string(index=False))


# ============================================================
# 示例 1: 基本用法 - 获取期权合约列表
# ============================================================
def example_option_list_basic():
    """基本用法: 获取上交所期权合约列表"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取上交所期权合约列表")
    print("=" * 60)

    service = get_service()

    try:
        # 底层使用 option_current_day_sse，返回上交所当日合约
        market, df = _fetch_option_list(service)
        if df is None or df.empty:
            print("无数据，切换样本回退")
            _show_option_list_sample()
            return

        # 打印数据形状
        print(f"实际使用 market: {market}")
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取期权列表失败: {e}")


# ============================================================
# 示例 2: 期权合约筛选 - 按标的资产
# ============================================================
def example_option_list_filter_by_underlying():
    """获取期权列表后按标的资产进行筛选"""
    print("\n" + "=" * 60)
    print("示例 2: 按标的资产筛选期权合约")
    print("=" * 60)

    service = get_service()

    try:
        _, df = _fetch_option_list(service)

        if df.empty:
            print("无数据")
            return

        print(f"上交所期权合约总数: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 尝试按标的资产筛选
        underlying_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["underlying", "标的", "symbol", "code"])
        ]

        if underlying_cols:
            col = underlying_cols[0]
            print(f"\n使用列 '{col}' 进行标的资产统计:")
            underlying_counts = df[col].value_counts().head(10)
            print(underlying_counts.to_string())
        else:
            print("\n打印前10行数据供参考:")
            print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 期权合约筛选 - 按到期月份
# ============================================================
def example_option_list_filter_by_expiry():
    """获取期权列表后按到期月份进行筛选"""
    print("\n" + "=" * 60)
    print("示例 3: 按到期月份筛选期权合约")
    print("=" * 60)

    service = get_service()

    try:
        _, df = _fetch_option_list(service)

        if df.empty:
            print("无数据")
            return

        print(f"上交所期权合约总数: {len(df)}")

        # 尝试找到到期日相关列
        expiry_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in ["expiry", "到期", "maturity", "end"])
        ]

        if expiry_cols:
            col = expiry_cols[0]
            print(f"\n使用列 '{col}' 进行到期月份统计:")
            if df[col].dtype == "object":
                df["expiry_month"] = df[col].str[:7]
                month_counts = df["expiry_month"].value_counts().head(10)
                print(month_counts.to_string())
        else:
            print("\n到期日相关列未找到，打印前10行:")
            print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 期权数据统计
# ============================================================
def example_option_list_stats():
    """期权合约统计信息"""
    print("\n" + "=" * 60)
    print("示例 4: 期权合约统计信息")
    print("=" * 60)

    service = get_service()

    try:
        _, df = _fetch_option_list(service)

        if df.empty:
            print("无数据（数据源不可用），展示样本")
            _show_option_list_sample()
            return

        print(f"上交所期权合约数量: {len(df)}")
        print(f"字段列表: {list(df.columns)}")
        print("\n前5行数据:")
        print(df.head().to_string(index=False))

        # 统计信息
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 错误处理演示
# ============================================================
def example_option_list_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 正常调用
    print("\n测试: 正常调用")
    try:
        _, df = _fetch_option_list(service)
        if df is None or df.empty:
            print("  结果: 无真实数据，展示样本")
            _show_option_list_sample()
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_option_list_basic()
    example_option_list_filter_by_underlying()
    example_option_list_filter_by_expiry()
    example_option_list_stats()
    example_option_list_error_handling()
