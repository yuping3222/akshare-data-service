"""
get_name_history() 接口示例

演示如何使用 akshare_data.get_name_history() 获取股票曾用名历史。

参数说明:
    symbol: 股票代码

返回字段: 包含变更日期、原名称、新名称等

注意: 该接口当前仅在 lixinger 数据源实现，akshare 数据源未配置对应接口。
      使用 akshare 数据源时调用会报错。
      可通过 source="lixinger" 强制使用 lixinger 数据源（需配置 LIXINGER_TOKEN）。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取股票曾用名
# ============================================================
def example_basic():
    """基本用法: 获取股票曾用名历史"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取股票曾用名历史")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_name_history(symbol="600519")
        if df is None or df.empty:
            print("无曾用名记录")
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n名称变更历史:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 多只股票曾用名对比
# ============================================================
def example_compare_stocks():
    """对比多只股票的曾用名"""
    print("\n" + "=" * 60)
    print("示例 2: 多股曾用名对比")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "600036"]

    for code in stocks:
        try:
            df = service.get_name_history(symbol=code)
            if df is None or df.empty:
                print(f"\n{code}: 无曾用名记录")
            else:
                print(f"\n{code}: {len(df)} 条变更记录")
                print(df)
        except Exception as e:
            print(f"\n{code}: 获取失败 - {e}")


# ============================================================
# 示例 3: 分析变更频率
# ============================================================
def example_frequency():
    """分析股票名称变更频率"""
    print("\n" + "=" * 60)
    print("示例 3: 名称变更频率分析")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "600036", "000002"]

    for code in stocks:
        try:
            df = service.get_name_history(symbol=code)
            if df is None or df.empty:
                print(f"{code}: 无变更记录")
            else:
                print(f"{code}: {len(df)} 次变更")
                print(df)
        except Exception as e:
            print(f"{code}: 获取失败 - {e}")


# ============================================================
# 示例 4: 获取有变更历史的股票
# ============================================================
def example_stocks_with_changes():
    """找出有名称变更历史的股票"""
    print("\n" + "=" * 60)
    print("示例 4: 有变更历史的股票")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "600036", "000002", "600050"]

    changed = []
    for code in stocks:
        try:
            df = service.get_name_history(symbol=code)
            if df is not None and not df.empty:
                changed.append((code, len(df)))
        except Exception:
            pass

    if changed:
        print(f"有变更记录: {len(changed)} 只")
        for code, count in changed:
            print(f"  {code}: {count} 次变更")
    else:
        print("未找到有变更记录的股票")


if __name__ == "__main__":
    example_basic()
    example_compare_stocks()
    example_frequency()
    example_stocks_with_changes()
