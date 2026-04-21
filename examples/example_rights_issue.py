"""
get_rights_issue() 接口示例

演示如何使用 akshare_data.get_rights_issue() 获取个股配股数据。

接口说明:
- 获取指定股票的历史配股信息
- symbol: 股票代码（必填）
- 返回字段包含: 配股年度、配股方案、配股价、股权登记日、除权日等

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_rights_issue(symbol="000001")
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取单只股票配股数据
# ============================================================
def example_basic():
    """基本用法: 获取平安银行配股数据"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取平安银行配股数据")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 股票代码
        df = service.get_rights_issue(symbol="000001")

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        if df is not None and not df.empty:
            print("\n前5行数据:")
            print(df.head())
        else:
            print("\n无数据")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 多只股票配股对比
# ============================================================
def example_compare():
    """对比多只股票的配股历史"""
    print("\n" + "=" * 60)
    print("示例 2: 多只股票配股对比")
    print("=" * 60)

    service = get_service()

    symbols = [
        ("000001", "平安银行"),
        ("600000", "浦发银行"),
        ("600519", "贵州茅台"),
    ]

    for symbol, name in symbols:
        try:
            df = service.get_rights_issue(symbol=symbol)

            if df is not None and not df.empty:
                print(f"\n{name} ({symbol}): {len(df)} 次配股")
                print(df.head(3).to_string(index=False))
            else:
                print(f"\n{name} ({symbol}): 无配股数据")

        except Exception as e:
            print(f"\n{name} ({symbol}): 获取失败 - {e}")


# ============================================================
# 示例 3: 配股方案分析
# ============================================================
def example_analysis():
    """分析配股方案详情"""
    print("\n" + "=" * 60)
    print("示例 3: 配股方案分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_rights_issue(symbol="000001")

        if df is None or df.empty:
            print("无数据")
            return

        print(f"平安银行配股历史: {len(df)} 次")
        print(f"字段列表: {list(df.columns)}")

        # 打印全部数据
        print("\n全部配股记录:")
        print(df.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 按时间顺序查看配股记录
# ============================================================
def example_chronological():
    """按时间顺序查看配股记录"""
    print("\n" + "=" * 60)
    print("示例 4: 按时间顺序查看配股记录")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_rights_issue(symbol="600000")

        if df is None or df.empty:
            print("无数据")
            return

        # 尝试按日期排序
        date_col = None
        for col in df.columns:
            if "日期" in col or col.lower() == "date":
                date_col = col
                break

        if date_col:
            df_sorted = df.sort_values(by=date_col)
            print(f"浦发银行配股记录 (按时间排序):")
            print(df_sorted.to_string(index=False))
        else:
            print(f"浦发银行配股记录:")
            print(df.to_string(index=False))

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

    try:
        print("\n测试 1: 无效股票代码")
        df = service.get_rights_issue(symbol="999999")
        print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    try:
        print("\n测试 2: 正常调用")
        df = service.get_rights_issue(symbol="000001")
        print(f"  结果: {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare()
    example_analysis()
    example_chronological()
    example_error_handling()
