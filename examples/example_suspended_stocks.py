"""
get_suspended_stocks() 接口示例

演示如何使用 akshare_data.get_suspended_stocks() 获取停牌股票列表。

停牌股票是指因重大事项、异常波动等原因被交易所暂停交易的股票。
常见停牌原因包括:
  - 重大资产重组
  - 股价异常波动
  - 信息披露违规
  - 其他重大事项

参数说明:
  - 无参数

返回字段: code, display_name (具体字段以实际返回为准)
"""

from akshare_data import get_suspended_stocks


def _mock_suspended_stocks():
    import pandas as pd

    return pd.DataFrame(
        {
            "code": ["000671", "600421", "300312"],
            "display_name": ["阳光城", "华嵘控股", "邦讯技术"],
            "reason": ["重大事项", "筹划重组", "信息披露"],
        }
    )


def _fetch_suspended_stocks():
    try:
        df = get_suspended_stocks()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"实时接口异常: {e}")
    print("[停牌接口不可用或无数据，使用演示数据]")
    return _mock_suspended_stocks()


# ============================================================
# 示例 1: 基本用法 - 获取全部停牌股票列表
# ============================================================
def example_basic():
    """基本用法: 获取全部停牌股票列表"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取全部停牌股票列表")
    print("=" * 60)

    try:
        # 该接口无需参数，直接调用即可获取当前所有停牌股票
        df = _fetch_suspended_stocks()

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n前10只停牌股票:")
        print(df.head(10))

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 统计停牌股票数量
# ============================================================
def example_count():
    """统计停牌股票数量"""
    print("\n" + "=" * 60)
    print("示例 2: 统计停牌股票数量")
    print("=" * 60)

    try:
        df = _fetch_suspended_stocks()

        print(f"当前停牌股票总数: {len(df)} 只")

        # 如果有停牌原因字段，可以按原因统计
        if "reason" in df.columns:
            print("\n按停牌原因统计:")
            reason_counts = df["reason"].value_counts()
            for reason, count in reason_counts.items():
                print(f"  {reason}: {count} 只")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 查看停牌股票详细信息
# ============================================================
def example_details():
    """查看停牌股票详细信息"""
    print("\n" + "=" * 60)
    print("示例 3: 查看停牌股票详细信息")
    print("=" * 60)

    try:
        df = _fetch_suspended_stocks()

        print(f"共 {len(df)} 只停牌股票\n")

        # 打印完整的前20只停牌股票信息
        print("前20只停牌股票详细信息:")
        print(df.head(20).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 按板块筛选停牌股票
# ============================================================
def example_filter_by_board():
    """按板块筛选停牌股票"""
    print("\n" + "=" * 60)
    print("示例 4: 按板块筛选停牌股票")
    print("=" * 60)

    try:
        df = _fetch_suspended_stocks()

        # 沪市主板停牌股票 (代码以 60 开头)
        sh_suspended = df[df["code"].str.startswith("60")]
        print(f"沪市主板停牌: {len(sh_suspended)} 只")
        if not sh_suspended.empty:
            print(sh_suspended.head(5))

        # 深市主板停牌股票 (代码以 00 开头)
        sz_suspended = df[df["code"].str.startswith("00")]
        print(f"\n深市主板停牌: {len(sz_suspended)} 只")
        if not sz_suspended.empty:
            print(sz_suspended.head(5))

        # 创业板停牌股票 (代码以 30 开头)
        cyb_suspended = df[df["code"].str.startswith("30")]
        print(f"\n创业板停牌: {len(cyb_suspended)} 只")
        if not cyb_suspended.empty:
            print(cyb_suspended.head(5))

        # 科创板停牌股票 (代码以 68 开头)
        kcb_suspended = df[df["code"].str.startswith("68")]
        print(f"\n科创板停牌: {len(kcb_suspended)} 只")
        if not kcb_suspended.empty:
            print(kcb_suspended.head(5))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 结合证券列表使用
# ============================================================
def example_with_securities_list():
    """结合证券列表获取更完整的停牌股票信息"""
    print("\n" + "=" * 60)
    print("示例 5: 结合证券列表使用")
    print("=" * 60)

    try:
        from akshare_data import get_securities_list

        # 获取停牌股票列表
        suspended_df = _fetch_suspended_stocks()

        # 获取全部股票列表
        all_stocks = get_securities_list()

        if all_stocks.empty:
            print("无法获取股票列表")
            return

        # 将停牌股票与证券列表关联，获取更多信息
        suspended_codes = suspended_df["code"].tolist()
        suspended_info = all_stocks[all_stocks["code"].isin(suspended_codes)]

        print(f"停牌股票数量: {len(suspended_df)}")
        print(f"成功匹配证券信息的数量: {len(suspended_info)}")

        if not suspended_info.empty:
            print("\n停牌股票详细信息:")
            print(suspended_info.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 检查指定股票是否停牌
# ============================================================
def example_check_specific_stock():
    """检查指定股票是否停牌"""
    print("\n" + "=" * 60)
    print("示例 6: 检查指定股票是否停牌")
    print("=" * 60)

    try:
        df = _fetch_suspended_stocks()

        # 要检查的股票代码列表
        check_codes = ["000001", "600519", "000002", "600036"]

        suspended_codes = set(df["code"].tolist())

        for code in check_codes:
            if code in suspended_codes:
                stock_info = df[df["code"] == code]
                print(f"{code}: 停牌中")
                print(stock_info.to_string(index=False))
            else:
                print(f"{code}: 正常交易")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 7: 导出停牌股票列表
# ============================================================
def example_export():
    """将停牌股票列表导出为 CSV 文件"""
    print("\n" + "=" * 60)
    print("示例 7: 导出停牌股票列表")
    print("=" * 60)

    try:
        df = _fetch_suspended_stocks()

        print(f"获取到 {len(df)} 只停牌股票")
        print(f"字段列表: {list(df.columns)}")

        # 示例: 导出到 CSV (取消注释即可使用)
        # df.to_csv("suspended_stocks.csv", index=False, encoding="utf-8-sig")
        # print("已导出到 suspended_stocks.csv")

        # 打印前5行预览
        print("\n前5行预览:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic()
    example_count()
    example_details()
    example_filter_by_board()
    example_with_securities_list()
    example_check_specific_stock()
    example_export()
