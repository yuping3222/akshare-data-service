"""
get_st_stocks() 接口示例

演示如何使用 akshare_data.get_st_stocks() 获取 ST 股票列表。

ST 股票是指被实施特别处理的股票，通常因为连续亏损或其他异常情况。
常见的 ST 类型包括:
  - ST: 其他特别处理
  - *ST: 退市风险警示

参数说明:
  - 无参数

返回字段: code, display_name (具体字段以实际返回为准)
"""

from akshare_data import get_st_stocks


# ============================================================
# 示例 1: 基本用法 - 获取全部 ST 股票列表
# ============================================================
def example_basic():
    """基本用法: 获取全部 ST 股票列表"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取全部 ST 股票列表")
    print("=" * 60)

    try:
        # 该接口无需参数，直接调用即可获取当前所有 ST 股票
        df = get_st_stocks()

        if df is None or df.empty:
            print("当前无 ST 股票")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n前10只ST股票:")
        print(df.head(10))

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 统计 ST 股票数量
# ============================================================
def example_count():
    """统计 ST 股票数量"""
    print("\n" + "=" * 60)
    print("示例 2: 统计 ST 股票数量")
    print("=" * 60)

    try:
        df = get_st_stocks()

        if df is None or df.empty:
            print("当前无 ST 股票")
            return

        total_count = len(df)
        print(f"当前 ST 股票总数: {total_count} 只")

        # 如果返回数据包含名称字段，可以进一步分析
        if "display_name" in df.columns:
            # 统计名称中包含 "*ST" 的股票 (*ST 表示有退市风险)
            star_st = df[df["display_name"].str.contains(r"\*ST", na=False)]
            normal_st = df[~df["display_name"].str.contains(r"\*ST", na=False)]

            print(f"  *ST 股票 (退市风险警示): {len(star_st)} 只")
            print(f"  ST 股票 (其他特别处理): {len(normal_st)} 只")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 3: 查看 ST 股票详细信息
# ============================================================
def example_details():
    """查看 ST 股票详细信息"""
    print("\n" + "=" * 60)
    print("示例 3: 查看 ST 股票详细信息")
    print("=" * 60)

    try:
        df = get_st_stocks()

        if df is None or df.empty:
            print("当前无 ST 股票")
            return

        print(f"共 {len(df)} 只 ST 股票\n")

        # 打印完整的前20只 ST 股票信息
        print("前20只ST股票详细信息:")
        print(df.head(20).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 筛选特定板块的 ST 股票
# ============================================================
def example_filter_by_board():
    """按板块筛选 ST 股票"""
    print("\n" + "=" * 60)
    print("示例 4: 按板块筛选 ST 股票")
    print("=" * 60)

    try:
        df = get_st_stocks()

        if df is None or df.empty:
            print("当前无 ST 股票")
            return

        # 沪市主板 ST 股票 (代码以 60 开头)
        sh_st = df[df["code"].str.startswith("60")]
        print(f"沪市主板 ST: {len(sh_st)} 只")
        if not sh_st.empty:
            print(sh_st.head(5))

        # 深市主板 ST 股票 (代码以 00 开头)
        sz_st = df[df["code"].str.startswith("00")]
        print(f"\n深市主板 ST: {len(sz_st)} 只")
        if not sz_st.empty:
            print(sz_st.head(5))

        # 创业板 ST 股票 (代码以 30 开头)
        cyb_st = df[df["code"].str.startswith("30")]
        print(f"\n创业板 ST: {len(cyb_st)} 只")
        if not cyb_st.empty:
            print(cyb_st.head(5))

        # 科创板 ST 股票 (代码以 68 开头)
        kcb_st = df[df["code"].str.startswith("68")]
        print(f"\n科创板 ST: {len(kcb_st)} 只")
        if not kcb_st.empty:
            print(kcb_st.head(5))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 5: 结合证券列表使用
# ============================================================
def example_with_securities_list():
    """结合证券列表获取更完整的 ST 股票信息"""
    print("\n" + "=" * 60)
    print("示例 5: 结合证券列表使用")
    print("=" * 60)

    try:
        from akshare_data import get_securities_list

        # 获取 ST 股票列表
        st_df = get_st_stocks()

        if st_df.empty:
            print("当前无 ST 股票")
            return

        # 获取全部股票列表
        all_stocks = get_securities_list(security_type="stock")

        if all_stocks.empty:
            print("无法获取股票列表")
            return

        # 将 ST 股票与证券列表关联，获取更多信息
        st_codes = st_df["code"].tolist()
        st_info = all_stocks[all_stocks["code"].isin(st_codes)]

        print(f"ST 股票数量: {len(st_df)}")
        print(f"成功匹配证券信息的数量: {len(st_info)}")

        if not st_info.empty:
            print("\nST 股票详细信息:")
            print(st_info.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 导出 ST 股票列表
# ============================================================
def example_export():
    """将 ST 股票列表导出为 CSV 文件"""
    print("\n" + "=" * 60)
    print("示例 6: 导出 ST 股票列表")
    print("=" * 60)

    try:
        df = get_st_stocks()

        if df is None or df.empty:
            print("当前无 ST 股票")
            return

        print(f"获取到 {len(df)} 只 ST 股票")
        print(f"字段列表: {list(df.columns)}")

        # 示例: 导出到 CSV (取消注释即可使用)
        # df.to_csv("st_stocks.csv", index=False, encoding="utf-8-sig")
        # print("已导出到 st_stocks.csv")

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
    example_export()
