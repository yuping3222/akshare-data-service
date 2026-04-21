"""
get_lof_spot() 接口示例

演示如何使用 DataService.get_lof_spot() 获取LOF基金实时行情数据。

接口说明:
- get_lof_spot(): 获取全部LOF(Listed Open-end Fund)基金的实时行情
- 无必需参数
- 返回: pd.DataFrame，包含LOF基金代码、名称、最新价、涨跌幅等字段

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_lof_spot()

注意:
- LOF基金是既可以在场外申购赎回，又可以在场内交易的基金
- 该接口返回LOF基金的实时交易行情
- 实时数据不走缓存，每次调用都会获取最新数据
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取全部LOF实时行情
# ============================================================
def example_basic():
    """基本用法: 获取全部LOF基金实时行情"""
    print("=" * 60)
    print("示例 1: 获取LOF基金实时行情")
    print("=" * 60)

    service = get_service()

    try:
        # 获取全部LOF基金实时行情
        df = service.get_lof_spot()

        if df is None or df.empty:
            print("无数据 (数据源未返回结果，可能是非交易时间)")
            return

        # 打印数据形状
        print(f"数据形状: {df.shape}")
        print(f"LOF基金数量: {len(df)}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10行
        print("\n前10行数据:")
        print(df.head(10).to_string(index=False))

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail().to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: LOF涨跌幅分析
# ============================================================
def example_price_change():
    """分析LOF基金的涨跌幅情况"""
    print("\n" + "=" * 60)
    print("示例 2: LOF涨跌幅分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_lof_spot()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"LOF基金总数: {len(df)}")

        # 查找涨跌幅列
        change_col = None
        for col in df.columns:
            if "涨跌幅" in str(col) or "change" in str(col).lower() or "pct" in str(col).lower():
                change_col = col
                break

        if change_col and change_col in df.columns:
            # 转换为数值类型
            df[change_col] = pd.to_numeric(df[change_col], errors="coerce")

            print(f"\n涨跌幅统计:")
            print(f"  平均涨跌幅: {df[change_col].mean():.2f}%")
            print(f"  最大涨幅: {df[change_col].max():.2f}%")
            print(f"  最大跌幅: {df[change_col].min():.2f}%")

            # 涨幅前5
            top_gainers = df.nlargest(5, change_col)
            print(f"\n涨幅前5:")
            print(top_gainers[[col for col in df.columns if col in ["代码", "名称", "最新价", change_col]]].to_string(index=False))

            # 跌幅前5
            top_losers = df.nsmallest(5, change_col)
            print(f"\n跌幅前5:")
            print(top_losers[[col for col in df.columns if col in ["代码", "名称", "最新价", change_col]]].to_string(index=False))
        else:
            print("未找到涨跌幅字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 3: 查找特定LOF基金
# ============================================================
def example_find_lof():
    """在LOF列表中查找特定基金"""
    print("\n" + "=" * 60)
    print("示例 3: 查找特定LOF基金")
    print("=" * 60)

    service = get_service()

    # 常见LOF基金代码
    target_codes = ["162605", "163402", "161005"]

    try:
        df = service.get_lof_spot()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"LOF基金总数: {len(df)}")

        # 查找代码列
        code_col = None
        for col in df.columns:
            if "代码" in str(col) or "code" in str(col).lower() or "symbol" in str(col).lower():
                code_col = col
                break

        if code_col:
            for code in target_codes:
                matched = df[df[code_col].astype(str).str.contains(code, na=False)]
                if not matched.empty:
                    print(f"\n找到基金代码 {code}:")
                    print(matched.to_string(index=False))
                else:
                    print(f"\n基金代码 {code}: 未找到")
        else:
            print("未找到代码字段")

    except Exception as e:
        print(f"查找失败: {e}")


# ============================================================
# 示例 4: LOF成交量分析
# ============================================================
def example_volume_analysis():
    """分析LOF基金的成交情况"""
    print("\n" + "=" * 60)
    print("示例 4: LOF成交量分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_lof_spot()

        if df is None or df.empty:
            print("无数据")
            return

        print(f"LOF基金总数: {len(df)}")

        # 查找成交量列
        volume_col = None
        for col in df.columns:
            if "成交量" in str(col) or "volume" in str(col).lower() or "成交额" in str(col):
                volume_col = col
                break

        if volume_col and volume_col in df.columns:
            df[volume_col] = pd.to_numeric(df[volume_col], errors="coerce")

            print(f"\n{volume_col}统计:")
            print(f"  总成交量: {df[volume_col].sum():,.0f}")
            print(f"  平均成交量: {df[volume_col].mean():,.0f}")
            print(f"  最大成交量: {df[volume_col].max():,.0f}")

            # 成交量前10
            top_volume = df.nlargest(10, volume_col)
            print(f"\n成交量前10:")
            display_cols = [col for col in df.columns if col in ["代码", "名称", "最新价", volume_col]]
            print(top_volume[display_cols].to_string(index=False))
        else:
            print("未找到成交量字段")

    except Exception as e:
        print(f"分析失败: {e}")


# ============================================================
# 示例 5: LOF价格分布
# ============================================================
def example_price_distribution():
    """分析LOF基金的价格分布"""
    print("\n" + "=" * 60)
    print("示例 5: LOF价格分布")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_lof_spot()

        if df is None or df.empty:
            print("无数据")
            return

        # 查找价格列
        price_col = None
        for col in df.columns:
            if "最新价" in str(col) or "close" in str(col).lower() or "price" in str(col).lower():
                price_col = col
                break

        if price_col and price_col in df.columns:
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")

            print(f"LOF基金价格分布 ({price_col}):")
            print(f"  平均价格: {df[price_col].mean():.3f}")
            print(f"  中位数: {df[price_col].median():.3f}")
            print(f"  最高价: {df[price_col].max():.3f}")
            print(f"  最低价: {df[price_col].min():.3f}")

            # 价格区间分布
            bins = [0, 1, 2, 5, 10, float("inf")]
            labels = ["<1", "1-2", "2-5", "5-10", ">10"]
            df["price_range"] = pd.cut(df[price_col], bins=bins, labels=labels)
            print(f"\n价格区间分布:")
            print(df["price_range"].value_counts().sort_index().to_string())
        else:
            print("未找到价格字段")
            print(f"可用字段: {list(df.columns)}")

    except Exception as e:
        print(f"分析失败: {e}")


# 导入 pandas 用于示例 2-5
import pandas as pd


if __name__ == "__main__":
    example_basic()
    example_price_change()
    example_find_lof()
    example_volume_analysis()
    example_price_distribution()
