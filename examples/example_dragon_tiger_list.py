"""
龙虎榜 (Dragon Tiger List) 接口补充示例

本示例作为 example_dragon_tiger.py 的补充，展示 DataService 层面的
get_dragon_tiger_list() 接口的使用。

接口说明:
    get_dragon_tiger_list(date)
    
    参数:
        date: 必填，查询日期，格式 "YYYY-MM-DD"
              内部会自动转换为 YYYYMMDD
    
    返回:
        pd.DataFrame，包含指定日期的龙虎榜明细数据
        
        典型字段:
        - 代码: 股票代码
        - 名称: 股票名称
        - 收盘价: 当日收盘价
        - 涨跌幅: 涨跌幅(%)
        - 龙虎榜成交额: 当日龙虎榜成交额
        - 买入金额: 买入席位合计金额
        - 卖出金额: 卖出席位合计金额
        - 净买入额: 净买入金额
        - 买入席位/卖出席位: 详细席位信息

使用方式:
    from akshare_data import get_service
    service = get_service()
    _, df = _safe_dragon_tiger(service, "2024-06-28")

龙虎榜说明:
- 龙虎榜是交易所公布的当日异动股票榜单
- 通常包括涨跌幅超过7%、换手率超过20%、连续3日涨跌幅累计超过20%等股票
- 数据每日收盘后发布，反映主力资金动向

注意:
- 非交易日调用返回空DataFrame
- 周末和节假日无数据
"""

import pandas as pd
from akshare_data import get_service
from _example_utils import fetch_with_date_fallback, stable_df


def _safe_dragon_tiger(service, target_date: str):
    hit_date, df = fetch_with_date_fallback(
        lambda d: service.get_dragon_tiger_list(date=d),
        base_date=target_date,
        fallback_days=7,
        retries_per_date=1,
    )
    return hit_date, stable_df(df)


# ============================================================
# 示例 1: 基本用法 - 通过 DataService 获取龙虎榜
# ============================================================
def example_basic_dataservice():
    """基本用法: 通过 DataService 获取龙虎榜数据"""
    print("=" * 60)
    print("示例 1: DataService 方式 - 获取龙虎榜数据")
    print("=" * 60)

    service = get_service()

    try:
        # 使用 DataService.get_dragon_tiger_list() 方法
        hit_date, df = _safe_dragon_tiger(service, "2024-06-28")

        print(f"命中日期: {hit_date}")
        print(f"数据形状: {df.shape}")

        if df.empty:
            print("该日期无龙虎榜数据（可能是非交易日或数据源不可用）")
            return

        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取龙虎榜数据失败: {e}")


# ============================================================
# 示例 2: DataService vs AkShare 对比
# ============================================================
def example_compare_methods():
    """对比 DataService 和 AkShare adapter 的调用方式"""
    print("\n" + "=" * 60)
    print("示例 2: DataService vs AkShare 调用方式对比")
    print("=" * 60)

    service = get_service()
    date = "2024-06-27"

    # 方式1: 通过 DataService 直接调用
    print("\n方式1: DataService.get_dragon_tiger_list()")
    try:
        hit1, df1 = _safe_dragon_tiger(service, date)
        print(f"  结果: {df1.shape}")
        print(f"  命中日期: {hit1}")
    except Exception as e:
        print(f"  失败: {e}")

    # 方式2: 通过 cn.stock.capital.dragon_tiger 调用
    print("\n方式2: service.cn.stock.capital.dragon_tiger()")
    try:
        hit2, df2 = fetch_with_date_fallback(
            lambda d: service.cn.stock.capital.dragon_tiger(date=d),
            base_date=date,
            fallback_days=7,
            retries_per_date=1,
        )
        df2 = stable_df(df2)
        print(f"  结果: {df2.shape}")
        print(f"  命中日期: {hit2}")
    except Exception as e:
        print(f"  失败: {e}")

    # 方式3: 通过 akshare adapter 调用
    print("\n方式3: service.akshare.get_dragon_tiger_list()")
    try:
        hit3, df3 = fetch_with_date_fallback(
            lambda d: service.akshare.get_dragon_tiger_list(date=d),
            base_date=date,
            fallback_days=7,
            retries_per_date=1,
        )
        df3 = stable_df(df3)
        print(f"  结果: {df3.shape}")
        print(f"  命中日期: {hit3}")
    except Exception as e:
        print(f"  失败: {e}")


# ============================================================
# 示例 3: 龙虎榜数据分析
# ============================================================
def example_analysis():
    """分析龙虎榜数据的常用方法"""
    print("\n" + "=" * 60)
    print("示例 3: 龙虎榜数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_dragon_tiger_list(date="2024-06-28")

        if df.empty:
            print("该日期无龙虎榜数据")
            return

        print(f"龙虎榜数据 ({len(df)} 只股票)")
        print(f"字段: {list(df.columns)}")

        # 找出数值列进行统计
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            print(f"\n数值字段统计:")
            print(df[numeric_cols].describe())

        # 如果有关键字段，展示排名
        for col in ["涨跌幅", "净买入额", "龙虎榜成交额"]:
            if col in df.columns:
                print(f"\n{col} 排名前三:")
                top3 = df.nlargest(3, col)[["代码", "名称", col]]
                print(top3.to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 获取多日龙虎榜数据并汇总
# ============================================================
def example_multi_date():
    """获取多个交易日的龙虎榜数据并汇总"""
    print("\n" + "=" * 60)
    print("示例 4: 多日龙虎榜数据汇总")
    print("=" * 60)

    service = get_service()

    # 测试多个交易日
    dates = [
        "2024-06-24",
        "2024-06-25",
        "2024-06-26",
        "2024-06-27",
        "2024-06-28",
    ]

    all_data = []

    for date in dates:
        try:
            df = service.get_dragon_tiger_list(date=date)
            if not df.empty:
                df["query_date"] = date
                all_data.append(df)
                print(f"{date}: {df.shape[0]} 条记录")
            else:
                print(f"{date}: 无数据")
        except Exception as e:
            print(f"{date}: 获取失败 - {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        print(f"\n汇总: 共 {len(all_data)} 个交易日，{len(combined)} 条记录")

        # 统计出现次数最多的股票
        if "代码" in combined.columns:
            stock_counts = combined["代码"].value_counts().head(10)
            print("\n本周龙虎榜出现次数最多的股票:")
            for code, count in stock_counts.items():
                names = combined[combined["代码"] == code]["名称"].unique()
                name = names[0] if len(names) > 0 else "未知"
                print(f"  {code} {name}: {count} 次")
    else:
        print("未获取到任何数据")


# ============================================================
# 示例 5: 龙虎榜股票筛选
# ============================================================
def example_filter_stocks():
    """筛选特定条件的龙虎榜股票"""
    print("\n" + "=" * 60)
    print("示例 5: 龙虎榜股票筛选")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_dragon_tiger_list(date="2024-06-28")

        if df.empty:
            print("该日期无龙虎榜数据")
            return

        print(f"原始数据: {len(df)} 只股票")

        # 根据条件筛选
        # 条件1: 涨幅较大的股票
        if "涨跌幅" in df.columns:
            limit_up = df[df["涨跌幅"] >= 9.5]
            print(f"\n涨停股票 (涨幅 >= 9.5%): {len(limit_up)} 只")
            if len(limit_up) > 0:
                print(limit_up[["代码", "名称", "涨跌幅"]].to_string(index=False))

        # 条件2: 净买入额较大的股票
        if "净买入额" in df.columns:
            big_buy = df.nlargest(5, "净买入额")
            print(f"\n净买入额前五:")
            print(big_buy[["代码", "名称", "净买入额"]].to_string(index=False))

        # 条件3: 成交额较大的股票
        if "龙虎榜成交额" in df.columns:
            big_volume = df.nlargest(5, "龙虎榜成交额")
            print(f"\n龙虎榜成交额前五:")
            print(big_volume[["代码", "名称", "龙虎榜成交额"]].to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 指定数据源
# ============================================================
def example_specify_source():
    """指定数据源获取龙虎榜数据"""
    print("\n" + "=" * 60)
    print("示例 6: 指定数据源")
    print("=" * 60)

    service = get_service()
    date = "2024-06-28"

    # 尝试使用不同数据源
    sources = ["akshare", "lixinger"]

    for source in sources:
        print(f"\n数据源: {source}")
        try:
            # 通过 source 参数指定数据源
            df = service.get_dragon_tiger_list(date=date, source=source)

            if df.empty:
                print(f"  结果: 无数据（数据源可能不支持此接口）")
            else:
                print(f"  结果: 获取到 {df.shape[0]} 条记录，{df.shape[1]} 个字段")

        except NotImplementedError as e:
            print(f"  结果: 该数据源不支持此接口 - {e}")
        except Exception as e:
            print(f"  失败: {type(e).__name__}: {e}")


# ============================================================
# 示例 7: 错误处理与边界情况
# ============================================================
def example_error_handling():
    """演示各种边界情况和错误处理"""
    print("\n" + "=" * 60)
    print("示例 7: 错误处理与边界情况")
    print("=" * 60)

    service = get_service()

    test_cases = [
        ("非交易日 (春节)", "2024-02-10"),
        ("周末", "2024-06-29"),
        ("无效日期格式", "invalid"),
        ("未来日期", "2025-12-31"),
        ("过去很远日期", "2010-01-01"),
    ]

    for case_name, date in test_cases:
        print(f"\n测试: {case_name} ({date})")
        try:
            df = service.get_dragon_tiger_list(date=date)
            if df.empty:
                print(f"  结果: 返回空DataFrame")
            else:
                print(f"  结果: 获取到 {len(df)} 条记录")
        except Exception as e:
            print(f"  捕获异常: {type(e).__name__}: {e}")


# ============================================================
# 示例 8: 缓存效果演示
# ============================================================
def example_caching():
    """演示缓存效果（重复调用同一数据）"""
    print("\n" + "=" * 60)
    print("示例 8: 缓存效果演示")
    print("=" * 60)

    service = get_service()
    date = "2024-06-28"

    import time

    # 第一次调用（从数据源获取）
    print("\n第一次调用（从数据源获取）:")
    start = time.time()
    df1 = service.get_dragon_tiger_list(date=date)
    elapsed1 = time.time() - start
    print(f"  耗时: {elapsed1:.4f} 秒，获取 {len(df1)} 条记录")

    # 第二次调用（应从缓存读取）
    print("\n第二次调用（应从缓存读取）:")
    start = time.time()
    df2 = service.get_dragon_tiger_list(date=date)
    elapsed2 = time.time() - start
    print(f"  耗时: {elapsed2:.4f} 秒，获取 {len(df2)} 条记录")

    if elapsed2 < elapsed1:
        speedup = elapsed1 / elapsed2 if elapsed2 > 0 else float('inf')
        print(f"\n缓存加速比: {speedup:.1f}x")
    else:
        print("\n两次调用耗时相近（可能是首次也命中缓存或数据源响应快）")


if __name__ == "__main__":
    example_basic_dataservice()
    example_compare_methods()
    example_analysis()
    example_multi_date()
    example_filter_stocks()
    example_specify_source()
    example_error_handling()
    example_caching()
