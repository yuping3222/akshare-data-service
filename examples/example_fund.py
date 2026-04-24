"""
基金相关接口示例

演示如何使用 DataService 获取基金数据:
  - get_fund_open_nav: 开放式基金净值历史数据 (基于 fund_open_fund_info_em)
  - get_fund_manager_info: 基金经理信息
  - get_etf_list / get_lof_list: 基金列表

使用方式:
  from akshare_data import get_service
  service = get_service()
"""

import pandas as pd

from akshare_data import get_service


def _as_dataframe(data, label: str) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        print(f"{label}: 返回类型异常，期望 DataFrame，实际 {type(data).__name__}")
        return pd.DataFrame()
    if data.empty:
        print(f"{label}: 返回空数据")
    return data


# ============================================================
# 示例 1: 获取基金净值历史数据 (基本用法)
# ============================================================
def example_fund_net_value_basic():
    """基本用法: 获取单只开放式基金的净值历史数据"""
    print("=" * 60)
    print("示例 1: 获取基金净值历史数据 - 易方达蓝筹精选 (110011)")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 基金代码 (6位数字)
        # indicator: 指标类型，默认 "单位净值走势"
        # period: 时间区间，默认 "成立来"
        df = _as_dataframe(service.get_fund_open_nav(
            fund_code="110011",
        ), "示例1")

        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前5行
        print("\n前5行数据:")
        print(df.head())

        # 打印后5行
        print("\n后5行数据:")
        print(df.tail())

    except Exception as e:
        print(f"获取基金净值失败: {e}")


# ============================================================
# 示例 2: 获取不同类型基金的净值
# ============================================================
def example_fund_net_value_types():
    """演示获取不同类型基金的净值数据"""
    print("\n" + "=" * 60)
    print("示例 2: 不同类型基金的净值数据")
    print("=" * 60)

    service = get_service()

    # 基金代码列表: (代码, 名称, 类型)
    funds = [
        ("110011", "易方达蓝筹精选", "混合型"),
        ("000001", "华夏成长", "混合型"),
        ("161725", "招商中证白酒指数", "指数型"),
        ("003834", "华夏能源革新", "股票型"),
    ]

    for code, name, fund_type in funds:
        try:
            df = _as_dataframe(service.get_fund_open_nav(fund_code=code), f"示例2-{code}")

            if not df.empty:
                print(f"\n{name} ({code}) - {fund_type}")
                print(f"  数据行数: {len(df)}")
                # 净值列可能因基金类型而异，尝试获取常见净值列
                nav_col = None
                for col in ["单位净值", "累计净值", "nav", "net_value", "close"]:
                    if col in df.columns:
                        nav_col = col
                        break
                if nav_col:
                    nav = pd.to_numeric(df[nav_col], errors="coerce").dropna()
                    if not nav.empty:
                        print(f"  净值范围: {nav.min():.4f} ~ {nav.max():.4f}")
                    else:
                        print(f"  {nav_col} 无有效数值")
            else:
                print(f"\n{name} ({code}) - 无数据")
        except Exception as e:
            print(f"\n{name} ({code}) - 获取失败: {e}")


# ============================================================
# 示例 3: 获取较长区间的基金净值
# ============================================================
def example_fund_net_value_long_term():
    """获取基金长期净值数据，用于分析年度收益"""
    print("\n" + "=" * 60)
    print("示例 3: 获取基金长期净值数据 (2023全年)")
    print("=" * 60)

    service = get_service()

    try:
        df = service.get_fund_open_nav(fund_code="110011")

        df = _as_dataframe(df, "示例3")
        if df.empty:
            return

        print("易方达蓝筹精选净值数据")
        print(f"数据形状: {df.shape}")
        print(f"全年净值更新天数: {len(df)}")

        # 查找净值列
        nav_col = None
        for col in ["单位净值", "累计净值", "nav", "net_value", "close"]:
            if col in df.columns:
                nav_col = col
                break

        if nav_col:
            nav = pd.to_numeric(df[nav_col], errors="coerce").dropna()
            if len(nav) < 2:
                print(f"{nav_col} 有效数据不足")
                return
            print(f"区间起点净值: {nav.iloc[0]:.4f}")
            print(f"区间终点净值: {nav.iloc[-1]:.4f}")
            yearly_return = ((nav.iloc[-1] - nav.iloc[0]) / nav.iloc[0] * 100)
            print(f"年度收益率: {yearly_return:.2f}%")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 获取基金经理信息 (基本用法)
# ============================================================
def example_fund_manager_basic():
    """基本用法: 获取基金经理信息"""
    print("\n" + "=" * 60)
    print("示例 4: 获取基金经理信息")
    print("=" * 60)

    service = get_service()

    try:
        # get_fund_manager_info 返回所有基金经理列表，不支持按基金代码筛选
        df = _as_dataframe(service.akshare.get_fund_manager_info(), "示例4")
        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 打印前10条基金经理信息
        print("\n前10条基金经理信息:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取基金经理信息失败: {e}")


# ============================================================
# 示例 5: 查找特定基金的经理
# ============================================================
def example_fund_manager_multiple():
    """获取多只基金的基金经理信息 (通过筛选)"""
    print("\n" + "=" * 60)
    print("示例 5: 查找特定基金的基金经理")
    print("=" * 60)

    service = get_service()

    funds = [
        ("110011", "易方达蓝筹精选"),
        ("161725", "招商中证白酒指数"),
        ("000001", "华夏成长"),
    ]

    try:
        # 先获取全部基金经理数据
        all_managers = _as_dataframe(service.akshare.get_fund_manager_info(), "示例5")
        if all_managers.empty:
            return

        print(f"共获取到 {len(all_managers)} 条基金经理记录")

        # 查找与目标基金代码相关的经理
        for code, name in funds:
            # 尝试不同的列名匹配基金代码
            matched = None
            for col in ["fund_code", "基金代码", "code"]:
                if col in all_managers.columns:
                    matched = all_managers[all_managers[col].astype(str).str.contains(code, na=False)]
                    if not matched.empty:
                        break

            if matched is not None and not matched.empty:
                print(f"\n{name} ({code})")
                print(f"  基金经理数量: {len(matched)}")
                name_col = None
                for col in ["基金经理姓名", "姓名", "manager_name", "name"]:
                    if col in matched.columns:
                        name_col = col
                        break
                if name_col:
                    print(f"  经理: {matched[name_col].tolist()}")
            else:
                print(f"\n{name} ({code}) - 未找到匹配的基金经理数据")

    except Exception as e:
        print(f"获取基金经理信息失败: {e}")


# ============================================================
# 示例 6: 获取基金列表 (ETF)
# ============================================================
def example_fund_list_etf():
    """获取 ETF 基金列表"""
    print("\n" + "=" * 60)
    print("示例 6: 获取 ETF 基金列表")
    print("=" * 60)

    service = get_service()

    try:
        # 通过 get_etf_list 获取 ETF 列表
        df = _as_dataframe(service.akshare.get_etf_list(), "示例6")
        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print(f"\nETF 总数: {len(df)}")
        print("\n前10只ETF:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取ETF列表失败: {e}")


# ============================================================
# 示例 7: 获取基金列表 (LOF)
# ============================================================
def example_fund_list_lof():
    """获取 LOF 基金列表"""
    print("\n" + "=" * 60)
    print("示例 7: 获取 LOF 基金列表")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.akshare.get_lof_list(), "示例7")
        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print(f"\nLOF 总数: {len(df)}")
        print("\n前10只LOF:")
        print(df.head(10).to_string(index=False))

    except Exception as e:
        print(f"获取LOF列表失败: {e}")


# ============================================================
# 示例 8: 基金净值 + 经理信息综合分析
# ============================================================
def example_fund_combined_analysis():
    """综合示例: 获取基金净值和经理信息进行分析"""
    print("\n" + "=" * 60)
    print("示例 8: 基金净值与经理信息综合分析")
    print("=" * 60)

    service = get_service()
    fund_code = "110011"

    try:
        # 获取净值数据
        print(f"--- 基金 {fund_code} 净值数据 ---")
        nav_df = _as_dataframe(service.get_fund_open_nav(fund_code=fund_code), "示例8-净值")
        if nav_df.empty:
            print("净值数据: 无数据")
        else:
            print(f"净值数据行数: {len(nav_df)}")

        # 获取经理信息 (全局列表，然后筛选)
        print(f"\n--- 基金经理信息 (全部) ---")
        manager_df = _as_dataframe(service.akshare.get_fund_manager_info(), "示例8-经理")
        if manager_df.empty:
            print("经理信息: 无数据")
        else:
            print(f"经理信息总记录数: {len(manager_df)}")
            print(f"字段列表: {list(manager_df.columns)}")
            print(manager_df.head(5).to_string(index=False))

    except Exception as e:
        print(f"综合分析失败: {e}")


if __name__ == "__main__":
    example_fund_net_value_basic()
    example_fund_net_value_types()
    example_fund_net_value_long_term()
    example_fund_manager_basic()
    example_fund_manager_multiple()
    example_fund_list_etf()
    example_fund_list_lof()
    example_fund_combined_analysis()
