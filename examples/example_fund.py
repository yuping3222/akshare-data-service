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

import warnings

import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)

from akshare_data import get_service


def _mock_fund_nav() -> pd.DataFrame:
    return pd.DataFrame({
        "date": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
        "单位净值": [1.8520, 1.8610, 1.8450, 1.8580, 1.8700],
        "累计净值": [1.8520, 1.8610, 1.8450, 1.8580, 1.8700],
    })


def _mock_fund_managers() -> pd.DataFrame:
    return pd.DataFrame({
        "fund_code": ["110011", "110011", "161725", "000001"],
        "基金经理姓名": ["张坤", "刘健维", "侯昊", "蔡向阳"],
        "任职日期": ["2012-09-28", "2020-07-28", "2020-07-28", "2021-01-01"],
        "任职回报": ["150.23", "45.67", "89.12", "12.34"],
    })


def _mock_etf_list() -> pd.DataFrame:
    return pd.DataFrame({
        "code": ["510300", "510050", "159919", "510500", "518880"],
        "name": ["沪深300ETF", "上证50ETF", "沪深300ETF", "中证500ETF", "黄金ETF"],
        "market": ["沪A", "沪A", "深A", "沪A", "沪A"],
    })


def _mock_lof_list() -> pd.DataFrame:
    return pd.DataFrame({
        "code": ["161725", "164905", "162605"],
        "name": ["招商中证白酒", "富国中证500", "景顺长城沪深300"],
        "market": ["深A", "深A", "深A"],
    })


def _safe_call(fetch_fn) -> pd.DataFrame:
    try:
        df = fetch_fn()
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    except Exception:
        pass
    return pd.DataFrame()


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
        df = _safe_call(lambda: service.get_fund_open_nav(fund_code="110011"))
        if df.empty:
            print("  无真实缓存数据，使用演示数据")
            df = _mock_fund_nav()

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print("\n前5行数据:")
        print(df.head())

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

    funds = [
        ("110011", "易方达蓝筹精选", "混合型"),
        ("000001", "华夏成长", "混合型"),
        ("161725", "招商中证白酒指数", "指数型"),
        ("003834", "华夏能源革新", "股票型"),
    ]

    for code, name, fund_type in funds:
        try:
            df = _safe_call(lambda c=code: service.get_fund_open_nav(fund_code=c))
            if df.empty:
                print(f"\n{name} ({code}) - 无缓存数据 (演示模式)")
                continue

            print(f"\n{name} ({code}) - {fund_type}")
            print(f"  数据行数: {len(df)}")
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
        df = _safe_call(lambda: service.get_fund_open_nav(fund_code="110011"))
        if df.empty:
            print("  无真实缓存数据，使用演示数据")
            df = _mock_fund_nav()

        print("易方达蓝筹精选净值数据")
        print(f"数据形状: {df.shape}")
        print(f"全年净值更新天数: {len(df)}")

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
        df = _safe_call(lambda: service.akshare.get_fund_manager_info())
        if df.empty:
            print("  无真实缓存数据，使用演示数据")
            df = _mock_fund_managers()

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

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
        all_managers = _safe_call(lambda: service.akshare.get_fund_manager_info())
        if all_managers.empty:
            print("  无真实缓存数据，使用演示数据")
            all_managers = _mock_fund_managers()

        print(f"共获取到 {len(all_managers)} 条基金经理记录")

        for code, name in funds:
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
        df = _safe_call(lambda: service.akshare.get_etf_list())
        if df.empty:
            print("  无真实缓存数据，使用演示数据")
            df = _mock_etf_list()

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
        df = _safe_call(lambda: service.akshare.get_lof_list())
        if df.empty:
            print("  无真实缓存数据，使用演示数据")
            df = _mock_lof_list()

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
        print(f"--- 基金 {fund_code} 净值数据 ---")
        nav_df = _safe_call(lambda: service.get_fund_open_nav(fund_code=fund_code))
        if nav_df.empty:
            print("  无真实缓存数据，使用演示数据")
            nav_df = _mock_fund_nav()
        print(f"净值数据行数: {len(nav_df)}")
        print(nav_df.head(5).to_string(index=False))

        print("\n--- 基金经理信息 (全部) ---")
        manager_df = _safe_call(lambda: service.akshare.get_fund_manager_info())
        if manager_df.empty:
            print("  无真实缓存数据，使用演示数据")
            manager_df = _mock_fund_managers()
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
