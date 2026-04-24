"""
get_fund_open_info() 接口示例

演示如何使用 DataService.get_fund_open_info() 获取开放式基金基本信息。

接口说明:
- get_fund_open_info(fund_code): 获取指定基金的基本信息
  - fund_code: 基金代码，如 "110011"
  - 底层调用 fund_open_fund_info_em(symbol, indicator, period)
- 返回: DataFrame (底层 fund_open_fund_info_em 返回 DataFrame)

使用方式:
    from akshare_data import get_service
    service = get_service()
    df = service.get_fund_open_info(fund_code="110011")

注意:
- 该接口底层调用 fund_open_fund_info_em，返回 DataFrame 而非 dict
- indicator 默认 "单位净值走势"，period 默认 "成立来"
- 数据会缓存到本地，重复查询速度快
"""

import logging
import warnings
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_service


def _as_dataframe(data, label: str) -> pd.DataFrame:
    if not isinstance(data, pd.DataFrame):
        print(f"{label}: 返回类型异常，期望 DataFrame，实际 {type(data).__name__}")
        return pd.DataFrame()
    if data.empty:
        print(f"{label}: 返回空数据")
    return data


# ============================================================
# 示例 1: 基本用法 - 获取单只基金信息
# ============================================================
def example_basic():
    """基本用法: 获取易方达蓝筹精选的基本信息"""
    print("=" * 60)
    print("示例 1: 获取基金基本信息 - 易方达蓝筹精选 (110011)")
    print("=" * 60)

    service = get_service()

    try:
        # fund_code: 基金代码 (6位数字)
        df = _as_dataframe(service.get_fund_open_info(fund_code="110011"), "示例1")
        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")
        print("\n基金详细信息:")
        print(df.head().to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 2: 对比多只基金的信息
# ============================================================
def example_compare_funds():
    """对比多只基金的基本信息"""
    print("\n" + "=" * 60)
    print("示例 2: 多只基金信息对比")
    print("=" * 60)

    service = get_service()

    funds = [
        ("110011", "易方达蓝筹精选"),
        ("000001", "华夏成长"),
        ("161725", "招商中证白酒指数"),
        ("003834", "华夏能源革新"),
    ]

    for code, name in funds:
        try:
            df = _as_dataframe(service.get_fund_open_info(fund_code=code), f"示例2-{code}")
            if not df.empty:
                print(f"\n{name} ({code}): {len(df)} 行数据")
                print(df.head(3).to_string(index=False))
            else:
                print(f"\n{name} ({code}): 无数据")
        except Exception as e:
            print(f"\n{name} ({code}): 获取失败 - {e}")


# ============================================================
# 示例 3: 获取基金成立日期并计算运行时间
# ============================================================
def example_fund_age():
    """根据成立日期计算基金运行时间"""
    print("\n" + "=" * 60)
    print("示例 3: 计算基金运行时间")
    print("=" * 60)

    service = get_service()

    try:
        df = _as_dataframe(service.get_fund_open_info(fund_code="110011"), "示例3")
        if df.empty:
            return

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        # 尝试查找日期列
        date_col = None
        for col in df.columns:
            if "日期" in str(col) or "date" in str(col).lower() or "成立" in str(col):
                date_col = col
                break

        if date_col:
            print(f"找到日期列: {date_col}")
            print(df[[date_col]].head().to_string(index=False))
        else:
            print("未找到明显的日期列")
            print(df.head().to_string(index=False))

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 4: 批量获取并整理基金信息
# ============================================================
def example_batch_info():
    """批量获取多只基金信息并整理成表格"""
    print("\n" + "=" * 60)
    print("示例 4: 批量获取基金信息")
    print("=" * 60)

    service = get_service()

    funds = ["110011", "000001", "161725", "003834", "005827"]

    import pandas as pd

    results = []

    for code in funds:
        try:
            df = _as_dataframe(service.get_fund_open_info(fund_code=code), f"示例4-{code}")
            if not df.empty:
                record = {"基金代码": code, "行数": len(df), "字段": ", ".join(list(df.columns)[:5])}
                results.append(record)
            else:
                results.append({"基金代码": code, "行数": 0, "字段": "无数据"})
        except Exception as e:
            results.append({"基金代码": code, "行数": -1, "字段": f"错误: {e}"})

    if results:
        df_summary = pd.DataFrame(results)
        print("\n基金信息汇总:")
        print(df_summary.to_string(index=False))
    else:
        print("未获取到任何数据")


# ============================================================
# 示例 5: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 5: 错误处理")
    print("=" * 60)

    service = get_service()

    test_cases = [
        ("999999", "不存在的基金代码"),
        ("", "空字符串"),
        ("ABCDEF", "非法格式"),
    ]

    for code, desc in test_cases:
        try:
            df = _as_dataframe(service.get_fund_open_info(fund_code=code), f"示例5-{code or 'EMPTY'}")
            if df.empty:
                print(f"{desc} ('{code}'): 返回空 DataFrame")
            else:
                print(f"{desc} ('{code}'): 获取到 {len(df)} 行数据")
        except Exception as e:
            print(f"{desc} ('{code}'): 异常 - {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_compare_funds()
    example_fund_age()
    example_batch_info()
    example_error_handling()
