"""
利润表接口示例 (get_income_statement)

演示如何使用 get_income_statement() 获取上市公司的利润表数据。

导入方式: from akshare_data import get_service
          service = get_service()
          df = service.get_income_statement(symbol="600519")
"""

import logging
logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_service
from _example_utils import first_non_empty_by_symbol


def example_basic():
    """基本用法: 获取单只股票的利润表"""
    print("=" * 60)
    print("示例 1: 获取贵州茅台利润表")
    print("=" * 60)

    service = get_service()

    try:
        df, used_symbol = first_non_empty_by_symbol(
            service.get_income_statement, ["600519", "000858", "000001"]
        )

        if df is None or df.empty:
            print("无数据 (数据源未返回结果)")
            return

        print(f"数据形状: {df.shape}")
        print(f"回退命中代码: {used_symbol}")
        print(f"字段列表: {list(df.columns)}")
        print("\n前5行数据:")
        print(df.head())

    except Exception as e:
        print(f"获取数据失败: {e}")


def example_multiple_stocks():
    """多只股票: 获取多只股票的利润表"""
    print("\n" + "=" * 60)
    print("示例 2: 获取多只股票利润表")
    print("=" * 60)

    service = get_service()
    symbols = {"600519": "贵州茅台", "000858": "五粮液", "000568": "泸州老窖"}

    for code, name in symbols.items():
        try:
            df = service.get_income_statement(symbol=code)
            if df is not None and not df.empty:
                print(f"\n{name} ({code}): {len(df)} 条记录")
                print(df.head(2))
            else:
                print(f"\n{name} ({code}): 无数据")
        except Exception as e:
            print(f"\n{name} ({code}): 获取失败 - {e}")


def example_analysis():
    """分析: 利润数据分析"""
    print("\n" + "=" * 60)
    print("示例 3: 利润数据分析")
    print("=" * 60)

    service = get_service()

    try:
        df, used_symbol = first_non_empty_by_symbol(
            service.get_income_statement, ["600519", "000858", "000001"]
        )

        if df is None or df.empty:
            print("无数据")
            return

        print(f"数据形状: {df.shape}")
        print(f"回退命中代码: {used_symbol}")
        print(f"字段数量: {len(df.columns)}")

        # 数值列统计
        numeric_cols = df.select_dtypes(include='number').columns
        if len(numeric_cols) > 0:
            print("\n描述统计:")
            print(df[numeric_cols].describe())

    except Exception as e:
        print(f"分析失败: {e}")


def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 4: 错误处理演示")
    print("=" * 60)

    service = get_service()

    print("\n测试 1: 正常股票代码")
    try:
        df, _ = first_non_empty_by_symbol(
            service.get_income_statement, ["600519", "000858", "000001"]
        )
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    print("\n测试 2: 无效股票代码")
    try:
        df = service.get_income_statement(symbol="INVALID")
        if df is None or df.empty:
            print("  结果: 返回空数据")
        else:
            print(f"  结果: 获取到 {len(df)} 行数据")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_multiple_stocks()
    example_analysis()
    example_error_handling()
