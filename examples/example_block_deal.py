"""
大宗交易接口使用示例

本示例展示如何通过 DataService 的 akshare adapter 获取大宗交易数据。
包含主要接口：
  - get_block_deal(): 获取最新交易日的大宗交易明细数据

注意：
    akshare 源的 get_block_deal() 底层调用 stock_fund_flow_big_deal()，
    该函数不接受任何参数，只返回最新交易日的大宗交易数据。
    如果传入 date/symbol/start_date/end_date 参数，会产生 warning 并被忽略。

    get_block_deal_summary 在 akshare 源中映射到与 get_block_deal 相同的底层接口，
    因此也不支持日期范围查询。如需日期范围汇总，请使用 lixinger 源。

lixinger 源的 get_block_deal(symbol, start_date, end_date) 支持指定股票和日期范围。
"""

import pandas as pd
from akshare_data import get_service


def example_basic_block_deal():
    """基础用法：获取最新交易日的大宗交易明细"""
    print("=" * 60)
    print("示例1: 获取最新交易日的大宗交易明细")
    print("=" * 60)

    service = get_service()

    try:
        # 获取最新交易日的大宗交易数据
        # 注意：该接口不支持 date 参数，只返回最新交易日数据
        df = service.akshare.get_block_deal()

        if df is None or df.empty:
            print("无大宗交易数据")
        else:
            # 打印数据基本信息
            print(f"数据形状: {df.shape}")
            print(f"大宗交易笔数: {len(df)}")
            print(f"列名: {df.columns.tolist()}")
            print(f"\n前5行数据:")
            print(df.head())
    except Exception as e:
        print(f"获取大宗交易数据失败: {e}")


def example_block_deal_recent():
    """获取最新交易日的大宗交易数据"""
    print("\n" + "=" * 60)
    print("示例2: 获取最新交易日的大宗交易数据")
    print("=" * 60)

    service = get_service()

    try:
        # 获取最新交易日的大宗交易数据
        df = service.akshare.get_block_deal()

        if df.empty:
            print("无大宗交易数据")
        else:
            print(f"共 {len(df)} 笔大宗交易")

            # 打印数据结构
            print(f"\n数据列说明:")
            for i, col in enumerate(df.columns):
                print(f"  {i + 1}. {col}")

            # 查看前10行数据
            print(f"\n前10行数据:")
            print(df.head(10))
    except Exception as e:
        print(f"获取大宗交易数据失败: {e}")


def example_block_deal_summary_basic():
    """基础用法：获取大宗交易汇总数据（跳过）

    注意: akshare 源的 block_deal 底层调用 stock_fund_flow_big_deal()，
    该函数不接受 start_date/end_date 参数。
    get_block_deal_summary 与 get_block_deal 映射到同一底层接口，
    因此无法按日期范围查询汇总数据。此示例展示说明信息。
    """
    print("\n" + "=" * 60)
    print("示例3: 大宗交易汇总数据（akshare 不支持日期范围）")
    print("=" * 60)
    print("注意: akshare 的 block_deal 接口不接受 start_date/end_date 参数")
    print("      只返回最新交易日的大宗交易明细数据，无法做日期范围汇总查询")
    print("      如需日期范围汇总，请使用 lixinger 源（如果可用）")


def example_block_deal_summary_analysis():
    """大宗交易汇总数据分析（跳过）

    注意: akshare 源的 block_deal 不支持 start_date/end_date 参数，
    无法做日期范围汇总。此示例展示说明信息。
    """
    print("\n" + "=" * 60)
    print("示例4: 大宗交易汇总数据分析（akshare 不支持日期范围）")
    print("=" * 60)
    print("注意: akshare 的 block_deal 接口不支持按日期范围查询汇总数据")


def example_block_deal_multiple_dates():
    """获取最新交易日的大宗交易数量"""
    print("\n" + "=" * 60)
    print("示例5: 获取最新交易日的大宗交易数量")
    print("=" * 60)

    service = get_service()

    try:
        # 注意：该接口不支持指定日期，只返回最新交易日数据
        df = service.akshare.get_block_deal()
        if df.empty:
            print("无大宗交易数据")
        else:
            print(f"最新交易日共 {len(df)} 笔大宗交易")
    except Exception as e:
        print(f"获取失败: {e}")


def example_block_deal_premium_analysis():
    """大宗交易溢价率分析"""
    print("\n" + "=" * 60)
    print("示例6: 大宗交易溢价率分析")
    print("=" * 60)

    service = get_service()

    try:
        # 获取最新交易日的大宗交易数据
        df = service.akshare.get_block_deal()

        if df.empty:
            print("无大宗交易数据")
        else:
            print(f"共 {len(df)} 笔大宗交易")

            # 查找溢价率相关字段
            # 常见字段名: 溢价率, premium_rate, 折溢率 等
            possible_premium_cols = ["溢价率", "premium_rate", "折溢率", "折溢价率"]
            premium_col = None
            for col in possible_premium_cols:
                if col in df.columns:
                    premium_col = col
                    break

            if premium_col:
                print(f"\n溢价率统计（基于'{premium_col}'列）:")
                # 转换为数值类型（如果还不是的话）
                premium_values = df[premium_col].astype(float)
                print(f"  平均溢价率: {premium_values.mean():.2f}%")
                print(f"  最高溢价率: {premium_values.max():.2f}%")
                print(f"  最低溢价率: {premium_values.min():.2f}%")

                # 统计折价和溢价的比例
                discount_count = (premium_values < 0).sum()
                premium_count = (premium_values > 0).sum()
                flat_count = (premium_values == 0).sum()
                print(f"\n  折价交易: {discount_count} 笔")
                print(f"  溢价交易: {premium_count} 笔")
                print(f"  平价交易: {flat_count} 笔")
            else:
                print("\n未找到溢价率字段，显示原始数据:")
                print(f"\n数据列:")
                for col in df.columns:
                    print(f"  - {col}")

            # 显示详细数据
            print(f"\n前10行详细数据:")
            print(df.head(10))
    except Exception as e:
        print(f"获取大宗交易数据失败: {e}")


def example_block_deal_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 60)
    print("示例7: 错误处理示例")
    print("=" * 60)

    service = get_service()

    # 测试正常调用（无参数）
    try:
        df = service.akshare.get_block_deal()
        if df.empty:
            print("无大宗交易数据")
        else:
            print(f"获取到 {len(df)} 条数据")
    except Exception as e:
        print(f"捕获到异常: {type(e).__name__}: {e}")

    # 注意: get_block_deal_summary 不接受 start_date/end_date 参数
    # akshare 的 block_deal 底层函数不支持日期范围
    print("\n注意: get_block_deal_summary 不支持 start_date/end_date 参数")
    print("      akshare 的 block_deal 只返回最新交易日数据")


def example_block_deal_combined_analysis():
    """实用场景：结合明细数据进行分析

    注意: akshare 不支持日期范围汇总，这里只展示最新交易日的明细分析。
    """
    print("\n" + "=" * 60)
    print("示例8: 最新交易日大宗交易数据分析")
    print("=" * 60)

    service = get_service()

    try:
        # 获取最新交易日明细
        detail_df = service.akshare.get_block_deal()

        print("最新交易日明细数据:")
        print(f"  交易笔数: {len(detail_df)}")

        if not detail_df.empty:
            print(f"\n明细数据列:")
            for col in detail_df.columns:
                print(f"  - {col}")

            # 简单的溢价分析
            possible_premium_cols = ["溢价率", "premium_rate", "折溢率", "折溢价率"]
            for col in possible_premium_cols:
                if col in detail_df.columns:
                    premium_values = pd.to_numeric(detail_df[col], errors="coerce")
                    print(f"\n溢价率统计（'{col}'列）:")
                    print(f"  平均: {premium_values.mean():.2f}%")
                    print(f"  最高: {premium_values.max():.2f}%")
                    print(f"  最低: {premium_values.min():.2f}%")
                    break

    except Exception as e:
        print(f"获取数据失败: {e}")


if __name__ == "__main__":
    example_basic_block_deal()
    example_block_deal_recent()
    example_block_deal_summary_basic()
    example_block_deal_summary_analysis()
    example_block_deal_multiple_dates()
    example_block_deal_premium_analysis()
    example_block_deal_error_handling()
    example_block_deal_combined_analysis()
