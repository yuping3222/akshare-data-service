"""指数成分股获取示例

演示如何使用 get_index_stocks() 和 get_index_components() 接口获取指数的成分股信息。

get_index_stocks(index_code) 参数说明:
    - index_code: 指数代码，如 "000300" (沪深300)、"000016" (上证50)、"000905" (中证500)

返回: List[str] - 成分股代码列表 (聚宽格式: "600519.XSHG")


get_index_components(index_code, include_weights) 参数说明:
    - index_code: 指数代码
    - include_weights: 是否包含权重信息，默认 True

返回: pd.DataFrame，包含字段:
    - index_code: 指数代码
    - code: 成分股代码
    - stock_name: 股票名称
    - weight: 权重 (仅当 include_weights=True 时)

注意:
    - 数据采用 Cache-First 策略，首次获取后会缓存到本地
    - 后续相同指数的请求会直接返回缓存数据
    - 支持带前缀的代码格式，系统会自动规范化
"""

import pandas as pd
from akshare_data import get_index_stocks, get_index_components
from _example_utils import fetch_with_retry, normalize_symbol_input, stable_df


def _get_index_stocks(index_code):
    """Get index stocks with graceful empty-data handling."""
    code = normalize_symbol_input(index_code)
    stocks = fetch_with_retry(lambda: get_index_stocks(code), retries=2)
    if not stocks:
        print(f"  [无数据] {index_code} 的成分股列表为空")
        return []
    return stocks


def _get_index_components(index_code, include_weights=True):
    """Get index components with graceful empty-data handling."""
    code = normalize_symbol_input(index_code)
    df = fetch_with_retry(
        lambda: get_index_components(code, include_weights=include_weights),
        retries=2,
    )
    df = stable_df(df)
    if df is None or df.empty or "code" not in df.columns:
        print(f"  [无数据] {index_code} 的成分股详情为空")
        return pd.DataFrame()
    return df


def example_basic_index_stocks():
    """示例1: 获取沪深300成分股列表"""
    print("=" * 60)
    print("示例1: 获取沪深300 (000300) 成分股列表")
    print("=" * 60)

    try:
        stocks = _get_index_stocks("000300")

        print(f"成分股数量: {len(stocks)}")
        print(f"数据类型: {type(stocks)}")

        # 打印前20只股票
        print("\n前20只成分股:")
        for i, stock in enumerate(stocks[:20], 1):
            print(f"  {i:3d}. {stock}")

        # 打印后10只股票
        print("\n后10只成分股:")
        for i, stock in enumerate(stocks[-10:], len(stocks) - 9):
            print(f"  {i:3d}. {stock}")

    except Exception as e:
        print(f"获取成分股失败: {e}")


def example_multiple_indices():
    """示例2: 获取多个主要指数的成分股"""
    print("\n" + "=" * 60)
    print("示例2: 获取多个主要指数的成分股")
    print("=" * 60)

    indices = {
        "000300": "沪深300",
        "000016": "上证50",
        "000905": "中证500",
        "000852": "中证1000",
    }

    for code, name in indices.items():
        try:
            stocks = _get_index_stocks(code)
            print(f"\n{name} ({code}):")
            print(f"  成分股数量: {len(stocks)}")
            print(f"  前5只: {stocks[:5]}")

        except Exception as e:
            print(f"\n{name} ({code}) 获取失败: {e}")


def example_basic_index_components():
    """示例3: 获取指数成分股详情 (含权重)"""
    print("\n" + "=" * 60)
    print("示例3: 获取沪深300成分股详情 (含权重)")
    print("=" * 60)

    try:
        df = _get_index_components("000300", include_weights=True)

        print(f"数据形状: {df.shape}")
        print(f"  - 成分股数量: {df.shape[0]}")
        print(f"  - 字段数: {df.shape[1]}")

        print(f"字段列表: {list(df.columns)}")

        print("\n前10只成分股详情:")
        print(df.head(10).to_string())

        # 打印权重最高的前10只股票
        if "weight" in df.columns:
            print("\n权重最高的前10只股票:")
            top10 = df.nlargest(10, "weight")
            for _, row in top10.iterrows():
                print(
                    f"  {str(row['stock_name']):10s} ({row['code']})  权重: {row['weight']:.2f}%"
                )

    except Exception as e:
        print(f"获取成分股详情失败: {e}")


def example_components_without_weights():
    """示例4: 获取指数成分股 (不含权重)"""
    print("\n" + "=" * 60)
    print("示例4: 获取上证50成分股 (不含权重)")
    print("=" * 60)

    try:
        df = _get_index_components("000016", include_weights=False)

        print(f"数据形状: {df.shape}")
        print(f"字段列表: {list(df.columns)}")

        print(f"\n前20只成分股:")
        print(df.head(20).to_string(index=False))

    except Exception as e:
        print(f"获取成分股失败: {e}")


def example_compare_stocks_vs_components():
    """示例5: 对比 get_index_stocks 和 get_index_components 的返回结果"""
    print("\n" + "=" * 60)
    print("示例5: 对比两种接口的返回结果")
    print("=" * 60)

    try:
        index_code = "000905"  # 中证500

        stocks_list = _get_index_stocks(index_code)
        print(f"\nget_index_stocks('{index_code}'):")
        print(f"  返回类型: {type(stocks_list).__name__}")
        print(f"  数量: {len(stocks_list)}")
        print(f"  示例: {stocks_list[:5]}")

        components_df = _get_index_components(index_code, include_weights=True)
        print(f"\nget_index_components('{index_code}', include_weights=True):")
        print(f"  返回类型: {type(components_df).__name__}")
        print(f"  数量: {len(components_df)}")
        print(f"  字段: {list(components_df.columns)}")
        print(f"  前5行:")
        print(components_df.head().to_string())

        if "code" in components_df.columns:
            codes_from_df = set(components_df["code"].tolist())
            codes_from_list = set(stocks_list)

            print(f"\n对比验证:")
            print(f"  get_index_stocks 返回的股票数: {len(codes_from_list)}")
            print(f"  get_index_components 返回的股票数: {len(codes_from_df)}")
            print(f"  两者是否一致: {codes_from_list == codes_from_df}")

    except Exception as e:
        print(f"对比分析失败: {e}")


def example_filter_by_weight():
    """示例6: 根据权重过滤成分股"""
    print("\n" + "=" * 60)
    print("示例6: 根据权重过滤成分股")
    print("=" * 60)

    try:
        df = _get_index_components("000300", include_weights=True)

        if "weight" not in df.columns:
            print("权重数据不可用")
            return

        # 过滤权重 >= 1% 的股票
        high_weight = df[df["weight"] >= 1.0]
        print(f"\n权重 >= 1% 的成分股 (共 {len(high_weight)} 只):")
        for _, row in high_weight.iterrows():
            print(
                f"  {str(row['stock_name']):10s} ({row['code']})  权重: {row['weight']:.2f}%"
            )

        # 过滤权重 < 0.1% 的股票
        low_weight = df[df["weight"] < 0.1]
        print(f"\n权重 < 0.1% 的成分股 (共 {len(low_weight)} 只):")
        for _, row in low_weight.head(10).iterrows():
            print(
                f"  {str(row['stock_name']):10s} ({row['code']})  权重: {row['weight']:.2f}%"
            )

        # 权重分布统计
        print(f"\n权重分布统计:")
        print(f"  最大权重: {df['weight'].max():.2f}%")
        print(f"  最小权重: {df['weight'].min():.2f}%")
        print(f"  平均权重: {df['weight'].mean():.2f}%")
        print(f"  中位数权重: {df['weight'].median():.2f}%")

    except Exception as e:
        print(f"权重过滤失败: {e}")


def example_cache_behavior():
    """示例7: 演示缓存行为"""
    print("\n" + "=" * 60)
    print("示例7: 缓存行为演示")
    print("=" * 60)

    try:
        import time

        # 第一次请求
        print("\n第一次请求...")
        start = time.time()
        stocks1 = _get_index_stocks("000016")
        elapsed1 = time.time() - start
        print(f"  耗时: {elapsed1:.3f} 秒")
        print(f"  返回股票数: {len(stocks1)}")

        # 第二次请求
        print("\n第二次请求...")
        start = time.time()
        stocks2 = _get_index_stocks("000016")
        elapsed2 = time.time() - start
        print(f"  耗时: {elapsed2:.3f} 秒")
        print(f"  返回股票数: {len(stocks2)}")

        if elapsed2 > 0:
            print(f"\n速度比: {elapsed1 / elapsed2:.1f}x")

    except Exception as e:
        print(f"缓存演示失败: {e}")


if __name__ == "__main__":
    example_basic_index_stocks()
    example_multiple_indices()
    example_basic_index_components()
    example_components_without_weights()
    example_compare_stocks_vs_components()
    example_filter_by_weight()
    example_cache_behavior()
