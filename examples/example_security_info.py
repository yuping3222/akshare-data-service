"""
get_security_info() 接口使用示例

该接口用于获取单只证券的基本信息，包括：
- code: 证券代码
- display_name: 显示名称
- type: 证券类型 (stock/index/etf 等)
- start_date: 上市日期
- industry: 所属行业

导入方式: from akshare_data import get_security_info

注意: 该接口底层调用 akshare 的 stock_individual_info_em(symbol)。
      如遇网络连接超时或连接错误，请检查网络状态后重试。
"""

import logging

logging.getLogger("akshare_data").setLevel(logging.ERROR)

from akshare_data import get_security_info


def example_basic_usage():
    """示例1: 获取单只股票的基本信息"""
    print("=" * 60)
    print("示例1: 获取单只股票的基本信息")
    print("=" * 60)

    try:
        # symbol: 证券代码，支持多种格式 (如 "000001", "000001.sz", "600519")
        symbol = "000001"  # 平安银行
        info = get_security_info(symbol)

        if not info:
            print(f"证券代码: {symbol}")
            print("未找到数据")
            return

        # 打印返回的完整字典
        print(f"证券代码: {symbol}")
        print(f"返回数据: {info}")
        print()

        # 访问各个字段
        print(f"  代码: {info.get('code')}")
        print(f"  名称: {info.get('display_name')}")
        print(f"  类型: {info.get('type')}")
        print(f"  上市日期: {info.get('start_date')}")
        print(f"  行业: {info.get('industry')}")

    except Exception as e:
        err_msg = str(e).lower()
        if "connection" in err_msg or "timeout" in err_msg or "network" in err_msg:
            print(f"网络连接错误: {e}")
            print("提示: 请检查网络状态，或稍后重试")
        else:
            print(f"获取证券信息时出错: {e}")

    print()


def example_multiple_stocks():
    """示例2: 批量获取多只股票信息"""
    print("=" * 60)
    print("示例2: 批量获取多只股票信息")
    print("=" * 60)

    # 定义要查询的股票列表
    stocks = [
        "000001",  # 平安银行
        "600519",  # 贵州茅台
        "000858",  # 五粮液
        "300750",  # 宁德时代
    ]

    try:
        for symbol in stocks:
            info = get_security_info(symbol)
            if info:
                print(
                    f"{symbol}: {info.get('display_name')} | 类型: {info.get('type')} | 行业: {info.get('industry')}"
                )
            else:
                print(f"{symbol}: 未找到信息")

    except Exception as e:
        err_msg = str(e).lower()
        if "connection" in err_msg or "timeout" in err_msg or "network" in err_msg:
            print(f"网络连接错误: {e}")
            print("提示: 请检查网络状态，或稍后重试")
        else:
            print(f"批量获取股票信息时出错: {e}")

    print()


def example_different_types():
    """示例3: 获取不同类型证券的信息 (股票/指数/ETF)"""
    print("=" * 60)
    print("示例3: 获取不同类型证券的信息")
    print("=" * 60)

    securities = {
        "000001": "股票 - 平安银行",
        "000300": "指数 - 沪深300",
        "510300": "ETF - 沪深300ETF",
    }

    try:
        for symbol, desc in securities.items():
            info = get_security_info(symbol)
            print(f"{desc} ({symbol}):")
            if info:
                for key, value in info.items():
                    print(f"  {key}: {value}")
            else:
                print("  未找到信息")
            print()

    except Exception as e:
        err_msg = str(e).lower()
        if "connection" in err_msg or "timeout" in err_msg or "network" in err_msg:
            print(f"网络连接错误: {e}")
            print("提示: 请检查网络状态，或稍后重试")
        else:
            print(f"获取证券信息时出错: {e}")


def example_with_cache():
    """示例4: 演示缓存机制 (第二次调用会从缓存读取)"""
    print("=" * 60)
    print("示例4: 演示缓存机制")
    print("=" * 60)

    symbol = "000001"

    try:
        # 第一次调用: 从数据源获取并写入缓存
        print("第一次调用 (从数据源获取):")
        info1 = get_security_info(symbol)
        print(f"  结果: {info1}")
        print()

        # 第二次调用: 直接从缓存读取，速度更快
        print("第二次调用 (从缓存读取):")
        info2 = get_security_info(symbol)
        print(f"  结果: {info2}")

    except Exception as e:
        err_msg = str(e).lower()
        if "connection" in err_msg or "timeout" in err_msg or "network" in err_msg:
            print(f"网络连接错误: {e}")
            print("提示: 请检查网络状态，或稍后重试")
        else:
            print(f"获取证券信息时出错: {e}")

    print()


def example_error_handling():
    """示例5: 错误处理演示"""
    print("=" * 60)
    print("示例5: 错误处理演示")
    print("=" * 60)

    # 测试无效代码
    invalid_symbols = ["999999", "ABCDEF", ""]

    for symbol in invalid_symbols:
        try:
            info = get_security_info(symbol)
            if info:
                print(f"{symbol}: {info}")
            else:
                print(f"{symbol}: 返回空结果 (证券不存在)")
        except Exception as e:
            print(f"{symbol}: 异常 - {type(e).__name__}: {e}")

    print()


if __name__ == "__main__":
    example_basic_usage()
    example_multiple_stocks()
    example_different_types()
    example_with_cache()
    example_error_handling()
