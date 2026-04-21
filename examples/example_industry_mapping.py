"""
get_industry_mapping() 接口示例

演示如何使用 akshare_data.get_industry_mapping() 获取股票到行业的映射。

接口说明:
  - get_industry_mapping(symbol, level=1) -> str
  - 返回股票所属行业的行业代码 (字符串)
  - level: 行业级别，默认为 1 (一级行业)

行业级别说明:
  - level=1: 一级行业 (如: 食品饮料、银行、房地产)
  - level=2: 二级行业 (如: 白酒、股份制银行)
  - level=3: 三级行业 (更细分的行业分类)

返回值为行业代码字符串，可通过 get_industry_stocks() 获取该行业下的所有股票。
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取股票一级行业映射
# ============================================================
def example_basic():
    """基本用法: 获取贵州茅台的一级行业代码"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取贵州茅台一级行业代码")
    print("=" * 60)

    service = get_service()

    try:
        # symbol: 证券代码
        # level: 行业级别，默认 1
        industry_code = service.get_industry_mapping(symbol="600519", level=1)

        print(f"股票代码: 600519")
        print(f"一级行业代码: {industry_code}")

    except Exception as e:
        print(f"获取行业映射失败: {e}")


# ============================================================
# 示例 2: 不同行业级别对比
# ============================================================
def example_different_levels():
    """对比同一股票在不同行业级别下的分类"""
    print("\n" + "=" * 60)
    print("示例 2: 不同行业级别对比")
    print("=" * 60)

    service = get_service()

    stocks = {
        "600519": "贵州茅台",
        "000001": "平安银行",
        "000002": "万科A",
    }

    for code, name in stocks.items():
        print(f"\n{name}({code}):")
        for level in [1, 2, 3]:
            try:
                industry_code = service.get_industry_mapping(symbol=code, level=level)
                print(f"  级别 {level}: {industry_code}")
            except Exception as e:
                print(f"  级别 {level}: 获取失败 - {e}")


# ============================================================
# 示例 3: 批量获取行业映射并统计
# ============================================================
def example_batch_mapping():
    """批量获取多只股票的行业映射并统计行业分布"""
    print("\n" + "=" * 60)
    print("示例 3: 批量获取行业映射")
    print("=" * 60)

    service = get_service()

    stocks = [
        "600519", "000858", "000568",  # 白酒
        "000001", "600036", "601398",  # 银行
        "000002", "600048",  # 地产
        "002594", "601318",  # 其他
    ]

    industry_counts = {}
    stock_industry_map = {}

    for code in stocks:
        try:
            industry_code = service.get_industry_mapping(symbol=code, level=1)
            stock_industry_map[code] = industry_code

            if industry_code not in industry_counts:
                industry_counts[industry_code] = 0
            industry_counts[industry_code] += 1

            print(f"{code}: {industry_code}")
        except Exception as e:
            print(f"{code}: 获取失败 - {e}")

    print("\n行业分布统计:")
    for industry, count in sorted(industry_counts.items()):
        print(f"  {industry}: {count} 只股票")


# ============================================================
# 示例 4: 使用行业映射进行分组
# ============================================================
def example_group_by_industry():
    """演示如何使用行业映射对股票进行分组"""
    print("\n" + "=" * 60)
    print("示例 4: 使用行业映射分组")
    print("=" * 60)

    service = get_service()

    stocks = ["600519", "000001", "000002", "600036", "000858", "002594"]

    groups = {}
    for code in stocks:
        try:
            industry_code = service.get_industry_mapping(symbol=code, level=1)
            if industry_code not in groups:
                groups[industry_code] = []
            groups[industry_code].append(code)
        except Exception as e:
            print(f"{code}: 获取失败 - {e}")

    print("\n按一级行业分组:")
    for industry, codes in groups.items():
        print(f"  行业 {industry}: {', '.join(codes)}")


# ============================================================
# 示例 5: 获取深市股票行业映射
# ============================================================
def example_sz_stock():
    """获取深市股票行业映射"""
    print("\n" + "=" * 60)
    print("示例 5: 获取深市股票行业映射")
    print("=" * 60)

    service = get_service()

    try:
        industry_code = service.get_industry_mapping(symbol="000001", level=1)
        print(f"平安银行(000001) 一级行业代码: {industry_code}")

        industry_code = service.get_industry_mapping(symbol="000001", level=2)
        print(f"平安银行(000001) 二级行业代码: {industry_code}")

    except Exception as e:
        print(f"获取数据失败: {e}")


# ============================================================
# 示例 6: 错误处理演示
# ============================================================
def example_error_handling():
    """演示错误处理 - 无效代码、无效级别等"""
    print("\n" + "=" * 60)
    print("示例 6: 错误处理演示")
    print("=" * 60)

    service = get_service()

    # 测试 1: 正常获取
    print("\n测试 1: 正常获取")
    try:
        result = service.get_industry_mapping(symbol="600519", level=1)
        print(f"  结果: {result}")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 2: 无效代码
    print("\n测试 2: 无效代码")
    try:
        result = service.get_industry_mapping(symbol="999999", level=1)
        print(f"  结果: {result!r}")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")

    # 测试 3: 默认级别 (不传入 level)
    print("\n测试 3: 默认级别")
    try:
        result = service.get_industry_mapping(symbol="600519")
        print(f"  结果: {result}")
    except Exception as e:
        print(f"  捕获异常: {type(e).__name__}: {e}")


if __name__ == "__main__":
    example_basic()
    example_different_levels()
    example_batch_mapping()
    example_group_by_industry()
    example_sz_stock()
    example_error_handling()
