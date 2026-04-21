"""
get_industry_stocks() 和 get_industry_mapping() 接口使用示例

get_industry_stocks(industry_code, level):
  - industry_code: 行业代码 (如 "801010" 农林牧渔)
  - level: 行业级别 1/2/3 (默认 1)，1为一级行业，2为二级行业，3为三级行业
  - 返回: List[str] - 该行业下的股票代码列表

get_industry_mapping(symbol, level):
  - symbol: 股票代码
  - level: 行业级别 (默认 1)
  - 返回: str - 该股票所属的行业代码

导入方式: from akshare_data import get_industry_stocks, get_industry_mapping
"""


# -- 常用申万一级行业代码参考 --
# 801010: 农林牧渔    801030: 基础化工    801050: 有色金属
# 801080: 电子        801110: 家用电器    801120: 食品饮料
# 801130: 纺织服饰    801140: 轻工制造    801150: 医药生物
# 801160: 公用事业    801170: 交通运输    801180: 房地产
# 801200: 商贸零售    801210: 社会服务    801710: 建筑材料
# 801730: 电力设备    801750: 计算机      801760: 传媒
# 801770: 通信        801780: 银行        801790: 非银金融
# 801880: 汽车        801890: 机械设备    801950: 煤炭
# 801960: 石油石化    801970: 环保        801980: 美容护理
# 801020: 采掘        801040: 钢铁        801100: 建筑材料
# 801230: 综合        801720: 建筑装饰    801740: 国防军工
# --


def example_get_industry_stocks_basic():
    """示例1: 获取一级行业成分股列表"""
    print("=" * 60)
    print("示例1: 获取一级行业成分股列表")
    print("=" * 60)

    from akshare_data import get_industry_stocks

    industry_code = "801120"
    level = 1

    stocks = get_industry_stocks(industry_code, level)

    if not stocks:
        print(f"行业代码 {industry_code} (级别 {level}): 无数据")
        return

    print(f"行业代码: {industry_code}")
    print(f"行业级别: {level}")
    print(f"成分股数量: {len(stocks)}")
    print(f"前10只股票: {stocks[:10]}")
    print()


def example_get_industry_stocks_multiple_levels():
    """示例2: 获取不同级别的行业成分股"""
    print("=" * 60)
    print("示例2: 获取不同级别的行业成分股")
    print("=" * 60)

    from akshare_data import get_industry_stocks

    industry_code = "801080"

    for level in [1, 2, 3]:
        stocks = get_industry_stocks(industry_code, level)
        if not stocks:
            print(f"行业 {industry_code} - 级别 {level}: 无数据")
        else:
            print(f"行业 {industry_code} - 级别 {level}: {len(stocks)} 只股票")
            print(f"  前5只: {stocks[:5]}")
        print()


def example_get_industry_stocks_multiple_industries():
    """示例3: 获取多个行业的成分股"""
    print("=" * 60)
    print("示例3: 获取多个热门行业的成分股")
    print("=" * 60)

    from akshare_data import get_industry_stocks

    industries = {
        "801120": "食品饮料",
        "801080": "电子",
        "801750": "计算机",
        "801150": "医药生物",
        "801780": "银行",
    }

    for code, name in industries.items():
        stocks = get_industry_stocks(code, level=1)
        if not stocks:
            print(f"{name} ({code}): 无数据")
        else:
            print(f"{name} ({code}): {len(stocks)} 只股票")

    print()


def example_get_industry_mapping_basic():
    """示例4: 查询单只股票所属行业"""
    print("=" * 60)
    print("示例4: 查询单只股票所属行业")
    print("=" * 60)

    from akshare_data import get_industry_mapping

    symbol = "000858"
    level = 1

    industry_code = get_industry_mapping(symbol, level)

    if not industry_code:
        print(f"股票代码 {symbol}: 无数据")
        return

    print(f"股票代码: {symbol}")
    print(f"行业级别: {level}")
    print(f"所属行业代码: {industry_code}")
    print()


def example_get_industry_mapping_multiple_stocks():
    """示例5: 批量查询多只股票的行业归属"""
    print("=" * 60)
    print("示例5: 批量查询多只股票的行业归属")
    print("=" * 60)

    from akshare_data import get_industry_mapping

    stocks = [
        "000001",
        "600519",
        "000858",
        "300750",
        "000063",
        "600276",
    ]

    print(f"{'股票代码':<10} {'行业代码':<10}")
    print("-" * 25)

    for symbol in stocks:
        industry = get_industry_mapping(symbol, level=1)
        if not industry:
            print(f"{symbol:<10} (无数据)")
        else:
            print(f"{symbol:<10} {industry:<10}")

    print()


def example_get_industry_mapping_different_levels():
    """示例6: 查询不同级别的行业分类"""
    print("=" * 60)
    print("示例6: 查询不同级别的行业分类")
    print("=" * 60)

    from akshare_data import get_industry_mapping

    symbol = "000858"

    for level in [1, 2, 3]:
        industry = get_industry_mapping(symbol, level)
        if not industry:
            print(f"股票 {symbol} - 级别 {level}: 无数据")
        else:
            print(f"股票 {symbol} - 级别 {level} 行业代码: {industry}")

    print()


def example_combined_usage():
    """示例7: 组合使用 - 先查行业再查成分股"""
    print("=" * 60)
    print("示例7: 组合使用 - 查询某股票同行业的其他股票")
    print("=" * 60)

    from akshare_data import get_industry_stocks, get_industry_mapping

    target_stock = "000858"

    industry_code = get_industry_mapping(target_stock, level=1)
    if not industry_code:
        print(f"步骤1: {target_stock} 所属行业: 无数据")
        return

    print(f"步骤1: {target_stock} 所属行业代码 = {industry_code}")

    stocks = get_industry_stocks(industry_code, level=1)
    if not stocks:
        print(f"步骤2: 行业 {industry_code}: 无数据")
        return

    print(f"步骤2: 该行业共有 {len(stocks)} 只股票")
    print(f"步骤3: 前10只成分股 = {stocks[:10]}")

    if target_stock in stocks:
        print(f"步骤4: {target_stock} 确实在 {industry_code} 行业中")
    else:
        print(f"步骤4: {target_stock} 不在成分股列表中")

    print()


def example_error_handling():
    """示例8: 错误处理演示"""
    print("=" * 60)
    print("示例8: 错误处理演示")
    print("=" * 60)

    from akshare_data import get_industry_stocks, get_industry_mapping

    # 测试无效的行业代码
    invalid_industry = "999999"
    stocks = get_industry_stocks(invalid_industry, level=1)
    if not stocks:
        print(f"无效行业代码 {invalid_industry}: 无数据 (空列表)")
    else:
        print(f"无效行业代码 {invalid_industry}: {len(stocks)} 只股票")

    # 测试无效的股票代码
    invalid_stock = "999999"
    industry = get_industry_mapping(invalid_stock, level=1)
    if not industry:
        print(f"无效股票代码 {invalid_stock}: 无数据")
    else:
        print(f"无效股票代码 {invalid_stock}: 行业代码 = '{industry}'")

    # 测试空字符串
    industry = get_industry_mapping("", level=1)
    if not industry:
        print(f"空字符串股票代码: 无数据")
    else:
        print(f"空字符串股票代码: 行业代码 = '{industry}'")

    print()


if __name__ == "__main__":
    example_get_industry_stocks_basic()
    example_get_industry_stocks_multiple_levels()
    example_get_industry_stocks_multiple_industries()
    example_get_industry_mapping_basic()
    example_get_industry_mapping_multiple_stocks()
    example_get_industry_mapping_different_levels()
    example_combined_usage()
    example_error_handling()
