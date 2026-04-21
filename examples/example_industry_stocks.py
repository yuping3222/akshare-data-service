"""
行业成分股 (Industry Stocks) 接口示例

演示如何使用 DataService 获取行业成分股数据。

接口说明:
    get_industry_stocks(industry_code, level)

    参数:
        industry_code: 行业代码 (如 "851921", "801010")
        level: 行业级别，默认 1
               - 1: 一级行业
               - 2: 二级行业
               - 3: 三级行业

    返回:
        List[str]，该行业下的所有股票代码列表

使用方式:
    from akshare_data import get_service
    service = get_service()
    stocks = service.get_industry_stocks("801010", level=1)

行业分类说明:
- 行业分类通常基于申万行业分类标准
- 一级行业: 如银行、非银金融、房地产、医药生物等
- 二级行业: 如银行下的国有大型银行、股份制银行等
- 三级行业: 更细分的行业分类

常见一级行业代码示例:
- 801010: 农林牧渔
- 801020: 基础化工
- 801030: 钢铁
- 801040: 有色金属
- 801050: 建筑材料
- 801080: 电子
- 801120: 食品饮料
- 801150: 医药生物
- 801180: 房地产
- 801190: 非银金融
- 801780: 银行
- 801880: 汽车

注意:
- 行业代码可能因数据源不同而有差异
- 建议使用 get_sw_industry_list() 获取最新的行业代码列表
- 返回结果可能因数据源不同而略有差异
"""

from akshare_data import get_service


# ============================================================
# 示例 1: 基本用法 - 获取一级行业成分股
# ============================================================
def example_basic():
    """基本用法: 获取食品饮料行业成分股"""
    print("=" * 60)
    print("示例 1: 基本用法 - 获取食品饮料行业成分股")
    print("=" * 60)

    service = get_service()

    try:
        # 获取食品饮料行业（一级行业）的成分股
        stocks = service.get_industry_stocks(
            industry_code="801120",
            level=1,
        )

        if not stocks:
            print("未获取到行业成分股数据")
            return

        print(f"食品饮料行业 (801120) 成分股数量: {len(stocks)} 只")
        print(f"\n前20只成分股:")
        for i, code in enumerate(stocks[:20], 1):
            print(f"  {i:2d}. {code}")

        if len(stocks) > 20:
            print(f"  ... 及其他 {len(stocks) - 20} 只股票")

    except Exception as e:
        print(f"获取行业成分股失败: {e}")


# ============================================================
# 示例 2: 获取不同行业的成分股
# ============================================================
def example_multiple_industries():
    """获取多个行业的成分股进行对比"""
    print("\n" + "=" * 60)
    print("示例 2: 多个行业成分股对比")
    print("=" * 60)

    service = get_service()

    # 常见一级行业代码
    industries = [
        ("801120", "食品饮料", 1),
        ("801150", "医药生物", 1),
        ("801780", "银行", 1),
        ("801190", "非银金融", 1),
        ("801880", "汽车", 1),
        ("801080", "电子", 1),
        ("801180", "房地产", 1),
    ]

    results = {}

    for code, name, level in industries:
        try:
            stocks = service.get_industry_stocks(
                industry_code=code,
                level=level,
            )
            results[code] = {
                "name": name,
                "count": len(stocks),
                "stocks": stocks[:5],  # 只保存前5个示例
            }
            print(f"\n{name} ({code}): {len(stocks)} 只成分股")
            print(f"  示例: {', '.join(stocks[:5])}")

        except Exception as e:
            print(f"\n{name} ({code}): 获取失败 - {e}")
            results[code] = {"name": name, "count": 0, "stocks": []}

    # 汇总
    print("\n" + "-" * 60)
    print("行业成分股数量汇总:")
    print("-" * 60)
    for code, info in results.items():
        print(f"  {info['name']:10s} ({code}): {info['count']:3d} 只")


# ============================================================
# 示例 3: 获取不同级别的行业成分股
# ============================================================
def example_different_levels():
    """对比同一行业不同级别的成分股"""
    print("\n" + "=" * 60)
    print("示例 3: 不同级别行业成分股对比")
    print("=" * 60)

    service = get_service()

    # 测试不同级别的行业
    test_cases = [
        ("801120", 1, "一级行业"),
        ("801121", 2, "二级行业"),
        ("801122", 3, "三级行业"),
    ]

    for code, level, level_name in test_cases:
        try:
            stocks = service.get_industry_stocks(
                industry_code=code,
                level=level,
            )

            if stocks:
                print(f"\n{level_name} ({code}, level={level}): {len(stocks)} 只")
                print(f"  前10只: {', '.join(stocks[:10])}")
            else:
                print(f"\n{level_name} ({code}, level={level}): 无数据")

        except Exception as e:
            print(f"\n{level_name} ({code}, level={level}): 获取失败 - {e}")


# ============================================================
# 示例 4: 获取行业代码列表
# ============================================================
def example_get_industry_list():
    """先获取行业列表，再获取成分股"""
    print("\n" + "=" * 60)
    print("示例 4: 获取行业列表并遍历")
    print("=" * 60)

    service = get_service()

    try:
        # 获取申万一级行业列表
        df = service.get_sw_industry_list()

        if df.empty:
            print("未获取到行业列表数据")
            return

        print(f"申万行业分类列表 ({len(df)} 个行业)")
        print(f"字段: {list(df.columns)}")

        # 显示前10个行业
        print("\n前10个一级行业:")
        for i, row in df.head(10).iterrows():
            print(f"  {i+1:2d}. {row.to_dict()}")

        # 获取第一个行业的成分股
        if len(df) > 0 and "industry_code" in df.columns:
            first_code = df.iloc[0]["industry_code"]
            first_name = df.iloc[0].get("industry_name", "未知")
            print(f"\n获取第一个行业 '{first_name}' ({first_code}) 的成分股:")

            stocks = service.get_industry_stocks(first_code, level=1)
            if stocks:
                print(f"  成分股数量: {len(stocks)}")
                print(f"  前10只: {', '.join(stocks[:10])}")
            else:
                print("  无数据")

    except Exception as e:
        print(f"获取行业列表失败: {e}")


# ============================================================
# 示例 5: 检查股票是否属于某个行业
# ============================================================
def example_check_membership():
    """检查特定股票是否属于某个行业"""
    print("\n" + "=" * 60)
    print("示例 5: 检查股票行业归属")
    print("=" * 60)

    service = get_service()

    # 要检查的股票
    check_stocks = [
        ("600519", "贵州茅台"),
        ("000001", "平安银行"),
        ("600036", "招商银行"),
        ("000002", "万科A"),
        ("300750", "宁德时代"),
    ]

    # 要检查的行业
    industries = [
        ("801120", "食品饮料"),
        ("801780", "银行"),
        ("801180", "房地产"),
        ("801880", "汽车"),
    ]

    for stock_code, stock_name in check_stocks:
        print(f"\n检查股票: {stock_name} ({stock_code})")

        found = False
        for ind_code, ind_name in industries:
            try:
                stocks = service.get_industry_stocks(ind_code, level=1)
                if stock_code in stocks:
                    print(f"  ✓ 属于 '{ind_name}' 行业")
                    found = True
                    break
            except Exception:
                continue

        if not found:
            print(f"  ✗ 不属于上述行业")


# ============================================================
# 示例 6: 统计各行业成分股数量
# ============================================================
def example_industry_statistics():
    """统计各行业成分股数量分布"""
    print("\n" + "=" * 60)
    print("示例 6: 行业成分股数量统计")
    print("=" * 60)

    service = get_service()

    # 更多一级行业代码
    industries = [
        ("801010", "农林牧渔"),
        ("801020", "基础化工"),
        ("801030", "钢铁"),
        ("801040", "有色金属"),
        ("801050", "建筑材料"),
        ("801080", "电子"),
        ("801110", "家用电器"),
        ("801120", "食品饮料"),
        ("801130", "纺织服饰"),
        ("801140", "轻工制造"),
        ("801150", "医药生物"),
        ("801160", "公用事业"),
        ("801170", "交通运输"),
        ("801180", "房地产"),
        ("801190", "非银金融"),
        ("801200", "商贸零售"),
        ("801210", "社会服务"),
        ("801230", "综合"),
        ("801710", "建筑材料"),
        ("801720", "建筑装饰"),
        ("801730", "电力设备"),
        ("801740", "国防军工"),
        ("801750", "计算机"),
        ("801760", "传媒"),
        ("801770", "通信"),
        ("801780", "银行"),
        ("801790", "房地产"),
        ("801880", "汽车"),
        ("801890", "机械设备"),
    ]

    results = []

    for code, name in industries:
        try:
            stocks = service.get_industry_stocks(code, level=1)
            results.append((name, code, len(stocks)))
        except Exception as e:
            results.append((name, code, 0))

    # 按成分股数量排序
    results.sort(key=lambda x: x[2], reverse=True)

    print("各行业成分股数量排名 (前20):")
    print("-" * 60)

    for i, (name, code, count) in enumerate(results[:20], 1):
        bar = "█" * (count // 5)  # 简单的条形图
        print(f"  {i:2d}. {name:10s} ({code}): {count:3d} 只 {bar}")

    # 统计信息
    total_stocks = sum(count for _, _, count in results)
    avg_stocks = total_stocks / len(results) if results else 0

    print("\n统计信息:")
    print(f"  行业总数: {len(results)}")
    print(f"  成分股总数: {total_stocks}")
    print(f"  平均每行业: {avg_stocks:.1f} 只")


# ============================================================
# 示例 7: 通过命名空间调用
# ============================================================
def example_namespace_call():
    """使用不同方式调用行业成分股接口"""
    print("\n" + "=" * 60)
    print("示例 7: 不同调用方式对比")
    print("=" * 60)

    service = get_service()
    industry_code = "801120"
    level = 1

    # 方式1: DataService.get_industry_stocks()
    print("\n方式1: service.get_industry_stocks()")
    try:
        stocks1 = service.get_industry_stocks(industry_code, level=level)
        print(f"  结果: {len(stocks1)} 只成分股")
    except Exception as e:
        print(f"  失败: {e}")

    # 方式2: 通过 akshare adapter 直接调用
    print("\n方式2: service.akshare.get_industry_stocks()")
    try:
        stocks2 = service.akshare.get_industry_stocks(industry_code, level=level)
        print(f"  结果: {len(stocks2)} 只成分股")
    except Exception as e:
        print(f"  失败: {e}")

    # 方式3: 通过 lixinger adapter
    print("\n方式3: service.lixinger.get_industry_stocks()")
    try:
        stocks3 = service.lixinger.get_industry_stocks(industry_code, level=level)
        print(f"  结果: {len(stocks3)} 只成分股")
    except Exception as e:
        print(f"  失败: {e}")


# ============================================================
# 示例 8: 错误处理
# ============================================================
def example_error_handling():
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("示例 8: 错误处理")
    print("=" * 60)

    service = get_service()

    test_cases = [
        ("无效行业代码", "invalid_code", 1),
        ("不存在的级别", "801120", 99),
        ("空行业代码", "", 1),
    ]

    for case_name, code, level in test_cases:
        print(f"\n测试: {case_name} (code={code}, level={level})")
        try:
            stocks = service.get_industry_stocks(code, level=level)
            if stocks:
                print(f"  结果: 获取到 {len(stocks)} 只成分股")
            else:
                print(f"  结果: 返回空列表")
        except Exception as e:
            print(f"  捕获异常: {type(e).__name__}: {e}")


# ============================================================
# 示例 9: 组合使用 - 获取行业行情
# ============================================================
def example_combine_with_quotes():
    """组合使用：获取行业成分股后获取行情数据"""
    print("\n" + "=" * 60)
    print("示例 9: 获取行业成分股并获取行情")
    print("=" * 60)

    service = get_service()

    try:
        # 获取银行行业成分股
        stocks = service.get_industry_stocks("801780", level=1)

        if not stocks:
            print("未获取到行业成分股")
            return

        print(f"银行行业成分股: {len(stocks)} 只")
        print(f"前5只: {', '.join(stocks[:5])}")

        # 获取第一只股票的日线数据
        if len(stocks) > 0:
            first_stock = stocks[0]
            print(f"\n获取第一只股票 {first_stock} 的日线数据:")

            df = service.get_daily(
                symbol=first_stock,
                start_date="2024-06-01",
                end_date="2024-06-30",
            )

            if df.empty:
                print("  无数据")
            else:
                print(f"  数据形状: {df.shape}")
                print(f"  字段: {list(df.columns)}")
                print(f"\n  前3行:")
                print(df.head(3))

    except Exception as e:
        print(f"操作失败: {e}")


# ============================================================
# 示例 10: 缓存效果演示
# ============================================================
def example_caching():
    """演示缓存效果"""
    print("\n" + "=" * 60)
    print("示例 10: 缓存效果演示")
    print("=" * 60)

    import time
    service = get_service()

    industry_code = "801120"
    level = 1

    # 第一次调用
    print("\n第一次调用（从数据源获取）:")
    start = time.time()
    stocks1 = service.get_industry_stocks(industry_code, level=level)
    elapsed1 = time.time() - start
    print(f"  耗时: {elapsed1:.4f} 秒，获取 {len(stocks1)} 只成分股")

    # 第二次调用（应从缓存读取）
    print("\n第二次调用（应从缓存读取）:")
    start = time.time()
    stocks2 = service.get_industry_stocks(industry_code, level=level)
    elapsed2 = time.time() - start
    print(f"  耗时: {elapsed2:.4f} 秒，获取 {len(stocks2)} 只成分股")

    if elapsed2 < elapsed1 and elapsed1 > 0:
        speedup = elapsed1 / elapsed2
        print(f"\n缓存加速比: {speedup:.1f}x")
    else:
        print("\n两次调用耗时相近")


if __name__ == "__main__":
    example_basic()
    example_multiple_industries()
    example_different_levels()
    example_get_industry_list()
    example_check_membership()
    example_industry_statistics()
    example_namespace_call()
    example_error_handling()
    example_combine_with_quotes()
    example_caching()
