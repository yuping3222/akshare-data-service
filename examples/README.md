# AkShare Data Service 示例代码

本目录包含所有 API 接口的使用示例，每个示例文件都是独立可运行的。

## 快速开始

确保已安装项目依赖：

```bash
pip install -e .
```

运行示例：

```bash
python examples/example_daily.py
```

## 示例文件列表

### 基础行情数据

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_daily.py` | `get_daily()` | 股票/ETF/指数日线行情 |
| `example_minute.py` | `get_minute()` | 股票分钟线数据 |
| `example_index.py` | `get_index()` | 指数日线数据 |
| `example_etf.py` | `get_etf()` | ETF 日线数据 |
| `example_realtime.py` | `get_realtime_data()` | 实时行情 |

### 指数和行业

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_index_stocks.py` | `get_index_stocks()`, `get_index_components()` | 指数成分股 |
| `example_industry.py` | `get_industry_stocks()`, `get_industry_mapping()` | 行业成分股和映射 |

### 交易和资金

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_trading_days.py` | `get_trading_days()` | 交易日列表 |
| `example_money_flow.py` | `get_money_flow()` | 个股资金流向 |
| `example_north_money_flow.py` | `get_north_money_flow()` | 北向资金流向 |
| `example_sector_fund.py` | `get_sector_fund_flow()`, `get_main_fund_flow_rank()` | 板块资金流向 |

### 财务和证券信息

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_finance_indicator.py` | `get_finance_indicator()` | 财务指标数据 |
| `example_security_info.py` | `get_security_info()` | 证券基本信息 |
| `example_securities_list.py` | `get_securities_list()` | 证券列表 |
| `example_st_stocks.py` | `get_st_stocks()` | ST 股票列表 |
| `example_suspended_stocks.py` | `get_suspended_stocks()` | 停牌股票列表 |

### 集合竞价

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_call_auction.py` | `get_call_auction()` | 集合竞价数据 |

### 可转债

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_convertible_bond.py` | `get_convert_bond_list()`, `get_convert_bond_info()` | 可转债列表和详情 |

### 基金和期货

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_fund.py` | `get_fund_net_value()`, `get_fund_manager_info()` | 基金净值和经理信息 |
| `example_futures.py` | `get_futures_hist_data()`, `get_futures_realtime_data()`, `get_futures_main_contracts()` | 期货数据 |

### 龙虎榜和涨跌停

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_dragon_tiger.py` | `get_dragon_tiger_list()`, `get_dragon_tiger_summary()` | 龙虎榜数据 |
| `example_limit_pool.py` | `get_limit_up_pool()`, `get_limit_down_pool()` | 涨跌停池 |

### 大宗交易

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_block_deal.py` | `get_block_deal()`, `get_block_deal_summary()` | 大宗交易数据 |

### 融资融券

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_margin.py` | `get_margin_data()`, `get_margin_summary()` | 融资融券数据 |

### 宏观经济

| 文件 | 接口 | 说明 |
|------|------|------|
| `example_macro.py` | `get_lpr_rate()`, `get_pmi_index()`, `get_cpi_data()`, `get_ppi_data()`, `get_m2_supply()` | 宏观经济数据 |

## 运行所有示例

```bash
# 运行单个示例
python examples/example_daily.py

# 运行所有示例（可能需要较长时间）
for f in examples/example_*.py; do
    echo "Running $f..."
    python "$f"
    echo "---"
done
```

## 注意事项

1. 首次运行时会从 AkShare 下载数据并缓存到本地
2. 部分接口需要网络连接，请确保网络畅通
3. 某些接口可能有调用频率限制，建议不要频繁调用
4. 示例中的日期和代码可以根据需要修改
