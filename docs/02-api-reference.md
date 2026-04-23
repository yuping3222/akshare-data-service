# API 参考

## 调用方式

两种等价用法：

```python
# 模块级函数（自动转发到 DataService 单例）
from akshare_data import get_daily
df = get_daily("000001", "2024-01-01", "2024-12-31")

# 命名空间 API
from akshare_data import DataService
service = DataService()
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31")
```

---

## 模块级函数

`__init__.py` 通过 `__getattr__` 将所有 `get_xxx` 函数调用转发到 `DataService` 单例（共 95+ 方法）。

### 行情数据

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_daily` | `(symbol, start_date, end_date, adjust="qfq", source=None)` | 股票日线 |
| `get_minute` | `(symbol, freq="1min", start_date=None, end_date=None, source=None)` | 分钟线 |
| `get_index` | `(index_code, start_date, end_date, source=None)` | 指数日线 |
| `get_etf` | `(symbol, start_date, end_date, source=None)` | ETF日线 |
| `get_realtime_data` | `(symbol=None, source=None)` | 实时行情快照 |
| `get_stock_hist` | `(symbol, period="daily", start_date=None, end_date=None, adjust="qfq", source=None)` | 原始历史行情 |
| `get_spot_em` | `(source=None)` | 东方财富实时快照 |
| `get_lof_daily` | `(symbol, start_date, end_date, source=None)` | LOF日线 |
| `get_futures_daily` | `(symbol, start_date, end_date, source=None)` | 期货日线 |
| `get_futures_spot` | `(source=None)` | 期货现货 |
| `get_option_daily` | `(symbol, start_date, end_date, source=None)` | 期权日线 |
| `get_option_list` | `(source=None)` | 期权列表 |
| `get_convert_bond_daily` | `(symbol, start_date, end_date, source=None)` | 转债日线 |
| `get_convert_bond_premium` | `(source=None)` | 转债溢价率 |
| `get_convert_bond_spot` | `(source=None)` | 转债现货 |

### 证券列表

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_securities_list` | `(security_type="stock", date=None, source=None)` | 证券列表 |
| `get_security_info` | `(symbol, source=None)` | 证券信息 |
| `get_basic_info` | `(symbol, source=None)` | 基本信息 |
| `get_trading_days` | `(start_date=None, end_date=None, source=None)` | 交易日历 |
| `get_st_stocks` | `(source=None)` | ST股票 |
| `get_suspended_stocks` | `(source=None)` | 停牌股票 |

### 指数

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_index_stocks` | `(index_code, date=None, source=None)` | 成分股代码列表 |
| `get_index_components` | `(index_code, date=None, include_weights=True, source=None)` | 成分股详情(含权重) |
| `get_index_valuation` | `(index_code, source=None)` | 指数估值 |

### 行业/概念

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_industry_stocks` | `(industry_code, date=None, level=1, source=None)` | 行业成分股 |
| `get_industry_mapping` | `(symbol, level=1, source=None)` | 所属行业 |
| `get_industry_list` | `(source=None)` | 行业列表 |
| `get_concept_list` | `(source=None)` | 概念列表 |
| `get_concept_stocks` | `(concept_code, source=None)` | 概念成分股 |
| `get_stock_concepts` | `(symbol, source=None)` | 个股所属概念 |
| `get_sw_industry_list` | `(source=None)` | 申万行业列表 |
| `get_sw_industry_daily` | `(industry_code, source=None)` | 申万行业行情 |
| `get_industry_performance` | `(source=None)` | 行业表现快照 |
| `get_concept_performance` | `(source=None)` | 概念表现快照 |
| `get_stock_industry` | `(symbol, source=None)` | 个股所属行业 |
| `get_hot_rank` | `(source=None)` | 热度排行 |

### 资金流向

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_money_flow` | `(symbol, start_date=None, end_date=None, source=None)` | 个股资金流 |
| `get_north_money_flow` | `(start_date=None, end_date=None, source=None)` | 北向资金流 |
| `get_northbound_holdings` | `(symbol, start_date=None, end_date=None, source=None)` | 北向持仓 |

### 财务数据

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_finance_indicator` | `(symbol, start_date=None, end_date=None, source=None)` | 财务指标(PE/PB等) |
| `get_balance_sheet` | `(symbol, source=None)` | 资产负债表 |
| `get_income_statement` | `(symbol, source=None)` | 利润表 |
| `get_cash_flow` | `(symbol, source=None)` | 现金流量表 |
| `get_financial_metrics` | `(symbol, source=None)` | 财务综合指标 |
| `get_stock_valuation` | `(symbol, source=None)` | 个股估值 |
| `get_goodwill_data` | `(symbol, source=None)` | 商誉数据 |
| `get_goodwill_impairment` | `(symbol, source=None)` | 商誉减值 |
| `get_goodwill_by_industry` | `(source=None)` | 商誉按行业 |

### 股东数据

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_shareholder_changes` | `(symbol, source=None)` | 股本变动 |
| `get_top_shareholders` | `(symbol, source=None)` | 十大股东 |
| `get_institution_holdings` | `(symbol, source=None)` | 机构持股 |
| `get_latest_holder_number` | `(symbol, source=None)` | 最新股东人数 |
| `get_insider_trading` | `(symbol, source=None)` | 内部人交易 |
| `get_equity_freeze` | `(symbol, source=None)` | 股权质押冻结 |
| `get_capital_change` | `(symbol, source=None)` | 资本变动 |

### 事件数据

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_dividend_data` | `(symbol, source=None)` | 分红数据 |
| `get_dividend_by_date` | `(date, source=None)` | 按日期的分红 |
| `get_restricted_release` | `(symbol, source=None)` | 限售解禁 |
| `get_restricted_release_detail` | `(start_date=None, end_date=None, source=None)` | 解禁详情 |
| `get_restricted_release_calendar` | `(source=None)` | 解禁日历 |
| `get_equity_pledge` | `(symbol, source=None)` | 股权质押 |
| `get_equity_pledge_rank` | `(source=None)` | 质押排行 |
| `get_repurchase_data` | `(symbol, source=None)` | 回购数据 |
| `get_stock_bonus` | `(symbol, source=None)` | 分红送转 |
| `get_rights_issue` | `(symbol, source=None)` | 配股 |
| `get_management_info` | `(symbol, source=None)` | 管理层信息 |
| `get_name_history` | `(symbol, source=None)` | 更名历史 |

### 评级/预测

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_esg_rating` | `(symbol, source=None)` | ESG评级 |
| `get_esg_rank` | `(source=None)` | ESG排行 |
| `get_performance_forecast` | `(symbol, source=None)` | 业绩预告 |
| `get_performance_express` | `(symbol, source=None)` | 业绩快报 |
| `get_analyst_rank` | `(source=None)` | 分析师排行 |
| `get_research_report` | `(symbol, source=None)` | 研究报告 |
| `get_chip_distribution` | `(symbol, source=None)` | 筹码分布 |
| `get_earnings_forecast` | `(symbol, source=None)` | 盈利预测 |

### 龙虎榜/大宗/融资

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_dragon_tiger_list` | `(date=None, source=None)` | 龙虎榜 |
| `get_block_deal` | `(symbol, start_date=None, end_date=None, source=None)` | 大宗交易 |
| `get_margin_data` | `(symbol, start_date=None, end_date=None, source=None)` | 融资融券 |

### 宏观数据

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_shibor_rate` | `(start_date=None, end_date=None, source=None)` | Shibor利率 |
| `get_social_financing` | `(start_date=None, end_date=None, source=None)` | 社会融资 |
| `get_macro_gdp` | `(start_date=None, end_date=None, source=None)` | GDP |
| `get_macro_exchange_rate` | `(start_date=None, end_date=None, source=None)` | 汇率 |

### 新股/IPO

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_new_stocks` | `(source=None)` | 新股列表 |
| `get_ipo_info` | `(source=None)` | IPO信息 |

### 港美股

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_hk_stocks` | `(source=None)` | 港股列表 |
| `get_us_stocks` | `(source=None)` | 美股列表 |

### 基金/FOF/LOF

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_fund_open_daily` | `(symbol, start_date, end_date, source=None)` | 开放式基金日净值 |
| `get_fund_open_nav` | `(fund_code, start_date, end_date, source=None)` | 开放式基金净值 |
| `get_fund_open_info` | `(fund_code, source=None)` | 开放式基金信息 |
| `get_fof_list` | `(source=None)` | FOF列表 |
| `get_fof_nav` | `(fund_code, source=None)` | FOF净值 |
| `get_lof_spot` | `(source=None)` | LOF现货 |
| `get_lof_nav` | `(fund_code, source=None)` | LOF净值 |

### 集合竞价

| 函数 | 签名 | 说明 |
|------|------|------|
| `get_call_auction` | `(symbol, date=None, source=None)` | 集合竞价 |

---

## 命名空间 API

### `service.cn` — 中国A股

#### `service.cn.stock.quote` — 行情

| 方法 | 参数 | 说明 |
|------|------|------|
| `daily` | `(symbol, start_date, end_date, adjust, source)` | 日线 |
| `minute` | `(symbol, freq, start_date, end_date, source)` | 分钟线 |
| `realtime` | `(symbol, source)` | 实时 |
| `call_auction` | `(symbol, date, source)` | 集合竞价 |

#### `service.cn.stock.finance` — 财务

| 方法 | 参数 | 说明 |
|------|------|------|
| `balance_sheet` | `(symbol, source)` | 资产负债表 |
| `income_statement` | `(symbol, source)` | 利润表 |
| `cash_flow` | `(symbol, source)` | 现金流量表 |
| `indicators` | `(symbol, start_date, end_date, source)` | 财务指标 |

#### `service.cn.stock.capital` — 资金

| 方法 | 参数 | 说明 |
|------|------|------|
| `money_flow` | `(symbol, start_date, end_date, source)` | 个股资金流 |
| `north` | `(start_date, end_date, source)` | 北向资金 |
| `northbound` | `(symbol, start_date, end_date, source)` | 北向持仓 |
| `block_deal` | `(symbol, start_date, end_date, source)` | 大宗交易 |
| `margin` | `(symbol, start_date, end_date, source)` | 融资融券 |
| `dragon_tiger` | `(date, source)` | 龙虎榜 |

#### `service.cn.stock.event` — 事件

| 方法 | 参数 | 说明 |
|------|------|------|
| `dividend` | `(symbol, source)` | 分红 |
| `restricted_release` | `(symbol, source)` | 解禁 |

### `service.cn.index` — 指数

#### `service.cn.index.quote`

| 方法 | 参数 | 说明 |
|------|------|------|
| `daily` | `(index_code, start_date, end_date, source)` | 指数日线 |

#### `service.cn.index.meta`

| 方法 | 参数 | 说明 |
|------|------|------|
| `components` | `(index_code, source)` | 成分股(含权重) |

### `service.cn.fund` — 基金

#### `service.cn.fund.quote`

| 方法 | 参数 | 说明 |
|------|------|------|
| `daily` | `(symbol, start_date, end_date, source)` | 基金日行情 |

### `service.macro` — 宏观经济

#### `service.macro.china`

| 方法 | 参数 | 说明 |
|------|------|------|
| `interest_rate` | `(start_date, end_date, source)` | 利率 |
| `gdp` | `(start_date, end_date, source)` | GDP |
| `social_financing` | `(start_date, end_date, source)` | 社融 |

---

## 代码格式

所有接口自动支持多种代码格式，自动标准化：

| 格式 | 示例 | 说明 |
|------|------|------|
| 纯数字 | `"600519"` | 6位数字，自动识别市场 |
| 聚宽 | `"600519.XSHG"` | 上交所 `.XSHG` / 深交所 `.XSHE` |
| AkShare | `"sh600519"` | `sh`=上交所 / `sz`=深交所 |
| Tushare | `"600519.SH"` | `.SH`=上交所 / `.SZ`=深交所 |
| BaoStock | `"sh.600519"` | 带点前缀 |

## 服务层查询契约 (Service Query Contract)

自 v0.3.0 起，服务层提供基于标准实体 schema 的查询契约，字段说明来自字段字典，不再手写散落注释。

```python
from akshare_data.service.query_contract import (
    get_contract,
    MarketQuoteDailyParams,
    QueryExecutor,
    QueryResult,
)
from akshare_data.service.error_mapper import ErrorMapper, ServiceErrorCategory
from akshare_data.service.docgen import generate_markdown, generate_all_markdown

# 查询数据集契约
contract = get_contract("market_quote_daily")
print(contract.schema.primary_key)  # ['security_id', 'trade_date', 'adjust_type']

# 构建查询参数（自动校验）
params = MarketQuoteDailyParams(
    security_id="000001",
    start_date="2024-01-01",
    end_date="2024-12-31",
    adjust_type="qfq",
    fields=["trade_date", "close_price", "volume"],
    sort_by="trade_date",
    sort_order="desc",
    limit=100,
)
params.validate(contract.schema)  # 抛出 QueryContractError 如果参数非法

# 生成文档（绑定字段字典）
md = generate_markdown("market_quote_daily")
all_md = generate_all_markdown()
```

### 错误语义

服务层清晰区分以下错误类型：

| 错误类型 | 异常类 | 说明 |
|----------|--------|------|
| 参数错误 | `QueryContractError` | 参数格式错误、字段不存在、日期范围非法 |
| 无数据 | `NoDataError` | 查询范围内确实没有数据记录 |
| 数据未发布 | `DataNotPublishedError` | 数据集尚未通过质量门禁发布到 Served 层 |
| 质量阻断 | `QualityBlockedError` | 最新批次质量门禁未通过，数据被阻断 |
| 版本不存在 | `VersionNotFoundError` | 指定的 release_version 不存在 |

```python
from akshare_data.service.query_contract import (
    DataNotPublishedError,
    QualityBlockedError,
    QueryContractError,
    VersionNotFoundError,
)
from akshare_data.service.error_mapper import ErrorMapper

try:
    ...
except QueryContractError as e:
    # 参数问题，客户端应修正请求
    ...
except DataNotPublishedError as e:
    # 数据尚未发布，不是"没数据"
    ...
except QualityBlockedError as e:
    # 质量门禁阻断，可查看 failed_rules
    print(e.failed_rules)
except VersionNotFoundError as e:
    # 请求的版本不存在
    ...
except NoDataError:
    # 数据已发布，但查询范围为空
    ...

# 统一错误映射
response = ErrorMapper.map_error(exc, dataset="market_quote_daily")
print(response.to_dict())
# {"category": "param_error", "code": "3003_INVALID_PARAMETER", ...}
```

---

## 错误处理

详见 [10-错误处理](10-error-handling.md)。所有方法支持 `source` 参数指定数据源，失败时抛出 `DataAccessException` 子类异常。

```python
from akshare_data.core.errors import DataAccessException, NoDataError

try:
    df = get_daily("000001", "2024-01-01", "2024-12-31")
except NoDataError:
    print("该日期范围内无数据")
except DataAccessException as e:
    print(f"数据获取失败: {e}")
```
