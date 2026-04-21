# 数据源架构

本文档描述 akshare-data-service 的数据源层，包括抽象基类、Lixinger 适配器、AkShare 适配器、多源路由与熔断机制。

---

## 1. DataSource 抽象基类

**位置**: `src/akshare_data/core/base.py`

`DataSource` 是所有数据源的抽象基类（ABC），定义了统一的数据访问接口。

### 1.1 类属性

| 属性 | 类型 | 默认值 |
|------|------|--------|
| `name` | `str` | `"abstract"` |
| `source_type` | `str` | `"abstract"` |

### 1.2 必须实现的抽象方法（`@abstractmethod`）

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `get_daily_data` | `symbol, start_date, end_date, adjust` | `pd.DataFrame` | 日线行情 |
| `get_index_components` | `index_code, include_weights` | `pd.DataFrame` | 指数成分股详情 |
| `get_trading_days` | `start_date, end_date` | `List[str]` | 交易日历 |
| `get_securities_list` | `security_type, date` | `pd.DataFrame` | 证券列表 |
| `get_security_info` | `symbol` | `Dict[str, Any]` | 证券信息 |
| `get_minute_data` | `symbol, freq, start_date, end_date` | `pd.DataFrame` | 分钟线 |
| `get_money_flow` | `symbol, start_date, end_date` | `pd.DataFrame` | 个股资金流 |
| `get_north_money_flow` | `start_date, end_date` | `pd.DataFrame` | 北向资金 |
| `get_industry_stocks` | `industry_code, level` | `List[str]` | 行业成分股 |
| `get_industry_mapping` | `symbol, level` | `str` | 股票所属行业 |
| `get_finance_indicator` | `symbol, fields, start_date, end_date` | `pd.DataFrame` | 财务指标 |
| `get_call_auction` | `symbol, date` | `pd.DataFrame` | 集合竞价 |

### 1.3 可选方法（带默认实现）

以下方法提供了默认实现，子类可选择性重写：

| 方法 | 默认实现 |
|------|----------|
| `get_etf_daily` | 委托 `get_daily_data(adjust="none")` |
| `get_index_daily` | 委托 `get_daily_data(adjust="none")` |
| `get_lof_daily` | 委托 `get_daily_data(adjust="none")` |
| `health_check` | 调用 `get_trading_days()` 测试连通性 |
| `get_source_info` | 返回 `{"name", "type", "description"}` |

### 1.4 扩展方法（默认 `raise NotImplementedError`）

约 80+ 个扩展方法默认抛出 `NotImplementedError`，按需实现，涵盖：ST/停牌、估值、融资融券、宏观、股东、财务报表、分红/解禁、行情扩展、行业/概念、ETF/LOF/可转债/期货/期权、龙虎榜、基金、债券、北向/沪深港通等。

---

## 2. LixingerAdapter（理杏仁数据源）

**位置**: `src/akshare_data/sources/lixinger_source.py`

LixingerAdapter 是理杏仁 OpenAPI 的数据源适配器，作为**主要数据源**使用。

### 2.1 基本信息

| 属性 | 值 |
|------|-----|
| `name` | `"lixinger"` |
| `source_type` | `"partial"` |

### 2.2 初始化与客户端

```python
def __init__(self, token: Optional[str] = None, **kwargs)
```

- `client` 属性懒加载 `LixingerClient` 单例
- `_token` 优先使用传入值，否则通过 `_get_token("lixinger")` 从 TokenManager 解析
- `_ensure_configured()` 检查 token 是否已配置

### 2.3 代码/日期格式化

| 方法 | 说明 |
|------|------|
| `_format_stock_code(symbol)` | 统一转为 6 位数字代码（调用 `format_stock_symbol`） |
| `_format_index_code(index_code)` | 同上，用于指数 |
| `_normalize_date(dt)` | `str`/`date`/`datetime` → `"YYYY-MM-DD"` |
| `_normalize_daily_df(df, symbol)` | 标准化日线 DataFrame（中英文列名映射） |

### 2.4 已实现的核心方法

#### 行情数据

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_daily_data` | `client.get_company_candlestick()` | 个股日线 |
| `get_index_daily` | `client.get_index_candlestick()` | 指数日线 |
| `get_etf_daily` | `client.get_fund_candlestick()` | ETF 日线 |
| `get_etf_hist_data` | 同 `get_etf_daily` | ETF 历史行情 |

#### 指数/行业/成分

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_index_stocks` | `client.get_index_constituents()` | 指数成分股列表 |
| `get_index_components` | `client.get_index_constituent_weightings()` | 指数成分（含权重） |
| `get_index_weights` | `client.get_index_constituent_weightings()` | 指数权重 |
| `get_index_weights_history` | 同上 | 指数权重历史 |
| `get_index_list` | `client.get_index_list()` | 指数列表 |
| `get_index_valuation` | `client.get_index_fundamental()` | 指数估值 |
| `get_industry_stocks` | `client.get_industry_constituents()` | 行业成分股 |
| `get_industry_mapping` | `client.get_company_industries()` | 股票行业映射 |
| `get_industry_list` | `client.get_industry_list()` | 行业列表 |
| `get_all_industries` | 同 `get_industry_list` | 所有行业 |

#### 证券列表/信息

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_securities_list` | `client.get_company_list()` / `get_index_list()` / `get_fund_list()` | 按类型获取 |
| `get_security_info` | `client.get_company_profile()` | 证券信息 |
| `get_basic_info` | 同 `get_security_info` | 基本信息 |
| `get_etf_list` | `client.get_fund_list()` | ETF 列表 |
| `get_lof_list` | 同 `get_etf_list` | LOF 列表 |

#### 财务数据

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_finance_indicator` | `client.get_stock_financial()` | 财务指标（PE/PB/PS/股息率） |
| `get_balance_sheet` | `client.get_company_fs_non_financial()` | 资产负债表 |
| `get_income_statement` | 同 `get_balance_sheet` | 利润表 |
| `get_cash_flow` | 同 `get_balance_sheet` | 现金流量表 |
| `get_financial_metrics` | `client.get_stock_financial()` | 财务综合指标 |
| `get_stock_valuation` | `client.get_stock_financial()` | 个股估值 |
| `get_stock_pe_pb` | 同 `get_stock_valuation` | PE/PB |

#### 股东数据

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_shareholder_changes` | `client.get_company_equity_change()` | 股本变动 |
| `get_top_shareholders` | `client.get_company_majority_shareholders()` | 前十大股东 |
| `get_institution_holdings` | `client.get_company_fund_shareholders()` | 机构持股 |
| `get_latest_holder_number` | `client.get_company_shareholders_num()` | 股东人数 |
| `get_topholder_change` | `client.get_company_major_shareholders_shares_change()` | 大股东增减持 |
| `get_major_holder_trade` | `client.get_company_senior_executive_shares_change()` | 高管交易 |

#### 公司事件

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_dividend_data` | `client.get_company_dividend()` | 分红数据 |
| `get_dividend_by_date` | 同 `get_dividend_data` | 按日期的分红 |
| `get_stock_bonus` | 同 `get_dividend_data` | 分红送转 |
| `get_equity_pledge` | `client.get_company_pledge()` | 股权质押 |
| `get_restricted_release` | `client.get_company_restricted_release()` | 限售解禁 |
| `get_restricted_release_calendar` | 同 `get_restricted_release` | 解禁日历 |
| `get_rights_issue` | `client.get_company_allotment()` | 配股 |
| `get_capital_change` | 同 `get_shareholder_changes` | 资本变动 |
| `get_name_history` | `client.get_company_indices()` | 更名历史 |
| `get_management_info` | `client.get_company_profile()` | 管理层信息 |
| `get_disclosure_news` | `client.get_company_announcement()` | 信息披露 |

#### 资金/交易

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_block_deal` | `client.get_company_block_deal()` | 大宗交易 |
| `get_margin_data` | `client.get_company_margin_trading()` | 融资融券 |

#### 宏观数据

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_lpr_rate` | `client.get_macro_interest_rates()` | LPR 利率 |
| `get_shibor_rate` | `client.get_macro_interest_rates()` | Shibor 利率 |
| `get_social_financing` | `client.get_macro_social_financing()` | 社会融资 |
| `get_pmi_index` | `client.get_macro_pmi()` | PMI 指数 |
| `get_cpi_data` | `client.get_macro_cpi()` | CPI |
| `get_ppi_data` | `client.get_macro_ppi()` | PPI |
| `get_m2_supply` | `client.get_macro_money_supply()` | M2 供应量 |

#### 海外市场

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_hk_stocks` | `client.get_hk_company_list()` | 港股列表 |
| `get_us_stocks` | `client.get_us_index_list()` | 美股列表 |

#### 新股/IPO

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_new_stocks` | `client.get_company_list()` | 新股列表 |
| `get_ipo_info` | 同 `get_new_stocks` | IPO 信息 |

#### 基金/LOF/FOF

| 方法 | 底层 Lixinger API | 说明 |
|------|-------------------|------|
| `get_fund_net_value` | `client.get_fund_net_value()` | 基金净值 |
| `get_fund_manager_info` | `client.get_fund_manager()` | 基金经理 |
| `get_lof_nav` | `client.get_fund_net_value()` | LOF 净值 |
| `get_lof_spot` | 同 `get_lof_nav` | LOF 现货 |
| `get_fof_list` | `client.get_fund_list()` | FOF 列表 |
| `get_fof_nav` | `client.get_fund_net_value()` | FOF 净值 |

#### 可转债

| 方法 | 说明 |
|------|------|
| `get_convert_bond_list` | 可转债列表 |
| `get_convert_bond_daily` | 转债日线 |
| `get_convert_bond_info` | 转债信息 |
| `get_convert_bond_spot` | 转债现货 |
| `get_convert_bond_premium` | 转债溢价率 |

#### 其他已实现方法

`get_performance_forecast`、`get_performance_express`、`get_analyst_rank`、`get_research_report`、`get_chip_distribution`、`get_esg_rating`、`get_goodwill_data`、`get_goodwill_impairment`、`get_goodwill_by_industry`、`get_equity_freeze`、`get_insider_trading`、`get_repurchase_data` 等。

### 2.5 不支持的方法（`raise NotImplementedError`）

| 方法 | 原因 |
|------|------|
| `get_minute_data` | Lixinger 不提供分钟线 |
| `get_money_flow` | Lixinger 不提供资金流向 |
| `get_north_money_flow` | Lixinger 不提供北向资金 |
| `get_call_auction` | Lixinger 不提供集合竞价 |
| `get_st_stocks` / `get_suspended_stocks` | Lixinger 不提供 |
| `get_realtime_data` | Lixinger 不提供实时行情 |
| `get_dragon_tiger_list` / `get_limit_up_pool` / `get_limit_down_pool` | Lixinger 不提供 |
| `get_concept_list` / `get_concept_stocks` | Lixinger 不提供概念板块 |

### 2.6 健康检查与信息

```python
def get_source_info(self) -> Dict[str, Any]:
    return {
        "name": "lixinger",
        "type": "partial",
        "description": "Lixinger OpenAPI 数据源",
        "token_configured": self.client.is_configured(),
    }
```

---

## 3. LixingerClient

**位置**: `src/akshare_data/sources/lixinger_client.py`

LixingerClient 是理杏仁 OpenAPI 的 HTTP 客户端，使用单例模式。

### 3.1 核心特性

- **单例**: `LixingerClient.__new__()` 保证全局唯一实例
- **Token 管理**: 通过 `TokenManager` 解析（环境变量 `LIXINGER_TOKEN` 或 `token.cfg` 文件）
- **重试机制**: `urllib3.Retry(total=3, backoff_factor=1)`，对 429/500/502/503/504 自动重试
- **结构化日志**: 每次请求记录 `log_type=api_request`、`provider=lixinger`、`duration_ms`、`status`

### 3.2 主要方法

```python
def query_api(api_suffix: str, params: dict, timeout: int = 30) -> dict
```

POST 请求到 `https://open.lixinger.com/api/{suffix}`，自动注入 token。

### 3.3 API 方法分类

| 分类 | 方法数量 | 示例 |
|------|----------|------|
| Index（指数） | 11 | `get_index_candlestick`, `get_index_constituents`, `get_index_fundamental` |
| Company（公司） | 21 | `get_company_candlestick`, `get_company_dividend`, `get_company_profile` |
| Shareholder（股东） | 7 | `get_company_majority_shareholders`, `get_company_shareholders_num` |
| Trading（交易） | 5 | `get_company_block_deal`, `get_company_margin_trading`, `get_company_pledge` |
| Industry（行业） | 4 | `get_industry_list`, `get_industry_constituents` |
| Fund（基金） | 12 | `get_fund_candlestick`, `get_fund_net_value`, `get_fund_profile` |
| Fund Manager | 4 | `get_fund_manager_list`, `get_fund_manager_profit_ratio` |
| Macro（宏观） | 14 | `get_macro_gdp`, `get_macro_cpi`, `get_macro_interest_rates` |
| HK（港股） | 7 | `get_hk_company_list`, `get_hk_company_candlestick` |
| US（美股） | 4 | `get_us_index_list`, `get_us_index_candlestick` |
| Stock Fundamental | 1 | `get_stock_financial` |

---

## 4. AkShareAdapter（AkShare 数据源）

**位置**: `src/akshare_data/sources/akshare_source.py`

AkShareAdapter 是 AkShare 库的数据源适配器，作为**次要数据源**使用。采用**配置驱动的薄分发器**设计模式。

### 4.1 基本信息

| 属性 | 值 |
|------|-----|
| `name` | `"akshare"` |
| `source_type` | `"real"` |
| `DEFAULT_DATA_SOURCES` | `["sina", "east_money", "tushare", "baostock"]` |

### 4.2 初始化参数

```python
def __init__(
    self,
    use_cache: bool = True,
    cache_ttl_hours: int = 24,
    offline_mode: bool = False,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    data_sources: List[str] = None,
)
```

- 自动检测 `akshare`、`scipy`、`numpy` 是否可用
- 注入全局 `StatsCollector` 用于指标收集

### 4.3 动态路由机制（`__getattr__`）

核心设计：不再编写 100+ 个重复方法，而是通过 `__getattr__` 动态拦截所有未定义的 `get_xxx()` 调用：

1. 从 `_METHOD_TO_INTERFACE` 查找接口名
2. 如果未找到，尝试去掉 `get_` 前缀再查找
3. 返回一个 dispatcher 函数，调用 `fetch(interface_name, akshare=self._akshare, **kwargs)`

> 完整的方法名到接口名映射详见 [12-配置参考](12-configuration-reference.md) 中的 akshare_registry.yaml 配置。

### 4.4 方法名 → 接口名映射

完整的方法名到接口名映射约 100+ 项，覆盖行情、期货/期权/可转债、宏观、财务、资金流向、龙虎榜、公司事件、评级/预测、行业/概念、基金、海外等所有分类。

关键映射示例：

| 方法名 | 接口名 |
|--------|--------|
| `get_daily_data` | `equity_daily` |
| `get_minute_data` | `equity_minute` |
| `get_north_money_flow` | `north_money_flow` |
| `get_trading_days` | `tool_trade_date_hist_sina` |
| `get_index_stocks` / `get_index_components` | `index_components` |
| `get_finance_indicator` | `finance_indicator` |

### 4.5 fetch() 函数

**位置**: `src/akshare_data/sources/akshare/fetcher.py`

`fetch(interface_name, akshare=None, **kwargs) → pd.DataFrame`

工作流程：
1. 从 `config/akshare_registry.yaml` 加载接口定义（带缓存）
2. 按优先级遍历接口定义中的所有数据源
3. 对每个数据源：通过 `input_mapping` 映射参数 → 调用 akshare 函数 → 通过 `output_mapping` 重命名列 → 通过 `column_types` 转换列类型
4. 所有数据源都失败时抛出 `SourceUnavailableError`

> fetch 返回的数据最终通过存储层写入缓存，详见 [04-存储层](04-storage-layer.md)。缓存表的 Schema 定义详见 [07-Schema 注册表](07-schema-registry.md)。

### 4.6 计算方法（显式定义）

以下方法不属于配置驱动，在 AkShareAdapter 中显式定义：

| 方法 | 说明 | 依赖 |
|------|------|------|
| `get_option_greeks` | 计算期权 Greeks | scipy, numpy |
| `calculate_option_implied_vol` | 计算隐含波动率（Brent 求根法） | scipy, numpy |
| `black_scholes_price` | BS 期权定价模型 | scipy, numpy |
| `calculate_conversion_value` | 计算转债转换价值和溢价率 | numpy |

---

## 5. MultiSourceRouter（多源路由器）

**位置**: `src/akshare_data/sources/router.py`

### 5.1 组件概览

| 组件 | 说明 |
|------|------|
| `ExecutionResult` | 执行结果数据类（success/data/source/error/attempts/error_details/is_empty/is_fallback/sources_tried） |
| `EmptyDataPolicy` | 空数据处理策略：`STRICT` / `RELAXED` / `BEST_EFFORT` |
| `DomainRateLimiter` | 域名级限流器，从 YAML 配置加载 |
| `SourceHealthMonitor` | 源健康监控（熔断器） |
| `MultiSourceRouter` | 多源路由执行器 |
| `create_simple_router()` | 便捷工厂函数 |

### 5.2 SourceHealthMonitor（熔断器）

```python
class SourceHealthMonitor:
    _ERROR_THRESHOLD = 5      # 连续错误阈值
    _DISABLE_DURATION = 300   # 禁用时长（秒）
```

工作原理：
- 每次请求后调用 `record_result(source, success, error)`
- 连续失败 5 次后，标记该源为不可用
- 5 分钟（300 秒）后自动恢复，重置错误计数
- `is_available(source)` 检查源是否可用

### 5.3 DomainRateLimiter（域名限流）

从 `config/sources/domains.yaml` 和 `config/rate_limits.yaml` 加载配置：

```python
# domains.yaml 示例
domains:
  eastmoney_push2his:
    url_pattern: "push2his.eastmoney.com"
    rate_limit_key: "em_push2his"

# rate_limits.yaml 示例
em_push2his:
  interval: 0.5    # 最小间隔 0.5 秒
default:
  interval: 0.3
```

`wait_if_needed(domain)` 根据上次请求时间自动等待。

### 5.4 MultiSourceRouter.execute() 故障转移逻辑

```python
def execute(self, *args, **kwargs) -> ExecutionResult:
```

执行流程：

1. **遍历所有提供者**（按注册顺序）
2. **检查健康状态**：跳过被熔断器标记为不可用的源
3. **尝试调用**：记录开始时间 → 调用 `func(*args, **kwargs)` → 记录健康状态
4. **结果验证**：
   - 空数据：记录并继续尝试下一个源
   - 验证失败（缺少必要列/行数不足）：继续尝试下一个源
   - 有效数据：立即返回 `ExecutionResult(success=True, ...)`
5. **全部失败处理**：根据 `EmptyDataPolicy` 决定返回策略

---

## 6. SourceProxy 类（数据源代理）

**位置**: `src/akshare_data/api.py`

SourceProxy 是 DataService 的包装器，用于支持指定数据源的查询。

```python
class SourceProxy:
    def __init__(self, service, requested_source: Optional[Union[str, List[str]]] = None):
        self.service = service
        self.requested_source = requested_source

    def __getattr__(self, method_name: str):
        def wrapper(*args, **kwargs):
            return self.service._execute_source_method(
                method_name, self.requested_source, *args, **kwargs
            )
        return wrapper
```

通过 `__getattr__` 实现动态分发：任何对 SourceProxy 实例的方法调用都会被拦截，并转发到 `DataService._execute_source_method()`。

### 使用方式

```python
service = DataService()

# 使用默认优先级（lixinger → akshare）
df = service._get_source().get_daily_data("600000", "2024-01-01", "2024-12-31")

# 指定单个数据源
df = service._get_source("lixinger").get_daily_data("600000", "2024-01-01", "2024-12-31")

# 指定多个数据源（按顺序尝试）
df = service._get_source(["akshare", "lixinger"]).get_daily_data("600000", "2024-01-01", "2024-12-31")
```

---

## 7. 数据源优先级与能力对比

### 7.1 默认优先级

```
lixinger → akshare
```

- **lixinger**：主源，提供高质量的财务、估值、股东数据
- **akshare**：备源，覆盖广泛的免费接口

每个 API 方法都支持 `source` 参数覆盖默认优先级。

### 7.2 数据源能力对比

| 数据类型 | Lixinger | AkShare |
|----------|----------|---------|
| 日线行情 | ✅ | ✅ |
| 分钟数据 | ❌ (NotImplementedError) | ✅ |
| 指数成分 | ✅ | ✅ |
| 财务指标 | ✅ | ✅ |
| 财务报表 | ✅ | ✅ |
| 估值数据 | ✅ | ✅ |
| 股东数据 | ✅ | ✅ |
| 资金流向 | ❌ (NotImplementedError) | ✅ |
| 北向资金 | ❌ (NotImplementedError) | ✅ |
| 龙虎榜 | ❌ (NotImplementedError) | ✅ |
| 宏观数据 | ✅ | ✅ |
| 期权数据 | ❌ (NotImplementedError) | ✅ |
| 期货数据 | ❌ (NotImplementedError) | ✅ |
| 实时行情 | ❌ (NotImplementedError) | ✅ |
| 概念板块 | ❌ (NotImplementedError) | ✅ |
| 可转债 | ✅ | ✅ |

### 7.3 可选数据源

| 数据源 | 安装 | 说明 |
|--------|------|------|
| Tushare | `pip install -e ".[tushare]"` | 需 Token |

---

## 8. 数据源扩展

### 8.1 添加新数据源

1. 继承 `DataSource` 基类，实现所有抽象方法
2. 在 `DataService.__init__()` 的 `adapters` 字典中注册

```python
class MyAdapter(DataSource):
    name = "my_source"
    source_type = "real"
    # 实现所有抽象方法...
```

### 8.2 AkShare 接口扩展

AkShare 的接口定义通过 `config/akshare_registry.yaml` 配置驱动：

```yaml
interfaces:
  equity_daily:
    sources:
      - name: east_money
        func: stock_zh_a_hist
        enabled: true
        input_mapping:
          symbol: symbol
          start_date: start_date
          end_date: end_date
        output_mapping:
          日期: datetime
          开盘: open
          ...
```

添加新接口只需在配置文件中添加定义，无需修改代码。
