# Schema Registry 完整参考

本文档提供 `core/schema.py` 中注册的所有 69 张表结构的完整参考。

## 概述

Schema Registry 是 akshare-data-service 缓存系统的核心组件，定义了所有缓存表的结构，包括：
- 列名与数据类型（Parquet 格式）
- 主键定义
- 分区策略
- TTL（过期时间）
- 优先级（P0-P3）
- 存储层（daily/minute/meta/snapshot）

## 统计摘要

### 按存储层分布

| 存储层 | 表数量 | 说明 |
|--------|--------|------|
| daily | 45 | 日线级别数据，按日期或其他时间字段分区 |
| meta | 15 | 元数据表，通常不分区或无过期时间 |
| snapshot | 7 | 快照数据，TTL 较短（168小时） |
| minute | 2 | 分钟级高频数据，按周分区 |

### 按优先级分布

| 优先级 | 表数量 | 说明 |
|--------|--------|------|
| P0 | 14 | 核心数据，最高优先级 |
| P1 | 24 | 重要数据 |
| P2 | 29 | 一般数据 |
| P3 | 2 | 低优先级数据 |

### TTL 分布

| TTL | 表数量 | 说明 |
|-----|--------|------|
| 0（永不过期） | 28 | 历史数据永久保留 |
| 24 小时（1天） | 1 | 停牌数据 |
| 168 小时（7天） | 10 | 快照数据、ST 数据、龙虎榜 |
| 720 小时（30天） | 21 | 月度过期 |
| 2160 小时（90天） | 9 | 季度过期 |

---

## 表结构详细参考

### 1. STOCK_DAILY (stock_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date, adjust |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |
| adjust | string | 复权类型 |

---

### 2. ETF_DAILY (etf_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | ETF 代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 3. INDEX_DAILY (index_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 指数代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 4. FUTURES_DAILY (futures_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 期货合约代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| open_interest | float64 | 持仓量 |

---

### 5. CONVERSION_BOND_DAILY (conversion_bond_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 可转债代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 6. INDEX_COMPONENTS (index_components)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | index_code, date, symbol |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| index_code | string | 指数代码 |
| date | date | 日期 |
| symbol | string | 成分股代码 |
| weight | float64 | 权重 |

---

### 7. INDEX_WEIGHTS (index_weights)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | index_code, stock_code, update_date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| index_code | string | 指数代码 |
| stock_code | string | 股票代码 |
| weight | float64 | 权重 |
| update_date | date | 更新日期 |
| update_time | timestamp | 更新时间 |

---

### 8. FINANCE_INDICATOR (finance_indicator)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, report_date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| pe | float64 | 市盈率 |
| pb | float64 | 市净率 |
| ps | float64 | 市销率 |
| roe | float64 | 净资产收益率 |
| net_profit | float64 | 净利润 |
| revenue | float64 | 营业收入 |

---

### 9. MONEY_FLOW (money_flow)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| main_net_inflow | float64 | 主力净流入 |
| super_large_net_inflow | float64 | 超大单净流入 |
| large_net_inflow | float64 | 大单净流入 |
| medium_net_inflow | float64 | 中单净流入 |
| small_net_inflow | float64 | 小单净流入 |

---

### 10. NORTH_FLOW (north_flow)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 交易日期 |
| net_flow | float64 | 净流入 |
| buy_amount | float64 | 买入金额 |
| sell_amount | float64 | 卖出金额 |

---

### 11. INDUSTRY_COMPONENTS (industry_components)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | industry_code, date, symbol |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| industry_code | string | 行业代码 |
| date | date | 日期 |
| symbol | string | 股票代码 |
| industry_name | string | 行业名称 |

---

### 12. HOLDER (holder)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, report_date, holder_name |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| holder_name | string | 股东名称 |
| hold_count | float64 | 持股数量 |
| hold_ratio | float64 | 持股比例 |
| holder_type | string | 股东类型 |

---

### 13. DIVIDEND (dividend)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announce_date |
| TTL | 0（永不过期） |
| 主键 | symbol, announce_date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announce_date | date | 公告日期 |
| dividend_cash | float64 | 现金分红 |
| dividend_stock | float64 | 股票分红 |
| record_date | date | 股权登记日 |
| ex_date | date | 除权除息日 |

---

### 14. VALUATION (valuation)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| pe | float64 | 市盈率 |
| pb | float64 | 市净率 |
| ps | float64 | 市销率 |
| market_cap | float64 | 总市值 |
| circulating_cap | float64 | 流通市值 |

---

### 15. UNLOCK (unlock)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announce_date |
| TTL | 0（永不过期） |
| 主键 | symbol, announce_date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announce_date | date | 公告日期 |
| unlock_date | date | 解禁日期 |
| unlock_count | float64 | 解禁数量 |
| unlock_ratio | float64 | 解禁比例 |
| unlock_type | string | 解禁类型 |

---

### 16. SPOT_SNAPSHOT (spot_snapshot)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | symbol, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| price | float64 | 价格 |
| change_pct | float64 | 涨跌幅 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |
| turnover_rate | float64 | 换手率 |
| pe | float64 | 市盈率 |
| pb | float64 | 市净率 |
| market_cap | float64 | 总市值 |

---

### 17. SECTOR_FLOW_SNAPSHOT (sector_flow_snapshot)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | date, sector_name, sector_type |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 交易日期 |
| sector_name | string | 板块名称 |
| sector_type | string | 板块类型 |
| change_pct | float64 | 涨跌幅 |
| net_inflow | float64 | 净流入 |
| stock_count | int64 | 股票数量 |

---

### 18. HSGT_HOLD_SNAPSHOT (hsgt_hold_snapshot)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | symbol, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| hold_count | float64 | 持股数量 |
| hold_ratio | float64 | 持股比例 |
| change_count | float64 | 变动数量 |

---

### 19. STOCK_MINUTE (stock_minute)

| 属性 | 值 |
|------|-----|
| 存储层 | minute |
| 分区字段 | week |
| TTL | 0（永不过期） |
| 主键 | symbol, datetime, period, adjust |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 50 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| datetime | timestamp | 时间戳 |
| period | string | 周期（1m/5m/15m/30m/60m） |
| adjust | string | 复权类型 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 20. ETF_MINUTE (etf_minute)

| 属性 | 值 |
|------|-----|
| 存储层 | minute |
| 分区字段 | week |
| TTL | 0（永不过期） |
| 主键 | symbol, datetime, period |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 50 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | ETF 代码 |
| datetime | timestamp | 时间戳 |
| period | string | 周期（1m/5m/15m/30m/60m） |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 21. SECURITIES (securities)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 0（永不过期） |
| 主键 | symbol |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 证券代码 |
| name | string | 证券名称 |
| type | string | 证券类型 |
| list_date | date | 上市日期 |
| delist_date | date | 退市日期 |
| exchange | string | 交易所 |

---

### 22. TRADE_CALENDAR (trade_calendar)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 0（永不过期） |
| 主键 | date |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 日期 |
| is_trading_day | bool | 是否交易日 |

---

### 23. INDUSTRY_LIST (industry_list)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | industry_code |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| industry_code | string | 行业代码 |
| industry_name | string | 行业名称 |
| source | string | 来源 |

---

### 24. CONCEPT_LIST (concept_list)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | concept_code |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| concept_code | string | 概念代码 |
| concept_name | string | 概念名称 |
| source | string | 来源 |

---

### 25. COMPANY_INFO (company_info)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | symbol |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| name | string | 公司名称 |
| industry | string | 所属行业 |
| area | string | 所属地区 |
| list_date | date | 上市日期 |
| market | string | 所属市场 |

---

### 26. FACTOR_CACHE (factor_cache)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | factor_name |
| TTL | 0（永不过期） |
| 主键 | factor_name, symbol, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 10 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| factor_name | string | 因子名称 |
| symbol | string | 股票代码 |
| date | date | 日期 |
| value | float64 | 因子值 |

---

### 27. FINANCIAL_REPORT (financial_report)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, report_date, report_type, item_name |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| report_type | string | 报告类型 |
| item_name | string | 项目名称 |
| item_value | float64 | 项目值 |

---

### 28. FINANCIAL_BENEFIT (financial_benefit)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, report_date, indicator |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| indicator | string | 指标名称 |
| value | float64 | 指标值 |

---

### 29. INDUSTRY_MAPPING (industry_mapping)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | symbol |
| 优先级 | P1 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| industry_code | string | 行业代码 |
| industry_name | string | 行业名称 |
| level | int64 | 行业层级 |

---

### 30. CONCEPT_COMPONENTS (concept_components)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | concept_code, date, symbol |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| concept_code | string | 概念代码 |
| concept_name | string | 概念名称 |
| date | date | 日期 |
| symbol | string | 股票代码 |

---

### 31. FUND_PORTFOLIO (fund_portfolio)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | fund_code, report_date, symbol |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| fund_code | string | 基金代码 |
| report_date | date | 报告日期 |
| symbol | string | 股票代码 |
| hold_count | float64 | 持股数量 |
| hold_ratio | float64 | 持股比例 |
| market_value | float64 | 市值 |

---

### 32. SHARE_CHANGE (share_change)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announce_date |
| TTL | 0（永不过期） |
| 主键 | symbol, announce_date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announce_date | date | 公告日期 |
| total_shares | float64 | 总股本 |
| circulating_shares | float64 | 流通股本 |
| change_type | string | 变动类型 |

---

### 33. HOLDING_CHANGE (holding_change)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announce_date |
| TTL | 0（永不过期） |
| 主键 | symbol, announce_date, holder_name |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announce_date | date | 公告日期 |
| holder_name | string | 股东名称 |
| change_count | float64 | 变动数量 |
| change_ratio | float64 | 变动比例 |
| change_type | string | 变动类型 |

---

### 34. MACRO_DATA (macro_data)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | indicator, date |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| indicator | string | 指标名称 |
| date | date | 日期 |
| value | float64 | 指标值 |
| change_pct | float64 | 变动百分比 |

---

### 35. MARGIN_DETAIL (margin_detail)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | market, date, symbol |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| market | string | 市场 |
| date | date | 交易日期 |
| symbol | string | 股票代码 |
| margin_balance | float64 | 融资余额 |
| short_balance | float64 | 融券余额 |

---

### 36. MARGIN_UNDERLYING (margin_underlying)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | market, date, symbol |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| market | string | 市场 |
| date | date | 交易日期 |
| symbol | string | 股票代码 |
| stock_name | string | 股票名称 |

---

### 37. STATUS_CHANGE (status_change)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | symbol, status_date |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| status_date | date | 状态变更日期 |
| status_type | string | 状态类型 |
| reason | string | 原因 |

---

### 38. OPTION_DAILY (option_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P3 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 期权合约代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| open_interest | float64 | 持仓量 |

---

### 39. CALL_AUCTION (call_auction)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, datetime |
| 优先级 | P3 |
| 聚合 enabled | true |
| 压缩阈值 | 50 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| datetime | timestamp | 时间戳 |
| price | float64 | 价格 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 40. EQUITY_PLEDGE (equity_pledge)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | pledge_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, pledge_date, shareholder_name |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| pledge_date | date | 质押日期 |
| shareholder_name | string | 股东名称 |
| pledge_shares | float64 | 质押股数 |
| pledge_ratio | float64 | 质押比例 |
| pledgee | string | 质权人 |
| start_date | date | 起始日期 |
| end_date | date | 结束日期 |

---

### 41. RESTRICTED_RELEASE (restricted_release)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | release_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, release_date, shareholder_name |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| release_date | date | 解禁日期 |
| release_shares | float64 | 解禁股数 |
| release_value | float64 | 解禁市值 |
| release_type | string | 解禁类型 |
| shareholder_name | string | 股东名称 |

---

### 42. GOODWILL (goodwill)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, report_date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| goodwill_balance | float64 | 商誉余额 |
| goodwill_impairment | float64 | 商誉减值 |
| net_assets | float64 | 净资产 |
| goodwill_ratio | float64 | 商誉占比 |

---

### 43. REPURCHASE (repurchase)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announcement_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, announcement_date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announcement_date | date | 公告日期 |
| progress | string | 进度 |
| amount | float64 | 金额 |
| quantity | float64 | 数量 |
| price_range | string | 价格区间 |

---

### 44. INSIDER_TRADE (insider_trade)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | transaction_date |
| TTL | 720 小时（30天） |
| 主键 | symbol, transaction_date, name |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| transaction_date | date | 交易日期 |
| name | string | 姓名 |
| title | string | 职务 |
| transaction_shares | float64 | 交易股数 |
| transaction_price | float64 | 交易价格 |
| transaction_value | float64 | 交易金额 |
| relationship | string | 与董监高关系 |

---

### 45. ESG_RATING (esg_rating)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | rating_date |
| TTL | 720 小时（30天） |
| 主键 | symbol, rating_date, rating_agency |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| rating_date | date | 评级日期 |
| esg_score | float64 | ESG 总分 |
| e_score | float64 | 环境得分 |
| s_score | float64 | 社会得分 |
| g_score | float64 | 治理得分 |
| rating_agency | string | 评级机构 |

---

### 46. PERFORMANCE_FORECAST (performance_forecast)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 2160 小时（90天） |
| 主键 | symbol, report_date, forecast_type |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| forecast_type | string | 预测类型 |
| net_profit_min | float64 | 净利润下限 |
| net_profit_max | float64 | 净利润上限 |
| change_pct_min | float64 | 变动百分比下限 |
| change_pct_max | float64 | 变动百分比上限 |

---

### 47. ANALYST_RANK (analyst_rank)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | analyst_name, date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| analyst_name | string | 分析师姓名 |
| broker_name | string | 券商名称 |
| date | date | 日期 |
| rank | int64 | 排名 |
| score | float64 | 得分 |

---

### 48. RESEARCH_REPORT (research_report)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | report_date |
| TTL | 720 小时（30天） |
| 主键 | symbol, report_date, title |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| report_date | date | 报告日期 |
| title | string | 报告标题 |
| analyst | string | 分析师 |
| broker | string | 券商 |
| rating | string | 评级 |
| target_price | float64 | 目标价 |

---

### 49. CHIP_DISTRIBUTION (chip_distribution)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | symbol, date, price |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 日期 |
| price | float64 | 价格 |
| volume | float64 | 成交量 |
| ratio | float64 | 占比 |

---

### 50. STOCK_BONUS (stock_bonus)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announce_date |
| TTL | 0（永不过期） |
| 主键 | symbol, announce_date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announce_date | date | 公告日期 |
| cash_dividend | float64 | 现金分红 |
| stock_dividend | float64 | 股票分红 |
| capitalization_reserve | float64 | 资本公积转增 |
| record_date | date | 股权登记日 |
| ex_date | date | 除权除息日 |
| pay_date | date | 派息日 |

---

### 51. RIGHTS_ISSUE (rights_issue)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | announce_date |
| TTL | 0（永不过期） |
| 主键 | symbol, announce_date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 5 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| announce_date | date | 公告日期 |
| rights_price | float64 | 配股价格 |
| rights_ratio | float64 | 配股比例 |
| actual_raise | float64 | 实际募资金额 |

---

### 52. COMPANY_MANAGEMENT (company_management)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | symbol, name |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| name | string | 姓名 |
| title | string | 职务 |
| age | int64 | 年龄 |
| education | string | 学历 |
| hold_shares | float64 | 持股数量 |

---

### 53. NAME_HISTORY (name_history)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 0（永不过期） |
| 主键 | symbol, change_date |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| old_name | string | 旧名称 |
| new_name | string | 新名称 |
| change_date | date | 变更日期 |

---

### 54. SHIBOR_RATE (shibor_rate)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 日期 |
| on | float64 | 隔夜利率 |
| 1w | float64 | 1周利率 |
| 2w | float64 | 2周利率 |
| 1m | float64 | 1个月利率 |
| 3m | float64 | 3个月利率 |
| 6m | float64 | 6个月利率 |
| 9m | float64 | 9个月利率 |
| 1y | float64 | 1年利率 |

---

### 55. SOCIAL_FINANCING (social_financing)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 日期 |
| total_amount | float64 | 社会融资总额 |
| yoy_change | float64 | 同比变动 |

---

### 56. MACRO_GDP (macro_gdp)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 日期 |
| gdp | float64 | GDP 总量 |
| gdp_yoy | float64 | GDP 同比增速 |
| primary_industry | float64 | 第一产业增加值 |
| secondary_industry | float64 | 第二产业增加值 |
| tertiary_industry | float64 | 第三产业增加值 |

---

### 57. MACRO_EXCHANGE_RATE (macro_exchange_rate)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | date, currency_pair |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | date | 日期 |
| currency_pair | string | 货币对 |
| rate | float64 | 汇率 |

---

### 58. FOF_FUND (fof_fund)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | fund_code |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| fund_code | string | 基金代码 |
| fund_name | string | 基金名称 |
| fund_type | string | 基金类型 |
| nav_date | date | 净值日期 |
| nav | float64 | 净值 |

---

### 59. LOF_FUND (lof_fund)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 720 小时（30天） |
| 主键 | fund_code |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| fund_code | string | 基金代码 |
| fund_name | string | 基金名称 |
| nav_date | date | 净值日期 |
| nav | float64 | 净值 |
| market_price | float64 | 市场价格 |
| premium_rate | float64 | 溢价率 |

---

### 60. CONVERT_BOND_PREMIUM (convert_bond_premium)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | bond_code, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| bond_code | string | 转债代码 |
| bond_name | string | 转债名称 |
| stock_code | string | 正股代码 |
| bond_price | float64 | 转债价格 |
| conversion_value | float64 | 转股价值 |
| premium_rate | float64 | 溢价率 |
| date | date | 日期 |

---

### 61. INDUSTRY_PERFORMANCE (industry_performance)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | industry_code, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| industry_code | string | 行业代码 |
| industry_name | string | 行业名称 |
| date | date | 日期 |
| change_pct | float64 | 涨跌幅 |
| turnover | float64 | 成交额 |
| stock_count | int64 | 股票数量 |

---

### 62. CONCEPT_PERFORMANCE (concept_performance)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | concept_code, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| concept_code | string | 概念代码 |
| concept_name | string | 概念名称 |
| date | date | 日期 |
| change_pct | float64 | 涨跌幅 |
| turnover | float64 | 成交额 |
| stock_count | int64 | 股票数量 |

---

### 63. NORTHBOUND_HOLDINGS (northbound_holdings)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 0（永不过期） |
| 主键 | symbol, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| hold_count | float64 | 持股数量 |
| hold_ratio | float64 | 持股比例 |
| net_buy | float64 | 净买入 |

---

### 64. BLOCK_DEAL (block_deal)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 720 小时（30天） |
| 主键 | symbol, date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| date | date | 交易日期 |
| deal_price | float64 | 成交价格 |
| deal_volume | float64 | 成交量 |
| deal_amount | float64 | 成交金额 |
| buyer | string | 买方营业部 |
| seller | string | 卖方营业部 |
| premium_ratio | float64 | 溢价率 |

---

### 65. DRAGON_TIGER_LIST (dragon_tiger_list)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | symbol, date |
| 优先级 | P1 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| name | string | 股票名称 |
| date | date | 交易日期 |
| change_pct | float64 | 涨跌幅 |
| turnover | float64 | 换手率 |
| reason | string | 上榜原因 |
| net_buy | float64 | 净买入 |
| buy_amount | float64 | 买入金额 |
| sell_amount | float64 | 卖出金额 |

---

### 66. SUSPENDED_STOCKS (suspended_stocks)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 24 小时（1天） |
| 主键 | symbol |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| name | string | 股票名称 |
| suspend_date | date | 停牌日期 |
| reason | string | 停牌原因 |

---

### 67. ST_STOCKS (st_stocks)

| 属性 | 值 |
|------|-----|
| 存储层 | meta |
| 分区字段 | 无 |
| TTL | 168 小时（7天） |
| 主键 | symbol |
| 优先级 | P2 |
| 聚合 enabled | false |
| 压缩阈值 | 0 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| symbol | string | 股票代码 |
| name | string | 股票名称 |
| st_type | string | ST 类型 |
| st_date | date | ST 日期 |

---

### 68. SW_INDUSTRY_DAILY (sw_industry_daily)

| 属性 | 值 |
|------|-----|
| 存储层 | daily |
| 分区字段 | index_code |
| TTL | 0（永不过期） |
| 主键 | index_code, date |
| 优先级 | P2 |
| 聚合 enabled | true |
| 压缩阈值 | 20 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| index_code | string | 申万行业指数代码 |
| date | date | 交易日期 |
| open | float64 | 开盘价 |
| high | float64 | 最高价 |
| low | float64 | 最低价 |
| close | float64 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

### 69. HOT_RANK (hot_rank)

| 属性 | 值 |
|------|-----|
| 存储层 | snapshot |
| 分区字段 | date |
| TTL | 168 小时（7天） |
| 主键 | rank, date |
| 优先级 | P0 |
| 聚合 enabled | true |
| 压缩阈值 | 1 |

| 字段名 | 类型 | 说明 |
|--------|------|------|
| rank | int64 | 排名 |
| symbol | string | 股票代码 |
| name | string | 股票名称 |
| price | float64 | 价格 |
| pct_change | float64 | 涨跌幅 |
| date | date | 日期 |

---

## Schema Registry API

### 核心类

#### CacheTable

不可变的表结构定义数据类。

```python
@dataclass(frozen=True)
class CacheTable:
    name: str                    # 表名（唯一标识）
    partition_by: str | None     # 分区字段名，或 None
    ttl_hours: int               # 过期时间（小时），0 表示永不过期
    schema: dict[str, str]       # 列名到 Parquet 数据类型的映射
    primary_key: list[str]       # 唯一标识一行的列
    aggregation_enabled: bool    # 是否启用聚合存储（默认 True）
    compaction_threshold: int    # 触发压缩的文件数阈值（默认 20）
    priority: str                # 优先级层级（P0-P3，默认 P0）
    storage_layer: str           # 存储层（daily/meta/snapshot/minute，默认 daily）
```

#### TableInfo

表的物理状态运行时元数据。

```python
@dataclass
class TableInfo:
    name: str                    # 表名
    file_count: int              # 磁盘上的 Parquet 文件数量
    total_size_bytes: int        # 所有 Parquet 文件的总大小
    last_updated: datetime | None # 最近写入时间戳
    partition_count: int         # 不同分区值的数量
    priority: str                # 优先级层级
```

#### TableRegistry

表结构注册表，提供查找、列表和过滤操作。

```python
class TableRegistry:
    def register(self, table: CacheTable) -> None
    def get(self, name: str) -> CacheTable
    def get_or_none(self, name: str) -> CacheTable | None
    def list_all(self) -> dict[str, CacheTable]
    def list_by_priority(self, priority: str) -> list[CacheTable]
    def list_by_layer(self, layer: str) -> list[CacheTable]
    def has(self, name: str) -> bool
```

### 模块级函数

#### get_table_schema(name: str) -> CacheTable | None

按名称查找表结构。

```python
from akshare_data.core.schema import get_table_schema

schema = get_table_schema("stock_daily")
if schema:
    print(schema.schema)  # 打印列定义
    print(schema.primary_key)  # 打印主键
```

#### list_tables() -> list[str]

返回所有已注册表名的排序列表。

```python
from akshare_data.core.schema import list_tables

all_tables = list_tables()
print(f"共注册了 {len(all_tables)} 张表")
```

#### init_schemas() -> None

将所有默认表结构注册到全局注册表。幂等操作，可安全多次调用。

```python
from akshare_data.core.schema import init_schemas

init_schemas()  # 注册所有默认表
```

---

## 如何添加新表

### 步骤 1：定义 CacheTable

在 `core/schema.py` 中添加新的 `CacheTable` 定义：

```python
NEW_TABLE = CacheTable(
    name="new_table",                    # 唯一表名
    partition_by="date",                 # 分区字段（或 None）
    ttl_hours=720,                       # TTL（小时），0=永不过期
    schema={                             # 列定义
        "symbol": "string",
        "date": "date",
        "value": "float64",
    },
    primary_key=["symbol", "date"],      # 主键
    aggregation_enabled=True,            # 是否启用聚合（可选，默认 True）
    compaction_threshold=20,             # 压缩阈值（可选，默认 20）
    priority="P1",                       # 优先级（可选，默认 P0）
    storage_layer="daily",               # 存储层（可选，默认 daily）
)
```

### 步骤 2：注册到 _DEFAULT_TABLES

将新表添加到 `_DEFAULT_TABLES` 元组中：

```python
_DEFAULT_TABLES = (
    STOCK_DAILY,
    # ... 其他表 ...
    NEW_TABLE,  # 添加到这里
)
```

### 步骤 3：选择合适的数据类型

支持的 Parquet 数据类型：

| 类型 | 说明 | 示例 |
|------|------|------|
| string | 字符串 | 股票代码、名称 |
| date | 日期 | 交易日期、报告日期 |
| timestamp | 时间戳 | 分钟级数据的 datetime |
| float64 | 64位浮点数 | 价格、金额、比例 |
| int64 | 64位整数 | 排名、数量、层级 |
| bool | 布尔值 | 是否交易日 |

### 步骤 4：选择存储层

| 存储层 | 用途 | 典型 TTL |
|--------|------|----------|
| daily | 日线数据、财务数据、资金流等 | 0 / 720h / 2160h |
| minute | 分钟级高频数据 | 0（永久保留） |
| meta | 元数据（证券列表、行业列表等） | 0 / 720h |
| snapshot | 快照数据（盘口、热度等） | 168h（7天） |

### 步骤 5：选择优先级

| 优先级 | 含义 | 示例 |
|--------|------|------|
| P0 | 核心数据，最高优先级 | 日线行情、财务报表、快照 |
| P1 | 重要数据 | 资金流、估值、股东数据 |
| P2 | 一般数据 | 元数据、分钟数据、ESG |
| P3 | 低优先级数据 | 期权、集合竞价 |

### 步骤 6：设置分区策略

- **按 date 分区**：适用于大多数日线数据
- **按 report_date 分区**：适用于财务/报告类数据
- **按 week 分区**：适用于分钟级高频数据
- **按 factor_name 分区**：适用于因子缓存
- **不分区 (None)**：适用于元数据表

### 步骤 7：设置压缩阈值

| 场景 | 推荐值 | 说明 |
|------|--------|------|
| 元数据表 | 0 | 禁用压缩（aggregation_enabled=False） |
| 快照数据 | 1 | 每次写入后立即压缩 |
| 日线数据 | 20 | 累积 20 个文件后压缩 |
| 分钟数据 | 50 | 高频数据，累积更多文件后压缩 |
| 财务数据 | 5 | 数据量较小，较早压缩 |

---

## 附录：完整表名索引

| 序号 | 常量名 | 表名 | 存储层 | 分区字段 | TTL(h) | 优先级 |
|------|--------|------|--------|----------|--------|--------|
| 1 | STOCK_DAILY | stock_daily | daily | date | 0 | P0 |
| 2 | ETF_DAILY | etf_daily | daily | date | 0 | P0 |
| 3 | INDEX_DAILY | index_daily | daily | date | 0 | P0 |
| 4 | FUTURES_DAILY | futures_daily | daily | date | 0 | P0 |
| 5 | CONVERSION_BOND_DAILY | conversion_bond_daily | daily | date | 0 | P0 |
| 6 | INDEX_COMPONENTS | index_components | daily | date | 720 | P0 |
| 7 | INDEX_WEIGHTS | index_weights | meta | - | 720 | P0 |
| 8 | FINANCE_INDICATOR | finance_indicator | daily | report_date | 2160 | P0 |
| 9 | MONEY_FLOW | money_flow | daily | date | 0 | P1 |
| 10 | NORTH_FLOW | north_flow | daily | date | 0 | P1 |
| 11 | INDUSTRY_COMPONENTS | industry_components | daily | date | 720 | P1 |
| 12 | HOLDER | holder | daily | report_date | 2160 | P1 |
| 13 | DIVIDEND | dividend | daily | announce_date | 0 | P1 |
| 14 | VALUATION | valuation | daily | date | 0 | P1 |
| 15 | UNLOCK | unlock | daily | announce_date | 0 | P1 |
| 16 | SPOT_SNAPSHOT | spot_snapshot | snapshot | date | 168 | P0 |
| 17 | SECTOR_FLOW_SNAPSHOT | sector_flow_snapshot | snapshot | date | 168 | P0 |
| 18 | HSGT_HOLD_SNAPSHOT | hsgt_hold_snapshot | snapshot | date | 168 | P0 |
| 19 | STOCK_MINUTE | stock_minute | minute | week | 0 | P2 |
| 20 | ETF_MINUTE | etf_minute | minute | week | 0 | P2 |
| 21 | SECURITIES | securities | meta | - | 0 | P2 |
| 22 | TRADE_CALENDAR | trade_calendar | meta | - | 0 | P2 |
| 23 | INDUSTRY_LIST | industry_list | meta | - | 720 | P2 |
| 24 | CONCEPT_LIST | concept_list | meta | - | 720 | P2 |
| 25 | COMPANY_INFO | company_info | meta | - | 720 | P2 |
| 26 | FACTOR_CACHE | factor_cache | daily | factor_name | 0 | P1 |
| 27 | FINANCIAL_REPORT | financial_report | daily | report_date | 2160 | P0 |
| 28 | FINANCIAL_BENEFIT | financial_benefit | daily | report_date | 2160 | P0 |
| 29 | INDUSTRY_MAPPING | industry_mapping | meta | - | 720 | P1 |
| 30 | CONCEPT_COMPONENTS | concept_components | daily | date | 720 | P1 |
| 31 | FUND_PORTFOLIO | fund_portfolio | daily | report_date | 2160 | P1 |
| 32 | SHARE_CHANGE | share_change | daily | announce_date | 0 | P2 |
| 33 | HOLDING_CHANGE | holding_change | daily | announce_date | 0 | P2 |
| 34 | MACRO_DATA | macro_data | meta | - | 720 | P2 |
| 35 | MARGIN_DETAIL | margin_detail | daily | date | 720 | P2 |
| 36 | MARGIN_UNDERLYING | margin_underlying | daily | date | 720 | P2 |
| 37 | STATUS_CHANGE | status_change | meta | - | 720 | P2 |
| 38 | OPTION_DAILY | option_daily | daily | date | 0 | P3 |
| 39 | CALL_AUCTION | call_auction | daily | date | 0 | P3 |
| 40 | EQUITY_PLEDGE | equity_pledge | daily | pledge_date | 2160 | P1 |
| 41 | RESTRICTED_RELEASE | restricted_release | daily | release_date | 2160 | P1 |
| 42 | GOODWILL | goodwill | daily | report_date | 2160 | P1 |
| 43 | REPURCHASE | repurchase | daily | announcement_date | 2160 | P1 |
| 44 | INSIDER_TRADE | insider_trade | daily | transaction_date | 720 | P1 |
| 45 | ESG_RATING | esg_rating | daily | rating_date | 720 | P2 |
| 46 | PERFORMANCE_FORECAST | performance_forecast | daily | report_date | 2160 | P1 |
| 47 | ANALYST_RANK | analyst_rank | daily | date | 720 | P2 |
| 48 | RESEARCH_REPORT | research_report | daily | report_date | 720 | P2 |
| 49 | CHIP_DISTRIBUTION | chip_distribution | daily | date | 720 | P2 |
| 50 | STOCK_BONUS | stock_bonus | daily | announce_date | 0 | P1 |
| 51 | RIGHTS_ISSUE | rights_issue | daily | announce_date | 0 | P1 |
| 52 | COMPANY_MANAGEMENT | company_management | meta | - | 720 | P2 |
| 53 | NAME_HISTORY | name_history | meta | - | 0 | P2 |
| 54 | SHIBOR_RATE | shibor_rate | daily | date | 0 | P2 |
| 55 | SOCIAL_FINANCING | social_financing | daily | date | 0 | P2 |
| 56 | MACRO_GDP | macro_gdp | daily | date | 0 | P2 |
| 57 | MACRO_EXCHANGE_RATE | macro_exchange_rate | daily | date | 0 | P2 |
| 58 | FOF_FUND | fof_fund | meta | - | 720 | P2 |
| 59 | LOF_FUND | lof_fund | meta | - | 720 | P2 |
| 60 | CONVERT_BOND_PREMIUM | convert_bond_premium | snapshot | date | 168 | P1 |
| 61 | INDUSTRY_PERFORMANCE | industry_performance | snapshot | date | 168 | P1 |
| 62 | CONCEPT_PERFORMANCE | concept_performance | snapshot | date | 168 | P1 |
| 63 | NORTHBOUND_HOLDINGS | northbound_holdings | daily | date | 0 | P1 |
| 64 | BLOCK_DEAL | block_deal | daily | date | 720 | P2 |
| 65 | DRAGON_TIGER_LIST | dragon_tiger_list | daily | date | 168 | P1 |
| 66 | SUSPENDED_STOCKS | suspended_stocks | meta | - | 24 | P2 |
| 67 | ST_STOCKS | st_stocks | meta | - | 168 | P2 |
| 68 | SW_INDUSTRY_DAILY | sw_industry_daily | daily | index_code | 0 | P2 |
| 69 | HOT_RANK | hot_rank | snapshot | date | 168 | P0 |
