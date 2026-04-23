# 项目概览

## 版本信息

- **当前版本**: 0.2.0
- **Python 要求**: >= 3.10

---

## 项目简介

**akshare-data-service** 是一个基于 Cache-First 策略的统一金融数据服务，整合了 Lixinger 和 AkShare 两大数据源，提供高性能、高可用的金融数据访问能力。

核心理念：**缓存优先（Cache-First）**。所有数据请求首先检查本地缓存，仅在缓存缺失或不完整时才向数据源发起网络请求。数据源对缓存一无所知，API 层完全拥有缓存策略的控制权。

---

## 核心特性

### Cache-First 策略

所有数据请求遵循"先查缓存、再补缺失、最后回源"的流程：

1. **内存缓存**：基于 `cachetools.TTLCache`，默认最多 5000 条，TTL 3600 秒
2. **Parquet 文件**：分区持久化存储，支持按 symbol/date 查询
3. **DuckDB 查询**：直接对 Parquet 文件执行 SQL，支持 WHERE/ORDER BY/LIMIT
4. **增量更新**：自动检测缺失日期区间，只拉取差量数据

完整缓存策略详见 [04-存储层](04-storage-layer.md) 和 [08-缓存策略](08-cache-strategy.md)。

### 多源备份

| 数据源 | 角色 | 说明 |
|--------|------|------|
| Lixinger | 主源 | 高质量财务、估值、股东数据，需 Token |
| AkShare | 备源 | 覆盖广泛的免费接口，配置驱动 |
| Tushare | 可选 | 需额外安装 `pip install -e ".[tushare]"` |
| JSL | 可选 | 集思录可转债数据 |

`MultiSourceRouter` 实现自动故障转移：主源不可用时自动切换到备源。`SourceHealthMonitor` 记录成功率，连续失败 5 次后熔断，5 分钟后自动恢复。

### 统一 API

两种调用方式：

- **便捷函数**：`from akshare_data import get_daily` — 模块级函数，开箱即用
- **命名空间 API**：`service.cn.stock.quote.daily(...)` — 结构化分类访问

### 离线工具

| 组件 | 说明 |
|------|------|
| BatchDownloader | 批量下载，支持并发、限速、增量/全量模式 |
| APIProber | 探测 AkShare 接口可用性，生成健康报告 |
| DataQualityChecker | 检查缓存数据质量（完整性、异常值） |
| CLI | 命令行入口：`python -m akshare_data.offline.cli` |

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                       入口层                                 │
│  __init__.py: __getattr__ 转发 → DataService 单例            │
│  导出: DataService, get_service, CacheManager,               │
│       BatchDownloader, APIProber, DataQualityChecker, Reporter│
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                      API 层 (api.py)                         │
│  薄封装/兼容导出：仅重导出 DataService 与命名空间              │
│  业务拆分：service_facade + legacy_adapter + namespace_assembly│
│  ├── 在线 DataService: 只读查询 + 缺数回填请求（异步）          │
│  ├── cached_fetch(): 只读缓存查询（不回源、不写入）             │
│  └── 命名空间类: cn / hk / us / macro                        │
│      ├── cn.stock.quote (daily/minute/realtime/call_auction) │
│      ├── cn.stock.finance (balance_sheet/income/cash/indicators)│
│      ├── cn.stock.capital (money_flow/north/block/margin)    │
│      ├── cn.stock.event (dividend/restricted_release)        │
│      ├── cn.index.quote (daily)                              │
│      ├── cn.index.meta (components)                          │
│      ├── cn.fund.quote (daily)                               │
│      ├── cn.trade_calendar()                                 │
│      ├── hk.stock.quote (daily)                              │
│      ├── us.stock.quote (daily)                              │
│      └── macro.china (interest_rate/gdp/social_financing)    │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   数据源层 (sources/)                         │
│  lixinger_source.py  ── LixingerAdapter（主源）               │
│  akshare_source.py   ── AkShareAdapter（配置驱动薄分发器）      │
│  router.py           ── MultiSourceRouter + 熔断器 + 限速     │
│  tushare_source.py   ── TushareAdapter（可选）               │
│  mock.py             ── 测试用 Mock 源                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                    存储层 (store/)                            │
│  manager.py      ── CacheManager（统一读写入口）               │
│  fetcher.py      ── CachedFetcher（带配置的缓存获取器）         │
│  memory.py       ── MemoryCache（TTL 内存缓存）               │
│  parquet.py      ── Parquet 原子写入 + 分区管理               │
│  duckdb.py       ── DuckDBEngine（SQL 查询引擎）              │
│  incremental.py  ── 增量引擎（缺失检测 + 自适应刷新）           │
│  validator.py    ── 数据验证                                  │
│  aggregator.py   ── 数据聚合器                                │
│  missing_ranges.py ── 缺失区间检测                            │
│  strategies/     ── 全量/增量存储策略                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                    核心层 (core/)                             │
│  base.py    ── DataSource 抽象基类                            │
│  config.py  ── CacheConfig / TableConfig                     │
│  config_cache.py ── 配置缓存机制                              │
│  config_dir.py   ── 配置目录发现                              │
│  schema.py  ── TableRegistry（69 张表注册）                    │
│  errors.py  ── 177 错误码 + 异常层次                          │
│  symbols.py ── 代码格式转换（jq/ak/ts/bs/6位数字）             │
│  fields.py  ── 字段映射（CN→EN，多源）                        │
│  normalize.py ── DataFrame 标准化                             │
│  options.py ── 期权相关工具                                   │
│  logging.py ── 结构化日志 + StatsCollector                    │
│  stats.py   ── 轻量指标收集                                  │
│  tokens.py  ── Token 管理                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 目录结构

```
akshare-data-service/
├── src/akshare_data/          # 92 个 Python 源文件
│   ├── __init__.py            # 入口，__getattr__ 转发到 DataService
│   ├── api.py                 # DataService + 命名空间 API（95+ 方法）
│   ├── core/                  # 15 个核心模块
│   ├── sources/               # 8 个数据源模块 + akshare/ 子包
│   ├── store/                 # 10 个存储模块 + strategies/ 子包
│   └── offline/               # 54 个离线工具文件（15 个目录）
│
├── config/                    # YAML 配置
│   ├── akshare_registry.yaml  # AkShare 接口注册表
│   ├── rate_limits.yaml       # 域名限速
│   ├── sources/               # 数据源配置
│   ├── interfaces/            # 按分类的接口定义
│   ├── download/              # 下载调度与优先级
│   ├── prober/                # 探测配置与状态
│   └── logging/               # 日志配置
│
├── cache/                     # 运行时缓存（自动生成）
│   ├── daily/                 # 日线数据
│   ├── minute/                # 分钟线数据
│   ├── meta/                  # 元数据
│   └── snapshot/              # 快照数据
│
├── tests/                     # 61 个测试文件（含 unit/integration/system）
├── examples/                  # 99 个示例脚本
├── docs/                      # 文档
├── reports/                   # 生成的报告
└── logs/                      # 访问日志
```

---

## 缓存表分类

系统预定义了 69 张缓存表，按存储层分类：

### daily 层（45张表）

| 表名 | 说明 | 分区字段 | TTL |
|------|------|----------|-----|
| stock_daily | A股日线 | date | 永久 |
| etf_daily | ETF日线 | date | 永久 |
| index_daily | 指数日线 | date | 永久 |
| finance_indicator | 财务指标 | report_date | 2160h |
| money_flow | 资金流向 | date | 永久 |
| north_flow | 北向资金 | date | 永久 |
| valuation | 估值数据 | date | 永久 |
| equity_pledge | 股权质押 | pledge_date | 2160h |
| restricted_release | 限售解禁 | release_date | 2160h |
| goodwill | 商誉数据 | report_date | 2160h |
| performance_forecast | 业绩预告 | report_date | 2160h |
| ... | 共 45 张表 | | |

### snapshot 层（快照数据）

| 表名 | 说明 | TTL |
|------|------|-----|
| spot_snapshot | 实时行情快照 | 168h |
| sector_flow_snapshot | 板块资金快照 | 168h |
| hsgt_hold_snapshot | 沪港通持仓快照 | 168h |
| hot_rank | 热门排名 | 168h |
| ... | 共 7 张表 | |

### minute 层（分钟线数据）

| 表名 | 说明 | 分区 |
|------|------|------|
| stock_minute | 股票分钟线 | 按周 |
| etf_minute | ETF分钟线 | 按周 |

### meta 层（元数据）

| 表名 | 说明 |
|------|------|
| securities | 证券列表 |
| trade_calendar | 交易日历 |
| index_weights | 指数权重 |
| industry_list / concept_list | 行业/概念列表 |
| company_info | 公司信息 |
| macro_data | 宏观数据 |
| ... | 共 15 张表 |

完整 Schema 参考 [07-Schema 注册表](docs/07-schema-registry.md)。

---

## 数据源优先级

默认数据源优先级：

1. **lixinger** — Lixinger OpenAPI（需 Token）
2. **akshare** — AkShare 开源库（免费）
3. **tushare** — Tushare（可选，需 Token）

当主源不可用时，`MultiSourceRouter` 自动尝试下一个源。连续失败 5 次后熔断，5 分钟后恢复。

---

## 依赖项

### 核心依赖

| 依赖 | 最低版本 | 说明 |
|------|----------|------|
| pandas | >=2.0 | 数据处理 |
| duckdb | >=0.9 | SQL 查询引擎 |
| pyarrow | >=14.0 | Parquet 读写 |
| cachetools | >=5.3 | 内存缓存 |
| akshare | >=1.10 | 数据源 |
| pyyaml | >=6.0 | YAML 解析 |

### 可选依赖

```bash
pip install -e ".[tushare]"    # Tushare 支持
pip install -e ".[baostock]"   # BaoStock 支持
```

---

## 设计原则

1. **关注点分离**：数据源不感知缓存，API 层负责缓存策略编排
2. **配置驱动**：AkShare 接口通过 YAML 配置定义，无需修改代码即可扩展
3. **原子写入**：Parquet 先写 `.tmp` 再 `rename`，防止写入中断
4. **线程安全**：共享状态使用锁保护，DuckDB 使用线程局部连接
5. **向后兼容**：保留 `get_xxx()` 便捷函数和 jk2bt 兼容别名
6. **可观测性**：结构化 JSON 日志 + 指标收集 + 访问日志
