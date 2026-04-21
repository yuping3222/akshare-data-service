# 统一金融数据服务 — 架构 v3

> **原则**: 架构要简，能力要全。不砍能力，不堆层次。

---

## 一、三区架构

```
┌─────────────────────────────────────────────────┐
│                 核心区 (Core)                     │
│  数据格式 │ Schema │ 字段映射 │ 代码转换 │ 错误码  │
│  ← 在线和离线都依赖的基础 →                       │
├──────────────────┬──────────────────────────────┤
│   在线区 (Online) │     离线区 (Offline)          │
│                  │                              │
│ ┌────────────┐   │  ┌────────────────────────┐  │
│ │ DataAPI    │   │  │ 批量下载 (增量/全量)    │  │
│ │ 读缓存优先 │   │  │ 接口探测 (全量审计)    │  │
│ └─────┬──────┘   │  │ 数据质量检查           │  │
│       │ 未命中    │  │ 健康报告生成           │  │
│ ┌─────▼──────┐   │  └────────────────────────┘  │
│ │ Fetcher    │   │                              │
│ │ 4源+熔断   │   │                              │
│ └─────┬──────┘   │                              │
│       │          │                              │
│ ┌─────▼──────────▼──────────────────────────┐   │
│ │         CacheStore (Parquet + DuckDB)      │   │
│ │         在线和离线共享存储层                 │   │
│ └────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────┤
│                纵向 (Cross-cutting)               │
│  日志 │ 指标收集 │ 配置管理                       │
└─────────────────────────────────────────────────┘
```

---

## 二、全量能力保留清单

> v1 审计出的所有能力，**一个不砍**，只是换了一个更简洁的组织方式

| # | 能力 | 来源 | 放在哪里 | 行数估算 |
|---|------|------|---------|---------|
| 1 | **DataSource ABC** (40+ 方法) | jk2bt `base.py` | `core/base.py` | ~350 |
| 2 | **AkShare 适配器** (完整实现) | jk2bt `akshare.py` | `sources/akshare_source.py` | ~800 |
| 3 | **4源备份系统** (sina/em/ts/bs) | jk2bt `data_source_backup.py` | `sources/backup.py` | ~500 |
| 4 | **多源路由 + 失败切换** | one-enhanced + jk2bt | `sources/router.py` | ~300 |
| 5 | **熔断器 + 健康监控** | jk2bt `SourceHealthMonitor` | `sources/router.py` | (合并在路由内) |
| 6 | **Schema 注册表** (38+ 表) | jk2bt `registry.py` | `core/schema.py` | ~400 |
| 7 | **字段标准化** (中→英映射) | one-enhanced `field_mapping/` | `core/fields.py` | ~150 |
| 8 | **代码格式转换** (jq/ak/ts/bs互转) | jk2bt `symbol.py` | `core/symbols.py` | ~150 |
| 9 | **Parquet 原子写入** | jk2bt `AtomicWriter` | `store/parquet.py` | ~200 |
| 10 | **DuckDB 查询引擎** | one-enhanced + stock-bt | `store/duckdb.py` | ~200 |
| 11 | **内存 TTL 缓存** | one-enhanced `cachetools` | `store/memory.py` | ~80 |
| 12 | **增量缓存 (区间合并)** | stock-bt `wrapper_v2` | `store/incremental.py` | ~250 |
| 13 | **自适应刷新策略** | stock-bt `wrapper_v2` | `store/incremental.py` | (合并) |
| 14 | **小文件合并** | stock-bt `wrapper_v2` | `store/incremental.py` | (合并) |
| 15 | **错误码体系** (130+) | one-enhanced | `core/errors.py` | ~100 |
| 16 | **结构化日志** (JSON) | one-enhanced | `core/logging.py` | ~60 |
| 17 | **指标收集** | one-enhanced + jk2bt | `core/metrics.py` | ~80 |
| 18 | **健康检查 / 接口审计** | analysis_tools v3.4 | `offline/prober.py` | ~300 |
| 19 | **批量增量下载器** | stock-bt + 新建 | `offline/downloader.py` | ~250 |
| 20 | **数据质量检查** | analysis_tools | `offline/quality.py` | ~150 |
| 21 | **健康报告生成** | analysis_tools | `offline/reporter.py` | ~100 |
| 22 | **DataFrame标准化** (类型/空值/索引) | stock-bt `_normalize_dataframe_for_parquet` | `core/normalize.py` | ~80 |
| 23 | **配置管理** | 新建 | `core/config.py` | ~80 |
| 24 | **在线统一 API** | 新建 | `api.py` | ~200 |
| **合计** | | | | **~4300行** |

---

## 三、目录结构

```
akshare-data-service/                 # 独立包
├── pyproject.toml
├── README.md
│
├── src/akshare_data/
│   ├── __init__.py                   # 导出: get_daily, get_minute, DataService, ...
│   │
│   ├── core/                         # ═══ 核心区: 在线+离线共用 ═══
│   │   ├── __init__.py
│   │   ├── base.py                   # DataSource ABC (40+方法, 从 jk2bt 迁入)
│   │   ├── schema.py                 # 全量 Schema 注册表 (38+表, 从 jk2bt 迁入)
│   │   ├── fields.py                 # 字段映射 (中→英, 多源→统一)
│   │   ├── symbols.py                # 代码格式转换 (jq/ak/ts/bs/6位 互转)
│   │   ├── normalize.py              # DataFrame标准化 (类型/空值/Parquet兼容)
│   │   ├── errors.py                 # 错误码 + 异常类
│   │   ├── config.py                 # 配置 (缓存路径/源优先级/限速/Token)
│   │   ├── logging.py                # 结构化日志
│   │   └── metrics.py                # 指标收集 (请求数/命中率/耗时)
│   │
│   ├── sources/                      # ═══ 数据源: 拉取+标准化合为一体 ═══
│   │   ├── __init__.py
│   │   ├── akshare_source.py         # AkShare 完整适配器 (从 jk2bt 迁入)
│   │   │                             #   实现 DataSource ABC 全部方法
│   │   │                             #   内置字段标准化 (每个源各管各的)
│   │   ├── backup.py                 # 4源备份系统 (sina/em/tushare/baostock)
│   │   │                             #   每种数据类型的多源拉取函数
│   │   │                             #   每个源的独立标准化
│   │   └── router.py                 # 多源路由 + 熔断器 + 健康监控
│   │                                 #   连续失败熔断, 自动恢复
│   │                                 #   域名级限速, 请求间隔控制
│   │
│   ├── store/                        # ═══ 存储: 在线+离线共用 ═══
│   │   ├── __init__.py
│   │   ├── manager.py                # CacheManager 统一入口
│   │   │                             #   read() / write() / has_range()
│   │   ├── memory.py                 # 内存 TTL 缓存 (cachetools)
│   │   ├── parquet.py                # Parquet 原子写入 + 分区管理
│   │   ├── duckdb.py                 # DuckDB 查询引擎 (SQL过滤/聚合)
│   │   └── incremental.py            # 增量缓存引擎
│   │                                 #   区间缺口计算 + 只拉差量
│   │                                 #   小文件合并 + 自适应刷新
│   │
│   ├── api.py                        # ═══ 在线区入口 ═══
│   │                                 #   DataService 类: cache-first 统一API
│   │                                 #   get_daily / get_minute / get_index / ...
│   │                                 #   全部方法: 读缓存 → 增量拉取 → 返回
│   │
│   └── offline/                      # ═══ 离线区 ═══
│       ├── __init__.py
│       ├── downloader.py             # 批量增量下载器
│       │                             #   全市场下载 / 按指数成分下载
│       │                             #   增量更新 (只拉新数据)
│       │                             #   并发控制 + 域名限速
│       ├── prober.py                 # 接口探测器 (从 analysis_tools 迁入)
│       │                             #   全量 akshare 接口可用性探测
│       │                             #   并发64线程 + 域名级限流
│       │                             #   断点续跑 + 变更检测
│       ├── quality.py                # 数据质量检查
│       │                             #   完整性检查 (缺失日期/缺失字段)
│       │                             #   异常值检测 (涨跌幅/成交量)
│       │                             #   一致性检查 (跨源比对)
│       └── reporter.py               # 报告生成
│                                     #   健康报告 / 质量报告 / 覆盖率报告
│
├── data_cache/                       # 默认缓存目录 (项目下)
│   ├── daily/                        # 日线数据 (按symbol分区)
│   ├── minute/                       # 分钟数据
│   ├── snapshot/                     # 快照数据
│   └── meta/                         # 元数据 (交易日历/证券列表)
│
├── config/
│   ├── default.yaml                  # 默认配置
│   └── health_state.json             # 探测状态 (断点续跑)
│
└── tests/
    ├── test_store.py
    ├── test_fetcher.py
    ├── test_api.py
    └── test_offline.py
```

**文件数**: core(9) + sources(3) + store(5) + api(1) + offline(4) + init(4) = **26 个文件**
**代码量**: ~4300 行 (全量能力)

---

## 四、核心模块能力详情

### 4.1 `core/base.py` — DataSource ABC (完整 40+ 方法)

从 jk2bt `base.py` 迁入，不删减：

```python
class DataSource(ABC):
    name: str
    source_type: str  # "real" | "mock" | "cached"

    # ── 核心行情 (必实现) ──
    @abstractmethod
    def get_daily_data(symbol, start_date, end_date, adjust="qfq") -> DataFrame
    @abstractmethod
    def get_minute_data(symbol, freq, start_date, end_date, adjust) -> DataFrame

    # ── 指数 ──
    @abstractmethod
    def get_index_stocks(index_code) -> List[str]
    @abstractmethod
    def get_index_components(index_code, include_weights) -> DataFrame

    # ── 基础信息 ──
    @abstractmethod
    def get_trading_days(start_date, end_date) -> List[str]
    @abstractmethod
    def get_securities_list(security_type, date) -> DataFrame
    @abstractmethod
    def get_security_info(symbol) -> Dict

    # ── 资金流 ──
    @abstractmethod
    def get_money_flow(symbol, start_date, end_date) -> DataFrame
    @abstractmethod
    def get_north_money_flow(start_date, end_date) -> DataFrame

    # ── 财务 ──
    @abstractmethod
    def get_finance_indicator(symbol, fields, start_date, end_date) -> DataFrame
    @abstractmethod
    def get_valuation(symbol, date) -> DataFrame

    # ── 可选扩展 (30+, 有默认实现) ──
    def get_etf_daily(...) -> DataFrame: return self.get_daily_data(...)
    def get_spot_snapshot() -> DataFrame: raise NotImplementedError
    def get_st_stocks() -> DataFrame: raise NotImplementedError
    def get_top10_holders(...) -> DataFrame: raise NotImplementedError
    def get_margin_detail(...) -> DataFrame: raise NotImplementedError
    def get_industry_list(...) -> DataFrame: raise NotImplementedError
    def get_industry_components(...) -> DataFrame: raise NotImplementedError
    def get_concept_list() -> DataFrame: raise NotImplementedError
    def get_concept_components(...) -> DataFrame: raise NotImplementedError
    def get_dividend(...) -> DataFrame: raise NotImplementedError
    def get_balance_sheet(...) -> DataFrame: raise NotImplementedError
    def get_income_statement(...) -> DataFrame: raise NotImplementedError
    def get_cashflow_statement(...) -> DataFrame: raise NotImplementedError
    def get_futures_daily(...) -> DataFrame: raise NotImplementedError
    def get_option_daily(...) -> DataFrame: raise NotImplementedError
    def get_call_auction(...) -> DataFrame: raise NotImplementedError
    def get_macro_data(...) -> DataFrame: raise NotImplementedError
    def get_fund_portfolio(...) -> DataFrame: raise NotImplementedError
    def get_share_change(...) -> DataFrame: raise NotImplementedError
    def get_holding_change(...) -> DataFrame: raise NotImplementedError
    def get_unlock_info(...) -> DataFrame: raise NotImplementedError
    # ...

    # ── 内建健康检查 ──
    def health_check() -> Dict: ...
```

### 4.2 `core/schema.py` — 全量 38+ 表定义

从 jk2bt `registry.py` 完整迁入：

```python
@dataclass(frozen=True)
class CacheTable:
    name: str
    partition_by: str | None       # 分区键: "date" / "symbol" / "week"
    ttl_hours: int                 # 过期时间: 0=永不过期
    schema: dict[str, str]         # 列类型定义
    primary_key: list[str]         # 主键
    priority: str = "P0"           # P0核心 / P1重要 / P2辅助 / P3低频
    storage_layer: str = "daily"   # daily / minute / snapshot / meta

# P0 核心表 (必须可靠)
STOCK_DAILY          # 股票日线
ETF_DAILY            # ETF日线
INDEX_DAILY          # 指数日线
SPOT_SNAPSHOT        # 实时快照
FINANCIAL_REPORT     # 财务报表
FINANCIAL_BENEFIT    # 财务效益

# P1 重要表
INDEX_COMPONENTS     # 指数成分
VALUATION            # 估值数据
MONEY_FLOW           # 资金流向
NORTH_FLOW           # 北向资金
INDUSTRY_COMPONENTS  # 行业成分
HOLDER               # 股东信息
DIVIDEND             # 分红
FACTOR_CACHE         # 因子缓存
INDUSTRY_MAPPING     # 行业映射
CONCEPT_COMPONENTS   # 概念成分
FUND_PORTFOLIO       # 基金持仓

# P2 辅助表
SECURITIES           # 证券列表
TRADE_CALENDAR       # 交易日历
INDUSTRY_LIST        # 行业列表
CONCEPT_LIST         # 概念列表
COMPANY_INFO         # 公司信息
MACRO_DATA           # 宏观数据
MARGIN_DETAIL        # 融资融券
SHARE_CHANGE         # 股本变动
HOLDING_CHANGE       # 增减持
STATUS_CHANGE        # 状态变更
FINANCE_INDICATOR    # 财务指标
INDEX_WEIGHTS        # 指数权重

# P3 低频表
STOCK_MINUTE         # 分钟线
ETF_MINUTE           # ETF分钟
FUTURES_DAILY        # 期货日线
CONVERSION_BOND_DAILY # 可转债日线
OPTION_DAILY         # 期权日线
CALL_AUCTION         # 集合竞价
SECTOR_FLOW_SNAPSHOT # 板块资金快照
HSGT_HOLD_SNAPSHOT   # 港股通持仓快照
UNLOCK               # 解禁信息
MARGIN_UNDERLYING    # 融资标的

# 总计: 38 表 → 全量保留
```

### 4.3 `sources/router.py` — 路由 + 熔断 (完整能力)

```python
class SourceHealthMonitor:
    """熔断器: 连续失败自动禁用, 超时后自动恢复"""
    ERROR_THRESHOLD = 5        # 连续失败 5 次 → 熔断
    DISABLE_DURATION = 300     # 熔断 5 分钟
    def record_result(source, success, error=None): ...
    def is_available(source) -> bool: ...
    def get_all_status() -> Dict: ...

class DomainRateLimiter:
    """域名级限速: 每个域名控制请求间隔 (解决东方财富限速)"""
    DEFAULT_LIMITS = {"eastmoney": 0.5, "sina": 0.3, "tushare": 0.2}
    def wait(source): ...

class MultiSourceRouter:
    """多源路由: 按优先级 + 熔断 + 限速"""
    def __init__(self, providers, health_monitor, rate_limiter): ...
    def execute(self, method, **kwargs) -> DataFrame:
        for provider in self.providers:
            if not self.health.is_available(provider.name):
                continue  # 跳过熔断的源
            self.limiter.wait(provider.name)
            try:
                result = getattr(provider, method)(**kwargs)
                if result is not None and not result.empty:
                    self.health.record_result(provider.name, True)
                    return result
            except Exception as e:
                self.health.record_result(provider.name, False, e)
        raise AllSourcesFailedError(method, kwargs)
```

### 4.4 `store/incremental.py` — 增量缓存 (从 stock-bt 完整迁入)

```python
class IncrementalEngine:
    """增量缓存: 区间合并 + 只拉差量 + 小文件合并"""

    def get_or_fetch(self, table, symbol, start, end, fetch_func) -> DataFrame:
        # 1. 查已有数据范围
        cached_ranges = self._get_cached_ranges(table, symbol)
        # 2. 计算缺失区间
        missing = self._find_missing_ranges(start, end, cached_ranges)
        # 3. 只拉缺失
        for ms, me in missing:
            data = fetch_func(symbol, ms, me)
            self.store.write(table, symbol, data, ms, me)
        # 4. 合并小文件 (可选)
        self._compact_if_needed(table, symbol)
        # 5. 返回完整数据
        return self.store.read(table, symbol, start, end)

    def _find_missing_ranges(self, start, end, cached) -> list:
        """从 stock-bt akshare_wrapper_v2 迁入: 日期区间 + 分页区间"""
        ...

    def _merge_ranges(self, ranges, fmt) -> list:
        """从 stock-bt 迁入: 合并重叠区间"""
        ...

    def _compact_if_needed(self, table, symbol):
        """从 stock-bt 迁入: 相邻小文件自动合并"""
        ...

    def adaptive_refresh(self, table, symbol):
        """从 stock-bt 迁入: 根据数据变更频率自动决定刷新间隔"""
        ...
```

### 4.5 `offline/prober.py` — 接口探测 (从 analysis_tools 完整迁入)

```python
class APIProber:
    """全量 akshare 接口探测 — 从 analysis_tools health_checker v3.4 迁入"""

    def __init__(self, max_workers=64, domain_semaphores=None):
        ...  # 并发控制 + 域名级限流

    def probe_all(self, resume=True) -> Dict:
        """全量探测所有 akshare 接口"""
        # - 自动发现所有 callable
        # - 智能参数推断
        # - 断点续跑 (health_state.json)
        # - 并发 64 线程
        # - 域名级 semaphore 限流
        ...

    def probe_sources(self) -> Dict:
        """探测 4 个数据源连通性"""
        ...

    def detect_changes(self, prev_report, curr_report) -> List:
        """对比两次探测结果, 发现接口变更"""
        ...
```

### 4.6 `offline/quality.py` — 数据质量检查

```python
class DataQualityChecker:
    """数据质量检查"""

    def check_completeness(self, table, symbol, start, end) -> Dict:
        """完整性: 检查缺失日期/缺失字段"""
        ...

    def check_anomalies(self, table, symbol) -> List:
        """异常值: 涨跌幅>20%, 成交量异常等"""
        ...

    def check_consistency(self, symbol, sources) -> Dict:
        """一致性: 跨源比对同一股票数据"""
        ...

    def generate_report(self) -> Dict:
        """汇总质量报告"""
        ...
```

---

## 五、数据流

### 在线 (DataService — cache-first)

```
get_daily("600519", "2024-01-01", "2024-12-31")
  │
  ├→ memory_cache.get() → 命中 → 返回 (< 1ms)
  │
  ├→ incremental.get_or_fetch()
  │     ├→ 无缺失 → parquet.read() → DuckDB过滤 → 返回 (< 50ms)
  │     └→ 有缺失 → router.execute("get_daily_data", ...)
  │                    ├→ sina → OK → 标准化 → 写入 → 返回
  │                    ├→ sina 失败 → eastmoney → OK → 返回
  │                    ├→ eastmoney 被限速 → tushare → OK → 返回
  │                    └→ 全部失败 → 返回已有缓存或报错
  │
  └→ 写入 memory_cache (TTL缓存)
```

### 离线 (BatchDownloader + APIProber)

```
# 每日定时任务
downloader.download_incremental()     # 增量更新全市场
prober.probe_sources()                # 检查 4 源可用性
quality.check_completeness()          # 数据完整性检查
reporter.generate_report()            # 生成报告

# 每周定时任务
prober.probe_all(resume=True)         # 全量 akshare 接口探测
quality.check_consistency()           # 跨源数据比对
```

---

## 六、与已有项目的关系

```
                akshare-data-service (本次新建)
                ┌────────────────────┐
                │  core/   sources/  │
                │  store/  api.py    │
                │  offline/          │
                └──────┬─────────────┘
                       │ import
          ┌────────────┼────────────────┐
          │            │                │
    ┌─────▼──────┐  ┌──▼─────────┐  ┌──▼──────────┐
    │ akshare-   │  │ jk2bt      │  │ stock-bt    │
    │ one-       │  │            │  │             │
    │ enhanced   │  │ data层改为 │  │ data层改为  │
    │            │  │ ↓          │  │ ↓           │
    │ MCP Server │  │ from       │  │ from        │
    │ jq_compat  │  │ akshare_   │  │ akshare_    │
    │ 保持不变   │  │ data import│  │ data import │
    └────────────┘  └────────────┘  └─────────────┘
```

- **akshare-one-enhanced**: MCP + jq_compat 保持不变，数据获取改为调用 `akshare-data-service`
- **jk2bt**: 回测引擎不动，`data/sources/` 改为调用 `akshare-data-service`
- **stock-bt**: 因子分析不动，`data/` 改为调用 `akshare-data-service`

---

## 七、实施顺序

> 由底向上，依赖关系决定顺序

```
Phase 1: 核心基座
  core/config.py → core/errors.py → core/symbols.py → core/fields.py
  → core/normalize.py → core/schema.py → core/base.py
  → core/logging.py → core/metrics.py

Phase 2: 存储层
  store/memory.py → store/parquet.py → store/duckdb.py
  → store/incremental.py → store/manager.py

Phase 3: 数据源
  sources/akshare_source.py → sources/backup.py → sources/router.py

Phase 4: 在线入口
  api.py → __init__.py

Phase 5: 离线工具
  offline/downloader.py → offline/prober.py
  → offline/quality.py → offline/reporter.py

Phase 6: 集成验证
  tests/ → 与 akshare-one-enhanced 集成 → 与 jk2bt 集成
```

全部 Phase 一起做，按文件顺序逐个实现。

---

## 八、已确认事项

| 项目 | 决策 |
|------|------|
| 包位置 | ✅ 独立包 `akshare-data-service` |
| 缓存目录 | ✅ 项目目录下 `./data_cache/` |
| 表范围 | ✅ 全量 38+ 表，不精简 |
| OSS | ✅ 不保留 |
| Tushare | ✅ 有 Token |
| Baostock | ✅ 已安装 |
| 能力 | ✅ 全量保留，不砍 |
| 架构 | ✅ 简洁但完整 |
| 接入层 | ✅ MCP/JQ compat 独立，非核心 |
