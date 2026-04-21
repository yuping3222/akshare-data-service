# 核心模块

本文档描述 `akshare-data-service` 的核心模块（`src/akshare_data/core/`），包括配置管理、Schema 注册、代码格式转换、字段映射、数据标准化、错误码体系、日志与统计。

---

## 1. CacheConfig & TableConfig (`config.py`)

**文件路径**: `src/akshare_data/core/config.py`

### 1.1 TableConfig

每个表的独立缓存配置。

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `partition_by` | `str \| None` | `None` | Parquet 分区列名 |
| `ttl_hours` | `int` | `0` | 缓存过期时间（小时），0 表示永不过期 |
| `compaction_threshold` | `int` | `20` | 触发文件合并的文件数阈值 |
| `aggregation_enabled` | `bool` | `True` | 是否启用聚合存储 |

| 方法 | 说明 |
|------|------|
| `from_schema(schema: CacheTable)` | 从 CacheTable Schema 构建 TableConfig |
| `resolve(table_name)` | 用 SCHEMA_REGISTRY 覆盖，Schema 优先于配置默认值 |

### 1.2 CacheConfig

顶层缓存配置 dataclass，包含 21 个配置项。

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `base_dir` | `"./cache"` | 缓存存储根目录 |
| `daily_dir` | `"daily"` | 日线数据子目录 |
| `minute_dir` | `"minute"` | 分钟线数据子目录 |
| `snapshot_dir` | `"snapshot"` | 快照数据子目录 |
| `meta_dir` | `"meta"` | 元数据子目录 |
| `source_priority` | `["lixinger", "akshare"]` | 数据源优先级 |
| `tushare_token` | `""` | Tushare API Token |
| `memory_cache_max_items` | `5000` | 内存缓存最大条目数 |
| `memory_cache_default_ttl_seconds` | `3600` | 内存缓存默认 TTL（秒） |
| `compression` | `"snappy"` | Parquet 压缩算法 |
| `row_group_size` | `100_000` | Parquet 行组大小 |
| `aggregation_enabled` | `True` | 全局聚合存储开关 |
| `aggregation_schedule` | `"daily"` | 聚合任务调度周期 |
| `lock_dir` | `""` | 文件锁目录（空则使用 `base_dir/_locks`） |
| `duckdb_read_only` | `True` | DuckDB 只读模式 |
| `duckdb_threads` | `4` | DuckDB 工作线程数 |
| `duckdb_memory_limit` | `"4GB"` | DuckDB 内存限制 |
| `cleanup_retention_hours` | `24` | 过期文件保留时间（小时） |
| `strict_schema` | `False` | 严格 Schema 模式 |
| `log_level` | `"INFO"` | 日志级别 |
| `log_format` | `"json"` | 日志格式 |
| `tables` | `{}` | 各表配置覆盖 |

### 1.3 方法

| 方法 | 说明 |
|------|------|
| `from_json(path)` | 从 JSON 文件加载配置 |
| `from_env(**overrides)` | 创建配置并应用环境变量覆盖 |
| `to_dict()` | 序列化为字典（用于 JSON） |
| `get_table_config(table_name)` | 获取指定表的 TableConfig，Schema 注册表为数据源 |
| `lock_dir_path` (property) | 返回锁目录 Path，空则 `{base_dir}/_locks` |
| `aggregated_dir` (property) | 返回 `{base_dir}/aggregated` |

### 1.4 环境变量

| 环境变量 | 覆盖属性 |
|----------|----------|
| `AKSHARE_DATA_CACHE_DIR` | `base_dir` |
| `AKSHARE_DATA_CACHE_MAX_ITEMS` | `memory_cache_max_items` |
| `AKSHARE_DATA_CACHE_TTL_SECONDS` | `memory_cache_default_ttl_seconds` |
| `AKSHARE_DATA_CACHE_COMPRESSION` | `compression` |
| `AKSHARE_DATA_CACHE_ROW_GROUP_SIZE` | `row_group_size` |
| `AKSHARE_DATA_CACHE_DUCKDB_THREADS` | `duckdb_threads` |
| `AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT` | `duckdb_memory_limit` |
| `AKSHARE_DATA_CACHE_LOG_LEVEL` | `log_level` |
| `AKSHARE_DATA_CACHE_RETENTION_HOURS` | `cleanup_retention_hours` |
| `AKSHARE_DATA_CACHE_STRICT_SCHEMA` | `strict_schema` |

---

## 2. Schema 注册表 (`schema.py`)

**文件路径**: `src/akshare_data/core/schema.py`

### 2.1 CacheTable

不可变的缓存表 Schema 定义。

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | `str` | - | 唯一表标识符 |
| `partition_by` | `str \| None` | - | Parquet 分区列 |
| `ttl_hours` | `int` | - | 过期时间（小时），0=永久 |
| `schema` | `dict[str, str]` | - | 列名 → Parquet 数据类型 |
| `primary_key` | `list[str]` | - | 唯一标识行的列 |
| `aggregation_enabled` | `bool` | `True` | 是否启用聚合 |
| `compaction_threshold` | `int` | `20` | 合并文件数阈值 |
| `priority` | `str` | `"P0"` | 优先级（P0-P3） |
| `storage_layer` | `str` | `"daily"` | 存储层（daily/meta/snapshot/minute） |

### 2.2 TableInfo

缓存表物理状态的运行时元数据。

| 属性 | 类型 | 说明 |
|------|------|------|
| `name` | `str` | 表名 |
| `file_count` | `int` | Parquet 文件数 |
| `total_size_bytes` | `int` | 文件总大小 |
| `last_updated` | `datetime \| None` | 最后写入时间 |
| `partition_count` | `int` | 不同分区值数量 |
| `priority` | `str` | 优先级 |

### 2.3 TableRegistry

| 方法 | 说明 |
|------|------|
| `register(table: CacheTable)` | 注册表 Schema |
| `get(name: str) -> CacheTable` | 按名称获取，不存在抛 `KeyError` |
| `get_or_none(name: str) -> CacheTable \| None` | 按名称获取，不存在返回 `None` |
| `list_all() -> dict[str, CacheTable]` | 返回所有注册 Schema 的副本 |
| `list_by_priority(priority: str) -> list[CacheTable]` | 按优先级过滤 |
| `list_by_layer(layer: str) -> list[CacheTable]` | 按存储层过滤 |
| `has(name: str) -> bool` | 检查是否已注册 |

### 2.4 模块级函数与常量

| 名称 | 说明 |
|------|------|
| `SCHEMA_REGISTRY` | 全局 TableRegistry 单例 |
| `init_schemas()` | 注册所有默认表（幂等） |
| `get_table_schema(name) -> CacheTable \| None` | 按名称查找 |
| `list_tables() -> list[str]` | 返回已注册表名排序列表 |

### 2.5 存储层分类

系统预定义 69 张缓存表，分为四层：

| 存储层 | 表数 | 典型数据 |
|--------|------|----------|
| `daily` | 45 | 股票日线、财务指标、资金流向、估值 |
| `snapshot` | 7 | 实时行情快照、板块资金、热度排行 |
| `minute` | 2 | 股票/ETF 分钟线（按周分区） |
| `meta` | 15 | 证券列表、交易日历、公司信息、宏观 |

完整 Schema 参考 [07-Schema 注册表](07-schema-registry.md)。

---

## 3. Symbol 转换工具 (`symbols.py`)

**文件路径**: `src/akshare_data/core/symbols.py`

### 3.1 支持的代码格式

| 格式 | 示例 | 说明 |
|------|------|------|
| 纯数字 | `"600519"` | 6 位数字 |
| 聚宽 | `"600519.XSHG"` | `.XSHG`=上交所 / `.XSHE`=深交所 |
| AkShare | `"sh600519"` | `sh`=上交所 / `sz`=深交所 |
| Tushare | `"600000.SH"` | `.SH`=上交所 / `.SZ`=深交所 |
| BaoStock | `"sh.600519"` | 带点前缀 |
| 场外基金 | `"159001.OF"` | `.OF` 后缀 |
| 期货 | `"RB2401.CCFX"` | `.CCFX` 后缀 |

### 3.2 核心函数

| 函数 | 说明 | 示例 |
|------|------|------|
| `format_stock_symbol(symbol)` | 统一转为 6 位纯数字 | `sh600000` → `600000` |
| `extract_code_num(symbol)` | 提取 6 位纯数字代码 | `600519.XSHG` → `600519` |
| `get_symbol_prefix(symbol)` | 获取交易所前缀 | `600519.XSHG` → `sh` |
| `normalize_symbol(symbol)` | `format_stock_symbol` 的别名 | 同 `format_stock_symbol` |
| `is_valid_stock_code(symbol)` | 验证股票代码格式 | — |
| `is_gem_or_star(code)` | 判断创业板(300)/科创板(688) | `300750` → `True` |
| `calculate_limit_price(prev_close, code, direction)` | 计算涨跌停价（创业板/科创板 20%，其他 10%） | `100.0, 600519, up` → `110.0` |

### 3.3 格式互转

| 函数 | 说明 | 示例 |
|------|------|------|
| `jq_code_to_ak(code)` | 聚宽 → AkShare | `600519.XSHG` → `sh600519` |
| `ak_code_to_jq(code)` | AkShare → 聚宽 | `sh600519` → `600519.XSHG` |
| `ts_code_to_jq(code)` | Tushare → 聚宽 | `000001.SZ` → `000001.XSHE` |
| `jq_code_to_baostock(code)` | 聚宽 → BaoStock | `600519.XSHG` → `sh.600519` |
| `baostock_to_jq(code)` | BaoStock → 聚宽 | `sh.600519` → `600519.XSHG` |
| `ak_code_to_baostock(code)` | AkShare → BaoStock | `sh600519` → `sh.600519` |
| `baostock_to_ak(code)` | BaoStock → AkShare | `sh.600519` → `sh600519` |

### 3.4 兼容别名

| 别名 | 指向 |
|------|------|
| `normalize_code` | `normalize_symbol` |
| `format_stock_symbol_for_akshare` | `format_stock_symbol` |

---

## 4. 字段映射 (`fields.py`)

**文件路径**: `src/akshare_data/core/fields.py`

### 4.1 字段类型常量

| 常量 | 字段 |
|------|------|
| `FLOAT_FIELDS` | open, high, low, close, volume, amount, pre_close, change, pct_chg, turnover, amplitude, limit_up, limit_down, weight, openinterest, settle |
| `INT_FIELDS` | symbol |
| `STR_FIELDS` | name, stock_name, datetime, date, trade_date, adjust_flag, trade_status |
| `DATE_FIELDS` | datetime, date, trade_date, report_date |

### 4.2 类型工具

| 函数 | 说明 |
|------|------|
| `get_field_type(field_name)` | 返回预期类型（float/int/str/date） |
| `validate_field_types(df)` | 验证 DataFrame 字段类型，返回 `(valid, errors)` |

### 4.3 中文→英文字段映射 (`CN_TO_EN`)

核心映射：

| 中文 | 英文 | 中文 | 英文 |
|------|------|------|------|
| 日期 | datetime | 开盘/今开 | open |
| 最高/最高价 | high | 最低/最低价 | low |
| 收盘/收盘价 | close | 成交量/成交量(手) | volume |
| 成交额/成交额(元) | amount | 振幅 | amplitude |
| 涨跌幅 | pct_chg | 涨跌额 | change |
| 换手率 | turnover | 涨停价/跌停价 | limit_up/limit_down |
| 名称 | name | 代码/股票代码/证券代码 | symbol |
| 持仓量 | openinterest | 结算价 | settle |
| 时间 | datetime | vol | volume |
| turn | turnover | pctChg | pct_chg |
| preclose | pre_close | adjustflag | adjust_flag |
| tradestatus | trade_status | | |

### 4.4 多源字段映射 (`FIELD_MAPS`)

| 数据源 Key | 用途 | 特殊映射 |
|------------|------|----------|
| `eastmoney` | 东财接口 | 日期→datetime, 中文→英文 |
| `sina` | 新浪接口 | date→datetime, symbol→symbol |
| `tushare` | Tushare | trade_date→datetime, ts_code→symbol, vol→volume |
| `baostock` | BaoStock | date→datetime, code→symbol, preclose→pre_close |
| `ohlcv` | 通用 OHLCV | 中英文均映射为标准字段 |
| `realtime` | 实时行情 | 代码→code, 名称→name |
| `minute` | 分钟数据 | 时间→datetime, 成交额→money |
| `futures` | 期货/期权 | 日期→datetime, 持仓量→openinterest |
| `options_chain` | 期权链 | 期权代码→option_code, 行权价→strike_price |
| `options_realtime` | 期权实时 | 隐含波动率→iv, Greeks→greeks |
| `options_hist` | 期权历史 | OHLCV + 持仓量 + 行权价 |

### 4.5 代码→名称映射

从 `data/mappings/` 目录下的 CSV 文件懒加载映射表。

| 函数 | 映射文件 | 说明 |
|------|----------|------|
| `get_stock_name(code)` | `stock_code_to_name.csv` | 获取股票名称 |
| `get_index_name(code)` | `index_code_to_name.csv` | 获取指数名称 |
| `get_etf_name(code)` | `etf_code_to_name.csv` | 获取 ETF 名称 |
| `get_industry_name(code)` | `industry_code_to_name.csv` | 获取行业名称 |
| `get_option_name(symbol)` | `option_symbol_to_name.csv` | 获取期权名称 |
| `get_option_underlying_patterns(code)` | `option_underlying_patterns.csv` | 获取期权底层资产匹配模式 |
| `search_by_name(table, pattern)` | 任意表 | 按名称模式搜索 |
| `preload_mappings()` | 常用表 | 预加载常用映射到内存 |

### 4.6 列名标准化工具

| 函数 | 说明 |
|------|------|
| `standardize_columns(df, source)` | 根据源标识标准化列名 |
| `standardize_columns_generic(df, col_map)` | 使用自定义映射标准化 |
| `select_ohlcv_columns(df, include_amount)` | 选择标准 OHLCV 列 |

---

## 5. 数据标准化 (`normalize.py`)

**文件路径**: `src/akshare_data/core/normalize.py`

### 5.1 核心函数

#### `normalize(df, source, select_cols, coerce_numeric, coerce_fields)`

通用标准化入口。流程：
1. 应用 `FIELD_MAPS[source]` 列名映射
2. 转换 `datetime` 列为 `pd.to_datetime(errors="coerce")`
3. 可选：对 `FLOAT_FIELDS`/`INT_FIELDS` 执行类型强制转换
4. 可选：按 `select_cols` 选择输出列

### 5.2 专用标准化函数

| 函数 | 内部委托 | 输出列 |
|------|----------|--------|
| `standardize_ohlcv(df)` | `normalize(df, "ohlcv")` | datetime, open, high, low, close, volume, amount |
| `normalize_stock_daily(df)` | `normalize(df, "eastmoney")` | datetime, open, high, low, close, volume, [amount] |
| `normalize_sina_daily(df)` | `normalize(df, "sina")` | datetime, open, high, low, close, volume, amount |
| `normalize_tushare_daily(df)` | `normalize(df, "tushare")` | datetime, open, high, low, close, volume, amount |
| `normalize_baostock_daily(df)` | `normalize(df, "baostock", coerce_numeric=True)` | datetime, open, high, low, close, volume, amount |
| `normalize_etf_daily(df)` | `normalize_stock_daily(df)` | 同 stock_daily |
| `normalize_minute_data(df)` | `normalize(df, "minute")` | datetime, open, high, low, close, volume, money |
| `normalize_futures_daily(df)` | 自定义映射 | datetime, open, high, low, close, volume, openinterest, settle |
| `normalize_option_daily(df)` | `normalize_futures_daily(df)` | 同 futures_daily |
| `normalize_dataframe_for_parquet(df)` | 无 | 处理混合类型列（object→str, int→int64, float→float64） |

---

## 12. 错误码与异常层次 (`errors.py`)

**文件路径**: `src/akshare_data/core/errors.py`

### 7.1 ErrorCode 枚举

177 个错误码，分为 9 个类别：

| 范围 | 类别 | 示例 |
|------|------|------|
| 1001-1099 | 数据源错误 | SOURCE_NOT_FOUND, SOURCE_UNAVAILABLE, INVALID_SYMBOL |
| 2001-2099 | 缓存错误 | CACHE_MISS, CACHE_WRITE_FAILED, CACHE_EXPIRED |
| 3001-3099 | 参数错误 | INVALID_DATE_FORMAT, MISSING_PARAMETER |
| 4001-4099 | 网络错误 | NETWORK_TIMEOUT, NETWORK_CONNECTION_FAILED, RATE_LIMITED |
| 5001-5099 | 数据质量 | DATA_EMPTY, MISSING_REQUIRED_COLUMNS, DATA_TYPE_MISMATCH |
| 6001-6099 | 系统错误 | INTERNAL_ERROR, MODULE_NOT_AVAILABLE |
| 7001-7099 | 存储错误 | STORAGE_WRITE_FAILED, STORAGE_FILE_NOT_FOUND, PARQUET_WRITE_ERROR |
| 8001-8099 | 认证错误 | AUTH_TOKEN_MISSING, AUTH_TOKEN_INVALID |
| 9001-9099 | 限流错误 | RATE_LIMIT_EXCEEDED, RATE_LIMIT_COOLDOWN |

### 7.2 异常层次

```
DataAccessException (基类)
├── DataSourceError (1xxx)
├── SourceUnavailableError (1xxx)
├── NoDataError (5xxx)
├── TimeoutError (4xxx)
├── RateLimitError (4xxx/9xxx)
├── CacheError (2xxx)
├── ValidationError (3xxx)
├── DataQualityError (5xxx)
├── StorageError (7xxx)
├── AuthError (8xxx)
├── NetworkError (4xxx)
└── SystemError (6xxx)
```

---

## 11. 缓存策略 (`store/strategies/`)

**文件路径**: `src/akshare_data/store/strategies/`

### 8.1 CacheStrategy（抽象基类）

定义三个抽象方法：
- `should_fetch(cached, **params) -> bool` — 判断是否需要拉取
- `merge(cached, fresh, **params) -> pd.DataFrame` — 合并新旧数据
- `build_where(**params) -> dict` — 构建缓存查询条件

### 8.2 FullCacheStrategy

适用：meta/snapshot 数据（如 securities_list、industry_stocks）。

逻辑：缓存存在即命中，不存在则全量拉取并替换。
- `should_fetch`: 缓存为 None 或空时返回 True
- `merge`: 直接返回 fresh 数据
- `build_where`: 按 filter_keys 构建等值查询

### 8.3 IncrementalStrategy

适用：时序数据（如 stock_daily、index_daily、north_flow）。

逻辑：读已有区间 → 计算缺失 → 只拉缺失 → 合并。
- `should_fetch`: 无日期参数时检查缓存是否非空；有日期参数时检查缓存是否覆盖完整区间
- `merge`: 按 date_col 排序去重（keep="last"）
- `build_where`: 按 filter_keys + 日期范围构建查询
- `find_missing_ranges`: 使用 `find_missing_ranges()` 函数检测缺失区间
- `_is_complete`: 检查 min_date <= start 且 max_date >= end

---

## 6. Token 管理 (`tokens.py`)

**文件路径**: `src/akshare_data/core/tokens.py`

TokenManager 负责解析 API Token，支持从环境变量或配置文件（`token.cfg`）读取。

---

## 7. 配置缓存 (`config_cache.py`)

**文件路径**: `src/akshare_data/core/config_cache.py`

提供配置文件的懒加载与缓存机制，避免重复读取 YAML 配置。

---

## 8. 配置目录发现 (`config_dir.py`)

**文件路径**: `src/akshare_data/core/config_dir.py`

负责定位配置目录（内置 bundle 或项目 config/ 目录）。

---

## 9. 期权工具 (`options.py`)

**文件路径**: `src/akshare_data/core/options.py`

期权计算相关工具函数。

---

## 10. 日志系统 (`logging.py`)

**文件路径**: `src/akshare_data/core/logging.py`

### 9.1 Formatter

| 类 | 格式 | 说明 |
|----|------|------|
| `StructuredFormatter` | JSON | 输出 timestamp/level/logger/message/context/exception/location |
| `StandardFormatter` | `%(asctime)s \| %(levelname)-8s \| %(name)s \| %(message)s` | 标准文本格式 |
| `JSONFormatter` | JSON | 使用 UTC 时间戳 |

| 格式类型 | 说明 |
|----------|------|
| `standard` | StandardFormatter |
| `simple` | `%(asctime)s [%(levelname)s] %(message)s` |
| `json` | JSONFormatter |
| `structured` | StructuredFormatter（默认） |
| `strategy` | `%(asctime)s - %(message)s` |

### 9.2 ContextFilter

为日志记录添加默认上下文的 Filter。

### 9.3 Handler 工具函数

| 函数 | 说明 |
|------|------|
| `create_rotating_file_handler()` | 基于大小的轮转文件处理器 |
| `create_timed_rotating_file_handler()` | 基于时间的轮转（默认每天午夜） |
| `create_strategy_log_file()` | 策略日志文件处理器 |
| `close_handler_safely()` | 安全关闭处理器 |

### 9.4 JQLogAdapter

JoinQuant 风格的日志适配器，支持 info/warn/warning/error/debug/critical 方法。可绑定 strategy 对象或直接输出到 stdout。

### 9.5 LoggingConfig

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `level` | `"INFO"` | 日志级别 |
| `log_file` | `"logs/akshare_data.log"` | 日志文件路径 |
| `format` | `"structured"` | 日志格式 |
| `max_bytes` | `10MB` | 单文件最大字节数 |
| `backup_count` | `5` | 备份文件数 |
| `suppress_third_party` | `True` | 抑制 matplotlib/PIL/urllib3/requests 日志 |

### 9.6 LogManager

单例日志管理器。

| 方法 | 说明 |
|------|------|
| `initialize(config)` | 初始化日志配置 |
| `get_jq_adapter(strategy, logger_name)` | 获取 JQLogAdapter |
| `get_logger(name)` | 获取 Logger 实例 |
| `get_stats_collector()` | 获取 StatsCollector |
| `shutdown()` | 关闭日志（输出统计汇总） |
| `reset()` | 重置日志配置与统计 |

### 9.7 setup_logging()

主入口函数，支持 console/file 双输出、多种格式、自动初始化 LogManager 单例。

### 9.8 LogContext

上下文管理器，用于临时向日志添加上下文。

### 9.9 辅助函数

| 函数 | 说明 |
|------|------|
| `get_logger(name)` | 获取 Logger，自动添加 `akshare_data.` 前缀 |
| `log_api_request()` | 记录 API 请求（结构化上下文） |
| `log_data_quality()` | 记录数据质量问题 |
| `log_exception()` | 记录异常（含错误码和上下文） |
| `setup_logging_simple()` | 简化版日志设置 |
| `get_jq_log()` | 获取 JQLogAdapter |
| `get_default_logger()` | 获取默认 Logger |

---

## 10. 统计收集 (`stats.py`)

**文件路径**: `src/akshare_data/core/stats.py`

> `logging.py` 通过 re-export 将 stats.py 的所有公开 API 作为向后兼容别名重新导出。

### 10.1 RequestStats

单个数据源的请求统计。

| 属性 | 说明 |
|------|------|
| `total_requests` | 总请求数 |
| `successful_requests` | 成功数 |
| `failed_requests` | 失败数 |
| `total_duration_ms` | 总耗时 |
| `avg_duration_ms` | 平均耗时 |
| `min_duration_ms` / `max_duration_ms` | 最小/最大耗时 |
| `success_rate` | 成功率 |
| `error_rate` | 错误率 |
| `errors` | 错误类型计数字典 |

### 10.2 CacheStats

单个缓存的统计。

| 属性 | 说明 |
|------|------|
| `hits` | 命中次数 |
| `misses` | 未命中次数 |
| `total_requests` | 总请求数（hits + misses） |
| `hit_rate` | 命中率 |

### 10.3 StatsCollector

线程安全的单例统计收集器。

| 方法 | 说明 |
|------|------|
| `record_request(source, duration_ms, success, error_type, endpoint)` | 记录请求 |
| `record_cache_hit(cache_name)` | 记录缓存命中 |
| `record_cache_miss(cache_name)` | 记录缓存未命中 |
| `get_source_stats(source)` | 获取指定源的统计 |
| `get_cache_stats(cache_name)` | 获取指定缓存的统计 |
| `get_all_stats()` | 获取所有统计（含 summary 汇总） |
| `get_summary_text()` | 获取格式化的汇总文本 |
| `print_summary(force)` | 打印汇总（force=True 用 INFO 级别） |
| `log_summary(logger, level)` | 用指定 Logger 输出汇总 |
| `export_json(filepath)` | 导出为 JSON（含 exported_at 时间戳） |
| `export_csv(output_dir)` | 按源导出 CSV |
| `reset()` | 清空所有统计 |
| `reset_instance()` (类方法) | 重置单例 |

### 10.4 模块级函数

| 函数 | 说明 |
|------|------|
| `get_stats_collector()` | 获取全局 StatsCollector 单例 |
| `reset_stats_collector()` | 重置全局单例 |
| `log_api_request(logger, source, endpoint, ...)` | 记录 API 请求（结构化上下文） |
| `log_data_quality(logger, source, data_type, issue)` | 记录数据质量问题 |

---

## 模块依赖关系

```
config.py ───── 配置基础（依赖 schema.py）
    │
config_cache.py ─ 配置缓存（依赖 config_dir.py）
    │
config_dir.py ─── 配置目录发现
    │
schema.py ───── 表结构定义
    │
tokens.py ───── Token 管理（独立）
    │
symbols.py ──── 代码格式转换（独立）
    │
fields.py ───── 字段映射（独立，提供 CN_TO_EN 和 FIELD_MAPS）
    │
normalize.py ── 数据标准化（依赖 fields.py）
    │
options.py ──── 期权工具（独立）
    │
errors.py ───── 错误码与异常（独立）
    │
strategies/ ─── 缓存策略（base → full/incremental）
    │
logging.py ──── 日志系统（re-export stats.py）
    │
stats.py ───── 统计收集（独立）
```
