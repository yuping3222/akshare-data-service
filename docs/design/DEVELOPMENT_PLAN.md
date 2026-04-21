# 统一金融数据服务 — 详细开发工作设计

> **原则**: 从四个源项目复制已有代码，适配到新架构，不重新实现。

---

## 源项目映射表

| 源项目 | 路径 | 迁入目标 |
|--------|------|---------|
| **jk2bt** | `/testlixingren/jk2bt-main/jk2bt/` | core/, sources/, store/ 主体 |
| **akshare-one-enhanced** | `/akshare-one-enhanced/src/akshare_one/` | core/, sources/, store/ 补充 |
| **stock-backtesting-system** | `/testlixingren/stock-backtesting-system/src/data/` | store/incremental.py 主体 |
| **akshare_analysis_tools** | `/akshare/akshare_analysis_tools/core/` | offline/ 主体 |

---

## Phase 1: 核心基座 (9 个文件)

### 1.1 `core/config.py` (~80行)
- **来源**: jk2bt `cache/config.py` + akshare-one-enhanced `cache/config.py`
- **迁入内容**:
  - 缓存路径配置 (`cache_dir`, `daily_dir`, `minute_dir`, `snapshot_dir`, `meta_dir`)
  - 源优先级配置 (`source_priority`)
  - Token 配置 (Tushare token)
  - 日志配置 (level, format)
- **适配工作**:
  - 去掉 jk2bt 项目特定路径，改为相对 `data_cache/` 的配置
  - 合并 akshare-one-enhanced 的 HTTP 客户端配置

### 1.2 `core/errors.py` (~100行)
- **来源**: jk2bt `data/sources/error_codes.py` + akshare-one-enhanced `modules/exceptions.py`
- **迁入内容**:
  - ErrorCode 枚举 (130+ 错误码)
  - DataSourceError, SourceUnavailableError, NoDataError 异常类
  - 错误码 → 人类可读消息映射
- **适配工作**:
  - 统一异常类命名，去除 jk2bt 特定的 `utils.exceptions` 依赖
  - 保留全部 130+ 错误码

### 1.3 `core/symbols.py` (~150行)
- **来源**: jk2bt `utils/symbol.py`
- **迁入内容**:
  - `jq_code_to_ak()` — 聚宽格式 → AkShare 格式
  - `ak_code_to_jq()` — AkShare 格式 → 聚宽格式
  - `ts_code_to_jq()` — Tushare 格式 → 聚宽格式
  - `normalize_symbol()` — 统一入口，自动识别格式
  - 6位纯代码 ↔ 带前缀代码转换
- **适配工作**:
  - 去掉 jk2bt `utils` 模块依赖，改为独立函数
  - 增加 baostock 格式支持 (`sh.600000` / `sz.000001`)

### 1.4 `core/fields.py` (~150行)
- **来源**: akshare-one-enhanced `mappings/` + `field_mappings/` + `field_naming/`
- **迁入内容**:
  - 中文 → 英文字段映射表 (日期→datetime, 开盘→open, 最高→high, 最低→low, 收盘→close, 成交量→volume, 成交额→amount)
  - 多源字段统一映射 (sina/em/ts/bs 各源的字段名 → 统一英文名)
  - 字段类型定义 (float/int/str/date)
- **适配工作**:
  - 合并 `mapping_utils.py` 中的映射逻辑
  - 从 `full_combined_mapping.csv` 提取代码→名称映射

### 1.5 `core/normalize.py` (~80行)
- **来源**: stock-bt `akshare_wrapper_v2.py` 的 `_normalize_dataframe_for_parquet()` + jk2bt `akshare.py` 的 `_standardize_ohlcv()`
- **迁入内容**:
  - `_normalize_dataframe_for_parquet()` — DataFrame 类型规范化（混合类型处理）
  - `_standardize_ohlcv()` — OHLCV 字段标准化
  - `_normalize_sina_daily()`, `_normalize_tushare_daily()`, `_normalize_baostock_daily()` — 各源标准化
  - `_normalize_minute_data()`, `_normalize_etf_daily()`, `_normalize_futures_daily()`, `_normalize_option_daily()`
- **适配工作**:
  - 从 akshare.py 和 data_source_backup.py 提取所有 `_normalize_*` 函数
  - 统一为 `normalize.py` 中的独立函数

### 1.6 `core/schema.py` (~400行)
- **来源**: jk2bt `data/sources/registry.py` + akshare-one-enhanced `cache/schema.py`
- **迁入内容**:
  - `CacheTable` dataclass (name, partition_by, ttl_hours, schema, primary_key, priority, storage_layer)
  - 全量 38+ 表定义 (P0~P3 优先级)
  - 表 → 分区策略映射
- **适配工作**:
  - 合并 jk2bt registry.py 中的数据表定义
  - 合并 akshare-one-enhanced cache/schema.py 中的 schema 定义
  - 保留全部 38 表，不精简

### 1.7 `core/base.py` (~350行)
- **来源**: jk2bt `data/sources/base.py` (完整 668 行)
- **迁入内容**:
  - `DataSource` ABC 完整 40+ 方法 (全部 abstract + 默认实现)
  - 所有方法签名和 docstring 原样迁入
- **适配工作**:
  - 去掉 `from jk2bt.utils.exceptions import DataSourceError`，改为 `from akshare_data.core.errors import ...`
  - 保留全部 40+ 方法，一个不删

### 1.8 `core/logging.py` (~60行)
- **来源**: akshare-one-enhanced `logging_config.py` + jk2bt `logging/`
- **迁入内容**:
  - JSON 结构化日志配置
  - 日志级别管理
  - 日志文件轮转配置
- **适配工作**:
  - 简化为独立的 `setup_logging()` 函数

### 1.9 `core/metrics.py` (~80行)
- **来源**: jk2bt `data/sources/stats_collector.py` + akshare-one-enhanced `metrics/`
- **迁入内容**:
  - `StatsCollector` 类 (请求数/命中率/耗时统计)
  - `record_request()`, `record_cache_hit()`, `record_cache_miss()`
  - `get_stats()` — 返回统计摘要
- **适配工作**:
  - 从 jk2bt stats_collector.py 直接迁入
  - 去掉循环导入问题 (`_try_import_stats_collector`)

---

## Phase 2: 存储层 (5 个文件)

### 2.1 `store/memory.py` (~80行)
- **来源**: jk2bt `cache/memory.py` + akshare-one-enhanced `cache/` 中的内存缓存部分
- **迁入内容**:
  - TTL 内存缓存 (使用 `cachetools.TTLCache`)
  - `get()`, `set()`, `has()` 方法
- **适配工作**:
  - 直接迁入，改动最小

### 2.2 `store/parquet.py` (~200行)
- **来源**: jk2bt `cache/writer.py` (AtomicWriter) + `cache/partition.py` + akshare-one-enhanced `cache/parquet_store.py`
- **迁入内容**:
  - `AtomicWriter` — Parquet 原子写入 (临时文件 + rename)
  - 分区管理 (按 symbol/date/week 分区)
  - 文件锁机制 (`fcntl.flock`)
  - `_write_parquet_with_lock()` — 从 stock-bt 迁入
- **适配工作**:
  - 合并 jk2bt writer.py + partition.py 的写入和分区逻辑
  - 从 stock-bt 迁入 `_write_parquet_with_lock()` 和 `_normalize_dataframe_for_parquet()`
  - 去掉 OSS 相关代码 (已确认不保留)

### 2.3 `store/duckdb.py` (~200行)
- **来源**: jk2bt `cache/query.py` + akshare-one-enhanced `cache/duckdb_engine.py` + stock-bt `data_duckdb.py`
- **迁入内容**:
  - DuckDB 连接管理
  - SQL 查询接口 (`query()`, `filter()`, `aggregate()`)
  - 表注册 (`register_table()`)
  - 从 stock-bt 迁入 DuckDB 存储相关逻辑
- **适配工作**:
  - 合并三个源的 DuckDB 逻辑
  - 统一为 `DuckDBEngine` 类

### 2.4 `store/incremental.py` (~250行)
- **来源**: stock-bt `akshare_wrapper_v2.py` (核心增量逻辑)
- **迁入内容**:
  - `_merge_ranges()` — 合并重叠区间 (完整实现)
  - `_find_missing_ranges()` — 找出缺失区间 (完整实现，支持日期和分页)
  - `_should_merge_small_files()` — 小文件合并检测
  - `_merge_adjacent_files()` — 相邻文件合并
  - `adaptive_refresh()` — 自适应刷新策略
  - `_should_refresh_cache()` — 缓存刷新判断
  - `_track_usage()` — 使用频率跟踪
  - `_adaptive_cache_strategy()` — 自适应缓存策略
- **适配工作**:
  - 从 stock-bt `akshare_wrapper_v2.py` 提取所有区间计算和增量逻辑
  - 去掉 OSS 存储相关代码
  - 适配新的 `store/manager.py` 接口
  - 保留 `_normalize_dataframe_for_parquet()` 和文件锁逻辑

### 2.5 `store/manager.py` (~150行)
- **来源**: jk2bt `cache/manager.py` + akshare-one-enhanced `cache/manager.py`
- **迁入内容**:
  - `CacheManager` 统一入口
  - `read()`, `write()`, `has_range()` 方法
  - 内存缓存 + Parquet + DuckDB 三层协调
  - 从 jk2bt 迁入 `get_cache_manager()` 单例
- **适配工作**:
  - 合并 jk2bt 和 akshare-one-enhanced 的两个 manager
  - 协调 memory/parquet/duckdb/incremental 四层

---

## Phase 3: 数据源 (3 个文件)

### 3.1 `sources/akshare_source.py` (~800行)
- **来源**: jk2bt `data/sources/akshare.py` (完整 1354+ 行)
- **迁入内容**:
  - `AkShareAdapter` 完整实现 (DataSource ABC 全部方法)
  - `get_daily_data()`, `get_index_stocks()`, `get_index_components()`, `get_trading_days()`
  - `get_securities_list()`, `get_security_info()`, `get_minute_data()`
  - `get_money_flow()`, `get_north_money_flow()`, `get_industry_stocks()`, `get_industry_mapping()`
  - `get_finance_indicator()`, `get_call_auction()`
  - `get_etf_daily()`, `get_index_daily()`
  - `get_st_stocks()`, `get_suspended_stocks()`
  - 所有 stats 收集逻辑 (`_record_request`, `_record_cache_hit`, `_record_cache_miss`)
  - `_normalize_symbol()`, `_normalize_date()` 辅助方法
  - `_to_jq_format()` 代码转换
- **适配工作**:
  - 去掉 `from jk2bt.data.sources import data_source_backup` 等循环依赖
  - 改为 `from akshare_data.core.*` 导入
  - 保留全部方法实现，一个不删
  - 缓存由 `DataService` 层通过 `CacheManager` 统一管理，`AkShareAdapter` 仅负责数据获取

### 3.2 `sources/backup.py` (~500行)
- **来源**: jk2bt `data/sources/data_source_backup.py` (完整 925 行)
- **迁入内容**:
  - 4 源备份系统完整实现 (sina/east_money/tushare/baostock)
  - `fetch_stock_daily_sina()`, `fetch_stock_daily_eastmoney()`, `fetch_stock_daily_tushare()`, `fetch_stock_daily_baostock()`
  - `get_stock_daily_with_fallback()` — 带备用数据源的股票日线
  - `fetch_etf_daily_sina()`, `fetch_etf_daily_eastmoney()`, `get_etf_daily_with_fallback()`
  - `fetch_stock_minute_eastmoney()`, `get_stock_minute_with_fallback()`
  - `fetch_index_components_sina()`, `fetch_index_components_csindex()`, `get_index_components_with_fallback()`
  - `fetch_futures_daily_sina()`, `fetch_option_daily_sina()`
  - `fetch_north_money_eastmoney()`, `get_north_money_with_fallback()`
  - `fetch_industry_list_eastmoney()`, `fetch_industry_stocks_eastmoney()`
  - 所有 `_normalize_*` 标准化函数
  - `set_tushare_token()`, `_get_tushare_token()`
- **适配工作**:
  - 去掉 `from jk2bt.data.sources import get_adapter` 循环依赖
  - 改为直接调用 akshare API
  - 保留全部 fetcher 函数和标准化逻辑

### 3.3 `sources/router.py` (~300行)
- **来源**: jk2bt `data/sources/router.py` (完整 339 行)
- **迁入内容**:
  - `SourceHealthMonitor` — 熔断器 (完整实现)
  - `MultiSourceRouter` — 多源路由 (完整实现)
  - `ExecutionResult` dataclass
  - `EmptyDataPolicy` 枚举
  - `create_simple_router()` 便捷函数
  - `_try_import_stats_collector()` 延迟导入
- **适配工作**:
  - 几乎原样迁入，只需修改 import 路径
  - 保留全部熔断和路由逻辑

---

## Phase 4: 在线入口 (2 个文件)

### 4.1 `api.py` (~200行)
- **来源**: 新建，但参考 jk2bt `api/` 和 akshare-one-enhanced `client.py` 的调用模式
- **迁入内容**:
  - `DataService` 类 — cache-first 统一 API
  - `get_daily()`, `get_minute()`, `get_index()`, `get_etf()` 等方法
  - 读缓存 → 增量拉取 → 返回的完整流程
- **适配工作**:
  - 基于已有的 `api.py` 框架补充实现
  - 使用 `store/manager.py` + `sources/router.py` 组合

### 4.2 `__init__.py` (~30行)
- **来源**: 参考 jk2bt `__init__.py` + akshare-one-enhanced `__init__.py`
- **迁入内容**:
  - 导出公共接口: `get_daily`, `get_minute`, `DataService`, `CacheManager`
  - 版本信息

---

## Phase 5: 离线工具 (4 个文件)

### 5.1 `offline/downloader.py` (~250行)
- **来源**: stock-bt `update_duckdb_daily.py` + `import_data_to_duckdb.py` + 新建
- **迁入内容**:
  - 批量增量下载器
  - 全市场下载逻辑
  - 按指数成分下载逻辑
  - 并发控制 + 域名限速
  - 从 stock-bt 迁入 DuckDB 数据更新逻辑
- **适配工作**:
  - 适配新的 `store/manager.py` 接口
  - 合并 stock-bt 的更新逻辑

### 5.2 `offline/prober.py` (~300行)
- **来源**: akshare_analysis_tools `core/health_checker.py` (完整 348 行)
- **迁入内容**:
  - `HealthChecker` → `APIProber` 重命名
  - `discover_interfaces()` — 自动发现所有 akshare callable
  - `get_smart_kwargs()` — 智能参数推断
  - `call_with_retry()` — 带 symbol 回退的重试
  - `run_check()` — 64 线程并发探测
  - `run_single_task()` — 单任务探测
  - `should_skip()` — TTL 跳过逻辑
  - `get_website_group()` — 域名分组
  - `get_rolling_date_range()` — 滚动日期范围
  - `parse_params_from_doc()` — 文档参数推断
  - 断点续跑 (`_load_checkpoint`, `_save_checkpoint`)
  - 域名级 semaphore 限流
- **适配工作**:
  - 重命名 `HealthChecker` → `APIProber`
  - 适配新的路径配置 (`health_state.json` → `config/health_state.json`)
  - 保留全部探测逻辑，一个不删

### 5.3 `offline/quality.py` (~150行)
- **来源**: akshare_analysis_tools `core/` 中的数据质量相关逻辑 + stock-bt 数据验证
- **迁入内容**:
  - `DataQualityChecker` 类
  - `check_completeness()` — 完整性检查 (缺失日期/缺失字段)
  - `check_anomalies()` — 异常值检测 (涨跌幅>20%, 成交量异常)
  - `check_consistency()` — 一致性检查 (跨源比对)
  - `generate_report()` — 汇总质量报告
- **适配工作**:
  - 从 health_checker 的验证逻辑中提取质量检查
  - 补充 stock-bt 中的数据验证逻辑

### 5.4 `offline/reporter.py` (~100行)
- **来源**: akshare_analysis_tools `core/health_checker.py` 的 `generate_report()` + `generate_final_report.py` + `generate_volume_report.py`
- **迁入内容**:
  - `generate_report()` — 健康报告生成 (完整实现)
  - `to_md()` — DataFrame → Markdown 转换
  - `integrate_with_summary()` — 报告整合
  - 域名性能统计 (`domain_perf`)
  - 最慢 API 排行 (`slowest_apis`)
  - 从 `generate_final_report.py` 迁入最终报告生成逻辑
  - 从 `generate_volume_report.py` 迁入数据量报告逻辑
- **适配工作**:
  - 合并三个报告生成脚本
  - 适配新的路径配置

---

## Phase 6: 集成验证 (4 个测试文件)

### 6.1 `tests/test_store.py`
- **来源**: jk2bt `cache/tests/` + stock-bt `tests/`
- **迁入内容**:
  - Parquet 读写测试
  - DuckDB 查询测试
  - 增量缓存测试
  - 内存缓存测试

### 6.2 `tests/test_fetcher.py`
- **来源**: jk2bt `tests/` + akshare-one-enhanced `tests/`
- **迁入内容**:
  - AkShare 适配器测试
  - 备份数据源测试
  - 多源路由测试
  - 熔断器测试

### 6.3 `tests/test_api.py`
- **来源**: akshare-one-enhanced `tests/`
- **迁入内容**:
  - DataService API 测试
  - 缓存命中/未命中测试
  - 增量拉取测试

### 6.4 `tests/test_offline.py`
- **来源**: akshare_analysis_tools 测试逻辑
- **迁入内容**:
  - 接口探测器测试
  - 数据质量检查测试
  - 报告生成测试

---

## 辅助文件

### `pyproject.toml`
- **来源**: jk2bt `pyproject.toml` + akshare-one-enhanced `pyproject.toml`
- **迁入内容**:
  - 依赖声明 (akshare, pandas, duckdb, pyarrow, cachetools, baostock, tushare)
  - 项目元数据

### `config/default.yaml`
- **来源**: jk2bt `config/` + akshare-one-enhanced `config/`
- **迁入内容**:
  - 默认配置模板

### `config/health_state.json`
- **来源**: akshare_analysis_tools `results/health_state.json` (格式参考)
- **迁入内容**:
  - 探测状态文件格式

---

## 代码迁移对照总表

| 目标文件 | 主要来源 | 行数估算 | 迁移方式 |
|---------|---------|---------|---------|
| `core/config.py` | jk2bt cache/config.py + akshare-one cache/config.py | ~80 | 复制+合并 |
| `core/errors.py` | jk2bt error_codes.py + akshare-one exceptions.py | ~100 | 复制+合并 |
| `core/symbols.py` | jk2bt utils/symbol.py | ~150 | 直接复制 |
| `core/fields.py` | akshare-one mappings/ + field_mappings/ | ~150 | 复制+合并 |
| `core/normalize.py` | jk2bt akshare.py + data_source_backup.py + stock-bt wrapper_v2 | ~80 | 提取+合并 |
| `core/schema.py` | jk2bt registry.py + akshare-one cache/schema.py | ~400 | 复制+合并 |
| `core/base.py` | jk2bt base.py | ~350 | 直接复制 |
| `core/logging.py` | akshare-one logging_config.py + jk2bt logging/ | ~60 | 复制+简化 |
| `core/metrics.py` | jk2bt stats_collector.py + akshare-one metrics/ | ~80 | 复制+合并 |
| `store/memory.py` | jk2bt cache/memory.py | ~80 | 直接复制 |
| `store/parquet.py` | jk2bt writer.py + partition.py + stock-bt wrapper_v2 | ~200 | 复制+合并 |
| `store/duckdb.py` | jk2bt query.py + akshare-one duckdb_engine.py + stock-bt data_duckdb.py | ~200 | 复制+合并 |
| `store/incremental.py` | stock-bt akshare_wrapper_v2.py | ~250 | 提取+适配 |
| `store/manager.py` | jk2bt cache/manager.py + akshare-one cache/manager.py | ~150 | 复制+合并 |
| `sources/akshare_source.py` | jk2bt akshare.py | ~800 | 直接复制 |
| `sources/backup.py` | jk2bt data_source_backup.py | ~500 | 直接复制 |
| `sources/router.py` | jk2bt router.py | ~300 | 直接复制 |
| `api.py` | 新建 (参考 jk2bt api/ + akshare-one client.py) | ~200 | 参考实现 |
| `offline/downloader.py` | stock-bt update_duckdb_daily.py + import_data_to_duckdb.py | ~250 | 复制+适配 |
| `offline/prober.py` | akshare_analysis_tools health_checker.py | ~300 | 直接复制 |
| `offline/quality.py` | akshare_analysis_tools + stock-bt 验证逻辑 | ~150 | 提取+合并 |
| `offline/reporter.py` | akshare_analysis_tools generate_*.py (3个文件) | ~100 | 复制+合并 |
| **合计** | | **~4300行** | |

---

## 10 个子 Agent 分工

| Agent | 负责模块 | 源文件 | 目标文件 |
|-------|---------|--------|---------|
| **Agent 1** | Core 基座 (1/3) | jk2bt base.py, error_codes.py, utils/symbol.py | core/base.py, core/errors.py, core/symbols.py |
| **Agent 2** | Core 基座 (2/3) | akshare-one mappings/, field_naming/, logging_config.py | core/fields.py, core/normalize.py, core/logging.py |
| **Agent 3** | Core 基座 (3/3) | jk2bt registry.py, stats_collector.py, cache/config.py | core/schema.py, core/metrics.py, core/config.py |
| **Agent 4** | 存储层 (1/2) | jk2bt cache/memory.py, writer.py, partition.py, query.py, manager.py | store/memory.py, store/parquet.py, store/duckdb.py, store/manager.py |
| **Agent 5** | 存储层 (2/2) | stock-bt akshare_wrapper_v2.py, data_duckdb.py, update_duckdb_daily.py | store/incremental.py, 补充 store/duckdb.py |
| **Agent 6** | 数据源 (1/2) | jk2bt akshare.py | sources/akshare_source.py |
| **Agent 7** | 数据源 (2/2) | jk2bt data_source_backup.py, router.py | sources/backup.py, sources/router.py |
| **Agent 8** | 在线入口 | 新建 (参考 jk2bt api/, akshare-one client.py) | api.py, __init__.py |
| **Agent 9** | 离线工具 (1/2) | akshare_analysis_tools health_checker.py, generate_*.py | offline/prober.py, offline/reporter.py |
| **Agent 10** | 离线工具 (2/2) + 测试 | stock-bt import_data_to_duckdb.py, akshare_analysis_tools quality 逻辑, jk2bt tests/ | offline/downloader.py, offline/quality.py, tests/* |

---

## 依赖关系与执行顺序

```
Agent 1, 2, 3 (Core 基座) → 并行执行，无依赖
    ↓
Agent 4 (存储层 1/2) → 依赖 Core 基座完成
Agent 5 (存储层 2/2) → 依赖 Core 基座完成，可与 Agent 4 并行
    ↓
Agent 6, 7 (数据源) → 依赖 Core 基座 + 存储层完成，可并行
    ↓
Agent 8 (在线入口) → 依赖 Core + 存储 + 数据源完成
Agent 9, 10 (离线工具 + 测试) → 依赖 Core + 存储完成，可与其他并行
```

**最优并行策略**:
1. 第一批: Agent 1, 2, 3 (Core 基座，3 个并行)
2. 第二批: Agent 4, 5, 9 (存储层 + 离线 prober/reporter，3 个并行，prober/reporter 只依赖 health_checker.py 不依赖 core)
3. 第三批: Agent 6, 7, 10 (数据源 + 离线 downloader/quality + 测试，3 个并行)
4. 第四批: Agent 8 (在线入口，1 个，依赖前面全部)

实际可启动 10 个并行，因为大部分文件之间没有强依赖，只需确保 import 路径正确。
