# 离线工具 (Offline Tools)

离线工具层提供批量下载、接口扫描、健康探测、数据质量审计、报告生成等独立运行的工具，不依赖在线服务模块。所有工具位于 `src/akshare_data/offline/` 目录下。

## 架构总览

```
offline/
├── core/              # 基础设施（路径、配置、错误、重试）
├── scanner/           # AkShare 接口扫描与推断
├── registry/          # 注册表管理（构建、合并、导出、验证）
├── prober/            # 接口健康探测
├── downloader/        # 批量数据下载
├── analyzer/          # 数据分析（访问日志、缓存质量）
├── scheduler/         # 定时任务调度
├── source_manager/    # 数据源健康管理
├── report/            # 报告生成
└── cli/               # 命令行入口
```

## 目录

- [1. BatchDownloader — 批量下载器](#1-batchdownloader--批量下载器)
- [2. APIProber — 接口探测器](#2-apiprober--接口探测器)
- [3. CompletenessChecker / DataQualityChecker — 缓存完整性检查](#3-completenesschecker--dataqualitychecker--缓存完整性检查)
- [4. AnomalyDetector — 异常值检测](#4-anomalydetector--异常值检测)
- [5. ReportRenderer — 报告生成器](#5-reportrenderer--报告生成器)
- [6. AccessLogger — 访问日志记录器](#6-accesslogger--访问日志记录器)
- [7. CallStatsAnalyzer — 调用统计分析器](#7-callstatsanalyzer--调用统计分析器)
- [8. Scanner + Registry — 扫描与注册表管理](#8-scanner--registry--扫描与注册表管理)

> 命令行参数和子命令的完整参考详见 [CLI_REFERENCE.md](CLI_REFERENCE.md)。

---

## 1. BatchDownloader — 批量下载器

**文件**: `src/akshare_data/offline/downloader/downloader.py`

批量下载器负责从 AkShare 原始函数异步下载数据并写入共享缓存（DuckDB + Parquet）。它读取 `akshare_registry.yaml` 获取接口定义，采用配置驱动模式。

### 1.1 架构

```
BatchDownloader
├── TaskBuilder      — 构建 DownloadTask 列表
├── TaskExecutor     — 执行单个下载任务
├── ProgressTracker  — 进度跟踪
└── DomainRateLimiter — 域名级限速
```

### 1.2 常量

| 常量 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_MAX_WORKERS` | `4` | 默认并发工作线程数 |
| `DEFAULT_BATCH_SIZE` | `50` | 批量进度日志间隔 |

### 1.3 构造函数

```python
BatchDownloader(
    cache_manager: Optional[CacheManager] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    rate_limiter_config: Optional[Dict[str, tuple]] = None,
)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `cache_manager` | `CacheManager` | 缓存管理器实例，默认通过 `get_cache_manager()` 获取 |
| `max_workers` | `int` | 并发线程数，默认 4 |
| `batch_size` | `int` | 批量大小，默认 50 |
| `rate_limiter_config` | `Dict[str, tuple]` | 域名限速配置 |

**初始化流程**:
1. 确保目录存在（`paths.ensure_dirs()`）
2. 从旧版 `akshare_registry.yaml` 加载接口定义
3. 从 `rate_limits.yaml` 加载限速配置
4. 从 `download_priority.yaml` 加载下载优先级配置
5. 初始化 `TaskBuilder`、`DomainRateLimiter`

### 1.4 下载方法

#### download_incremental() — 增量下载

```python
download_incremental(
    stock_list: Optional[List[str]] = None,
    start: Optional[str] = None,
    days_back: int = 1,
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]
```

按接口定义中是否有日期参数筛选出增量接口（最多 10 个），构建任务并执行。

| 参数 | 说明 |
|------|------|
| `stock_list` | 股票列表 |
| `start` | 指定开始日期 |
| `days_back` | 回溯天数，默认 1 天 |
| `progress_callback` | 进度回调函数 |

#### download_full() — 全量下载

```python
download_full(
    interfaces: Optional[List[str]] = None,
    start: str = "2020-01-01",
    end: Optional[str] = None,
    force: bool = False,
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]
```

基于注册表配置的全量下载，默认最多取前 20 个接口。

#### _execute_tasks() — 执行任务

```python
_execute_tasks(
    tasks: List[DownloadTask],
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]
```

使用 `ThreadPoolExecutor(max_workers)` 并发执行任务，通过 `TaskExecutor` 和 `ProgressTracker` 跟踪进度。返回汇总统计，包含 `success_count`、`failed_count`、`failed_stocks`（最多 20 条）。

### 1.5 静态工具方法

| 方法 | 说明 |
|------|------|
| `_get_stock_list_static()` | 调用 `stock_zh_a_spot_em` 获取 A 股代码列表（前 100 个） |
| `_get_symbol_list_static(category)` | 按类别获取代码：index→`stock_zh_index_spot_em`, fund→`fund_etf_spot_em`, futures→`futures_main_sina`（前 50 个） |

### 1.6 DownloadTask 数据类

**文件**: `src/akshare_data/offline/downloader/task_builder.py`

```python
DownloadTask(
    interface: str,           # 接口名称
    func: str,                # AkShare 函数名
    table: str,               # 缓存表名
    kwargs: Dict[str, Any],   # 调用参数
    rate_limit_key: str = "default",
    primary_key: Optional[List[str]] = None,
)
```

### 1.7 DomainRateLimiter

**文件**: `src/akshare_data/offline/downloader/rate_limiter.py`

基于固定间隔的域名级限速器，线程安全。

```python
DomainRateLimiter(intervals: Dict[str, float])
```

| 方法 | 说明 |
|------|------|
| `wait(key="default")` | 等待至满足限速间隔后返回 |

限速间隔从 `rate_limits.yaml` 读取，例如 `em_push2his: 0.5s`, `sina_hq: 0.3s`, `default: 0.5s`。

### 1.8 工具函数

| 函数 | 文件 | 说明 |
|------|------|------|
| `validate_ohlcv_data(df)` | `downloader/utils.py` | 检查必需列（open/high/low/close）是否存在且非空 |
| `convert_wide_to_long(ohlcv_dict, symbols)` | `downloader/utils.py` | 将宽格式 OHLCV 转为长格式 |

---

## 2. APIProber — 接口探测器

**文件**: `src/akshare_data/offline/prober/prober.py`

接口探测器用于并发审计 AkShare 所有接口的可用性，自动发现函数、智能推断参数、带重试和符号回退机制调用，并生成健康报告。

### 2.1 常量

| 常量 | 值 | 说明 |
|------|-----|------|
| `MAX_WORKERS` | `64` | 最大并发工作线程数 |
| `DOMAIN_CONCURRENCY_DEFAULT` | `3` | 每个域名的默认并发数 |
| `DELAY_BETWEEN_CALLS` | `1.0` | 调用间隔（秒） |
| `TIMEOUT_LIMIT` | `20` | 超时判定阈值（秒） |
| `DEFAULT_STABLE_TTL` | `30 * 24 * 3600` | 稳定接口默认 TTL（30 天） |
| `SYMBOL_FALLBACKS` | `["000001", "sh000001", "USD", "1.0"]` | 符号回退列表 |
| `SIZE_LIMIT_PARAMS` | `["limit", "count", "top", ...]` | 数据量限制参数名 |

### 2.2 ValidationResult 数据类

**文件**: `src/akshare_data/offline/prober/task_builder.py`

```python
@dataclass
class ValidationResult:
    func_name: str
    domain_group: str
    status: str          # Success / Success (Empty) / Failed / Failed (Timeout)
    error_msg: str
    exec_time: float
    data_size: int
    last_check: float
    check_count: int
```

### 2.3 构造函数

```python
APIProber(mode="run")
```

**初始化流程**:
1. 确保目录存在
2. 初始化 `CheckpointManager`（检查点管理）、`SampleManager`（样本管理）、`TaskBuilder`（任务构建）、`TaskExecutor`（任务执行）
3. 从 `akshare_registry.yaml` 的 `probe` 段加载探测配置
4. 初始化域名信号量

### 2.4 核心方法

#### run_check() — 主健康检查循环

```python
run_check() -> Dict[str, ValidationResult]
```

**执行流程**:
1. 通过 `TaskBuilder.build_tasks()` 构建任务列表
2. 使用 `ThreadPoolExecutor(max_workers=64)` 并发执行
3. 每个任务通过 `_run_single_task()` 执行
4. 结果保存至 `CheckpointManager`
5. 完成后记录总耗时

#### get_smart_kwargs() — 智能参数推断

```python
get_smart_kwargs(func: Callable) -> Dict[str, Any]
```

优先级：
1. 从配置中读取（`probe.params`）
2. 从函数签名中提取默认值
3. 对 `SIZE_LIMIT_PARAMS` 中的参数设为 `1`（限制数据量）

#### discover_interfaces() — 发现所有接口函数

```python
discover_interfaces() -> List[Callable]
```

遍历 `akshare` 模块所有公开函数，跳过 `__dir__`, `__getattr__`, `update_all_data`, `version` 等内部函数。

#### should_skip() — TTL 跳过逻辑

```python
should_skip(func_name: str) -> Tuple[bool, str]
```

跳过条件：
1. 配置中 `probe.skip` 为 True → `"Manual Skip"`
2. 上次检查状态为 Success 且未超过 TTL → `"TTL Fresh"`

#### call_with_retry() — 带重试的调用

```python
call_with_retry(func: Callable, kwargs: Dict[str, Any]) -> Tuple[Any, str]
```

委托给 `TaskExecutor._call_with_retry()`，对 symbol 类错误进行暴力回退尝试。

#### generate_report() — 生成健康报告

```python
generate_report()
```

生成 Markdown 表格格式的健康报告至 `reports/health_report.md`，包含函数名、域名、状态、执行时间。

#### generate_full_config() — 生成完整探测配置

```python
generate_full_config()
```

扫描前 10 个接口，生成智能参数和跳过配置，输出至 `config/health_config_generated.json`。

### 2.5 检查点与样本管理

| 组件 | 文件 | 说明 |
|------|------|------|
| `CheckpointManager` | `prober/checkpoint.py` | 序列化/反序列化 `ProbeResult`，支持跳过逻辑 |
| `SampleManager` | `prober/samples.py` | 保存探测成功的样本数据至 CSV |

### 2.6 命令行用法

```bash
# 运行健康检查
python -m akshare_data.offline.prober
```

---

## 3. DataQualityChecker — 数据质量检查器

**文件**: `src/akshare_data/offline/quality.py`

数据质量检查器提供完整性检查、异常值检测、跨源一致性比对等功能。

### 3.1 构造函数

```python
DataQualityChecker(cache_manager: Optional[CacheManager] = None)
```

### 3.2 检查方法

#### check_completeness() — 完整性检查

```python
check_completeness(
    table: str,
    symbol: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    expected_trading_days: Optional[List[str]] = None,
) -> Dict[str, Any]
```

通过 `cache_manager.read()` 读取数据，检查缺失日期和缺失字段。

**返回报告**:
```python
{
    "has_data": bool,
    "total_records": int,
    "missing_dates_count": int,
    "completeness_ratio": float,
    "is_complete": bool,
    "missing_dates": List[str],    # 最多 100 条
    "missing_fields": List[str],   # 可选
}
```

#### check_anomalies() — 异常值检测

```python
check_anomalies(
    df: pd.DataFrame,
    price_change_threshold: float = 20.0,
    volume_change_threshold: float = 10.0,
) -> Dict[str, Any]
```

检测三类异常：

| 异常类型 | 检测逻辑 |
|----------|----------|
| 价格异常 | `abs(pct_chg) > price_change_threshold`（默认 20%） |
| 高低异常 | `high < low` |
| 成交量异常 | 成交量 Z-Score >= `volume_change_threshold`（默认 10） |

**返回报告**:
```python
{
    "total_rows": int,
    "anomaly_count": int,
    "anomalies": List[Dict],    # 最多 50 条
    "price_anomalies": List[Dict],
    "volume_anomalies": List[Dict],
    "high_low_anomalies": List[Dict],
}
```

#### check_consistency() — 跨源一致性比对

```python
check_consistency(
    table1: str,
    table2: str,
    symbol: str,
) -> Dict[str, Any]
```

比较两个表中同一 symbol 的数据覆盖范围。

#### generate_report() — 综合质量报告

```python
generate_report(
    table: str,
    symbol: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]
```

组合完整性检查和异常检测。评分逻辑：
- 完整且无异常 → `100.0`
- 完整性 > 80% → `75.0`
- 其他 → `62.5`

### 3.3 QualityChecker — 兼容性类

```python
class QualityChecker:
    @staticmethod
    def check_daily_completeness(df, expected_days) -> Dict[str, Any]
    @staticmethod
    def detect_anomalies(df) -> List[str]
```

静态方法接口，用于快速检查。`detect_anomalies()` 检测涨跌幅 > 20% 和 high < low。

---

## 4. Reporter — 报告生成器

**文件**: `src/akshare_data/offline/reporter.py`

报告生成器将健康探测、接口分类、数据量统计等结果格式化为 Markdown 报告。

### 4.1 报告路径

| 常量 | 路径 |
|------|------|
| `HEALTH_REPORT_FILE` | `reports/health_report.md` |
| `QUALITY_REPORT_FILE` | `reports/quality_report.md` |
| `VOLUME_REPORT_FILE` | `reports/volume_report.md` |

### 4.2 报告方法

| 方法 | 说明 |
|------|------|
| `generate_health_report(results)` | 生成健康审计报告：API 总数、可用 API 数及比率、最慢 20 个接口 |
| `generate_quality_report(df)` | 生成接口分类报告：总接口数、数据接口数、分类分布、缓存策略建议 |
| `generate_volume_report(df)` | 生成数据量报告：总行数、总内存、按分类统计、Top 20 最大接口 |

### 4.3 工具方法

| 方法 | 说明 |
|------|------|
| `to_md(df)` | DataFrame 转 Markdown 表格，自动格式化日期和时间列 |
| `save_json(data, output_path)` | 保存字典为 JSON 文件 |
| `generate_summary(probe_results)` | 生成简短摘要字符串 |
| `integrate_with_summary(total, available, rate, avg_time)` | 将健康度信息写入 `reports/final_summary.txt` |

---

## 5. AccessLogger — 访问日志记录器

**文件**: `src/akshare_data/offline/access_logger.py`

异步访问日志记录器，为在线模块的每次 API 调用记录一行 JSON 日志。

### 5.1 特性

- 异步批量刷盘，不阻塞 API 请求
- 按天轮转，保留 N 天
- 线程安全

### 5.2 构造函数

```python
AccessLogger(
    log_dir: str = "logs",
    max_buffer: int = 100,
    flush_interval: float = 5.0,
    backup_days: int = 30,
)
```

### 5.3 日志格式

每行一条 JSON：

```json
{"ts": "2024-01-15T10:30:00.123", "interface": "equity_daily", "symbol": "000001", "cache_hit": false, "latency_ms": 450, "source": "akshare_em"}
```

| 字段 | 说明 |
|------|------|
| `ts` | ISO 格式时间戳 |
| `interface` | 接口名称 |
| `symbol` | 股票代码（可选） |
| `cache_hit` | 是否命中缓存 |
| `latency_ms` | 延迟（毫秒） |
| `source` | 数据源（可选） |

### 5.4 核心方法

| 方法 | 说明 |
|------|------|
| `record(interface, symbol, cache_hit, latency_ms, source)` | 非阻塞记录，队列满时丢弃并警告 |
| `shutdown()` | 设置停止事件，等待工作线程完成，最后刷盘一次 |

### 5.5 后台刷盘

- `_flush_loop()`: 后台线程循环，按 `flush_interval` 间隔刷盘
- `_flush()`: 将队列中所有条目写入日志文件
- `_rotate()`: 跨天时重命名旧文件为 `access.log.YYYY-MM-DD`
- `_cleanup_old_logs()`: 清理超过 `backup_days` 的日志文件

---

## 6. CallStatsAnalyzer — 调用统计分析器

**文件**: `src/akshare_data/offline/analyzer/access_log/stats.py`

日志分析器，读取 AccessLogger 生成的访问日志，分析调用统计并生成下载优先级配置。

### 6.1 构造函数

```python
CallStatsAnalyzer(
    log_dir: Optional[str] = None,
    output_path: Optional[str] = None,
)
```

默认日志目录为 `paths.logs_dir`，默认输出路径为 `paths.priority_file`。

### 6.2 核心方法

#### analyze() — 分析并生成配置

```python
analyze(window_days: int = 7) -> Dict
```

**处理流程**:
1. `_read_logs(window_days)` — 读取最近 N 天的日志文件
2. `_aggregate(entries)` — 按 `{interface}:{symbol}` 聚合统计
3. `_score(aggregated)` — 计算优先级分数
4. `_rank(scored)` — 按分数排序生成排名
5. `_build_config(ranked, entries, window_days)` — 构建最终配置
6. `_save(config)` — 保存为 YAML 文件

#### 评分算法

```python
score = call_count_normalized * 0.4 + miss_rate * 0.3 + recency_decay * 0.3
```

| 因子 | 权重 | 说明 |
|------|------|------|
| `call_count_normalized` | 0.4 | 归一化调用次数（相对于最大调用数） |
| `miss_rate` | 0.3 | 缓存未命中率 |
| `recency_decay` | 0.3 | 时间衰减因子 |

#### 时间衰减因子

```python
recency_decay = Σ(e^(-λ × days_ago)) / count
```

其中 `λ = 0.5`，越近的调用权重越高。

#### 策略推荐

| 条件 | 模式 | 频率 |
|------|------|------|
| `miss_rate > 0.5` 且 `call_count > 50` | incremental | hourly |
| `miss_rate > 0.3` 或 `score > 60` | incremental | daily (15:30) |
| `score > 30` | full | weekly |
| 其他 | full | monthly |

### 6.3 配置输出格式

```yaml
generated_at: "2024-01-15T10:30:00"
window: "7d"
priorities:
  equity_daily:
    score: 85.5
    rank: 1
    call_count_7d: 1500
    miss_rate_7d: 0.45
    avg_latency_ms: 320.5
    symbols:
      - code: "000001"
        calls: 200
        misses: 90
    recommendation:
      mode: incremental
      frequency: daily
      time: "15:30"
global:
  total_calls_7d: 5000
  total_misses_7d: 2000
  overall_miss_rate: 0.4
```

### 6.4 命令行用法

```bash
python -m akshare_data.offline.analyzer.access_log.stats --window 7
```

---

## 7. Scanner + Registry — 扫描与注册表管理

### 7.1 AkShareScanner — 模块扫描器

**文件**: `src/akshare_data/offline/scanner/akshare_scanner.py`

扫描 akshare 模块，提取所有公开函数的签名、文档、模块信息。

```python
AkShareScanner().scan_all() -> Dict[str, Dict[str, Any]]
```

跳过 `__dir__`, `__getattr__`, `update_all_data`, `version` 等内部函数。

### 7.2 推断组件

| 组件 | 文件 | 说明 |
|------|------|------|
| `DomainExtractor` | `scanner/domain_extractor.py` | 从源码中正则提取 URL 域名 |
| `CategoryInferrer` | `scanner/category_inferrer.py` | 基于函数名前缀推断分类（equity/fund/index/futures 等） |
| `ParamInferrer` | `scanner/param_inferrer.py` | 智能推断探测参数（limit→1, start_date→3天前等） |

### 7.3 RegistryBuilder — 注册表构建器

**文件**: `src/akshare_data/offline/registry/builder.py`

组合 Scanner 和推断组件，构建完整注册表。

```python
RegistryBuilder().build(scan_results=None) -> Dict[str, Any]
```

输出格式：
```python
{
    "version": "2.0",
    "generated_at": str,
    "description": str,
    "interfaces": {name: interface_def, ...},
    "domains": {domain: {"rate_limit_key": ...}, ...},
    "rate_limits": {...},
}
```

每个接口定义包含：name, category, description, signature, domains, rate_limit_key, sources, probe。

### 7.4 注册表管理组件

| 组件 | 文件 | 说明 |
|------|------|------|
| `RegistryMerger` | `registry/merger.py` | 从旧版 interfaces.yaml/rate_limits.yaml 合并手工定义的字段 |
| `RegistryExporter` | `registry/exporter.py` | 导出为 YAML 或 JSON 格式 |
| `RegistryValidator` | `registry/validator.py` | 验证注册表格式正确性 |

---

## 工具间协作关系

```
AkShare 模块 ──扫描──► AkShareScanner + 推断组件
                            │
                     RegistryBuilder
                            │
                   akshare_registry.yaml
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
    APIProber         BatchDownloader   QualityChecker
    接口健康探测        批量下载数据       数据质量审计
          │                 │                 │
          ▼                 ▼                 ▼
    health_report      共享缓存          quality_report
    health_state       (DuckDB/Parquet)
                            │
                            ▼
                     在线服务模块 (API)
                            │
                            ▼
                     AccessLogger
                     记录访问日志
                            │
                            ▼
                     CallStatsAnalyzer
                     分析日志生成优先级
                            │
                     download_priority.yaml
                            │
                            ▼
                     BatchDownloader (按优先级)
```

## 快速使用示例

### 批量下载

```python
from akshare_data.offline import BatchDownloader

downloader = BatchDownloader(max_workers=8)

# 增量下载（最近 1 天）
result = downloader.download_incremental(days_back=1)

# 全量下载
result = downloader.download_full(interfaces=["equity_daily"], start="2024-01-01")
```

### 接口健康探测

```python
from akshare_data.offline import APIProber

prober = APIProber()
prober.run_check()
```

### 数据质量检查

```python
from akshare_data.offline import DataQualityChecker

checker = DataQualityChecker()
report = checker.check_completeness("stock_daily", symbol="000001")
report = checker.check_anomalies(df)
```

### 访问日志分析

```python
from akshare_data.offline import CallStatsAnalyzer

analyzer = CallStatsAnalyzer()
config = analyzer.analyze(window_days=7)
```

### 生成报告

```python
from akshare_data.offline import Reporter

reporter = Reporter()
report = reporter.generate_health_report(results)
report = reporter.generate_quality_report(classification_df)
report = reporter.generate_volume_report(volume_df)
```
