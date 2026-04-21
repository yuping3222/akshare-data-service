# Configuration Reference

This document provides a comprehensive reference for all YAML configuration files in the `config/` directory.

> For a high-level overview of the project architecture and core features, see [01-Overview](01-overview.md). For API usage and function signatures, see [02-API Reference](02-api-reference.md).

---

## Directory Structure

```
config/
├── akshare_registry.yaml           # AkShare interface registry (master)
├── rate_limits.yaml                # Domain rate limiting
├── schemas.yaml                    # Schema registry configuration
├── system.yaml                     # System-level configuration
├── interfaces/                     # Interface definitions by category
│   ├── equity.yaml                 # Equity/stock interfaces
│   ├── index.yaml                  # Index interfaces
│   ├── fund.yaml                   # Fund/ETF interfaces
│   ├── bond.yaml                   # Bond interfaces
│   ├── options.yaml                # Options interfaces
│   ├── futures.yaml                # Futures interfaces
│   └── macro.yaml                  # Macro economic interfaces
├── sources/
│   ├── domains.yaml                # Domain-to-rate-limit-key mapping
│   ├── failover.yaml               # Failover/circuit breaker strategy
│   └── sources.yaml                # Data source definitions
├── download/
│   ├── priority.yaml               # Download priority (generated from logs)
│   └── schedule.yaml               # Scheduled download tasks
├── prober/
│   ├── config.yaml                 # API probe configuration
│   ├── state.json                  # Probe checkpoint (auto-generated)
│   └── samples/                    # Probe sample data
├── domains/
│   └── by_domain.yaml              # Domain grouping configuration
├── logging/
│   └── access.yaml                 # Access logging configuration
├── fields/
│   └── mappings/                   # Interface field mapping files
├── field_mappings/                 # Legacy field mapping configuration
├── registry/                       # Registry intermediate files
└── generated/                      # Auto-generated files
    ├── field_mappings/
    ├── health_samples/
    ├── registry_raw/
    └── reports/
```

---

## 1. akshare_registry.yaml — AkShare Interface Registry

**File**: `config/akshare_registry.yaml`

**Purpose**: The master registry that defines all AkShare interfaces, their data sources, input/output column mappings, rate limit keys, and probe configuration. This is the central configuration that drives the AkShare adapter's dynamic routing.

**How it is loaded**: Read by `src/akshare_data/sources/akshare/fetcher.py` with caching via `config_cache.py`. Also consumed by `BatchDownloader`, `APIProber`, and the offline `config generate/validate/merge` commands.

**Structure**:

```yaml
version: "2.0"
generated_at: "2026-04-20T10:00:00"
description: "AkShare interface registry"

interfaces:
  equity_daily:
    category: equity
    description: "A-share daily OHLCV data"
    sources:
      - name: east_money
        func: stock_zh_a_hist
        enabled: true
        input_mapping:
          symbol: symbol
          start_date: start_date
          end_date: end_date
          period: "daily"
          adjust: adjust
        output_mapping:
          日期: datetime
          开盘: open
          最高: high
          最低: low
          收盘: close
          成交量: volume
          成交额: amount
        column_types:
          datetime: datetime64[ns]
          open: float64
          high: float64
          low: float64
          close: float64
          volume: float64
          amount: float64
    rate_limit_key: em_push2his
    probe:
      skip: false
      ttl: 86400

  # ... more interfaces

domains:
  eastmoney_push2his:
    url_pattern: "push2his.eastmoney.com"
    rate_limit_key: em_push2his

rate_limits:
  em_push2his:
    interval: 0.5
  default:
    interval: 0.3
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `version` | string | `"2.0"` | Registry schema version |
| `interfaces.<name>` | object | — | Interface definition key |
| `interfaces.<name>.category` | string | — | Category: equity/index/fund/bond/options/futures/macro |
| `interfaces.<name>.sources[]` | array | — | List of data source backends |
| `interfaces.<name>.sources[].name` | string | — | Source identifier (east_money/sina/tushare/baostock) |
| `interfaces.<name>.sources[].func` | string | — | AkShare function name to call |
| `interfaces.<name>.sources[].enabled` | bool | `true` | Whether this source is active |
| `interfaces.<name>.sources[].input_mapping` | object | — | Maps API params to AkShare function params |
| `interfaces.<name>.sources[].output_mapping` | object | — | Maps output column names (CN → EN) |
| `interfaces.<name>.sources[].column_types` | object | — | Column type coercions |
| `interfaces.<name>.rate_limit_key` | string | `default` | Rate limiter key |
| `interfaces.<name>.probe.skip` | bool | `false` | Skip during probing |
| `interfaces.<name>.probe.ttl` | int | `86400` | Probe result TTL in seconds |

---

## 2. rate_limits.yaml — Domain Rate Limiting

**File**: `config/rate_limits.yaml`

**Purpose**: Defines per-domain minimum request intervals to prevent being blocked by upstream APIs. Used by `DomainRateLimiter` in both the online `MultiSourceRouter` and offline `BatchDownloader`.

**How it is loaded**: Read at startup by `DomainRateLimiter` class in `src/akshare_data/sources/router.py`. Values are keyed by `rate_limit_key` referenced in `akshare_registry.yaml`.

**Structure**:

```yaml
em_push2his:
  interval: 0.5          # minimum seconds between requests
  max_requests_per_minute: 120

sina_hq:
  interval: 0.3

tushare_api:
  interval: 1.0
  max_requests_per_minute: 60

default:
  interval: 0.3
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `<key>.interval` | float | `0.3` | Minimum seconds between requests for this domain |
| `<key>.max_requests_per_minute` | int | — | Optional hard cap on requests per minute |

---

## 3. sources/domains.yaml — Domain Configuration

**File**: `config/sources/domains.yaml`

**Purpose**: Maps URL patterns to rate limit keys. Allows the router to identify which rate limiter to apply based on the actual HTTP endpoint being called.

**How it is loaded**: Parsed by `DomainRateLimiter` alongside `rate_limits.yaml`.

**Structure**:

```yaml
domains:
  eastmoney_push2his:
    url_pattern: "push2his.eastmoney.com"
    rate_limit_key: em_push2his

  sina_hq:
    url_pattern: "hq.sinajs.cn"
    rate_limit_key: sina_hq

  tushare:
    url_pattern: "api.tushare.pro"
    rate_limit_key: tushare_api
```

**Key parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `<domain>.url_pattern` | string | URL pattern or hostname to match |
| `<domain>.rate_limit_key` | string | Key to look up in `rate_limits.yaml` |

---

## 4. sources/failover.yaml — Failover Strategy

**File**: `config/sources/failover.yaml`

**Purpose**: Configures the circuit breaker and failover behavior for `MultiSourceRouter`. Controls when a data source is considered unhealthy and how long it stays disabled.

**How it is loaded**: Read by `SourceHealthMonitor` in `src/akshare_data/sources/router.py` (may also use hardcoded defaults if file is absent).

**Structure**:

```yaml
circuit_breaker:
  error_threshold: 5         # consecutive failures before opening circuit
  disable_duration_seconds: 300   # how long to keep circuit open (5 minutes)

failover:
  empty_data_policy: RELAXED   # STRICT / RELAXED / BEST_EFFORT
  retry_on_empty: true         # try next source if current returns empty data
  validate_columns: true       # validate required columns before accepting data
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `circuit_breaker.error_threshold` | int | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.disable_duration_seconds` | int | `300` | Seconds to keep circuit open before auto-recovery |
| `failover.empty_data_policy` | string | `RELAXED` | How to handle empty data: `STRICT` (reject), `RELAXED` (accept), `BEST_EFFORT` (return whatever) |
| `failover.retry_on_empty` | bool | `true` | Whether to try next source on empty response |
| `failover.validate_columns` | bool | `true` | Whether to validate required columns before accepting |

---

## 5. sources/sources.yaml — Data Source Configuration

**File**: `config/sources/sources.yaml`

**Purpose**: Defines available data sources, their priority order, and source-specific settings (tokens, timeouts, retry counts).

**How it is loaded**: Read by `DataService` and `MultiSourceRouter` during initialization to build the adapter list and priority chain.

**Structure**:

```yaml
sources:
  - name: lixinger
    priority: 1
    type: partial
    enabled: true
    timeout_seconds: 30
    max_retries: 3
    requires_token: true

  - name: akshare
    priority: 2
    type: real
    enabled: true
    timeout_seconds: 20
    max_retries: 3
    data_sources:
      - east_money
      - sina
      - tushare
      - baostock

  - name: tushare
    priority: 3
    type: partial
    enabled: false
    requires_token: true
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sources[].name` | string | — | Source identifier |
| `sources[].priority` | int | — | Priority order (lower = higher priority) |
| `sources[].type` | string | — | `real` (full source) or `partial` (subset of data) |
| `sources[].enabled` | bool | `true` | Whether the source is active |
| `sources[].timeout_seconds` | int | `30` | HTTP request timeout |
| `sources[].max_retries` | int | `3` | Max retry attempts on failure |
| `sources[].requires_token` | bool | `false` | Whether an API token is required |
| `sources[].data_sources` | list | — | Sub-source list for AkShare |

---

## 6. interfaces/*.yaml — Interface Definitions by Category

**Directory**: `config/interfaces/`

**Purpose**: Modular interface definitions organized by asset category. These files can be merged into `akshare_registry.yaml` by the `RegistryBuilder` or used as standalone definitions.

**Files**:

| File | Category | Description |
|------|----------|-------------|
| `equity.yaml` | equity | Stock daily, minute, real-time, margin trading, block deals |
| `index.yaml` | index | Index daily, components, weights, valuation |
| `fund.yaml` | fund | ETF/LOF/FOF daily NAV, fund manager info |
| `bond.yaml` | bond | Convertible bonds, treasury yields, bond spot |
| `options.yaml` | options | Option chains, Greeks, implied volatility |
| `futures.yaml` | futures | Futures daily, spot, main contracts |
| `macro.yaml` | macro | GDP, CPI, PPI, PMI, M2, interest rates, social financing |

**Structure** (example from `equity.yaml`):

```yaml
category: equity
interfaces:
  equity_daily:
    description: "A-share daily OHLCV"
    sources:
      - name: east_money
        func: stock_zh_a_hist
        enabled: true
    rate_limit_key: em_push2his

  equity_minute:
    description: "A-share minute-level data"
    sources:
      - name: east_money
        func: stock_zh_a_min
        enabled: true
    rate_limit_key: em_push2his
```

---

## 7. download/priority.yaml — Download Priority

**File**: `config/download/priority.yaml`

**Purpose**: Generated priority scores for each interface based on access log analysis. Higher scores indicate interfaces that are called more frequently, have higher cache miss rates, or were accessed more recently.

**How it is generated**: Produced by `CallStatsAnalyzer.analyze()` when running `python -m akshare_data.offline.cli analyze logs --window 7`. The scoring algorithm weights call count (40%), miss rate (30%), and recency decay (30%).

**Structure**:

```yaml
generated_at: "2026-04-20T10:30:00"
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

  index_daily:
    score: 62.3
    rank: 2
    call_count_7d: 800
    miss_rate_7d: 0.32
    avg_latency_ms: 280.0
    recommendation:
      mode: incremental
      frequency: daily
      time: "15:30"

global:
  total_calls_7d: 5000
  total_misses_7d: 2000
  overall_miss_rate: 0.4
```

**Key parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `generated_at` | string | ISO timestamp of generation |
| `window` | string | Analysis window (e.g., `"7d"`) |
| `priorities.<name>.score` | float | Priority score (0–100) |
| `priorities.<name>.rank` | int | Rank among all interfaces |
| `priorities.<name>.call_count_7d` | int | Number of calls in window |
| `priorities.<name>.miss_rate_7d` | float | Cache miss ratio |
| `priorities.<name>.recommendation.mode` | string | `incremental` or `full` |
| `priorities.<name>.recommendation.frequency` | string | `hourly`/`daily`/`weekly`/`monthly` |
| `global.total_calls_7d` | int | Total calls across all interfaces |
| `global.overall_miss_rate` | float | Global cache miss ratio |

---

## 8. download/schedule.yaml — Scheduled Download Tasks

**File**: `config/download/schedule.yaml`

**Purpose**: Defines cron-based download schedules for the `BatchDownloader` scheduler. Controls which interfaces are downloaded at what times.

**How it is used**: Read by the scheduler component when running `python -m akshare_data.offline.cli download --schedule`. The scheduler runs continuously, triggering downloads at the specified times.

**Structure**:

```yaml
schedules:
  - name: daily_equity
    interfaces:
      - equity_daily
      - index_daily
      - etf_daily
    mode: incremental
    days_back: 1
    cron: "30 15 * * 1-5"    # weekdays at 15:30
    enabled: true

  - name: weekly_meta
    interfaces:
      - securities
      - trade_calendar
    mode: full
    cron: "0 6 * * 0"        # Sunday at 06:00
    enabled: true

  - name: monthly_macro
    interfaces:
      - macro_china_gdp
      - macro_china_cpi
    mode: full
    cron: "0 8 1 * *"        # 1st of month at 08:00
    enabled: true
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schedules[].name` | string | — | Schedule identifier |
| `schedules[].interfaces` | list | — | List of interface names to download |
| `schedules[].mode` | string | `incremental` | `incremental` or `full` |
| `schedules[].days_back` | int | `1` | Days of data to fetch (incremental mode) |
| `schedules[].cron` | string | — | Cron expression (5-field: M H DoM Mon DoW) |
| `schedules[].enabled` | bool | `true` | Whether this schedule is active |

---

## 9. prober/config.yaml — Probe Configuration

**File**: `config/prober/config.yaml`

**Purpose**: Controls API health probing behavior: which interfaces to probe, concurrency settings, timeout thresholds, symbol fallbacks, and TTL-based skip logic.

**How it is used**: Read by `APIProber` in `src/akshare_data/offline/prober/prober.py`. Also loaded by the CLI `probe --all` command.

**Structure**:

```yaml
prober:
  max_workers: 64
  domain_concurrency: 3
  delay_between_calls: 1.0
  timeout_limit: 20
  stable_ttl: 2592000          # 30 days in seconds

  symbol_fallbacks:
    - "000001"
    - "sh000001"
    - "USD"
    - "1.0"

  size_limit_params:
    - limit
    - count
    - top
    - n
    - page_size

  skip:
    manual:
      - some_deprecated_function

  params:
    stock_zh_a_hist:
      symbol: "000001"
      period: "daily"
      start_date: "2024-01-01"
      end_date: "2024-12-31"
      adjust: "qfq"
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `prober.max_workers` | int | `64` | Max concurrent probe threads |
| `prober.domain_concurrency` | int | `3` | Max concurrent calls per domain |
| `prober.delay_between_calls` | float | `1.0` | Seconds between calls |
| `prober.timeout_limit` | int | `20` | Timeout threshold in seconds |
| `prober.stable_ttl` | int | `2592000` | TTL for stable interfaces (30 days) |
| `prober.symbol_fallbacks` | list | — | Symbols to try if primary fails |
| `prober.skip.manual` | list | — | Interfaces to always skip |
| `prober.params.<func>` | object | — | Default kwargs for probing specific functions |

### prober/state.json — Probe Checkpoint

**File**: `config/prober/state.json`

**Purpose**: Auto-generated checkpoint file that stores the last probe results for each interface. Enables TTL-based skip logic — interfaces probed recently with successful results are skipped on subsequent runs.

**Structure**:

```json
{
  "last_probe_time": "2026-04-20T13:00:59",
  "entries": {
    "stock_zh_a_hist": {
      "status": "Success",
      "exec_time": 0.45,
      "data_size": 250,
      "last_check": 1713600059,
      "check_count": 1
    }
  }
}
```

---

## 10. logging/access.yaml — Logging Configuration

**File**: `config/logging/access.yaml`

**Purpose**: Configures the access logging system for the `AccessLogger` component. Controls log directory, buffer size, flush interval, and backup retention.

**How it is used**: Read by `AccessLogger` in `src/akshare_data/offline/access_logger.py`. Each API call records a JSON line to the access log.

**Structure**:

```yaml
access_log:
  log_dir: "logs"
  max_buffer: 100
  flush_interval: 5.0
  backup_days: 30
  format: json
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `access_log.log_dir` | string | `logs` | Directory for log files |
| `access_log.max_buffer` | int | `100` | Max entries in memory before flush |
| `access_log.flush_interval` | float | `5.0` | Seconds between background flushes |
| `access_log.backup_days` | int | `30` | Days to retain rotated log files |
| `access_log.format` | string | `json` | Log format (`json` or `text`) |

**Log entry format** (each line is a JSON object):

```json
{"ts": "2024-01-15T10:30:00.123", "interface": "equity_daily", "symbol": "000001", "cache_hit": false, "latency_ms": 450, "source": "akshare_em"}
```

---

## 11. fields/mappings/ — Field Mapping Files

**Directory**: `config/fields/mappings/`

**Purpose**: Contains per-interface field mapping definitions that specify how to translate raw API output columns into the standardized internal schema.

**How it is used**: Referenced by the `fetch()` function in `src/akshare_data/sources/akshare/fetcher.py` during data retrieval. Mappings are applied after the AkShare function returns, before the data is written to cache.

**Structure** (example file `config/fields/mappings/equity_daily.yaml`):

```yaml
interface: equity_daily
input_mapping:
  symbol: symbol
  start_date: start_date
  end_date: end_date
output_mapping:
  日期: datetime
  开盘: open
  最高: high
  最低: low
  收盘: close
  成交量: volume
  成交额: amount
  涨跌幅: pct_chg
  涨跌额: change
  换手率: turnover
  振幅: amplitude
column_types:
  datetime: datetime64[ns]
  open: float64
  high: float64
  low: float64
  close: float64
  volume: float64
  amount: float64
  pct_chg: float64
  change: float64
  turnover: float64
  amplitude: float64
```

**Key parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `interface` | string | Interface identifier |
| `input_mapping` | object | Maps API call parameters to function arguments |
| `output_mapping` | object | Maps raw column names to standard names |
| `column_types` | object | Specifies target column types for coercion |

---

## 12. schemas.yaml — Schema Registry Configuration

**File**: `config/schemas.yaml`

**Purpose**: Defines cache table schemas and their storage policies. Maps to the `TableRegistry` in `src/akshare_data/core/schema.py`.

**Structure**:

```yaml
tables:
  stock_daily:
    partition_by: date
    ttl_hours: 0
    aggregation_enabled: true
    compaction_threshold: 20
    priority: P0
    storage_layer: daily

  finance_indicator:
    partition_by: report_date
    ttl_hours: 2160
    aggregation_enabled: true
    compaction_threshold: 5
    priority: P1
    storage_layer: daily

  securities:
    partition_by: null
    ttl_hours: 0
    aggregation_enabled: false
    compaction_threshold: 0
    priority: P2
    storage_layer: meta
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tables.<name>.partition_by` | string/null | `null` | Column used for Parquet partitioning |
| `tables.<name>.ttl_hours` | int | `0` | Cache TTL in hours (0 = never expire) |
| `tables.<name>.aggregation_enabled` | bool | `true` | Whether to enable file compaction |
| `tables.<name>.compaction_threshold` | int | `20` | File count threshold to trigger compaction |
| `tables.<name>.priority` | string | `P0` | Priority level (P0–P3) |
| `tables.<name>.storage_layer` | string | `daily` | Storage layer: `daily`/`minute`/`meta`/`snapshot` |

---

## 13. system.yaml — System Configuration

**File**: `config/system.yaml`

**Purpose**: System-level configuration including cache paths, DuckDB settings, compression, and logging defaults. Maps to the `CacheConfig` dataclass in `src/akshare_data/core/config.py`.

**Structure**:

```yaml
cache:
  base_dir: "./cache"
  daily_dir: "daily"
  minute_dir: "minute"
  snapshot_dir: "snapshot"
  meta_dir: "meta"

  memory_cache_max_items: 5000
  memory_cache_default_ttl_seconds: 3600

  compression: "snappy"
  row_group_size: 100000

  aggregation_enabled: true
  aggregation_schedule: "daily"

  duckdb_read_only: true
  duckdb_threads: 4
  duckdb_memory_limit: "4GB"

  cleanup_retention_hours: 24
  strict_schema: false

logging:
  level: "INFO"
  format: "structured"
  log_file: "logs/akshare_data.log"

source_priority:
  - lixinger
  - akshare
```

**Key parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cache.base_dir` | string | `./cache` | Root cache directory |
| `cache.memory_cache_max_items` | int | `5000` | Max entries in memory cache |
| `cache.memory_cache_default_ttl_seconds` | int | `3600` | Default TTL for memory cache |
| `cache.compression` | string | `snappy` | Parquet compression algorithm |
| `cache.row_group_size` | int | `100000` | Parquet row group size |
| `cache.duckdb_threads` | int | `4` | DuckDB worker threads |
| `cache.duckdb_memory_limit` | string | `4GB` | DuckDB memory cap |
| `cache.cleanup_retention_hours` | int | `24` | Hours to retain expired files |
| `cache.strict_schema` | bool | `false` | Strict schema validation mode |
| `logging.level` | string | `INFO` | Log level |
| `logging.format` | string | `structured` | Log format (`standard`/`json`/`structured`/`simple`) |
| `source_priority` | list | — | Default data source priority order |

---

## Environment Variables

All YAML config values can be overridden via environment variables:

| Environment Variable | Overrides | Default |
|---------------------|-----------|---------|
| `AKSHARE_DATA_CACHE_DIR` | `cache.base_dir` | `./cache` |
| `AKSHARE_DATA_CACHE_MAX_ITEMS` | `cache.memory_cache_max_items` | `5000` |
| `AKSHARE_DATA_CACHE_TTL_SECONDS` | `cache.memory_cache_default_ttl_seconds` | `3600` |
| `AKSHARE_DATA_CACHE_COMPRESSION` | `cache.compression` | `snappy` |
| `AKSHARE_DATA_CACHE_ROW_GROUP_SIZE` | `cache.row_group_size` | `100000` |
| `AKSHARE_DATA_CACHE_DUCKDB_THREADS` | `cache.duckdb_threads` | `4` |
| `AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT` | `cache.duckdb_memory_limit` | `4GB` |
| `AKSHARE_DATA_CACHE_LOG_LEVEL` | `logging.level` | `INFO` |
| `AKSHARE_DATA_CACHE_RETENTION_HOURS` | `cache.cleanup_retention_hours` | `24` |
| `AKSHARE_DATA_CACHE_STRICT_SCHEMA` | `cache.strict_schema` | `false` |
| `LIXINGER_TOKEN` | — | (required for Lixinger source) |

---

## Configuration Loading Order

1. **Built-in defaults** from `CacheConfig` dataclass
2. **YAML config files** from `config/` directory
3. **Environment variables** (highest priority, overrides both)

The `config_cache.py` module provides lazy-loading with caching to avoid repeated YAML reads.
