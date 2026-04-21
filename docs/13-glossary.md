# Glossary

Key terms and concepts used throughout the akshare-data-service documentation.

---

## A

### Aggregation
The process of merging multiple small Parquet files into larger consolidated files to improve DuckDB query performance. Controlled by `aggregation_enabled` and `compaction_threshold` in `TableConfig`.

### aggregation_enabled
A boolean flag on `TableConfig` / `CacheTable` that determines whether the Aggregator should compact small files for this table. Default: `True`.

### AkShare
An open-source Python library that provides free access to Chinese financial market data. In akshare-data-service, AkShare is the **backup data source** (priority 2), driven entirely by YAML configuration.

### AkShareAdapter
The data source adapter (`sources/akshare_source.py`) that wraps AkShare. Uses a configuration-driven thin dispatcher pattern with dynamic `__getattr__` routing, avoiding the need to write 100+ wrapper methods.

### APIProber
An offline tool that concurrently audits AkShare interface availability, generates health reports, and persists probe state for TTL-based skip logic. Located in `src/akshare_data/offline/prober/`.

### AtomicWriter
A Parquet writer (`store/parquet.py`) that writes to a `.tmp` file first, then atomically renames it via `os.replace()`. This prevents data corruption if the process is interrupted mid-write.

---

## B

### BaoStock
An optional data source providing Chinese stock data. Install with `pip install -e ".[baostock]"`. Accessed through AkShareAdapter's dynamic routing.

### BatchDownloader
An offline tool (`offline/downloader/`) that downloads data from AkShare in bulk using concurrent threads, rate limiting, and incremental/full modes.

---

## C

### Cache-First
The core design principle of akshare-data-service: all data requests check the cache before making network calls. The flow is: check memory → check Parquet → query DuckDB → fetch from source → write to cache → return. Data sources know nothing about caching; the API layer owns cache strategy.

### CacheConfig
A dataclass (`core/config.py`) that holds all top-level cache configuration: directory paths, memory cache settings, DuckDB settings, compression, logging, and per-table overrides.

### CacheManager
The unified entry point for all cache read/write operations (`store/manager.py`). Combines MemoryCache, DuckDBEngine, AtomicWriter, and PartitionManager. Implements a singleton pattern.

### CacheTable
An immutable dataclass representing a cache table's schema definition. Fields include: `name`, `partition_by`, `ttl_hours`, `schema`, `primary_key`, `aggregation_enabled`, `compaction_threshold`, `priority`, `storage_layer`.

### CachedFetcher
The cache-first execution engine (`store/fetcher.py`) that takes a `FetchConfig` and a fetch function, infers the appropriate strategy (incremental or full), reads cache, decides whether to fetch, and returns merged results.

### Circuit Breaker
A fault-tolerance pattern implemented by `SourceHealthMonitor`. After a data source fails consecutively `error_threshold` (default: 5) times, the circuit opens and the source is skipped for `disable_duration_seconds` (default: 300 seconds), after which it auto-recovers.

### compaction_threshold
The number of small Parquet files in a partition that triggers the Aggregator to merge them into a single larger file. Default: `20`. Higher thresholds delay compaction but may slow query performance.

### Configuration-driven
The design pattern where AkShare interface definitions (function names, parameter mappings, column mappings) are specified in YAML files rather than hardcoded in Python. New interfaces can be added without modifying source code.

---

## D

### DataSource
The abstract base class (`core/base.py`) that all data source adapters must implement. Defines ~90 methods including `get_daily_data`, `get_minute_data`, `get_finance_indicator`, etc. Subclasses: `LixingerAdapter`, `AkShareAdapter`, `TushareAdapter`, `MockSource`.

### DataSourceAdapter
A concrete implementation of `DataSource` for a specific data provider. Examples: `LixingerAdapter` (primary), `AkShareAdapter` (backup), `TushareAdapter` (optional).

### Domain Rate Limiting
Per-API-domain request throttling. Each domain (e.g., `push2his.eastmoney.com`) has an independent minimum request interval configured in `rate_limits.yaml`. Prevents upstream APIs from blocking the service.

### DuckDB
An in-process SQL OLAP database engine used to query Parquet files. Configurable with `duckdb_threads` (default: 4) and `duckdb_memory_limit` (default: `4GB`). Uses thread-local connections for thread safety.

### DuckDBEngine
The SQL query wrapper (`store/duckdb.py`) over Parquet files. Supports WHERE/ORDER BY/LIMIT/aggregation. Queries the aggregated layer first, falls back to raw files if no data found.

---

## E

### ExecutionResult
A dataclass returned by `MultiSourceRouter.execute()` containing: `success`, `data`, `source`, `error`, `attempts`, `error_details`, `is_empty`, `is_fallback`, `sources_tried`.

---

## F

### Failover
The automatic switching from a failed data source to the next available source in priority order. Implemented by `MultiSourceRouter` which tries sources sequentially, skipping any that are circuit-broken, until one returns valid data.

### FetchConfig
A dataclass (`store/fetcher.py`) that specifies how a data fetch should be executed: `table`, `storage_layer`, `strategy`, `partition_by`, `partition_value`, `date_col`, `interface_name`, `filter_keys`.

### FullCacheStrategy
A cache strategy for non-time-series data (meta/snapshot tables). Logic: if cache is empty, fetch all; if cache exists, return it. New data replaces old data entirely (not merged). Used for tables like `securities`, `industry_list`, `spot_snapshot`.

---

## I

### IncrementalStrategy
A cache strategy for time-series data (daily tables). Logic: read existing data → detect missing date ranges → fetch only the missing ranges → merge with cached data. Used for tables like `stock_daily`, `index_daily`, `north_flow`.

### IncrementalStrategy vs FullCacheStrategy
| Aspect | IncrementalStrategy | FullCacheStrategy |
|--------|---------------------|-------------------|
| Data type | Time-series | Static/snapshot |
| Missing detection | Yes (date ranges) | No |
| Merge behavior | Append + deduplicate | Replace entirely |
| Trigger | Has `start_date`/`end_date` params | No date params |

---

## J

### jq_code
JoinQuant-format symbol code. Format: `600519.XSHG` (Shanghai) or `000001.XSHE` (Shenzhen). The `.XSHG`/`.XSHE` suffix identifies the exchange. Converted to/from other formats via `jq_code_to_ak()`, `ak_code_to_jq()`, etc.

---

## L

### Lixinger
A commercial financial data API service. In akshare-data-service, Lixinger is the **primary data source** (priority 1), providing high-quality financial, valuation, and shareholder data. Requires `LIXINGER_TOKEN` environment variable.

### LixingerAdapter
The data source adapter (`sources/lixinger_source.py`) for Lixinger's OpenAPI. Implements 80+ methods. Does NOT support minute data, money flow, north flow, or real-time quotes (raises `NotImplementedError`).

### LixingerClient
The HTTP client for Lixinger's OpenAPI (`sources/lixinger_client.py`). Implements singleton pattern, automatic retry (3 attempts with backoff), and structured logging.

---

## M

### MemoryCache
L1 cache layer based on `cachetools.TTLCache`. Thread-safe with `threading.Lock`. Default: max 5000 items, 3600-second TTL. LRU eviction when full. Returns copies of cached DataFrames to prevent external mutation.

### MultiSourceRouter
The request router (`sources/router.py`) that manages multiple data sources. Implements automatic failover, health monitoring via `SourceHealthMonitor`, and domain-based rate limiting.

---

## O

### OHLCV
Standard financial data fields: **O**pen, **H**igh, **L**ow, **C**lose, **V**olume. The core fields in daily and minute candlestick data. Often extended with **A**mount (trading value) to form OHLCVA.

---

## P

### Parquet
A columnar storage file format optimized for analytical queries. Used as the L2 (disk) cache in akshare-data-service. Supports compression (default: `snappy`), partitioned writes, and direct SQL querying via DuckDB.

### partition_by
A configuration field on `TableConfig` / `CacheTable` that specifies which column is used to partition Parquet files on disk. For `stock_daily`, this is `date`. Files are stored as `{table}/date=YYYY-MM-DD/part_*.parquet`. When `None`, all data is stored in a single unpartitioned directory.

### partition_value
The specific value of the `partition_by` column for a given write or read operation. For example, when writing `stock_daily` data for `2024-01-15`, the `partition_value` is `"2024-01-15"`.

### primary_key
A list of column names that uniquely identify a row in a cache table. Used for deduplication during Parquet writes (`drop_duplicates(subset=primary_key, keep="last")`).

---

## S

### Schema Registry
The `TableRegistry` singleton (`core/schema.py`) that holds all `CacheTable` definitions. Provides methods: `register()`, `get()`, `list_all()`, `list_by_priority()`, `list_by_layer()`. Initialized by `init_schemas()`.

### SourceHealthMonitor
The circuit breaker component. Tracks consecutive failures per source. Opens the circuit after 5 failures, auto-recovers after 300 seconds.

### SourceProxy
A dynamic proxy in `api.py` that allows specifying a data source for queries. Enables patterns like `service._get_source("lixinger").get_daily_data(...)`.

### storage_layer
A classification of cache tables into four categories:

| Layer | Purpose | Typical TTL |
|-------|---------|-------------|
| `daily` | Time-series daily data (OHLCV, financials, money flow) | Permanent (0 = no expiry) |
| `minute` | Intraday minute-level data | Permanent |
| `meta` | Static/reference data (securities list, trade calendar, industry list) | Permanent |
| `snapshot` | Real-time or near-real-time snapshots | 168 hours (7 days) |

The storage layer determines the directory structure under `cache/` and influences the cache strategy (daily → IncrementalStrategy, meta → FullCacheStrategy).

### Symbol Normalization
The process of converting various symbol formats into a canonical 6-digit numeric format. Handled by `format_stock_symbol()` and related functions in `core/symbols.py`.

### Tushare
An optional data source for Chinese financial data. Requires a separate token and installation via `pip install -e ".[tushare]"`. Priority 3 (lowest).

### ts_code
Tushare-format symbol code. Format: `600000.SH` (Shanghai) or `000001.SZ` (Shenzhen). The `.SH`/`.SZ` suffix identifies the exchange. Converted via `ts_code_to_jq()`.

### TTL (Time To Live)
The duration after which a cached item is considered expired and will be refreshed on next access. Applied at two levels:
- **Memory cache TTL**: Default 3600 seconds (1 hour), configurable via `AKSHARE_DATA_CACHE_TTL_SECONDS`.
- **Table-level TTL**: Defined in `TableConfig.ttl_hours` (0 = permanent). Used for snapshot tables (e.g., 168 hours = 7 days).

---

## U

### Uniform API
Two calling styles provided by akshare-data-service:
1. **Convenience functions**: `from akshare_data import get_daily` — module-level functions, ready to use out of the box.
2. **Namespace API**: `service.cn.stock.quote.daily(...)` — structured, category-based access for discoverability.

---

## Additional Terms

| Term | Definition |
|------|-----------|
| `ak_code` | AkShare-format symbol code (e.g., `sh600519`, `sz000001`) |
| `CacheStrategy` | Abstract base class defining `should_fetch()`, `merge()`, and `build_where()` |
| `DataSourceError` | Exception raised when a data source operation fails |
| `DataAccessException` | Base exception class for all data access errors |
| `EmptyDataPolicy` | Strategy for handling empty responses: `STRICT`, `RELAXED`, or `BEST_EFFORT` |
| `ErrorCode` | Enum with 177 standardized error codes across 9 categories |
| `FetchConfig` | Configuration for a single cache-fetch operation |
| `filter_keys` | Parameters used to build cache WHERE conditions (excludes date/adjust/source) |
| `JQLogAdapter` | JoinQuant-style logging adapter with structured output |
| `StatsCollector` | Thread-safe singleton that collects request and cache hit/miss statistics |
| `TableConfig` | Per-table cache configuration (partition_by, ttl_hours, etc.) |
| `TableInfo` | Runtime metadata about a cache table's physical state (file count, size, etc.) |
