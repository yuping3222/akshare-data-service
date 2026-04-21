# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- Placeholder for upcoming features.

---

## [0.2.0] — 2026-04-21

Cache-First unified financial data service with Lixinger + AkShare integration.

### Added

#### Core Architecture
- **Cache-First strategy** — All data requests check memory cache (TTLCache) → Parquet files → DuckDB SQL → network source before fetching
- **Three-level caching** — L1: memory (`cachetools.TTLCache`, 5000 items, 3600s TTL), L2: Parquet partitioned files, L3: DuckDB SQL query engine
- **Four-layer architecture** — Entry (`__init__.py` with `__getattr__` forwarding), API (`DataService` in `api.py`), Sources (`sources/`), Store (`store/`), Core (`core/`)

#### Data Sources
- **LixingerAdapter** — Primary data source with 80+ implemented methods covering daily quotes, financials, valuation, shareholders, macro, HK/US stocks, convertible bonds
- **AkShareAdapter** — Configuration-driven thin dispatcher with dynamic `__getattr__` routing; no need to write 100+ wrapper methods
- **MultiSourceRouter** — Automatic failover across Lixinger (primary) → AkShare (backup) → Tushare (optional)
- **Circuit Breaker** (`SourceHealthMonitor`) — Opens after 5 consecutive failures, auto-recovers after 300 seconds
- **DomainRateLimiter** — Per-domain rate limiting loaded from YAML config

#### Storage
- **CacheManager** — Unified read/write entry point with singletons
- **AtomicWriter** — Parquet writes to `.tmp` then `os.replace()`, preventing corruption on interruption
- **PartitionManager** — Partitioned storage by symbol/date with glob path resolution
- **CachedFetcher** — Cache-first execution engine with strategy inference (IncrementalStrategy vs FullCacheStrategy)
- **IncrementalStrategy** — Auto-detects missing date ranges, pulls only delta data
- **FullCacheStrategy** — Replace-mode caching for meta/snapshot data
- **DuckDBEngine** — Thread-local connections, configurable threads (4) and memory limit (4GB), aggregated layer query with raw fallback
- **MemoryCache** — Thread-safe TTLCache with LRU eviction and MD5-based cache keys

#### Schema & Tables
- **69 pre-registered cache tables** across 4 storage layers:
  - `daily` (45 tables): stock_daily, etf_daily, index_daily, finance_indicator, money_flow, north_flow, valuation, etc.
  - `snapshot` (7 tables): spot_snapshot, sector_flow_snapshot, hsgt_hold_snapshot, hot_rank, etc.
  - `minute` (2 tables): stock_minute, etf_minute (partitioned by week)
  - `meta` (15 tables): securities, trade_calendar, index_weights, industry_list, macro_data, etc.
- **SchemaRegistry** (`TableRegistry`) — Immutable `CacheTable` definitions with partition_by, ttl_hours, compaction_threshold, aggregation_enabled

#### API
- **95+ convenience methods** — `get_daily()`, `get_index()`, `get_etf()`, `get_minute()`, `get_money_flow()`, etc.
- **Namespace API** — Structured access: `service.cn.stock.quote.daily()`, `service.cn.stock.finance.indicators()`, `service.cn.stock.capital.north()`, `service.macro.china.gdp()`, etc.
- **SourceProxy** — Dynamic data source selection via `__getattr__`
- **Symbol normalization** — Supports 6 formats: pure numeric, JoinQuant (`600519.XSHG`), AkShare (`sh600519`), Tushare (`600000.SH`), BaoStock (`sh.600519`), OTC fund (`159001.OF`)
- **Field mapping** — CN→EN column mapping for 8+ source types (eastmoney, sina, tushare, baostock, etc.)

#### Offline Tools
- **BatchDownloader** — Concurrent download (ThreadPoolExecutor, 4 workers), rate-limited, incremental/full modes, progress callbacks
- **APIProber** — 64-thread concurrent health auditing of AkShare interfaces, smart parameter inference, TTL-based skip, checkpoint persistence, sample data saving
- **DataQualityChecker** — Completeness checks, anomaly detection (price/volume/high-low), cross-source consistency, scoring system
- **CallStatsAnalyzer** — Access log analysis with priority scoring (call count 40% + miss rate 30% + recency 30%)
- **AkShareScanner** — Module scanning with domain extraction, category inference, parameter inference
- **RegistryBuilder/Merger/Exporter/Validator** — Registry lifecycle management
- **AccessLogger** — Async batch-flush JSON access logs with daily rotation
- **ReportRenderer** — Markdown health reports, quality reports, volume reports
- **CLI** — Unified command-line entry (`python -m akshare_data.offline.cli`) with download/probe/analyze/report/config subcommands
- **Scheduler** — Cron-based scheduled download tasks

#### Error Handling
- **177 error codes** across 9 categories (data source, cache, parameter, network, data quality, system, storage, auth, rate limit)
- **Exception hierarchy** — `DataAccessException` base with specialized subclasses (DataSourceError, CacheError, ValidationError, StorageError, AuthError, NetworkError, SystemError, etc.)
- **Error context** — All exceptions carry error_code, source, symbol, and human-readable message

#### Configuration
- **Configuration-driven AkShare interfaces** — YAML-defined endpoints, no code changes needed for new interfaces
- **Modular config structure** — Separate files for registry, rate limits, sources, interfaces, download, prober, logging
- **Environment variable overrides** — All cache settings configurable via `AKSHARE_DATA_CACHE_*` env vars
- **Config caching** — Lazy-loaded YAML with in-memory caching

#### Backward Compatibility
- **jk2bt migration aliases** — `get_stock_daily()`, `get_etf_daily()`, `get_index_daily()`, etc. compatible with legacy jk2bt API
- **Compatible function renames** — `normalize_code` → `normalize_symbol`, etc.

### Changed

- Refactored AkShare adapter from hardcoded 100+ methods to configuration-driven dynamic dispatch
- Replaced legacy `IncrementalEngine` with strategy-based `IncrementalStrategy` / `FullCacheStrategy`
- Unified cache access through `CacheManager` instead of scattered read/write functions

---

## [0.1.0] — (Pre-release)

- Initial jk2bt-based AkShare wrapper
- Basic caching with no multi-level strategy
- Direct AkShare function calls without configuration-driven routing

---

## Template for Future Releases

```
## [X.Y.Z] — YYYY-MM-DD

### Added
- New feature or component

### Changed
- Modification to existing behavior

### Fixed
- Bug fix

### Deprecated
- Soon-to-be-removed feature

### Removed
- Previously deprecated feature removed

### Security
- Security-related fix or improvement
```
