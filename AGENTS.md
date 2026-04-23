# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

**akshare-data-service** is a Cache-First unified financial data service integrating Lixinger + AkShare data sources. Provides stock, ETF, index, macro and other financial data with memory → Parquet → DuckDB three-level caching.

- **Version**: 0.2.0 | **Python**: >= 3.10
- **Core principle**: Cache-First — check cache first, fill missing data, then fallback to source

## Common Commands

```bash
# Install dependencies (development mode)
pip install -e ".[all,dev]"

# Run all tests
make test

# Run unit tests only
make test-unit

# Run tests with coverage
make test-cov

# Lint and format
make lint         # ruff check
make lint-fix     # ruff check --fix
make format       # ruff format

# Type checking
make type-check   # mypy src/akshare_data/

# Clean cache and build artifacts
make clean

# CLI tools
python -m akshare_data.offline.cli download --days 1    # Daily incremental download
python -m akshare_data.offline.cli probe --all           # API health probing
python -m akshare_data.offline.cli report quality --table stock_daily  # Data quality check

# Interactive Python with service loaded
make dev
```

## Architecture

Four-layer architecture:

```
__init__.py → DataService (api.py) → Sources (sources/) → Store (store/) → Core (core/)
```

- **Entry**: `__init__.py` uses `__getattr__` to forward module-level calls like `get_daily()` to a `DataService` singleton
- **API**: `DataService` orchestrates cache-first strategy. Two call styles: convenience functions (`get_daily`) and namespace API (`service.cn.stock.quote.daily`)
- **Sources**: `MultiSourceRouter` handles failover across Lixinger (primary), AkShare (backup), Tushare (optional). Circuit breaker after 5 consecutive failures
- **Store**: `CacheManager` manages memory cache (TTL), Parquet files (partitioned), and DuckDB (SQL queries)
- **Core**: DataSource ABC, SchemaRegistry (69 tables), error codes (177), symbol normalization, field mapping

## Key Design Principles

1. **Separation of concerns**: Sources know nothing about caching; API layer owns cache-first strategy
2. **Configuration-driven**: AkShare interfaces defined in YAML config; new endpoints don't require code changes
3. **Atomic writes**: Parquet writes to `.tmp` then renames, preventing corruption on interruption
4. **Thread-safe**: Shared state uses locks; DuckDB uses thread-local connections

## Data Sources (priority order)

1. **lixinger** — Primary, requires `LIXINGER_TOKEN` env var
2. **akshare** — Backup, config-driven thin dispatcher
3. **tushare** — Optional, install with `pip install -e ".[tushare]"`

## Storage

- **Memory**: `cachetools.TTLCache`, max 5000 items, 3600s TTL
- **Parquet**: Partitioned by symbol/date, atomic writes via `AtomicWriter`
- **DuckDB**: SQL queries over Parquet files, configurable threads (default 4) and memory (default 4GB)
- **Incremental**: Auto-detects missing date ranges, pulls only delta data

## Cache Tables

69 pre-registered tables organized in 4 layers:
- **daily**: stock_daily, etf_daily, index_daily, finance_indicator, money_flow, etc. (45 tables)
- **snapshot**: spot_snapshot, sector_flow_snapshot, etc. (7 tables, 168h TTL)
- **minute**: stock_minute, etf_minute (partitioned by week)
- **meta**: securities, trade_calendar, macro_data, etc. (15 tables)

Full schema reference: `docs/07-schema-registry.md`

## Documentation

14 user-facing docs in `docs/`: 01-overview through 14-getting-started, plus CLI_REFERENCE.md.
Internal design docs in `docs/design/`: 8 design/archival documents (see `docs/design/README.md` index).
Full docs index: `docs/README.md`.

## Configuration

Environment variables:
- `AKSHARE_DATA_CACHE_DIR` — cache root (default: `./cache`)
- `AKSHARE_DATA_CACHE_MAX_ITEMS` — memory cache max (default: 5000)
- `AKSHARE_DATA_CACHE_TTL_SECONDS` — memory TTL (default: 3600)
- `AKSHARE_DATA_CACHE_DUCKDB_THREADS` — DuckDB threads (default: 4)
- `AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT` — DuckDB memory (default: 4GB)
- `LIXINGER_TOKEN` — Lixinger API token

YAML configs in `config/`: akshare registry, rate limits, source configs, interface definitions, download scheduling, prober config, logging config.

## Testing

- Tests in `tests/`, organized as unit/integration/system
- Markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.system`, `@pytest.mark.network`, `@pytest.mark.slow`
- Use `pytest -m unit` to skip network tests
- Mock source available at `src/akshare_data/sources/mock.py`

## Offline Tools

Located in `src/akshare_data/offline/`:
- **downloader**: BatchDownloader with concurrent, rate-limited, incremental/full modes
- **prober**: API health probing for AkShare interfaces
- **analyzer**: Cache analysis (completeness, anomalies), access log stats
- **registry**: Interface registry management (merge, export, validate)
- **report**: Health reports, quality reports, dashboard rendering
- **scanner**: AkShare interface scanning, domain/category/param inference
- **scheduler**: Download scheduling
- **source_manager**: Source health tracking
- **cli**: Command-line entry point (`python -m akshare_data.offline.cli`)
