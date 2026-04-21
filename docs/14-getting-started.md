# Getting Started

A step-by-step tutorial to install, configure, and start using akshare-data-service.

---

## Prerequisites

- **Python >= 3.10**
- **pip** (included with Python)
- **Git** (to clone the repository)

Check your Python version:

```bash
python --version
# Should output: Python 3.10.x or higher
```

---

## Step 1: Installation

Clone the repository and install in development mode:

```bash
# Clone the repository
git clone <repository-url>
cd akshare-data-service

# Install in development mode with all optional dependencies
pip install -e ".[all,dev]"
```

This installs the package along with core dependencies:
- `pandas >= 2.0` — data processing
- `duckdb >= 0.9` — SQL query engine
- `pyarrow >= 14.0` — Parquet file I/O
- `cachetools >= 5.3` — memory caching
- `akshare >= 1.10` — data source library
- `pyyaml >= 6.0` — YAML configuration parsing

Verify the installation:

```bash
python -c "import akshare_data; print('OK')"
```

---

## Step 2: Configure LIXINGER_TOKEN

Lixinger is the primary data source and requires an API token.

### Option A: Environment Variable

```bash
export LIXINGER_TOKEN="your_lixinger_token_here"
```

Add this to your `~/.bashrc` or `~/.zshrc` to make it permanent.

### Option B: Token Config File

Create a `token.cfg` file in the project root or `~/.akshare_data/`:

```ini
[lixinger]
token = your_lixinger_token_here
```

### Verify the Configuration

```python
from akshare_data import get_service

service = get_service()
health = service.lixinger.health_check()
print(health)
# Output: {'status': 'ok', 'message': 'Lixinger API reachable...', 'latency_ms': 123}
```

> **Note**: If Lixinger is not configured, the service will still work using AkShare as a fallback, but you will get `Lixinger token not configured` warnings.

---

## Step 3: Your First API Call

The simplest way to get data is using module-level convenience functions:

```python
from akshare_data import get_daily

# Fetch A-share daily data for Ping An Bank (000001)
df = get_daily("000001", "2024-01-01", "2024-12-31")

print(df.head())
# Output:
#     symbol       date     open     high      low    close    volume     amount
# 0   000001 2024-01-02  10.5600  10.6300  10.4500  10.5000  512345.0  5398765432.0
# 1   000001 2024-01-03  10.5000  10.5800  10.4200  10.5500  489012.0  5123456789.0
# ...
```

This is your first Cache-First call:
1. The service checks the memory cache (L1)
2. Then checks Parquet files on disk (L2)
3. Queries via DuckDB (L3)
4. Only if data is missing does it call the network (Lixinger → AkShare)
5. Fetched data is written to Parquet and cached in memory

Subsequent calls for the same symbol/date range will return instantly from cache.

### Other Convenience Functions

```python
from akshare_data import get_index, get_etf, get_minute, get_money_flow

# Index data (CSI 300)
df = get_index("000300", "2024-01-01", "2024-12-31")

# ETF data (CSI 300 ETF)
df = get_etf("510300", "2024-01-01", "2024-12-31")

# Minute-level data (5-minute bars)
df = get_minute("000001", freq="5min", start_date="2024-06-01", end_date="2024-06-05")

# Money flow data
df = get_money_flow("000001", "2024-01-01", "2024-12-31")
```

### Supported Symbol Formats

```python
# All of these work — the service auto-normalizes:
get_daily("000001", ...)          # Pure numeric (recommended)
get_daily("sz000001", ...)        # With exchange prefix
get_daily("000001.XSHE", ...)     # JoinQuant format
```

---

## Step 4: Understanding the Cache

### Check Cache Status

```python
from akshare_data import get_service

service = get_service()
stats = service.cache.get_stats()

print(f"Memory cache size: {stats['memory_cache_size']}")
print(f"Cache hit rate: {stats['memory_cache_hit_rate']:.2%}")
print(f"Tables: {list(stats['tables'].keys())}")
```

### View Table Info

```python
info = service.cache.table_info("stock_daily", "daily")
print(f"Files: {info['file_count']}")
print(f"Total size: {info['total_size_mb']:.1f} MB")
print(f"Last updated: {info['last_updated']}")
```

### Cache Directory Structure

Data is stored in the `cache/` directory (default: `./cache`):

```
cache/
├── daily/stock_daily/date=2024-01-02/part_12345_abc123.parquet
├── daily/stock_daily/date=2024-01-03/part_12345_def456.parquet
├── daily/etf_daily/date=2024-01-02/part_12345_ghi789.parquet
├── meta/securities/part_12345_jkl012.parquet
├── meta/trade_calendar/part_12345_mno345.parquet
├── snapshot/spot_snapshot/part_12345_pqr678.parquet
└── _locks/                        # File locks for concurrent writes
```

### Customize Cache Location

```bash
# Use a different cache directory
export AKSHARE_DATA_CACHE_DIR="/data/akshare_cache"

# Increase memory cache size
export AKSHARE_DATA_CACHE_MAX_ITEMS=10000

# Change memory TTL to 2 hours
export AKSHARE_DATA_CACHE_TTL_SECONDS=7200
```

---

## Step 5: Using the Namespace API

The namespace API provides structured, discoverable access to all data categories:

```python
from akshare_data import get_service

service = get_service()

# A-Share stock quotes
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31")
df = service.cn.stock.quote.minute("000001", freq="5min")
df = service.cn.stock.quote.realtime("000001")

# Financial data
df = service.cn.stock.finance.balance_sheet("000001")
df = service.cn.stock.finance.income_statement("000001")
df = service.cn.stock.finance.cash_flow("000001")
df = service.cn.stock.finance.indicators("000001", start_date="2023-01-01", end_date="2024-12-31")

# Capital flow
df = service.cn.stock.capital.money_flow("000001")
df = service.cn.stock.capital.north(start_date="2024-01-01", end_date="2024-12-31")

# Index
df = service.cn.index.quote.daily("000300", "2024-01-01", "2024-12-31")
df = service.cn.index.meta.components("000300")  # Index constituents with weights

# ETF
df = service.cn.fund.quote.daily("510300", "2024-01-01", "2024-12-31")

# Events (dividends, lock-up releases)
df = service.cn.stock.event.dividend("000001")
df = service.cn.stock.event.restricted_release("000001")

# Macro data
df = service.macro.china.interest_rate("2024-01-01", "2024-12-31")
df = service.macro.china.gdp("2024-01-01", "2024-12-31")

# Trade calendar
days = service.cn.trade_calendar(start_date="2024-01-01", end_date="2024-12-31")
```

### Specify a Data Source

```python
# Use only Lixinger
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source="lixinger")

# Use only AkShare
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source="akshare")

# Try Lixinger first, fall back to AkShare
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source=["lixinger", "akshare"])
```

---

## Step 6: Using Offline Tools

### Download Data in Bulk

The `BatchDownloader` downloads data from AkShare directly to the shared cache:

```python
from akshare_data import BatchDownloader

downloader = BatchDownloader(max_workers=4)

# Incremental download (last 1 day)
result = downloader.download_incremental(days_back=1)
print(f"Success: {result['success_count']}, Failed: {result['failed_count']}")

# Full download for specific interfaces
result = downloader.download_full(
    interfaces=["equity_daily", "index_daily"],
    start="2024-01-01",
    end="2024-12-31",
)
```

Or from the command line:

```bash
# Daily incremental download
python -m akshare_data.offline.cli download --days 1

# Full historical download
python -m akshare_data.offline.cli download --mode full --start 2020-01-01 --workers 8

# Start scheduled download (runs continuously)
python -m akshare_data.offline.cli download --schedule
```

### Probe API Health

The `APIProber` audits AkShare interface availability:

```python
from akshare_data import APIProber

prober = APIProber()
prober.run_check()
```

From the command line:

```bash
# Probe all interfaces
python -m akshare_data.offline.cli probe --all

# View last probe status
python -m akshare_data.offline.cli probe --status
```

### Check Data Quality

```python
from akshare_data.offline.quality import DataQualityChecker

checker = DataQualityChecker()

# Completeness check
report = checker.check_completeness(
    table="stock_daily",
    symbol="000001",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
print(f"Completeness: {report['completeness_ratio']:.2%}")
print(f"Missing dates: {report['missing_dates_count']}")

# Anomaly detection
anomalies = checker.check_anomalies(df)
print(f"Anomalies found: {anomalies['anomaly_count']}")
```

From the command line:

```bash
# Generate data quality report
python -m akshare_data.offline.cli report quality --table stock_daily
```

### Analyze Access Logs

```bash
# Analyze last 7 days of access logs, generate download priority config
python -m akshare_data.offline.cli analyze logs --window 7
```

### Generate Reports

```bash
# Health audit report
python -m akshare_data.offline.cli report health

# Data quality report
python -m akshare_data.offline.cli report quality --table stock_daily

# Dashboard
python -m akshare_data.offline.cli report dashboard
```

---

## Step 7: Error Handling

akshare-data-service provides a structured error system with 177 error codes:

```python
from akshare_data import get_daily
from akshare_data.core.errors import (
    DataAccessException,
    SourceUnavailableError,
    NoDataError,
    TimeoutError,
)

try:
    df = get_daily("000001", "2024-01-01", "2024-12-31")
except NoDataError as e:
    print(f"No data: {e}")
except TimeoutError as e:
    print(f"Timeout: {e}")
except SourceUnavailableError as e:
    print(f"Source unavailable: {e}")
except DataAccessException as e:
    print(f"Error {e.error_code}: {e.message}")
    print(f"Category: {e.to_dict()['category']}")
```

**Error code categories**:

| Prefix | Category | Example |
|--------|----------|---------|
| `1xxx` | Data source errors | `1002_SOURCE_TIMEOUT` |
| `2xxx` | Cache errors | `2001_CACHE_MISS` |
| `3xxx` | Parameter errors | `3001_INVALID_SYMBOL` |
| `4xxx` | Network errors | `4001_NETWORK_TIMEOUT` |
| `5xxx` | Data quality errors | `5001_NO_DATA` |
| `6xxx` | System errors | `6001_INTERNAL_ERROR` |
| `7xxx` | Storage errors | `7001_FILE_NOT_FOUND` |
| `8xxx` | Auth errors | `8001_AUTH_TOKEN_MISSING` |
| `9xxx` | Rate limit errors | `9010_CIRCUIT_BREAKER_OPEN` |

---

## Next Steps

Now that you have the basics, explore these resources:

| Document | What You'll Learn |
|----------|-------------------|
| [01-overview.md](01-overview.md) | Project architecture, features, cache tables |
| [02-api-reference.md](02-api-reference.md) | Full API reference for convenience functions and namespace API |
| [03-data-sources.md](03-data-sources.md) | Data source configuration, Lixinger/AkShare/Tushare details |
| [04-storage-layer.md](04-storage-layer.md) | CacheManager, Parquet storage, DuckDB queries |
| [05-core-modules.md](05-core-modules.md) | Config, schema registry, symbol conversion, field mapping |
| [06-offline-tools.md](06-offline-tools.md) | BatchDownloader, APIProber, DataQualityChecker details |
| [07-schema-registry.md](07-schema-registry.md) | All 69 cache table schema definitions |
| [08-cache-strategy.md](08-cache-strategy.md) | Cache-First strategy, incremental updates, atomic writes |
| [10-error-handling.md](10-error-handling.md) | 177 error codes and exception hierarchy |
| [12-configuration-reference.md](12-configuration-reference.md) | All YAML configuration files |
| [13-glossary.md](13-glossary.md) | Key terms and definitions |
| [CLI_REFERENCE.md](CLI_REFERENCE.md) | Complete command-line reference |

### Example Scripts

The `examples/` directory contains 99 runnable example scripts:

```bash
# Run a specific example
python examples/example_daily.py
python examples/example_index.py
python examples/example_money_flow.py

# Run all examples
for f in examples/example_*.py; do
    python "$f"
done
```

### Interactive Development

Start an interactive Python session with the service pre-loaded:

```bash
make dev
```
