# Design: Adding Non-AkShare Data Sources

## Background

The `akshare-data-service` project currently only has AkShare as the primary data source. The `akshare-one-enhanced` project has implemented additional data sources (Lixinger, Tushare, JSL) that should be integrated.

## Design

### Architecture

```
sources/
├── __init__.py              # Exports all data sources
├── akshare_source.py         # AkShare implementation (exists)
├── lixinger_source.py        # NEW: Lixinger implementation
├── tushare_source.py         # NEW: Tushare implementation
├── router.py                # MultiSourceRouter (exists)
└── backup.py                # Low-level fetchers (exists)
```

### DataSource ABC

All sources must implement `DataSource` abstract base class from `core/base.py`:
- `get_daily_data()` - 日线数据
- `get_index_stocks()` - 指数成分股
- `get_trading_days()` - 交易日
- `get_securities_list()` - 证券列表
- `get_money_flow()` - 资金流向
- etc. (40+ abstract methods)

### Implementation Pattern

Each source adapter follows this structure:

```python
class LixingerAdapter(DataSource):
    name = "lixinger"
    source_type = "real"

    def __init__(self, token: str = None, **kwargs):
        self._token = token or self._load_token()
        self._client = LixingerClient()
        # ...

    def get_daily_data(self, symbol, start_date, end_date, adjust="qfq", **kwargs) -> pd.DataFrame:
        # Query Lixinger API
        # Normalize to standard fields: datetime, open, high, low, close, volume
        return df

    def get_index_stocks(self, index_code: str, **kwargs) -> List[str]:
        # ...
```

### Standardization

All sources must normalize data to the standard schema:
- `datetime` (or `date`): datetime
- `open`, `high`, `low`, `close`: float
- `volume`: int
- `amount`: float (optional)

### Router Integration

New sources registered in `MultiSourceRouter` for fallback:
```
Lixinger (primary) -> AkShare (fallback) -> Tushare (fallback) -> Baostock (last resort)
```

## Data Sources to Implement

### 1. Lixinger (理杏仁)

**覆盖数据:**
- Index historical (cn/index/candlestick)
- Index constituents (cn/index/constituents, cn/index/constituent-weightings)
- Index fundamentals PE/PB (cn/index/fundamental)
- Index drawdown (cn/index/drawdown)
- Stock fundamentals (cn/stock/fundamental)
- Financial statements (cn/stock/fs/hybrid)

**Token配置:**
- Environment: `LIXINGER_TOKEN`
- Config file: `token.cfg` in project root

### 2. Tushare (吐司)

**覆盖数据:**
- Daily data (pro.daily)
- Financial reports
- Money flow
- Index components

**Token配置:**
- Environment: `TUSHARE_TOKEN`
- Config file: `token.cfg`

### 3. JSL (集思录)

**覆盖数据:**
- Bond yield curve
- Convertible bonds
- ETF data

**无需认证**

## Implementation Files

| File | Description |
|------|-------------|
| `sources/lixinger_source.py` | Lixinger adapter |
| `sources/lixinger_client.py` | Lixinger HTTP client |
| `sources/tushare_source.py` | Tushare adapter |
| `sources/__init__.py` | Updated exports |

## Backward Compatibility

All new sources are optional - existing code using `AkShareAdapter` continues to work unchanged.