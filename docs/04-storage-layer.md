# 存储层

存储层实现了 Cache-First 策略的物理载体：内存缓存 → Parquet 文件 → DuckDB SQL 查询引擎。

---

## 1. CacheManager（统一读写入口）

**位置**: `src/akshare_data/store/manager.py`

CacheManager 是存储层的统一入口，组合了内存缓存、Parquet 写入、DuckDB 查询和分区管理。

### 1.1 单例模式

```python
@classmethod
def get_instance(cls, config: CacheConfig | None = None) -> "CacheManager":
    # 双重检查锁
```

### 1.2 初始化组件

| 组件 | 说明 |
|------|------|
| `config` | `CacheConfig` 配置对象 |
| `partition_manager` | `PartitionManager` 分区管理 |
| `writer` | `AtomicWriter` Parquet 原子写入 |
| `engine` | `DuckDBEngine` SQL 查询引擎 |
| `memory_cache` | `MemoryCache` TTL 内存缓存 |

### 1.3 核心方法

#### read() — 读缓存

```python
def read(
    self, table: str,
    storage_layer: str | None = None,
    partition_by: str | None = None,
    partition_value: str | None = None,
    where: dict[str, Any] | None = None,
    columns: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
```

流程：
1. 从 Schema 注册表自动推断 `storage_layer` 和 `partition_by`
2. **先查内存缓存**（带 MD5 哈希 key），命中则返回
3. 未命中则通过 DuckDB 查询 Parquet 文件
4. 查询结果写入内存缓存，返回副本

#### write() — 写缓存

```python
def write(
    self, table: str, data: pd.DataFrame,
    storage_layer: str | None = None,
    partition_by: str | None = None,
    partition_value: str | None = None,
    schema: dict[str, str] | None = None,
    primary_key: list[str] | None = None,
) -> str:
```

流程：
1. 空 DataFrame 直接跳过
2. 从 Schema 注册表自动推断 `storage_layer`、`partition_by`、`schema`、`primary_key`
3. 通过 `AtomicWriter` 写入 Parquet 文件
4. 同步更新内存缓存

#### has_range() — 检查日期范围覆盖

```python
def has_range(
    self, table: str,
    start: str | None = None, end: str | None = None,
    date_col: str | None = None,
) -> bool:
```

检查缓存是否覆盖了 `[start, end]` 的日期范围：读取缓存中该表的日期列，判断 min_date <= start 且 max_date >= end。

#### 其他方法

| 方法 | 说明 |
|------|------|
| `exists(table, where)` | 通过 DuckDB 检查是否存在数据 |
| `invalidate(table, partition_by, partition_value)` | 清除指定表的内存缓存 + 磁盘文件 |
| `invalidate_all(table)` | 清除所有内存缓存 |
| `table_info(table)` | 返回文件数、大小、最后更新时间 |
| `list_tables(storage_layer)` | 列出所有表 |
| `get_stats()` | 返回内存缓存命中率 + 各表信息 |
| `aggregate(table)` | 运行 compaction（小文件合并） |
| `cleanup(retention_hours)` | 清理旧 raw 文件 |
| `aggregation_status(table)` | 查看聚合状态 |

---

## 2. MemoryCache（TTL 内存缓存）

**位置**: `src/akshare_data/store/memory.py`

```python
def __init__(self, max_items: int = 5000, default_ttl_seconds: int = 3600):
    self._ttl_cache = TTLCache(maxsize=max_items, ttl=default_ttl_seconds)
```

基于 `cachetools.TTLCache` 实现，带 LRU 淘汰和 TTL 过期。线程安全（`threading.Lock`）。

| 方法 | 说明 |
|------|------|
| `get(key)` | 获取缓存，过期返回 None，未过期更新 accessed_at |
| `put(key, value, ttl_seconds)` | 写入缓存，`ttl_seconds=0` 表示永不过期 |
| `set(key, value, ttl_seconds)` | `put` 的别名 |
| `invalidate(key)` | 清除单个 key 或全部缓存 |
| `has(key)` | 检查 key 是否存在且未过期 |
| `cleanup_expired()` | 清理所有过期条目 |
| `size` | 当前缓存条目数 |
| `hit_rate` | 命中率 `hits / (hits + misses)` |

---

## 3. DuckDBEngine（SQL 查询引擎）

**位置**: `src/akshare_data/store/duckdb.py`

DuckDBEngine 提供对 Parquet 文件的 SQL 查询能力。

### 3.1 初始化

```python
def __init__(self, base_dir: str | Path, threads: int = 4, memory_limit: str = "4GB"):
    self._local = threading.local()  # 线程局部连接
```

### 3.2 核心方法

#### query() — 高级查询

```python
def query(
    self, table: str, storage_layer: str,
    partition_by: str | None = None,
    where: dict[str, Any] | None = None,
    columns: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    prefer_aggregated: bool = True,
) -> pd.DataFrame | None:
```

查询策略：
1. **优先查询 aggregated（聚合）层** 的 Parquet 文件
2. 如果 aggregated 无数据，则查询 raw（原始）层
3. 先尝试严格 schema 查询，失败后 fallback 到 `union_by_name=true` 模式

#### query_by_paths() — 按文件路径查询

使用 `read_parquet([...])` 读取多个 Parquet 文件，构建 SQL 执行。

#### exists() / count()

| 方法 | 说明 |
|------|------|
| `exists(table, where)` | 检查是否有数据（LIMIT 1） |
| `count(table, where)` | 统计记录数 |

#### query_simple() — 简化查询

```python
def query_simple(
    self, table: str,
    symbol: str | None = None,
    start: str | None = None, end: str | None = None,
    columns: list[str] | None = None,
    order_by: str | None = "date",
    limit: int | None = None,
) -> pd.DataFrame:
```

按 `symbol`/`start`/`end` 条件构建 WHERE 子句。

#### aggregate() — 聚合查询

```python
def aggregate(
    self, table: str, agg_expr: str,
    group_by: str | None = None, where: str | None = None,
) -> pd.DataFrame:
```

支持 SUM/AVG/MAX/MIN 等聚合函数。

### 3.3 线程安全

使用 `threading.local()` 为每个线程维护独立的 DuckDB 连接：

```python
def _get_connection(self) -> duckdb.DuckDBPyConnection:
    if not hasattr(self._local, "conn") or self._local.conn is None:
        self._local.conn = duckdb.connect(database=":memory:")
    return self._local.conn
```

---

## 4. AtomicWriter（Parquet 原子写入）

**位置**: `src/akshare_data/store/parquet.py`

### 4.1 原子写入流程

```python
def _write_atomic(self, data: pd.DataFrame, target_path: Path) -> Path:
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    table = pa.Table.from_pandas(data)
    pq.write_table(table, str(tmp_path), compression="snappy", row_group_size=100_000)
    os.replace(str(tmp_path), str(target_path))
```

关键特性：
1. 先写入 `.tmp` 临时文件
2. 写入完成后通过 `os.replace()` 原子替换
3. 如果写入失败，清理 `.tmp` 文件

### 4.2 写入流程

```python
def write(
    self, table: str, storage_layer: str, data: pd.DataFrame,
    partition_by: str | None = None,
    partition_value: str | None = None,
    schema: dict[str, str] | None = None,
    primary_key: list[str] | None = None,
) -> Path:
```

流程：
1. 确定分区路径
2. 确保目录存在
3. 验证 Schema + 类型转换 + 去重（按 primary_key）
4. 生成文件名 `part_{pid}_{uid}.parquet`
5. 原子写入

### 4.3 验证与类型转换

类型映射：

| Schema 类型 | Python/NumPy 类型 |
|-------------|-------------------|
| `string` | `str` |
| `float64` | `float64` |
| `int64` | `int64` |
| `bool` | `bool` |
| `date` / `timestamp` | `datetime64[ns]` |

- 使用 `SchemaValidator` 进行严格验证
- 验证失败时 fallback 到 `_coerce_columns` 做宽松类型转换
- 按 `primary_key` 去重（`drop_duplicates(subset=key, keep="last")`）

### 4.4 PartitionManager（分区管理）

| 方法 | 说明 |
|------|------|
| `raw_partition_path(table, storage_layer, partition_by, partition_value)` | 原始分区路径 |
| `aggregated_path(table, storage_layer, partition_by, partition_value)` | 聚合文件路径 |
| `generate_filename()` | 生成 `part_{pid}_{uid}.parquet` |
| `list_partition_files(...)` | 列出分区文件 |
| `list_all_partitions(...)` | 列出所有分区值 |
| `list_all_glob_paths(..., layer)` | 生成 glob 模式路径 |
| `lock_path(name)` | 文件锁路径 `{base_dir}/_locks/{name}.lock` |
| `remove_file(path)` / `remove_dir(path)` | 删除文件/目录 |

### 4.5 ParquetWriter（带锁写入器）

```python
class ParquetWriter:
    def write_parquet_with_lock(parquet_path, df, timeout=10) -> bool:
        # 使用 fcntl.flock 获取排他锁
        # 写入临时文件 → fsync → rename
        # 释放锁
```

用于并发写入场景。

---

## 5. CachedFetcher（缓存执行器）

**位置**: `src/akshare_data/store/fetcher.py`

CachedFetcher 是 DataService 的底层缓存执行引擎，替代了旧的 `_fetch_with_cache` 函数。

### 5.1 FetchConfig

```python
@dataclass
class FetchConfig:
    table: str
    storage_layer: str | None = None
    strategy: CacheStrategy | None = None
    partition_by: str | None = None
    partition_value: str | None = None
    date_col: str = "date"
    interface_name: str | None = None
    filter_keys: list[str] = field(default_factory=list)
```

### 5.2 执行流程

```python
def execute(self, config: FetchConfig, fetch_fn: Callable, **params) -> pd.DataFrame:
```

1. **推断策略**：有 `start_date`/`end_date` → IncrementalStrategy，否则 → FullCacheStrategy
2. **构建查询条件**：`where = strategy.build_where(**params)`
3. **读缓存**：`cached = cache.read(...)`
4. **判断是否需要拉取**：`strategy.should_fetch(cached, **params)`
5. **执行拉取**：全量或增量
6. **返回结果**

### 5.3 增量执行逻辑

```python
def _execute_incremental(self, config, strategy, cached, fetch_fn, params):
    missing_ranges = strategy.find_missing_ranges(cached, start_date, end_date)
    for m_start, m_end in missing_ranges:
        # 只拉取缺失区间
        df = fetch_fn()
        self.cache.write(table, df, ...)
        fetched_parts.append(df)
    # 合并 cached + fetched_parts，按 date 排序去重
```

### 5.4 策略推断

| 条件 | 策略 |
|------|------|
| 有 `start_date` 或 `end_date` | `IncrementalStrategy` |
| 无日期参数 | `FullCacheStrategy` |

`filter_keys` 自动从 params 中排除 `start_date`, `end_date`, `adjust`, `source` 等保留字。

---

## 6. IncrementalStrategy（增量策略）

**位置**: `src/akshare_data/store/strategies/incremental.py`

IncrementalStrategy 是时序数据的缓存策略实现，替代了旧的 `IncrementalEngine`。

### 6.1 核心方法

| 方法 | 说明 |
|------|------|
| `should_fetch(cached, **params)` | 检查缓存是否覆盖完整日期区间 |
| `merge(cached, fresh, **params)` | 按 date_col 排序去重合并 |
| `build_where(**params)` | 构建日期范围的 WHERE 条件 |

### 6.2 增量执行逻辑

由 `CachedFetcher._execute_incremental()` 执行：

```python
def _execute_incremental(self, config, strategy, cached, fetch_fn, params):
    missing_ranges = strategy.find_missing_ranges(cached, start_date, end_date)
    for m_start, m_end in missing_ranges:
        # 只拉取缺失区间
        df = fetch_fn()
        self.cache.write(table, df, ...)
        fetched_parts.append(df)
    # 合并 cached + fetched_parts，按 date 排序去重
```

### 6.3 缺失区间检测

**位置**: `src/akshare_data/store/missing_ranges.py`

`find_missing_ranges(start, end, existing_ranges)` 计算目标范围内的缺失区间。

---

## 7. 缓存策略（Strategies）

**位置**: `src/akshare_data/store/strategies/`

| 类 | 位置 | 说明 |
|----|------|------|
| `CacheStrategy` | `base.py` | 抽象基类，定义 `build_where()` 和 `should_fetch()` |
| `FullCacheStrategy` | `full.py` | 全量策略，按 filter_keys 构建等值查询 |
| `IncrementalStrategy` | `incremental.py` | 增量策略，按日期范围构建查询 + 缺失区间检测 |

---

## 8. SchemaValidator（数据验证）

**位置**: `src/akshare_data/store/validator.py`

验证 DataFrame 是否符合预定义的 Schema，确保列名和类型正确。

---

## 9. Aggregator（数据聚合器）

**位置**: `src/akshare_data/store/aggregator.py`

负责将多个小 Parquet 文件合并为较大的聚合文件，提升 DuckDB 查询性能。

---

## 10. MissingRanges（缺失区间检测）

**位置**: `src/akshare_data/store/missing_ranges.py`

检测已有数据中的日期缺失区间，为增量更新提供差量拉取范围。

---

## 11. 存储层架构总览

```
CacheManager
├── MemoryCache (TTLCache, 5000 items, 3600s TTL)
├── DuckDBEngine (threads=4, memory=4GB, thread-local connections)
├── AtomicWriter (PyArrow, snappy compression, os.replace atomic)
│   └── PartitionManager (raw/aggregated/meta paths)
├── CachedFetcher
│   └── Strategies: FullCacheStrategy / IncrementalStrategy
├── SchemaValidator (数据验证)
├── Aggregator (数据聚合)
└── MissingRanges (缺失区间检测)
```

### 缓存层级

| 层级 | 组件 | TTL | 容量 |
|------|------|-----|------|
| L1 内存 | `MemoryCache` (TTLCache) | 3600s | 5000 items |
| L2 磁盘 | Parquet 文件 | 永久 | 无限制 |
| L3 SQL | DuckDB 查询引擎 | - | - |

### 文件组织

```
{base_dir}/
├── daily/{table}/date=YYYY-MM-DD/part_{pid}_{uid}.parquet    # 原始日线
├── snapshot/{table}/part_{pid}_{uid}.parquet                  # 快照数据
├── minute/{table}/part_{pid}_{uid}.parquet                    # 分钟线
├── meta/{table}/part_{pid}_{uid}.parquet                      # 元数据
├── aggregated/{layer}/{table}/{partition}.parquet             # 聚合文件
└── _locks/{name}.lock                                         # 文件锁
```
