# 缓存策略

> **注意**：本文档与 [04-存储层](04-storage-layer.md) 有内容重叠。04-storage-layer.md 侧重存储组件实现细节，本文档侧重缓存策略编排流程。

本文档描述 akshare-data-service 的缓存优先（Cache-First）策略、多层缓存架构和增量更新机制。

---

## 1. 核心原则

DataService 是缓存策略的核心编排器。在线 API 为只读门面：仅查询已落地数据与提交异步补数请求；不做同步回源、不做写入。

### 1.1 统一入口：`cached_fetch()`

在线查询可通过 `DataService.cached_fetch()` 走只读缓存查询：

```python
def cached_fetch(
    self,
    table: str,
    storage_layer: str | None = None,
    partition_by: str | None = None,
    partition_value: str | None = None,
    date_col: str = "date",
    fetch_fn: Callable[[], pd.DataFrame | None] | None = None,
    **params,
) -> pd.DataFrame:
```

该方法仅构建查询条件并委托 Served 查询；`fetch_fn` 参数仅保留签名兼容，不会被在线 API 执行。

### 1.2 执行流程

```
请求 → 构建 WHERE 条件 → ServedDataService.query()
                     ├── 命中数据 → 返回 DataFrame
                     └── 缺数 → 返回空结果 + 缺数策略（可异步 backfill）
```

---

## 2. 多层缓存架构

```
L1 内存缓存（MemoryCache）
    │  TTLCache, 5000 items, 3600s TTL
    │  命中 → 返回副本
    ▼ 未命中
L2 磁盘缓存（Parquet 文件）
    │  分区存储: {base_dir}/{layer}/{table}/{partition}=value/part_*.parquet
    │  通过 DuckDB SQL 查询
    ▼ 无文件
L3 回源（Data Source）
    └─ 仅在离线/ingestion 场景触发（不在在线 API 同步执行）
```

### 2.1 MemoryCache

- 基于 `cachetools.TTLCache`，LRU 淘汰
- 默认最大 5000 条，TTL 3600 秒
- 线程安全（`threading.Lock`）
- 返回副本，防止外部修改

### 2.2 Parquet 分区存储

```
{base_dir}/
├── daily/{table}/date=YYYY-MM-DD/part_{pid}_{uid}.parquet
├── snapshot/{table}/part_{pid}_{uid}.parquet
├── minute/{table}/part_{pid}_{uid}.parquet
├── meta/{table}/part_{pid}_{uid}.parquet
├── aggregated/{layer}/{table}/{partition}.parquet
└── _locks/{name}.lock
```

### 2.3 DuckDB 查询引擎

- 线程局部连接（`threading.local()`）
- 优先查询 aggregated 层，无数据则回退到 raw 层
- 支持 WHERE/ORDER BY/LIMIT/聚合

### 2.4 缓存键生成

```python
f"{table}:{storage_layer}:{where_md5[:8]}:{columns_md5[:8]}"
```

`where` 字典和 `columns` 列表分别通过 JSON 序列化（按键排序）后计算 MD5 前 8 位。

---

## 3. 缓存策略（Strategies）

### 3.1 FullCacheStrategy

适用：meta/snapshot 数据（如 securities、industry_list）。

- `should_fetch`: 缓存为空时返回 True
- `merge`: 直接返回 fresh 数据（替换模式）
- `build_where`: 按 filter_keys 构建等值查询

### 3.2 IncrementalStrategy

适用：时序数据（如 stock_daily、index_daily、north_flow）。

- `should_fetch`: 无日期参数时检查缓存是否非空；有日期参数时检查是否覆盖完整区间
- `merge`: 按 date_col 排序去重（keep="last"）
- `build_where`: 按 filter_keys + 日期范围构建查询
- `find_missing_ranges`: 检测缺失日期区间

### 3.3 策略推断

| 条件 | 推断策略 |
|------|----------|
| 有 `start_date` 或 `end_date` | `IncrementalStrategy` |
| 无日期参数 | `FullCacheStrategy` |

---

## 4. 增量更新机制

### 4.1 完整性检查

缓存是否覆盖 `[start, end]` 区间：

```python
min_date <= pd.to_datetime(start_date) and max_date >= pd.to_datetime(end_date)
```

### 4.2 缺失区间检测

`find_missing_ranges(start, end, existing_ranges)` 计算目标范围内的缺失区间：

```
期望: [2024-01-01, 2024-12-31]
已有: [(2024-03-01, 2024-06-30)]
缺失: [(2024-01-01, 2024-02-29), (2024-07-01, 2024-12-31)]
```

### 4.3 增量执行流程

```
1. 读缓存（完整区间）
2. 检查完整性
3. 找缺失区间
4. 逐个拉取缺失区间 → 写入缓存
5. 合并 cached + fetched_parts，按 date 排序去重
6. 返回
```

---

## 5. 写入策略

### 6.1 原子写入

1. 先写入 `.tmp` 临时文件
2. 写入完成后通过 `os.replace()` 原子替换
3. 写入失败则清理 `.tmp` 文件

### 6.2 去重

按 `primary_key` 去重：`drop_duplicates(subset=key, keep="last")`

### 6.3 Schema 验证与类型转换

- 使用 `SchemaValidator` 严格验证
- 验证失败 fallback 到宽松类型转换
- 类型映射：`string`→`str`、`float64`→`float64`、`int64`→`int64`、`bool`→`bool`、`date`/`timestamp`→`datetime64[ns]`

---

## 7. 缓存失效

| 方法 | 说明 |
|------|------|
| `invalidate(table, partition_by, partition_value)` | 清除指定表的内存缓存 + 磁盘文件 |
| `invalidate_all(table)` | 清除所有内存缓存 |
| TTL 过期 | `cachetools.TTLCache` 自动过期 |
| `cleanup(retention_hours)` | 清理旧 raw 文件 |

---

## 8. 配置

### 8.1 环境变量

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

### 8.2 表级配置

通过 `CacheConfig.tables` 或 `TableConfig.from_schema(CacheTable)` 配置：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `partition_by` | `None` | 分区列 |
| `ttl_hours` | `0` | 过期时间（小时），0=永久 |
| `compaction_threshold` | `20` | 合并文件数阈值 |
| `aggregation_enabled` | `True` | 是否启用聚合 |
