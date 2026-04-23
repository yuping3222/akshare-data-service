# Standardized (L1) 存储规范

> 任务编号: `T5-001`
> 最后更新: 2026-04-22

## 1. 设计原则

- **业务时间分区**：Standardized 层以实体的业务时间（`trade_date` / `report_date` / `observation_date`）为分区键，不复用 Raw 的 `extract_date` 抽取分区。
- **版本可追溯**：每次写入都记录 `batch_id`、`normalize_version`、`schema_version`，支持按版本回放。
- **幂等写入**：同一批次重跑不会产生重复数据，upsert 语义保证增量覆盖。
- **原子写入**：Parquet 写入使用 tmp → rename 模式，防止中断导致数据损坏。
- **Compaction 安全**：小文件合并不破坏版本追溯，compaction 后仍可通过 manifest 追溯到原始批次。

## 2. 目录结构

```
data/standardized/
  <domain>/
    <dataset>/
      <partition_key>=<partition_value>/
        part-<idx>.parquet
        _manifest.json
      ...
  _compacted/
    <domain>/
      <dataset>/
        <partition_key>=<partition_value>/
          compacted-<compaction_id>.parquet
          _compaction_manifest.json
```

### 2.1 分区规则

| 实体 | 分区键 | 分区值格式 |
|------|--------|------------|
| `market_quote_daily` | `trade_date` | `YYYY-MM-DD` |
| `financial_indicator` | `report_date` | `YYYY-MM-DD` |
| `macro_indicator` | `indicator_code` + `observation_date` | `<indicator_code>/YYYY-MM-DD` |

分区路径示例：

```
data/standardized/market/market_quote_daily/trade_date=2026-04-21/part-000.parquet
data/standardized/macro/macro_indicator/indicator_code=cpi/observation_date=2026-04-01/part-000.parquet
```

### 2.2 与 Raw 层的区别

| 维度 | Raw (L0) | Standardized (L1) |
|------|----------|-------------------|
| 分区键 | `extract_date` + `batch_id` | 业务时间字段 |
| 字段名 | 原始源字段名 | 标准字段名 |
| 系统字段 | 抽取审计字段 | 标准化版本字段 |
| 写入语义 | 追加（append） | Upsert（幂等覆盖） |

## 3. 系统字段

所有 Standardized 数据集统一使用以下系统字段：

| 字段名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `batch_id` | string | 是 | 来源批次标识 |
| `source_name` | string | 是 | 来源数据源 |
| `interface_name` | string | 是 | 来源接口 |
| `ingest_time` | timestamp | 是 | 进入系统时间 |
| `normalize_version` | string | 是 | 标准化规则版本 |
| `schema_version` | string | 是 | 实体 schema 版本 |
| `quality_status` | string | 是 | 质量状态（pending/pass/fail） |
| `publish_time` | timestamp | 否 | 发布到 Served 的时间 |
| `release_version` | string | 否 | 发布版本 |

默认值：
- `quality_status` = `"pending"`（写入时尚未执行质量检查）

## 4. 写入协议

### 4.1 写入流程

```
Normalized DataFrame
  -> schema 校验
  -> 主键去重
  -> 系统字段补齐
  -> 按业务时间分区
  -> 原子写入 Parquet
  -> 更新 manifest
```

### 4.2 Schema 校验

- 输入 DataFrame 必须包含实体 schema 中定义的所有 `required_fields`
- 字段类型必须与 schema 定义兼容
- 不允许出现 schema 中未定义的额外字段（strict 模式）

### 4.3 主键去重

- 在写入前，按实体定义的 `primary_key` 去重，保留最后一条
- 去重日志记录丢弃行数

### 4.4 分区写入

- 按业务时间字段自动分区
- 每个分区写入一个 `part-<idx>.parquet` 文件
- 使用 atomic write（tmp → rename）

## 5. Manifest 规范

每个分区目录下维护一个 `_manifest.json`，记录该分区的所有写入批次：

```json
{
  "manifest_version": "1.0",
  "dataset": "market_quote_daily",
  "domain": "market",
  "partition_key": "trade_date",
  "partition_value": "2026-04-21",
  "batches": [
    {
      "batch_id": "20260421_abc123",
      "normalize_version": "v1",
      "schema_version": "v1",
      "source_name": "akshare",
      "record_count": 5000,
      "files": ["part-000.parquet"],
      "written_at": "2026-04-21T16:00:00Z",
      "status": "success"
    }
  ],
  "total_record_count": 5000,
  "last_updated": "2026-04-21T16:00:00Z"
}
```

### 5.1 Manifest 更新规则

- 新批次写入时追加到 `batches` 列表
- 同一 `batch_id` 的重复写入会替换原有条目（幂等）
- `total_record_count` 为当前分区所有有效批次的记录总数

## 6. Merge / Upsert 规则

### 6.1 晚到数据（Late-Arriving Data）

- 晚到数据按业务时间写入对应分区
- 如果该分区已存在相同主键的记录，按以下规则处理：
  - 如果晚到数据的 `normalize_version` > 已有记录的版本，覆盖
  - 如果版本相同但 `ingest_time` 更新，覆盖
  - 否则跳过

### 6.2 重复数据

- 同一批次内：按主键去重，保留最后一条
- 跨批次：通过 manifest 追踪，compaction 时合并

### 6.3 增量覆盖

- 重跑同一批次时，先读取该批次已写入的文件
- 按主键合并新旧数据，新数据覆盖旧数据
- 更新 manifest 中的对应条目

## 7. Compaction 规范

### 7.1 触发条件

- 单个分区文件数 >= `compaction_threshold`（默认 10）
- 单个分区总大小 < `compaction_min_size`（默认 50MB）时不触发

### 7.2 Compaction 流程

```
读取分区所有 parquet 文件
  -> 按主键去重合并
  -> 写入 compacted-<id>.parquet 到 _compacted/ 目录
  -> 更新 _compaction_manifest.json
  -> 标记原始文件为 compacted（不删除）
```

### 7.3 版本追溯保护

- Compaction 不删除原始文件，只标记状态
- `_compaction_manifest.json` 记录原始文件到 compacted 文件的映射
- Reader 优先读取 compacted 文件，回退到原始文件
- 可通过原始 batch_id 追溯到 compaction 前的数据

### 7.4 Compaction Manifest

```json
{
  "compaction_id": "comp-20260422-001",
  "dataset": "market_quote_daily",
  "partition_key": "trade_date",
  "partition_value": "2026-04-21",
  "source_files": ["part-000.parquet", "part-001.parquet"],
  "compacted_file": "compacted-comp-20260422-001.parquet",
  "record_count": 10000,
  "compacted_at": "2026-04-22T02:00:00Z",
  "source_batches": ["20260421_abc123", "20260421_def456"]
}
```

## 8. 读取协议

### 8.1 读取优先级

1. `_compacted/` 下的 compacted 文件
2. 分区目录下的原始 parquet 文件

### 8.2 查询模式

- 按实体 + 时间范围查询
- 按实体 + 主键查询
- 按实体 + batch_id 查询（用于回放和调试）

### 8.3 版本选择

- 默认读取最新有效数据
- 可指定 `normalize_version` 过滤
- 可指定 `batch_id` 精确回放

## 9. 清理与归档

### 9.1 保留策略

- Standardized 层默认保留 365 天
- 可按 dataset 配置不同的保留周期
- 超过保留期的数据标记为归档，不自动删除

### 9.2 原始文件清理

- Compaction 后 30 天可清理原始文件
- 清理前需确认 compaction manifest 完整
- 清理操作记录到审计日志

## 10. 错误处理

| 错误场景 | 处理策略 |
|----------|----------|
| Schema 不匹配 | 抛出 ValidationError，不写入 |
| 主键为空 | 丢弃该行，记录到日志 |
| 写入中断 | tmp 文件自动清理，不产生脏数据 |
| Manifest 损坏 | 从分区文件重建 manifest |
| Compaction 失败 | 保留原始文件，重试或告警 |
