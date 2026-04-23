# 可观测性指标规范

> 任务编号: `T11-001`
> 最后更新: 2026-04-22

## 1. 目标

建立统一的指标口径，覆盖任务执行、数据质量、发布状态、服务读取和数据新鲜度五大域，
使系统具备"出了问题知道怎么看"的能力。

## 2. 设计原则

- 所有指标必须可关联 `dataset`、`batch_id`、`release_version` 三个核心维度
- 指标名使用 `akshare_data.<domain>.<metric>` 格式
- 指标类型分为 Counter、Gauge、Histogram 三类
- 埋点以轻量 hook 形式提供，不侵入主业务逻辑
- 指标定义与告警规则（101-alert-rules.md）一一对应

## 3. 通用标签 (Labels)

所有指标至少携带以下标签：

| 标签 | 说明 | 示例 |
|------|------|------|
| `dataset` | 标准数据集名 | `market_quote_daily` |
| `domain` | 数据域 | `quote` / `finance` / `macro` |
| `layer` | 数据层 | `raw` / `standardized` / `served` |

特定场景附加标签：

| 标签 | 说明 | 示例 |
|------|------|------|
| `batch_id` | 批次号 | `20260422_001` |
| `release_version` | 发布版本 | `v20260422_001` |
| `source_name` | 数据源 | `lixinger` / `akshare` / `tushare` |
| `error_code` | 错误码 | `SRC_TIMEOUT` / `QUALITY_FAIL` |
| `rule_id` | 质量规则ID | `mq_daily_pk_unique` |
| `status` | 状态 | `success` / `failed` / `partial` |
| `endpoint` | 服务接口 | `cn.stock.quote.daily` |
| `http_method` | HTTP方法 | `GET` / `POST` |

## 4. 指标清单

### 4.1 任务执行域 (ingestion)

| 指标名 | 类型 | 说明 | 关键标签 |
|--------|------|------|----------|
| `akshare_data.ingestion.task_total` | Counter | 任务执行总数 | `dataset`, `status`, `source_name` |
| `akshare_data.ingestion.task_duration_seconds` | Histogram | 任务执行耗时 | `dataset`, `source_name` |
| `akshare_data.ingestion.task_retry_total` | Counter | 任务重试次数 | `dataset`, `source_name`, `error_code` |
| `akshare_data.ingestion.records_extracted_total` | Counter | 抽取记录总数 | `dataset`, `batch_id`, `source_name` |
| `akshare_data.ingestion.circuit_breaker_trips_total` | Counter | 熔断触发次数 | `source_name` |
| `akshare_data.ingestion.rate_limit_hits_total` | Counter | 限流触发次数 | `source_name` |

### 4.2 数据质量域 (quality)

| 指标名 | 类型 | 说明 | 关键标签 |
|--------|------|------|----------|
| `akshare_data.quality.rule_executions_total` | Counter | 规则执行总数 | `dataset`, `batch_id`, `rule_id`, `result` |
| `akshare_data.quality.gate_pass_rate` | Gauge | 门禁通过率 (0-1) | `dataset`, `batch_id`, `layer` |
| `akshare_data.quality.failed_rules_total` | Counter | 失败规则数 | `dataset`, `batch_id`, `rule_id`, `severity` |
| `akshare_data.quality.quarantine_records_total` | Counter | 隔离区记录数 | `dataset`, `batch_id` |
| `akshare_data.quality.check_duration_seconds` | Histogram | 质量检查耗时 | `dataset`, `batch_id` |
| `akshare_data.quality.schema_drifts_total` | Counter | Schema漂移次数 | `dataset`, `batch_id` |

### 4.3 发布状态域 (served)

| 指标名 | 类型 | 说明 | 关键标签 |
|--------|------|------|----------|
| `akshare_data.served.publish_total` | Counter | 发布总次数 | `dataset`, `release_version`, `status` |
| `akshare_data.served.publish_duration_seconds` | Histogram | 发布耗时 | `dataset`, `release_version` |
| `akshare_data.served.rollback_total` | Counter | 回滚总次数 | `dataset`, `release_version` |
| `akshare_data.served.active_release_version` | Gauge | 当前活跃发布版本(数值化) | `dataset` |
| `akshare_data.served.release_manifest_complete` | Gauge | manifest完整性(0/1) | `dataset`, `release_version` |

### 4.4 服务读取域 (service)

| 指标名 | 类型 | 说明 | 关键标签 |
|--------|------|------|----------|
| `akshare_data.service.request_total` | Counter | 请求总数 | `endpoint`, `status` |
| `akshare_data.service.request_duration_seconds` | Histogram | 请求耗时 | `endpoint`, `http_method` |
| `akshare_data.service.error_total` | Counter | 错误总数 | `endpoint`, `error_code` |
| `akshare_data.service.cache_hit_rate` | Gauge | 缓存命中率 (0-1) | `endpoint` |
| `akshare_data.service.missing_data_responses_total` | Counter | 缺数响应次数 | `endpoint`, `dataset`, `policy` |

### 4.5 数据新鲜度域 (freshness)

| 指标名 | 类型 | 说明 | 关键标签 |
|--------|------|------|----------|
| `akshare_data.freshness.data_lag_seconds` | Gauge | 数据延迟(秒) | `dataset`, `layer` |
| `akshare_data.freshness.missing_partitions_total` | Counter | 缺失分区数 | `dataset`, `batch_id` |
| `akshare_data.freshness.last_successful_ingest_timestamp` | Gauge | 最近成功入库时间戳 | `dataset`, `source_name` |
| `akshare_data.freshness.staleness_alerts_total` | Counter | 过期告警次数 | `dataset` |

### 4.6 存储域 (storage)

| 指标名 | 类型 | 说明 | 关键标签 |
|--------|------|------|----------|
| `akshare_data.storage.parquet_files_total` | Gauge | Parquet文件总数 | `dataset`, `layer` |
| `akshare_data.storage.storage_bytes` | Gauge | 存储占用(字节) | `dataset`, `layer` |
| `akshare_data.storage.small_files_total` | Gauge | 小文件数(<1MB) | `dataset`, `layer` |
| `akshare_data.storage.compaction_duration_seconds` | Histogram | 合并耗时 | `dataset` |

## 5. 指标关联模型

```
batch_id ──┐
           ├── ingestion.task_* ──┐
release_version ──┤                ├── quality.* ──┐
                  │                │               ├── served.*
dataset ──────────┤                │               │
                  ├── quality.* ───┘               ├── freshness.*
                  │                                │
                  ├── served.* ────────────────────┘
                  │
                  └── service.* (通过 dataset 间接关联)
```

通过 `batch_id` 可以追溯一次抽取从 raw → standardized → quality → served 的全链路。
通过 `release_version` 可以追溯一次发布包含哪些批次及其质量状态。

## 6. 埋点方式

### 6.1 轻量 Hook

在关键路径提供 hook 点，由外部监控系统接入：

```python
# 示例：任务完成 hook
from akshare_data.common.metrics import emit_counter, emit_histogram

def on_task_complete(dataset, batch_id, status, duration, records):
    emit_counter("akshare_data.ingestion.task_total", 1,
                 labels={"dataset": dataset, "status": status})
    emit_histogram("akshare_data.ingestion.task_duration_seconds", duration,
                   labels={"dataset": dataset})
    emit_counter("akshare_data.ingestion.records_extracted_total", records,
                 labels={"dataset": dataset, "batch_id": batch_id})
```

### 6.2 注册表

指标注册表统一管理指标元数据：

```python
METRIC_REGISTRY = {
    "akshare_data.ingestion.task_total": {
        "type": "counter",
        "description": "任务执行总数",
        "labels": ["dataset", "status", "source_name"],
    },
    # ...
}
```

### 6.3 导出格式

默认支持以下导出格式：

- **Prometheus**: `/metrics` 端点，文本格式
- **JSON**: 结构化输出，便于日志聚合
- **Stdout**: 开发模式直接打印

## 7. 看板设计

### 7.1 统一看板结构

看板分为四个面板，回答"是源问题、质量问题、发布问题还是服务问题"：

```
┌─────────────────────────────────────────────────────────────┐
│ Panel 1: 任务执行健康度                                        │
│ - 任务成功率趋势 (按 source_name 分组)                         │
│ - 熔断/限流触发次数                                            │
│ - 数据新鲜度 (data_lag_seconds)                               │
│ → 判断：源站是否稳定？数据是否按时到达？                          │
├─────────────────────────────────────────────────────────────┤
│ Panel 2: 数据质量状态                                          │
│ - 门禁通过率趋势 (按 dataset 分组)                              │
│ - 失败规则 TOP N (按 rule_id 分组)                             │
│ - 隔离区积压量                                                 │
│ → 判断：数据本身是否有问题？哪个规则在频繁失败？                   │
├─────────────────────────────────────────────────────────────┤
│ Panel 3: 发布状态                                              │
│ - 发布成功率趋势                                               │
│ - 当前活跃版本 (按 dataset)                                    │
│ - 回滚次数                                                     │
│ → 判断：发布是否顺利？是否需要回滚？                             │
├─────────────────────────────────────────────────────────────┤
│ Panel 4: 服务读取                                              │
│ - 请求成功率 & P95 延迟                                        │
│ - 缓存命中率                                                   │
│ - 缺数响应次数                                                 │
│ → 判断：用户是否能正常获取数据？                                 │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 问题定位路径

```
服务报错?
  ├─ 看 Panel 4: 缺数响应高?
  │   └─ 看 Panel 3: 最近发布失败/回滚?
  │       ├─ 是 → 发布问题 → 看质量报告 → 执行 rollback runbook
  │       └─ 否 → 看 Panel 1: 任务失败?
  │           ├─ 是 → 源问题 → 看熔断/限流 → 执行 backfill runbook
  │           └─ 否 → 看 Panel 2: 质量门禁阻断?
  │               └─ 是 → 质量问题 → 看失败规则 → 执行 replay runbook
  └─ 看 Panel 4: P95延迟高?
      └─ 缓存命中率低? → 存储/查询优化
```

## 8. 与告警规则的关系

本指标规范定义的指标是告警规则（101-alert-rules.md）的数据源。
每个告警规则引用一个或多个指标，并定义阈值和分级。

## 9. 验收标准

- [x] 指标覆盖任务执行、数据质量、发布状态、服务读取、数据新鲜度五大域
- [x] 所有指标可关联 `dataset`、`batch_id`、`release_version`
- [x] 提供轻量 hook 机制，不侵入主业务逻辑
- [x] 看板设计能回答"是源问题、质量问题、发布问题还是服务问题"
- [x] 与告警规则文档一一对应
