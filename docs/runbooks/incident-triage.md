# 运行手册：故障分诊 (Incident Triage)

> 文档编号: `RUNBOOK-INCIDENT-TRIAGE`
> 最后更新: 2026-04-22

## 1. 目标

提供标准化的故障分诊流程，快速定位问题域（源 / 质量 / 发布 / 服务），
并引导到对应的处理流程。

## 2. 分诊流程图

```
收到告警/报告
    │
    ▼
┌─────────────────────────────────────────┐
│ Step 1: 确认告警级别                        │
│ - P0: 立即处理，15分钟内响应                  │
│ - P1: 1小时内响应                            │
│ - P2: 4小时内响应                            │
│ - P3: 24小时内处理                           │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│ Step 2: 查看统一看板                          │
│ - Panel 1: 任务执行健康度                     │
│ - Panel 2: 数据质量状态                       │
│ - Panel 3: 发布状态                           │
│ - Panel 4: 服务读取                           │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│ Step 3: 判断问题域                           │
│                                          │
│ 服务报错/延迟高？                             │
│   ├─ 缺数响应高 → 看发布状态                  │
│   │   ├─ 发布失败/回滚 → 【发布问题】          │
│   │   └─ 发布正常 → 看任务执行                │
│   │       ├─ 任务失败 → 【源问题】             │
│   │       └─ 任务正常 → 看质量状态             │
│   │           └─ 质量阻断 → 【质量问题】        │
│   └─ P95延迟高 → 【服务问题】                 │
│                                          │
│ 数据未更新？                                 │
│   ├─ 任务失败 → 【源问题】                    │
│   └─ 任务成功但质量失败 → 【质量问题】          │
│                                          │
│ 质量门禁失败？                                │
│   └─ 【质量问题】                             │
│                                          │
│ 发布失败？                                   │
│   └─ 【发布问题】                             │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│ Step 4: 执行对应 Runbook                     │
│ - 源问题 → [Backfill](backfill.md)          │
│ - 质量问题 → [Replay](replay.md)            │
│ - 发布问题 → [Rollback](rollback.md)        │
│ - 服务问题 → 本节 Step 5                     │
└─────────────────────────────────────────┘
```

## 3. 各问题域诊断步骤

### 3.1 源问题诊断

**症状**: 任务失败率高、熔断触发、数据延迟

```bash
# 查看源站健康状态
python -m akshare_data.offline.cli probe --all

# 查看失败任务分布
python -c "
from akshare_data.ingestion.source_health import SourceHealthMonitor
monitor = SourceHealthMonitor()
for source in ['lixinger', 'akshare', 'tushare']:
    status = monitor.get_source_status(source)
    print(f'{source}: {status}')
"

# 查看熔断状态
python -c "
from akshare_data.common.metrics import get_metric
trips = get_metric('akshare_data.ingestion.circuit_breaker_trips_total')
print(trips)
"
```

**处理**: 转向 [Backfill](backfill.md)

### 3.2 质量问题诊断

**症状**: 质量门禁失败、隔离区积压、Schema 漂移

```bash
# 查看质量报告
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --batch-id 20260422_001

# 查看失败规则详情
python -c "
from akshare_data.quality.report import QualityReport
report = QualityReport.load(dataset='market_quote_daily', batch_id='20260422_001')
for failure in report.failed_rules:
    print(f'Rule: {failure.rule_id}, Severity: {failure.severity}')
    print(f'  Failed records: {failure.failed_count}')
    print(f'  Details: {failure.details[:3]}')
"

# 查看隔离区数据
python -c "
from akshare_data.quality.quarantine import QuarantineManager
qm = QuarantineManager()
records = qm.list(dataset='market_quote_daily', batch_id='20260422_001')
print(f'Quarantined records: {len(records)}')
"
```

**处理**: 转向 [Replay](replay.md)

### 3.3 发布问题诊断

**症状**: 发布失败、回滚、版本不一致

```bash
# 查看发布状态
python -m akshare_data.offline.cli report publish-status \
    --dataset market_quote_daily

# 查看发布日志
python -c "
from akshare_data.served.publisher import ServedPublisher
publisher = ServedPublisher()
logs = publisher.get_publish_logs(dataset='market_quote_daily', limit=10)
for log in logs:
    print(f'{log.timestamp} | {log.status} | {log.release_version}')
    if log.error:
        print(f'  Error: {log.error}')
"

# 查看版本差异
python -c "
from akshare_data.served.publisher import ServedPublisher
publisher = ServedPublisher()
diff = publisher.compare_versions(
    dataset='market_quote_daily',
    version_a='v20260422_001',
    version_b='v20260421_001'
)
print(f'Record diff: {diff.record_count_diff}')
print(f'Schema diff: {diff.schema_diff}')
"
```

**处理**: 转向 [Rollback](rollback.md)

### 3.4 服务问题诊断

**症状**: 请求延迟高、错误率高、缓存命中率低

```bash
# 查看服务健康状态
python -m akshare_data.offline.cli probe --service --all

# 查看慢查询
python -c "
from akshare_data.service.reader import ServiceReader
reader = ServiceReader()
# 检查存储层响应时间
import time
start = time.time()
df = reader.query(dataset='market_quote_daily', start_date='2026-04-01', end_date='2026-04-22')
print(f'Query time: {time.time() - start:.2f}s, Records: {len(df)}')
"

# 查看缓存状态
python -c "
from akshare_data.store.cache_manager import CacheManager
cache = CacheManager()
stats = cache.get_stats()
print(f'Memory cache: {stats.memory_items}/{stats.memory_max}')
print(f'Hit rate: {stats.hit_rate:.2%}')
"

# 查看存储层状态
python -c "
from akshare_data.common.metrics import get_metric
small_files = get_metric('akshare_data.storage.small_files_total')
print(f'Small files: {small_files}')
"
```

**处理**:

| 问题 | 处理方式 |
|------|----------|
| 缓存命中率低 | 预热缓存或增加缓存容量 |
| 慢查询 | 检查 DuckDB 配置和 Parquet 文件分布 |
| 小文件过多 | 执行 compaction |
| 内存不足 | 增加 DuckDB 内存限制 |

## 4. 升级路径

| 时间 | 动作 |
|------|------|
| T+0 | 收到告警，开始分诊 |
| T+5min | 确认问题域 |
| T+15min | P0 告警开始执行修复 |
| T+30min | P0 未响应，自动升级 |
| T+1h | P1 开始执行修复 |
| T+2h | P1 未响应，自动升级 |
| T+4h | P2 开始执行修复 |

## 5. 故障记录模板

```yaml
incident:
  id: INC-20260422-001
  started_at: "2026-04-22T15:30:00Z"
  detected_by: "alert P1-001"
  severity: P1
  affected_datasets:
    - market_quote_daily
  root_cause_domain: quality
  root_cause_summary: "质量规则 mq_daily_pk_unique 失败，主键重复"
  actions_taken:
    - "15:35 确认问题域为质量问题"
    - "15:40 查看质量报告，确认失败规则"
    - "15:45 执行 replay 重放批次"
    - "16:00 重放完成，质量通过"
    - "16:05 重新发布"
  resolved_at: "2026-04-22T16:05:00Z"
  duration_minutes: 35
  related_batch_id: "20260422_001"
  related_release_version: "v20260422_001"
  follow_up:
    - "分析主键重复根因"
    - "检查源数据是否有重复"
```

## 6. 关联告警

本 Runbook 适用于所有告警级别的分诊：

| 告警规则 | 可能的问题域 |
|----------|-------------|
| P0-001 | 服务问题 |
| P0-002 | 源问题 |
| P0-003 | 源问题 |
| P0-004 | 发布问题 |
| P1-001 | 质量问题 |
| P1-002 | 源问题 |
| P1-003 | 源问题 |
| P1-004 | 质量问题 |
| P1-005 | 质量问题 |
| P1-006 | 发布问题 |
| P2-001 | 质量问题 |
| P2-002 | 源问题 |
| P2-003 | 服务问题 |
| P2-004 | 服务问题 |
| P2-005 | 存储问题 |
| P2-006 | 服务问题 |
| P2-007 | 源问题 |

## 7. 常见问题

### Q: 多个告警同时触发怎么办？

A: 按优先级处理：P0 > P1 > P2。
注意抑制规则，如 P0-003 触发时会抑制 P1-002。

### Q: 无法确定问题域？

A: 按看板顺序排查：Panel 1 → Panel 2 → Panel 3 → Panel 4。
大多数情况下可以看到明显异常的面板。

### Q: 分诊后发现是已知问题？

A: 检查是否有已有的故障记录，关联到已有 incident。
如果是重复发生，考虑是否需要调整告警阈值或修复根因。
