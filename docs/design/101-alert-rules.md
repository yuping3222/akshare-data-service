# 告警规则规范

> 任务编号: `T11-005`
> 最后更新: 2026-04-22

## 1. 目标

基于可观测性指标（100-observability-metrics.md）定义分级告警规则，
确保问题能在影响扩大前被发现和处理。

## 2. 告警分级

| 级别 | 名称 | 定义 | 响应时限 | 通知方式 | 升级策略 |
|------|------|------|----------|----------|----------|
| P0 | 紧急 | 服务不可用或数据大面积异常 | 15分钟 | 电话+短信+即时通讯 | 30分钟未响应自动升级 |
| P1 | 严重 | 核心数据未更新或质量门禁失败 | 1小时 | 短信+即时通讯 | 2小时未响应升级 |
| P2 | 警告 | 非核心数据异常或性能下降 | 4小时 | 即时通讯 | 次日未处理升级 |
| P3 | 提示 | 趋势性变化或潜在风险 | 24小时 | 邮件/看板 | 周报汇总 |

## 3. 告警规则清单

### 3.1 P0 — 紧急

| 规则ID | 名称 | 指标 | 条件 | 影响范围 | 关联Runbook |
|--------|------|------|------|----------|-------------|
| P0-001 | 服务完全不可用 | `service.request_total` | 5分钟内错误率 > 90% | 全部用户 | incident-triage |
| P0-002 | 核心数据集断更 | `freshness.data_lag_seconds` | `market_quote_daily` 延迟 > 2h (交易日) | 行情用户 | backfill |
| P0-003 | 全部源站熔断 | `ingestion.circuit_breaker_trips_total` | 3个源同时熔断 > 10min | 全部数据 | incident-triage |
| P0-004 | 发布系统异常 | `served.publish_total{status="failed"}` | 连续3次发布失败 | 新版本不可用 | rollback |

### 3.2 P1 — 严重

| 规则ID | 名称 | 指标 | 条件 | 影响范围 | 关联Runbook |
|--------|------|------|------|----------|-------------|
| P1-001 | 核心数据质量门禁失败 | `quality.gate_pass_rate` | `market_quote_daily` / `financial_indicator` 门禁 = 0 | 对应数据集 | replay |
| P1-002 | 单源站持续失败 | `ingestion.task_total{status="failed"}` | 单一源连续失败 > 20次 | 依赖该源的数据集 | backfill |
| P1-003 | 数据延迟超标 | `freshness.data_lag_seconds` | 核心数据集延迟 > 1h (交易日) | 对应数据集 | backfill |
| P1-004 | Schema漂移 | `quality.schema_drifts_total` | 24h内 > 0 次 | 对应数据集 | incident-triage |
| P1-005 | 隔离区积压 | `quality.quarantine_records_total` | 积压 > 10000条 | 对应数据集 | replay |
| P1-006 | 发布回滚 | `served.rollback_total` | 24h内 > 1 次 | 对应数据集 | rollback |

### 3.3 P2 — 警告

| 规则ID | 名称 | 指标 | 条件 | 影响范围 | 关联Runbook |
|--------|------|------|------|----------|-------------|
| P2-001 | 非核心数据质量波动 | `quality.gate_pass_rate` | 非核心数据集门禁通过率 < 95% | 对应数据集 | replay |
| P2-002 | 限流频繁触发 | `ingestion.rate_limit_hits_total` | 单源1h内限流 > 50次 | 对应源 | backfill |
| P2-003 | 服务延迟升高 | `service.request_duration_seconds` | P95延迟 > 2s (基线2倍) | 对应endpoint | incident-triage |
| P2-004 | 缓存命中率下降 | `service.cache_hit_rate` | 命中率 < 50% | 全服务 | incident-triage |
| P2-005 | 小文件过多 | `storage.small_files_total` | 单数据集小文件 > 500 | 对应数据集 | - |
| P2-006 | 缺数响应增多 | `service.missing_data_responses_total` | 1h内缺数响应 > 100次 | 对应endpoint | backfill |
| P2-007 | 非核心数据延迟 | `freshness.data_lag_seconds` | 非核心数据集延迟 > 4h | 对应数据集 | backfill |

### 3.4 P3 — 提示

| 规则ID | 名称 | 指标 | 条件 | 影响范围 | 关联Runbook |
|--------|------|------|------|----------|-------------|
| P3-001 | 存储增长趋势 | `storage.storage_bytes` | 周环比增长 > 30% | 全局 | - |
| P3-002 | 任务重试率上升 | `ingestion.task_retry_total` | 重试率 > 10% (7日均值) | 对应源 | - |
| P3-003 | 质量规则持续告警 | `quality.failed_rules_total{severity="warning"}` | 同一规则连续7天告警 | 对应数据集 | replay |

## 4. 告警抑制与去重

### 4.1 抑制规则

| 抑制场景 | 规则 |
|----------|------|
| 源站熔断 | 熔断期间抑制该源的 P1-002 告警 |
| 维护窗口 | 维护窗口期间抑制所有 P2/P3 告警 |
| 级联失败 | P0-003 触发时抑制所有源的 P1-002 告警 |
| 发布中 | 发布进行中抑制 P0-004 告警（改为P2） |

### 4.2 告警聚合

- 同一 `dataset` + 同一规则 5分钟内多次触发 → 合并为1条
- 同一 `source_name` 的多个数据集告警 → 按源聚合
- 告警恢复时发送恢复通知

## 5. 告警上下文

每条告警必须包含以下上下文信息：

```yaml
alert:
  rule_id: P1-001
  severity: P1
  dataset: market_quote_daily
  batch_id: "20260422_001"
  release_version: "v20260422_001"
  triggered_at: "2026-04-22T15:30:00Z"
  metric_value: 0.0
  threshold: 1.0
  duration: "5m"
  related_metrics:
    - quality.failed_rules_total
    - quality.quarantine_records_total
  runbook_url: "docs/runbooks/replay.md"
  suggested_action: "检查质量报告，确认失败规则，执行replay或回滚"
```

## 6. 告警配置格式

```yaml
# config/alerts/rules.yaml
version: "1.0"
rules:
  - rule_id: P0-001
    name: "服务完全不可用"
    severity: P0
    metric: "akshare_data.service.request_total"
    condition:
      expression: "rate(error[5m]) / rate(total[5m]) > 0.9"
      for: "5m"
    labels:
      team: "data-service"
    annotations:
      summary: "服务错误率超过90%"
      runbook: "docs/runbooks/incident-triage.md"
    inhibit:
      - during_maintenance: true
```

## 7. 与指标文档的关系

本告警规则引用 100-observability-metrics.md 中定义的指标。
新增告警规则时，需确保对应指标已定义。

## 8. 验收标准

- [x] 告警分级为 P0/P1/P2/P3
- [x] P0 覆盖服务不可用、核心数据断更、全源熔断、发布异常
- [x] P1 覆盖质量门禁失败、源站持续失败、数据延迟、Schema漂移
- [x] P2 覆盖非核心质量波动、限流、性能下降、缺数
- [x] 每条告警关联 dataset、batch_id、release_version
- [x] 每条告警关联对应 Runbook
- [x] 包含告警抑制和去重策略
