# 任务 20：补齐可观测性、告警、运行手册与统一看板

## 目标

建立从任务执行、数据质量、发布状态到服务读取的观测闭环，让系统不仅“能跑”，还“出了问题知道怎么看、怎么补、怎么回滚”。

## 必读文档

- `docs/all.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/06-tech-debt-register.md`
- `docs/design/50-quality-rule-spec.md`

## 任务范围

- 新建 `docs/design/100-observability-metrics.md`
- 新建 `docs/design/101-alert-rules.md`
- 新建 `docs/runbooks/replay.md`
- 新建 `docs/runbooks/backfill.md`
- 新建 `docs/runbooks/rollback.md`
- 新建 `docs/runbooks/incident-triage.md`
- 新建 `src/akshare_data/common/metrics.py`
- 如有必要，补充 `src/akshare_data/common/events.py`

## 关键要求

- 指标至少覆盖：任务成功率、失败原因、质量通过率、发布状态、服务响应、数据新鲜度
- 指标和事件要能关联 `dataset`、`batch_id`、`release_version`
- 告警规则至少区分 `P0`、`P1`、`P2`
- 运行手册必须覆盖 replay、backfill、rollback、故障分诊
- 看板设计要能回答“是源问题、质量问题、发布问题还是服务问题”

## 协作边界

- 本任务优先拥有 `common/metrics.py`、`common/events.py` 和 `docs/runbooks/`
- 不大改 `quality/engine.py`、`served/publisher.py`、`service/data_service.py` 主逻辑
- 如需埋点，优先提供轻量 hook 和文档约定

## 非目标

- 不实现新的业务数据集
- 不把监控逻辑塞回旧 `api.py`

## 验收标准

- 有统一的指标口径和告警分级文档
- 关键运维操作有可执行 runbook
- 后续任务可以基于统一指标和事件埋点接入监控系统
