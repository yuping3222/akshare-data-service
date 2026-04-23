# Served 层规范（T6-010, T7-001）

> 最后更新：2026-04-23
> 目标：将 Served 定义为“只读发布层 + 门禁层”，并移除硬编码评分语义。

## 1. Served 的职责

Served 层只负责：

1. 从 Standardized 读取已标准化数据
2. 执行发布门禁（质量、完整性、版本一致性）
3. 产出可查询的发布版本（release）
4. 提供回滚与审计能力

Served 层不负责：

- 回源抓取
- 字段归一化
- 业务评分硬编码

## 2. 发布输入契约

最小输入字段：

- `security_id` / 主键字段
- 业务主时间字段（如 `trade_date`）
- `normalize_version`
- `schema_version`
- `quality_status`
- `publish_time`
- `release_version`

## 3. 发布门禁

门禁来源：`config/quality/<dataset>.yaml`。

- `gate_action=block` 的失败规则会阻断发布
- `gate_action=alert` 只产生告警，不阻断
- `gate_action=ignore` 仅记录审计

## 4. 移除硬编码评分（T6-010）

执行要求：

- 禁止在 Served 发布流程中写死阈值评分逻辑（如固定 60/80/90）
- 质量得分统一由规则执行结果 + `RuleBasedScorer` 计算
- 得分仅作为观测指标，门禁判定仍以规则 `gate_action` 为准

迁移结论：

- 历史硬编码评分归档为 legacy 行为，不再作为发布真相
- 新发布语义只接受 rule-based score 与 gate 决策

## 5. Manifest 规范

每个发布版本必须有 manifest，至少包含：

- `dataset`
- `release_version`
- `schema_version`
- `normalize_versions`
- `record_count`
- `generated_at`
- `gate_summary`（通过/失败规则统计）

## 6. 回滚规范

- 回滚粒度：`dataset + release_version`
- 回滚必须保留审计日志（触发人、时间、原因）
- 回滚不改写历史 manifest，只新增 rollback 事件记录
