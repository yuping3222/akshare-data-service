# 并行执行提示词

本目录现在包含 20 个可并行执行的提示词文件。每个提示词对应一个明确任务，默认给独立执行代理使用。

## 下一阶段目标

- 用 20 个并行任务，把项目从“设计已收口”推进到“首批 3 个数据集可落 Raw、可标准化、可门禁、可发布、可服务、可观测”。
- 试点范围只允许聚焦：
  - `market_quote_daily`
  - `financial_indicator`
  - `macro_indicator`
- 本阶段优先做架构主链路，不继续扩张新接口、新表和新命名体系。

## 使用规则

- 每个代理只处理一个提示词文件。
- 所有代理都必须先阅读以下文档：
  - `docs/design/00-project-goal.md`
  - `docs/design/01-architecture-rfc.md`
  - `docs/design/10-target-repo-layout.md`
  - `docs/design/30-standard-entities.md`
  - `docs/design/50-quality-rule-spec.md`
- 如需补充背景，优先参考：
  - `docs/design/05-current-to-target-mapping.md`
  - `docs/design/06-tech-debt-register.md`
  - `docs/all.md`
- 若引用历史文档，例如 `DATA_SOURCE_REDESIGN.md`、`MIGRATION_PLAN.md`，只可作为补充参考；若与当前有效文档冲突，以当前有效文档为准。
- 所有代理都必须遵守：
  - Service 默认只读 Served
  - Dataset 只使用标准名字
  - 质量规则只使用标准字段名
  - Raw 分区使用 `extract_date + batch_id`
  - 兼容层可以保留，但新逻辑不得继续堆进旧 `api.py`、`store/manager.py`、`core/schema.py`
- 如果依赖任务尚未合入，优先做 additive skeleton、接口契约、配置和测试夹具，不等待整条链路完全就绪。
- 如果任务之间发生文件冲突，优先保证架构契约，不为兼容旧实现继续扩散旧命名。

## 建议发车方式

- 第一组，先打基础和入口边界：`01` `02` `03` `11` `12` `13`
- 第二组，补齐标准化与存储主链路：`04` `05` `14` `15` `16` `17`
- 第三组，收口发布、服务和治理：`06` `07` `08` `18` `19`
- 第四组，做验证与运维闭环：`09` `10` `20`

如果你要一次起 20 个 agent，可以全部启动，但要求每个 agent 严格遵守自己的文件边界，优先新增目标模块和兼容壳，不抢改别人的主文件。

## 提示词列表

1. `01-ingestion-task-model.md`：统一抓取任务、批次、审计、checkpoint 模型
2. `02-raw-writer-manifest.md`：实现 Raw writer、manifest、schema fingerprint
3. `03-standard-entity-config.md`：落标准实体配置和字段字典骨架
4. `04-market-quote-normalizer.md`：实现 `market_quote_daily` 标准化器
5. `05-financial-and-macro-normalizer.md`：实现 `financial_indicator` 和 `macro_indicator` 标准化器
6. `06-quality-engine-and-gate.md`：实现质量引擎、门禁、报告、隔离区
7. `07-served-publisher-reader.md`：实现 Served 发布、读取、回滚
8. `08-service-read-only-refactor.md`：把 Service 主路径改造成只读 Served
9. `09-governance-catalog-lineage.md`：实现 catalog、schema registry、字段血缘骨架
10. `10-tests-and-regression-guard.md`：补齐契约、回放、发布、服务主路径回归测试
11. `11-common-foundation-and-compat.md`：抽出 `common/` 基础层并保留兼容壳
12. `12-ingestion-adapters-and-router.md`：把 source adapters 和 router 迁入 `ingestion/`
13. `13-ingestion-rate-limit-health-scheduler.md`：统一限流、源健康、调度与异步补数入口
14. `14-raw-replay-and-sample-archive.md`：实现 Raw 重放读取和样本存档
15. `15-source-mapping-and-normalize-version.md`：建立源字段映射包和标准化版本表
16. `16-standardized-storage-and-merge.md`：实现 Standardized writer、reader、merge、manifest、compaction
17. `17-quality-check-pack-and-score-cleanup.md`：补齐完整性、一致性、异常检查器并移除硬编码评分
18. `18-service-query-contract-docs-errors.md`：定义查询契约、文档绑定和服务错误语义
19. `19-governance-change-management-and-ownership.md`：补齐 schema 变更、字段废弃、owner 制度
20. `20-observability-alerts-runbooks-dashboard.md`：补齐可观测性、告警规则、运行手册和统一看板
