# 任务 19：补齐 Schema 变更、字段废弃与 Owner 制度

## 目标

在 catalog、schema registry、lineage 骨架之外，补齐治理流程层，让字段变更、下线和责任归属真正可执行。

## 必读文档

- `docs/all.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/30-standard-entities.md`
- `docs/design/05-current-to-target-mapping.md`

## 任务范围

- 新建 `docs/design/81-schema-change-process.md`
- 新建 `docs/design/82-field-deprecation.md`
- 新建 `docs/design/83-owner-model.md`
- 新建 `src/akshare_data/governance/ownership.py`
- 新建 `src/akshare_data/governance/change_log.py`
- 新建 `src/akshare_data/governance/deprecation.py`

## 关键要求

- schema 变更要明确新增、修改、删除字段的评审口径和版本策略
- 字段废弃必须有 deprecation window、替代字段和影响面说明
- owner 至少覆盖数据域、核心数据集、质量规则包
- 流程设计要和 catalog、schema registry、lineage 可串起来，而不是单独写文档

## 协作边界

- 本任务优先拥有 `ownership.py`、`change_log.py`、`deprecation.py`
- 不重写 `catalog.py`、`lineage.py`、`schema_registry.py`
- 如需和 catalog 联动，优先新增接口，不大改别人的主实现

## 非目标

- 不实现 Service 查询逻辑
- 不实现质量门禁

## 验收标准

- 首批 3 个数据集可以登记 owner
- schema 变更和字段下线有明确流程与落地载体
- 治理信息不再只存在于零散文档中
