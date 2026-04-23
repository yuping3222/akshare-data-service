# 任务 09：实现元数据目录与血缘骨架

## 目标

建立 dataset、schema、字段、版本、血缘的治理骨架，避免后续继续散落在文档和代码里。

## 必读文档

- `docs/design/01-architecture-rfc.md`
- `docs/design/30-standard-entities.md`
- `docs/design/50-quality-rule-spec.md`
- `config/standards/entities/*.yaml`（如已存在）

## 任务范围

- 新建 `src/akshare_data/governance/catalog.py`
- 新建 `src/akshare_data/governance/lineage.py`
- 新建 `src/akshare_data/governance/schema_registry.py`
- 如需要，补充实体注册加载逻辑

## 关键要求

- dataset 名使用标准名
- schema registry 以标准实体 schema 为中心，不再只表达 cache table
- 字段血缘至少能表达 `source field -> standard field`
- 支持 `batch_id`、`schema_version`、`normalize_version`、`release_version` 追踪
- schema registry 优先从 `config/standards/entities/*.yaml` 加载，而不是继续把 `core/schema.py` 当事实源
- lineage 需要为任务 15 的映射配置和任务 07 的发布 manifest 预留接入点

## 协作边界

- 本任务优先拥有 `governance/catalog.py`、`lineage.py`、`schema_registry.py`
- 不实现 owner、deprecation、schema change flow，那属于任务 19
- 不修改 `config/standards/entities/*.yaml` 的事实定义，只消费它们

## 验收标准

- 可以注册首批 3 个数据集
- 可以查询字段来源和版本信息
- 后续 owner、deprecation、change log 能在不推翻本骨架的情况下继续接上
