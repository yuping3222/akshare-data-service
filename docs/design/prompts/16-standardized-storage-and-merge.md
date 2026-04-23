# 任务 16：实现 Standardized 存储、读取、合并与清理

## 目标

补齐 L1 Standardized 的物理落地能力，让标准化结果真正成为可写、可读、可追踪、可重跑的数据资产层。

## 必读文档

- `docs/all.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/30-standard-entities.md`

## 任务范围

- 新建 `docs/design/40-standardized-storage-spec.md`
- 新建 `src/akshare_data/standardized/writer.py`
- 新建 `src/akshare_data/standardized/reader.py`
- 新建 `src/akshare_data/standardized/merge.py`
- 新建 `src/akshare_data/standardized/manifest.py`
- 新建 `src/akshare_data/standardized/compaction.py`

## 关键要求

- 分区语义以标准实体的业务时间为准，不复用 Raw 的抽取分区
- writer 必须做 schema 校验、主键去重、系统字段补齐
- merge/upsert 需要明确晚到数据、重复数据、增量覆盖的规则
- manifest 要能追踪 `dataset`、`schema_version`、`normalize_version`、`batch_id`
- compaction 不能破坏版本追溯

## 协作边界

- 本任务优先拥有 `src/akshare_data/standardized/writer.py`、`reader.py`、`merge.py`
- 不实现 dataset-specific normalizer
- 不实现质量门禁和 Served 发布

## 非目标

- 不直接改 `store/manager.py` 为新主资产层
- 不实现 Raw writer

## 验收标准

- Standardized 数据可按实体和时间范围稳定读写
- 重跑同一批次不会制造重复数据
- 后续质量引擎和发布器可以直接消费 Standardized 层
