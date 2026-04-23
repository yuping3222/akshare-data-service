# 任务 01：统一抓取任务模型

## 目标

实现统一的抓取任务和批次模型，收口当前在线抓取、离线下载、脚本补数三套概念。

## 必读文档

- `docs/design/00-project-goal.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/20-raw-spec.md`
- `docs/design/05-current-to-target-mapping.md`

## 任务范围

- 新建 `src/akshare_data/ingestion/models/task.py`
- 新建 `src/akshare_data/ingestion/models/batch.py`
- 新建 `src/akshare_data/ingestion/task_state.py`
- 新建 `src/akshare_data/ingestion/idempotency.py`
- 新建 `src/akshare_data/ingestion/checkpoint.py`
- 新建 `src/akshare_data/ingestion/audit.py`
- 如有必要，补充 `src/akshare_data/ingestion/__init__.py`

## 关键要求

- 定义 `ExtractTask`、`BatchContext` 或同等语义对象
- 字段至少包含：`task_id`、`batch_id`、`dataset`、`source_name`、`interface_name`、`params`、`extract_date`
- 任务模型还应表达：`domain`、`task_window`、`status`、`retry_count`、`idempotency_key`
- 不使用旧的 cache table 名作为 dataset 名
- 为后续 Raw writer 提供稳定输入对象
- 审计对象要能记录请求参数、耗时、状态、错误
- 状态机至少支持：`pending`、`running`、`succeeded`、`failed`、`partial`、`retrying`
- 幂等键必须稳定可重算，供 Raw writer、scheduler、backfill 复用

## 协作边界

- 本任务优先拥有 `src/akshare_data/ingestion/models/`、`task_state.py`、`idempotency.py`
- 不实现 `scheduler.py`、`rate_limiter.py`、`source_health.py`，这些由后续任务负责
- 如果其他任务已创建 `ingestion/__init__.py`，只做增量导出，不回退他人改动

## 非目标

- 不直接改造 DataService 主逻辑
- 不直接实现具体抓取 adapter

## 验收标准

- 新模型可表达单任务、批次、重试、断点续跑
- 字段命名符合 `docs/design/30-standard-entities.md` 和 `20-raw-spec.md`
- 不引入第二套任务命名体系
- scheduler、audit、raw writer 后续都能以同一任务对象为输入
