# 任务 02：实现 Raw Writer 与 Manifest

## 目标

按 `docs/design/20-raw-spec.md` 实现 L0 Raw 写入基础设施。

## 必读文档

- `docs/design/01-architecture-rfc.md`
- `docs/design/20-raw-spec.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/00-project-goal.md`

## 任务范围

- 新建 `src/akshare_data/raw/system_fields.py`
- 新建 `src/akshare_data/raw/writer.py`
- 新建 `src/akshare_data/raw/manifest.py`
- 新建 `src/akshare_data/raw/schema_fingerprint.py`
- 如有必要，补充 `src/akshare_data/raw/__init__.py`

## 关键要求

- 路径使用 `data/raw/<domain>/<dataset>/extract_date=<date>/batch_id=<id>`
- Raw 只保留原字段名，不做 rename
- 支持 manifest 和 schema snapshot 输出
- 记录系统字段：`batch_id`、`source_name`、`interface_name`、`request_params_json`、`request_time`、`ingest_time`、`extract_date`、`extract_version`、`source_schema_fingerprint`、`raw_record_hash`
- 支持原子写入
- writer 输入优先兼容任务 01 定义的 `ExtractTask` / `BatchContext`
- manifest 记录“抽取行为”和技术证据，不表达业务发布语义

## 协作边界

- 本任务优先拥有 `src/akshare_data/raw/system_fields.py`、`writer.py`、`manifest.py`、`schema_fingerprint.py`
- 不创建 `raw/reader.py` 和 replay 逻辑，那属于任务 14
- 不实现 Standardized 层写入协议

## 非目标

- 不做 Standardized writer
- 不做服务层读取

## 验收标准

- 可将任意 DataFrame 以 Raw 规范写盘
- Manifest 表示“抽取行为”，不是业务日期快照
- 为后续 `raw/reader.py` 和 replay 留出稳定目录与 manifest 契约
