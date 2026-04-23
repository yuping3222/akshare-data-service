# 任务 14：实现 Raw 重放读取与样本存档

## 目标

补齐 Raw 层的读取、重放和样本存档能力，让首批数据集具备可回放、可复现、可做契约测试的基础。

## 必读文档

- `docs/all.md`
- `docs/design/20-raw-spec.md`
- `docs/design/01-architecture-rfc.md`

## 任务范围

- 新建或补充 `src/akshare_data/raw/reader.py`
- 新建 `src/akshare_data/raw/replay.py`
- 新建 `tests/fixtures/raw_samples/README.md`
- 如有必要，建立 `data/system/raw_samples/` 的目录规范说明

## 关键要求

- 支持按 `batch_id`、`dataset`、`extract_date`、分区读回 Raw 数据
- replay 输入是 Raw 证据，不是重新回源抓取
- 样本存档要能服务字段映射测试、schema 漂移测试、标准化回放测试
- 样本命名和元数据必须保留 `source_name`、`interface_name`、`batch_id`

## 协作边界

- 不重写任务 02 的 writer/manifest 主逻辑
- 如果 `raw/reader.py` 已由其他任务创建，优先补 replay 能力和读取契约
- 不直接实现 Standardized normalizer

## 非目标

- 不做质量门禁
- 不做 Served 发布

## 验收标准

- 能按批次和分区读回 Raw 数据
- 首批核心接口有稳定的 Raw 样本存档规范
- 后续测试可以基于 Raw 样本做重放，而不是依赖真实源站
