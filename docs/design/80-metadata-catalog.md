# Metadata Catalog 设计（T8-007, T9-001）

> 最后更新：2026-04-23

## 1. 目标

建立统一元数据目录，管理“数据集定义、字段字典、版本、质量、错误语义”。

## 2. Catalog 核心对象

1. `dataset_catalog`：数据集清单、owner、SLA
2. `field_catalog`：字段定义、类型、是否必填、弃用状态
3. `version_catalog`：schema/normalize/release 版本关联
4. `quality_catalog`：质量规则、门禁结果、得分摘要
5. `error_catalog`：服务错误码分层与语义

## 3. 错误语义分层（T8-007）

错误码按层分段：

- `1xxx`：参数与请求错误
- `2xxx`：服务路由与查询错误
- `3xxx`：存储层错误
- `4xxx`：依赖/外部系统错误
- `5xxx`：质量规则与门禁错误
- `6xxx`：元数据与版本治理错误

每个错误记录至少包含：

- `code`
- `domain`
- `layer`
- `http_status`
- `retryable`
- `message_template`
- `operator_action`

## 4. 最小查询能力

Catalog 需要支持：

- 按 `dataset` 查询当前 schema/normalize/release 版本
- 按字段查询 canonical 名称与 alias
- 按 `release_version` 查询门禁结果与 manifest
- 按错误码查询语义与处置建议

## 5. 与现有资产关系

- 字段字典来源：`config/standards/field_dictionary.yaml`
- 规则来源：`config/quality/*.yaml`
- 发布信息来源：`served/manifest`
- 错误模型来源：`akshare_data.common.errors`
