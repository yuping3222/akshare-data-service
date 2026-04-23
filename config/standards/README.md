# Standards — 标准实体与字段配置

本目录存放标准实体 schema 和字段字典，是 normalizer、quality、service 的统一契约。

## 目录结构

```
config/standards/
├── README.md                          # 本文件
├── field_dictionary.yaml              # 标准字段字典（全局）
├── entities/
│   ├── market_quote_daily.yaml        # 日线行情实体
│   ├── financial_indicator.yaml       # 财务指标实体
│   └── macro_indicator.yaml           # 宏观指标实体
```

## 文件说明

| 文件 | 用途 | 加载方 |
|------|------|--------|
| `field_dictionary.yaml` | 全局字段字典，含系统字段、业务字段、别名映射 | normalizer、quality、governance catalog |
| `entities/*.yaml` | 单个标准实体的完整 schema 定义 | normalizer、quality DSL、service docgen |

## 命名规则

- 实体名使用 `snake_case`，如 `market_quote_daily`
- 字段名使用 `snake_case`，如 `security_id`、`close_price`
- 禁止使用历史别名作为新配置名（如 `stock_daily`、`quote_daily`、`finance_indicator`）

## 与文档的关系

- 字段主名来源：`docs/design/30-standard-entities.md`
- 架构约束来源：`docs/design/01-architecture-rfc.md`
- 质量 DSL 规范：`docs/design/50-quality-rule-spec.md`
- 字段盘点来源：`docs/design/32-field-inventory.md`

## 系统字段

所有 Standardized 与 Served 数据集统一使用以下系统字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `batch_id` | string | 来源批次 |
| `source_name` | string | 来源数据源 |
| `interface_name` | string | 来源接口 |
| `ingest_time` | timestamp | 进入系统时间 |
| `normalize_version` | string | 标准化规则版本 |
| `schema_version` | string | 实体 schema 版本 |
| `quality_status` | string | 质量状态 |
| `publish_time` | timestamp | 发布到 Served 的时间 |
| `release_version` | string | 发布版本 |

不再使用 `_batch_id` 等下划线前缀版本。
