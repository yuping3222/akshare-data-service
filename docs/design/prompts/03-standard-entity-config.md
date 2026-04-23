# 任务 03：建立标准实体配置与字段字典骨架

## 目标

把标准实体和标准字段从文档落到配置，作为后续 normalizer、quality、service 的统一契约。

## 必读文档

- `docs/design/30-standard-entities.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/50-quality-rule-spec.md`
- `docs/design/32-field-inventory.md`

## 任务范围

- 新建 `config/standards/field_dictionary.yaml`
- 新建 `config/standards/entities/market_quote_daily.yaml`
- 新建 `config/standards/entities/financial_indicator.yaml`
- 新建 `config/standards/entities/macro_indicator.yaml`
- 如有必要，补充 `config/standards/README.md`

## 关键要求

- 字段命名必须完全对齐 `30-standard-entities.md`
- 使用 `security_id`、`trade_date`、`adjust_type`、`turnover_amount` 等标准名字
- 统一系统字段，不使用 `_batch_id` 一类旧写法
- 每个实体要写清主键、分区字段、时间字段、必填字段、字段类型
- 字段字典至少覆盖首批 3 个数据集当前会被 normalizer、quality、service 用到的字段
- 实体配置应可被 governance catalog、quality DSL、service docgen 直接加载

## 协作边界

- 本任务优先拥有 `config/standards/field_dictionary.yaml` 和 `config/standards/entities/*.yaml`
- 不创建 `config/mappings/sources/*.yaml`，那属于任务 15
- 不编辑 `config/standards/normalize_versions.yaml`，那也属于任务 15

## 非目标

- 不实现 normalizer 代码
- 不改历史 schema registry 全量定义

## 验收标准

- 首批 3 个数据集的配置可被质量规则和 normalizer 直接引用
- 不存在 `stock_daily`、`quote_daily` 之类的新配置文件
- 不引入第二套字段主名或系统字段命名
