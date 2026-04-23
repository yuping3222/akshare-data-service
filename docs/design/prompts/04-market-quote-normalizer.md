# 任务 04：实现 `market_quote_daily` 标准化器

## 目标

打通首个核心标准数据集 `market_quote_daily` 的 `Raw -> Standardized` 转换。

## 必读文档

- `docs/design/20-raw-spec.md`
- `docs/design/30-standard-entities.md`
- `docs/design/50-quality-rule-spec.md`
- `config/standards/entities/market_quote_daily.yaml`（如已存在）
- `config/mappings/sources/` 下对应映射文件（如已存在）

## 任务范围

- 新建 `src/akshare_data/standardized/normalizer/base.py`
- 新建 `src/akshare_data/standardized/normalizer/market_quote_daily.py`
- 如需要，补充 `tests/fixtures/standardized_samples/market_quote_daily/`

## 关键要求

- 输入来自 Raw，输出字段严格使用标准字段名
- 把旧字段映射到：`security_id`、`trade_date`、`open_price`、`high_price`、`low_price`、`close_price`、`volume`、`turnover_amount`、`adjust_type`
- 补齐系统字段：`batch_id`、`source_name`、`interface_name`、`ingest_time`、`normalize_version`、`schema_version`
- 不允许再输出 `symbol/date/close/amount`
- `base.py` 只放通用流程和 hook，不把 `market_quote_daily` 专属映射硬编码进去
- 如果任务 15 的映射配置尚未合入，可先在模块内保留临时映射常量，但必须预留统一 loader 接口，不另起第二套配置路径

## 协作边界

- 本任务优先拥有 `src/akshare_data/standardized/normalizer/base.py` 和 `market_quote_daily.py`
- 不修改 `src/akshare_data/standardized/writer.py`、`reader.py`、`merge.py`
- 尽量不要抢写共享 `standardized/fields.py`、`symbols.py`；若必须补 helper，优先放到本任务私有模块内

## 非目标

- 不实现 Served 发布
- 不改 service 层查询接口

## 验收标准

- 标准化器能处理至少一个主数据源样例
- 生成字段与实体 schema 完全一致
- 后续 `financial_indicator` 和 `macro_indicator` normalizer 可以复用 `base.py`，但不受本任务的私有逻辑耦合
