# 任务 05：实现 `financial_indicator` 与 `macro_indicator` 标准化器

## 目标

打通另外两个 P0 数据集的标准化能力。

## 必读文档

- `docs/design/30-standard-entities.md`
- `docs/design/50-quality-rule-spec.md`
- `docs/design/01-architecture-rfc.md`
- `config/standards/entities/financial_indicator.yaml`（如已存在）
- `config/standards/entities/macro_indicator.yaml`（如已存在）

## 任务范围

- 新建 `src/akshare_data/standardized/normalizer/financial_indicator.py`
- 新建 `src/akshare_data/standardized/normalizer/macro_indicator.py`
- 如需要，补充 `tests/fixtures/standardized_samples/financial_indicator/`
- 如需要，补充 `tests/fixtures/standardized_samples/macro_indicator/`

## 关键要求

- `financial_indicator` 使用 `security_id/report_date/report_type`
- `macro_indicator` 使用 `indicator_code/observation_date`
- 比率字段统一带 `_pct` 或标准后缀
- 避免把 `date`、`symbol` 继续作为标准字段主名
- 优先复用任务 04 的 `standardized/normalizer/base.py`，不要重复实现第二套基类
- 如果映射配置尚未合入，可先按标准实体文档和 Raw 样本落最小映射，但必须为统一 mapping loader 预留接入点

## 协作边界

- 本任务优先拥有 `financial_indicator.py` 和 `macro_indicator.py`
- 不修改 `standardized/normalizer/base.py`，除非只是兼容性极小补丁
- 不创建 `config/mappings/sources/*.yaml`，那属于任务 15

## 非目标

- 不实现 Standardized writer
- 不实现 Served 发布

## 验收标准

- 两个 normalizer 都能从 Raw 产生标准字段
- 字段命名和实体文档一致
- 为后续质量规则执行做好准备
- 不与 `market_quote_daily` normalizer 抢占共享基类或共享 helper 的所有权
