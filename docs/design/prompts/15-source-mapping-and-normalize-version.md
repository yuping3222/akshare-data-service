# 任务 15：建立源字段映射包与标准化版本表

## 目标

把“代码里 scattered rename”收口成配置化、版本化的源字段映射能力，作为标准化器和血缘系统的共同输入。

## 必读文档

- `docs/all.md`
- `docs/design/30-standard-entities.md`
- `docs/design/32-field-inventory.md`
- `docs/design/50-quality-rule-spec.md`

## 任务范围

- 新建 `config/mappings/sources/akshare/market_quote_daily.yaml`
- 新建 `config/mappings/sources/akshare/financial_indicator.yaml`
- 新建 `config/mappings/sources/akshare/macro_indicator.yaml`
- 如有必要，补充 `config/mappings/sources/lixinger/*.yaml`
- 如有必要，补充 `config/mappings/sources/tushare/*.yaml`
- 补充 `config/standards/normalize_versions.yaml`
- 新建 `src/akshare_data/standardized/mapping_loader.py`

## 关键要求

- 每个源字段都必须映射到标准字段、标记废弃，或显式说明暂不接入
- 字段名必须使用标准实体字段，不能重新发明 `date/close/amount` 这类历史主名
- 映射规则必须带版本，支持 `normalize_version`
- 映射配置要能被 normalizer、lineage、测试直接复用

## 协作边界

- 本任务优先拥有 `config/mappings/sources/` 和 `normalize_versions.yaml`
- 不实现具体 normalizer 业务逻辑
- 不改质量 DSL 字段名

## 非目标

- 不实现 Standardized writer
- 不改 Service 层输出

## 验收标准

- 首批 3 个数据集具备可加载的源字段映射配置
- 可以根据数据集和 source 解析出当前 `normalize_version`
- 后续 normalizer 不再需要手写大段 rename 逻辑
