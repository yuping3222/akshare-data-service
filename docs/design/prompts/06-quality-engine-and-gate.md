# 任务 06：实现质量引擎与门禁

## 目标

按新 DSL 落地质量执行与门禁，阻断不合格数据发布。

## 必读文档

- `docs/design/50-quality-rule-spec.md`
- `docs/design/30-standard-entities.md`
- `docs/design/01-architecture-rfc.md`
- `config/standards/entities/*.yaml`（如已存在）

## 任务范围

- 新建 `src/akshare_data/quality/engine.py`
- 新建 `src/akshare_data/quality/gate.py`
- 新建 `src/akshare_data/quality/report.py`
- 新建 `src/akshare_data/quality/quarantine.py`
- 新建 `config/quality/market_quote_daily.yaml`
- 新建 `config/quality/financial_indicator.yaml`
- 新建 `config/quality/macro_indicator.yaml`

## 关键要求

- 规则文件按标准 dataset 命名
- 规则字段名必须和标准实体一致
- Raw 只做技术规则
- Standardized 执行业务规则
- 门禁输出可供 Served 发布前直接判断
- 引擎要做成可扩展框架，后续任务 17 可以继续补 `checks/` 而不推翻主干
- 配置文件先落最小可用规则集，不必在本任务一次塞满所有规则类型

## 协作边界

- 本任务优先拥有 `quality/engine.py`、`gate.py`、`report.py`、`quarantine.py` 和 `config/quality/*.yaml`
- 不在本任务中实现 `quality/checks/completeness.py`、`consistency.py`、`anomaly.py`，这些属于任务 17
- 如果后续任务更新 `config/quality/*.yaml`，以增量补充为主，不另起新命名

## 非目标

- 不实现 Served 发布器
- 不改 Service 查询契约

## 验收标准

- 任意 `error + block` 失败时可以阻断发布
- 可以产出结构化质量结果和隔离记录
- 质量结果结构足以被发布器和服务层状态读取直接消费
