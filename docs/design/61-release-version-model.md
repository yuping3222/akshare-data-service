# 发布版本模型（T7-002）

> 最后更新：2026-04-23

## 1. 模型目标

定义 Served 层 `release_version` 的语义、状态机与生命周期。

## 2. 版本标识

建议格式：`{dataset}-r{YYYYMMDDHHmm}-{seq}`。

示例：

- `market_quote_daily-r202604230830-01`
- `financial_indicator-r202604231015-02`

约束：

- 同一 `dataset` 下必须唯一
- 时间前缀递增
- 重跑同批次使用 `seq` 递增

## 3. 状态机

`draft -> gated -> published -> deprecated -> archived`

状态说明：

- `draft`：已生成候选数据
- `gated`：门禁执行完成
- `published`：对查询流量可见
- `deprecated`：仍可读但不推荐
- `archived`：仅留审计，不对在线查询开放

## 4. 元数据字段

| 字段 | 说明 |
|---|---|
| release_version | 发布版本号 |
| dataset | 标准数据集 |
| schema_version | 结构版本 |
| normalize_versions | 各来源归一化版本集合 |
| quality_score | 规则评分（可选观测） |
| gate_result | pass/fail |
| record_count | 记录数 |
| generated_at | 生成时间 |
| published_at | 发布时间 |
| source_batches | 输入批次列表 |

## 5. 兼容与回滚

- 查询默认读取最新 `published` 版本
- 支持按 `release_version` 精确查询
- 回滚通过切换“当前生效版本指针”实现
