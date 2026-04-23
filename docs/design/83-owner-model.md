# Owner 制度

> 任务编号: `T9-006`
> 文档编号: `GOV-083`
> 最后更新: 2026-04-22
> 状态: Draft

## 1. 目标

建立数据资产的责任归属体系，确保：

- 每个数据域有明确负责人
- 每个核心数据集有明确负责人
- 每组质量规则有明确负责人
- 变更审批有权限控制

## 2. Owner 层级

```
Domain Owner -> Dataset Owner -> QualityRule Owner
```

| 层级 | 覆盖范围 | 职责 |
|------|----------|------|
| `DomainOwner` | 数据域（如 quote、finance、macro） | 域内数据口径、schema 变更审批 |
| `DatasetOwner` | 具体数据集（如 market_quote_daily） | 数据集质量、字段变更、废弃审批 |
| `QualityRuleOwner` | 质量规则包 | 规则维护、阈值调整 |

## 3. 数据域划分

首批 3 个数据域：

| 域名 | 说明 | 包含数据集 |
|------|------|------------|
| `quote` | 行情域 | `market_quote_daily`, `market_quote_minute` |
| `finance` | 财务域 | `financial_indicator`, `financial_statement_item` |
| `macro` | 宏观域 | `macro_indicator` |

## 4. Owner 登记

### 4.1 Owner 信息

每个 Owner 记录包含：

| 字段 | 说明 |
|------|------|
| `owner_id` | Owner 唯一标识（如 GitHub username） |
| `owner_name` | 显示名称 |
| `owner_type` | `domain` / `dataset` / `quality_rule` |
| `scope` | 负责范围（域名、数据集名或规则包名） |
| `since` | 担任 Owner 的起始日期 |
| `backup_owner_id` | 备份负责人（可选） |

### 4.2 登记方式

Owner 信息登记在 `config/governance/owners.yaml` 中，
同时通过 `OwnershipRegistry`（见 `governance/ownership.py`）加载和管理。

## 5. Owner 职责

### 5.1 Domain Owner

- 审批域内 schema 变更提案
- 审批域内字段废弃申请
- 维护域内数据口径文档
- 协调跨数据集的依赖关系

### 5.2 Dataset Owner

- 维护数据集 schema 定义
- 审批字段级别的变更
- 确保数据集质量达标
- 响应下游消费者的问题

### 5.3 QualityRule Owner

- 维护质量规则配置
- 调整质量阈值
- 处理质量异常

## 6. 权限矩阵

| 操作 | Domain Owner | Dataset Owner | QualityRule Owner |
|------|--------------|---------------|-------------------|
| 发起 schema 变更 | 是 | 是（限自己数据集） | 否 |
| 审批 schema 变更 | 是 | 否 | 否 |
| 发起字段废弃 | 是 | 是 | 否 |
| 审批字段废弃 | 是 | 否 | 否 |
| 修改质量规则 | 否 | 是（限自己数据集） | 是 |
| 注册新数据集 | 是 | 否 | 否 |

## 7. 与治理模块的串联

### 7.1 与 SchemaRegistry 的集成

- `SchemaChange` 记录必须包含 `owner_id`
- `OwnershipRegistry` 验证变更发起者是否有权限

### 7.2 与 DatasetCatalog 的集成

- `Dataset.metadata` 中记录 `owner_id`
- 可通过 `catalog.get_owner(dataset_name)` 查询

### 7.3 与 ChangeLog 的集成

- `ChangeLog` 中每条变更记录关联 `owner_id`
- 支持按 Owner 查询变更历史

### 7.4 与 LineageTracker 的集成

- 血缘记录可关联 `owner_id`
- 支持查询某 Owner 负责的所有血缘链路

## 8. 首批 Owner 登记示例

```yaml
# config/governance/owners.yaml
domains:
  quote:
    owner_id: "data-team-lead"
    owner_name: "数据负责人"
    since: "2026-04-22"
    backup_owner_id: "data-team-backup"

  finance:
    owner_id: "finance-data-owner"
    owner_name: "财务数据负责人"
    since: "2026-04-22"

  macro:
    owner_id: "macro-data-owner"
    owner_name: "宏观数据负责人"
    since: "2026-04-22"

datasets:
  market_quote_daily:
    owner_id: "data-team-lead"
    owner_name: "数据负责人"
    domain: quote
    since: "2026-04-22"

  financial_indicator:
    owner_id: "finance-data-owner"
    owner_name: "财务数据负责人"
    domain: finance
    since: "2026-04-22"

  macro_indicator:
    owner_id: "macro-data-owner"
    owner_name: "宏观数据负责人"
    domain: macro
    since: "2026-04-22"

quality_rules:
  market_quote_daily_rules:
    owner_id: "data-team-lead"
    owner_name: "数据负责人"
    dataset: market_quote_daily
    since: "2026-04-22"

  financial_indicator_rules:
    owner_id: "finance-data-owner"
    owner_name: "财务数据负责人"
    dataset: financial_indicator
    since: "2026-04-22"

  macro_indicator_rules:
    owner_id: "macro-data-owner"
    owner_name: "宏观数据负责人"
    dataset: macro_indicator
    since: "2026-04-22"
```

## 9. Owner 变更流程

Owner 变更需要：

1. 原 Owner 或 Domain Owner 发起变更申请
2. 新 Owner 确认接受
3. 更新 `owners.yaml` 配置
4. 在 `ChangeLog` 中记录 `OWNER_CHANGE` 事件
5. 通知相关下游消费者
