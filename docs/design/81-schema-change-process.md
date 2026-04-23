# Schema 变更流程

> 任务编号: `T9-004`
> 文档编号: `GOV-081`
> 最后更新: 2026-04-22
> 状态: Draft

## 1. 目标

规范 `EntitySchema` 的字段新增、修改、删除流程，确保每次 schema 变更：

- 有版本可追溯
- 有评审可执行
- 有下游影响面评估
- 与 `SchemaRegistry`、`DatasetCatalog`、`LineageTracker` 可串联

## 2. 适用范围

本文档适用于以下变更类型：

| 变更类型 | 说明 | 示例 |
|----------|------|------|
| `ADD_FIELD` | 新增字段 | 给 `market_quote_daily` 增加 `pre_close_price` |
| `MODIFY_FIELD` | 修改字段属性 | 将 `change_pct` 从可选改为必填 |
| `REMOVE_FIELD` | 删除字段 | 移除已废弃的 `amount` 别名 |
| `RENAME_FIELD` | 字段重命名 | `symbol` -> `security_id`（需走废弃流程） |

## 3. 版本策略

### 3.1 Schema 版本号规则

版本号采用 `MAJOR.MINOR.PATCH` 语义：

| 变更类型 | 版本升级规则 |
|----------|--------------|
| `ADD_FIELD`（可选字段） | `MINOR + 1` |
| `ADD_FIELD`（必填字段） | `MAJOR + 1` |
| `MODIFY_FIELD`（向后兼容） | `MINOR + 1` |
| `MODIFY_FIELD`（破坏性） | `MAJOR + 1` |
| `REMOVE_FIELD` | `MAJOR + 1` |
| `RENAME_FIELD` | `MAJOR + 1` |

### 3.2 向后兼容定义

- **兼容**：旧消费者仍能正常读取数据，新增字段有默认值或可空
- **破坏性**：旧消费者可能报错或产生错误结果

## 4. 变更流程

```
提案 -> 评审 -> 实施 -> 发布 -> 通知
```

### 4.1 提案阶段

提案人填写变更申请，包含：

1. 变更类型（ADD/MODIFY/REMOVE/RENAME）
2. 目标实体名
3. 变更详情（字段定义、前后对比）
4. 影响面分析（下游消费者、质量规则、映射配置）
5. 版本升级建议

### 4.2 评审阶段

由以下角色参与评审：

| 角色 | 关注点 |
|------|--------|
| 数据域 Owner | 业务合理性、口径一致性 |
| 质量规则 Owner | 质量规则是否需要同步更新 |
| 服务层 Owner | 查询接口是否受影响 |

评审通过条件：

- 至少 1 名数据域 Owner 批准
- 影响面分析完整
- 版本升级规则正确

### 4.3 实施阶段

1. 更新 `config/standards/entities/<entity>.yaml`
2. 更新 `SchemaRegistry` 中的 schema 版本
3. 如有字段映射变更，更新 `config/mappings/sources/*.yaml`
4. 如有质量规则变更，更新 `config/quality/*.yaml`
5. 在 `change_log` 中登记变更记录

### 4.4 发布阶段

1. 更新 `schema_version` 到新版本
2. 在 `DatasetCatalog` 中注册新版本
3. 更新 `LineageTracker` 中的版本引用

### 4.5 通知阶段

1. 生成变更公告
2. 通知下游消费者
3. 更新字段字典文档

## 5. 与治理模块的串联

### 5.1 与 SchemaRegistry 的集成

`SchemaChange` 记录通过 `SchemaRegistry` 的 `get_schema_version()` 获取当前版本，
变更后调用 `register()` 注册新版本。

### 5.2 与 DatasetCatalog 的集成

变更发布后，通过 `DatasetCatalog.register()` 更新数据集版本快照，
`DatasetVersion.schema_version` 反映最新 schema 版本。

### 5.3 与 LineageTracker 的集成

字段变更时，`LineageTracker` 中的 `schema_version` 字段需要更新，
确保血缘记录的版本一致性。

### 5.4 与 Owner 制度的集成

变更提案必须指定 `owner_id`，由 `OwnershipRegistry` 验证权限。
只有域 Owner 或表 Owner 才能发起对应实体的变更。

## 6. 变更载体

变更落地载体为 `ChangeLog`（见 `governance/change_log.py`）：

- 每次变更记录为 `SchemaChange` 对象
- 包含变更类型、变更人、时间、版本前后对比
- 持久化到 `data/system/metadata/schema_changes.jsonl`

## 7. 紧急变更

P0 级数据问题允许紧急变更，但必须：

1. 事后 24 小时内补齐评审记录
2. 在 `ChangeLog` 中标记 `emergency=true`
3. 通知所有下游消费者

## 8. 变更示例

### 8.1 新增可选字段

```yaml
# 变更前 schema_version: 1.0
# 变更后 schema_version: 1.1
# 变更类型: ADD_FIELD
entity: market_quote_daily
change:
  type: ADD_FIELD
  field: pre_close_price
  definition:
    type: double
    unit: CNY
    required: false
    description: 前收盘价
```

### 8.2 删除字段

```yaml
# 变更前 schema_version: 2.0
# 变更后 schema_version: 3.0
# 变更类型: REMOVE_FIELD（需先走废弃流程）
entity: market_quote_daily
change:
  type: REMOVE_FIELD
  field: amount
  reason: 已废弃，使用 turnover_amount 替代
  deprecation_since: "2026-03-01"
```
