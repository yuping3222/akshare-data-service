# 字段废弃流程

> 任务编号: `T9-005`
> 文档编号: `GOV-082`
> 最后更新: 2026-04-22
> 状态: Draft

## 1. 目标

规范字段从"正常使用"到"完全下线"的全生命周期管理，确保：

- 废弃有窗口期，下游有迁移时间
- 有替代字段说明，降低迁移成本
- 有影响面分析，避免误删
- 与 `SchemaRegistry`、`DatasetCatalog`、`ChangeLog` 可串联

## 2. 废弃状态机

```
ACTIVE -> DEPRECATED -> REMOVED
```

| 状态 | 说明 | 数据行为 |
|------|------|----------|
| `ACTIVE` | 正常使用 | 正常写入和读取 |
| `DEPRECATED` | 已标记废弃，仍在窗口期 | 继续写入，读取时返回废弃警告 |
| `REMOVED` | 窗口期结束，已下线 | 不再写入，读取返回空或报错 |

## 3. 废弃窗口期

### 3.1 默认窗口期

| 字段优先级 | 默认窗口期 | 说明 |
|------------|------------|------|
| P0 实体核心字段 | 90 天 | 行情、财务核心字段 |
| P0 实体非核心字段 | 60 天 | 辅助字段 |
| P1/P2 实体字段 | 30 天 | 非核心实体字段 |

### 3.2 窗口期计算

```
废弃宣布日 + 窗口期 = 可移除日
```

- 废弃宣布日：`deprecation_date`
- 可移除日：`removable_date`
- 窗口期内：字段继续可用，但标记为 `deprecated`

## 4. 废弃申请

废弃申请必须包含以下信息：

| 字段 | 说明 |
|------|------|
| `entity_name` | 所属实体 |
| `field_name` | 废弃字段名 |
| `replacement_field` | 替代字段名（可空） |
| `reason` | 废弃原因 |
| `deprecation_date` | 废弃宣布日期 |
| `window_days` | 窗口期天数 |
| `removable_date` | 可移除日期（自动计算） |
| `impact_analysis` | 影响面说明 |

### 4.1 影响面分析

必须分析以下维度：

1. **下游消费者**：哪些查询、报表、服务依赖该字段
2. **质量规则**：哪些质量规则引用该字段
3. **字段映射**：哪些源映射指向该字段
4. **血缘记录**：`LineageTracker` 中有多少条血缘涉及该字段

## 5. 废弃执行流程

```
申请 -> 评审 -> 标记 DEPRECATED -> 窗口期 -> 标记 REMOVED -> 物理删除
```

### 5.1 标记 DEPRECATED

1. 在实体 YAML 中标记字段 `deprecated: true`
2. 设置 `deprecation_date` 和 `removable_date`
3. 在 `DeprecationRegistry` 中登记
4. 在 `ChangeLog` 中记录 `DEPRECATE_FIELD` 变更
5. 通知下游消费者

### 5.2 窗口期内

- 字段继续正常写入
- 读取时附带废弃警告（可在服务层实现）
- 消费者应迁移到替代字段

### 5.3 标记 REMOVED

窗口期结束后：

1. 验证无活跃消费者（或已迁移）
2. 在 `DeprecationRegistry` 中更新状态为 `REMOVED`
3. 在 `ChangeLog` 中记录 `REMOVE_FIELD` 变更
4. 从实体 YAML 中删除字段定义
5. 更新 `SchemaRegistry` 版本（MAJOR + 1）

## 6. 与治理模块的串联

### 6.1 与 SchemaRegistry 的集成

- 废弃字段在 `EntitySchema` 中标记 `deprecated` 状态
- 删除字段后更新 `schema_version`

### 6.2 与 DatasetCatalog 的集成

- `Dataset.metadata` 中记录废弃字段列表
- 版本快照中反映废弃状态变化

### 6.3 与 LineageTracker 的集成

- 废弃字段对应的血缘记录标记 `deprecated: true`
- 消费者可通过 `get_deprecated_lineage()` 查询受影响的血缘

### 6.4 与 ChangeLog 的集成

- 废弃申请记录为 `SchemaChange(type=DEPRECATE_FIELD)`
- 删除记录为 `SchemaChange(type=REMOVE_FIELD)`

## 7. 废弃示例

### 7.1 字段废弃登记

```yaml
entity: market_quote_daily
field: amount
replacement: turnover_amount
reason: 命名不规范，统一使用 turnover_amount
deprecation_date: "2026-03-01"
window_days: 60
removable_date: "2026-04-30"
status: DEPRECATED
```

### 7.2 窗口期结束后的删除

```yaml
entity: market_quote_daily
field: amount
status: REMOVED
removed_date: "2026-05-01"
schema_version_before: "2.0"
schema_version_after: "3.0"
```

## 8. 紧急废弃

发现字段存在严重口径问题时，可缩短窗口期：

1. 需数据域 Owner 批准
2. 在 `ChangeLog` 中标记 `emergency=true`
3. 最短窗口期不低于 7 天
4. 必须通知所有已知消费者
