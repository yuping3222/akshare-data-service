# 查询契约规范

> 任务编号: `T8-005`
> 最后更新: 2026-04-22

## 1. 目标

定义 Service 层对外查询的统一契约，涵盖分页、排序、过滤、字段裁剪、时间范围、版本选择等维度。

本契约是 `query_contract.py` 的设计依据，也是后续 API 文档生成的输入。

## 2. 适用范围

首批覆盖 3 个 P0 数据集：

| 数据集 | 实体 | 说明 |
|--------|------|------|
| `market_quote_daily` | `market_quote_daily` | 日线行情 |
| `financial_indicator` | `financial_indicator` | 财务指标 |
| `macro_indicator` | `macro_indicator` | 宏观指标 |

## 3. 核心原则

- 对外契约使用**标准实体字段名**，不使用源字段别名和历史 cache table 名
- 字段说明来自 `field_dictionary.yaml` 和实体 schema，不手写散落注释
- 错误语义清晰区分：无数据、未发布、质量阻断、参数错误、版本不存在
- 查询契约是强类型、可校验的 dataclass，不是自由格式 dict

## 4. 查询参数模型

### 4.1 通用参数

所有数据集共享以下查询参数：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `start_date` | str | 否 | None | 起始日期（含），格式 YYYY-MM-DD |
| `end_date` | str | 否 | None | 结束日期（含），格式 YYYY-MM-DD |
| `fields` | list[str] | 否 | None | 字段裁剪，None 表示返回全部业务字段 |
| `sort_by` | str | 否 | 主时间字段 | 排序字段，必须是实体字段名 |
| `sort_order` | str | 否 | `asc` | 排序方向，`asc` 或 `desc` |
| `limit` | int | 否 | None | 最大返回行数，None 表示不限制 |
| `offset` | int | 否 | 0 | 分页偏移量 |
| `release_version` | str | 否 | None | 指定发布版本，None 表示最新稳定版 |

### 4.2 数据集特有参数

#### `market_quote_daily`

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `security_id` | str | 是 | - | 证券标识 |
| `adjust_type` | str | 否 | `qfq` | 复权类型：`qfq`/`hfq`/`none` |

#### `financial_indicator`

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `security_id` | str | 是 | - | 证券标识 |
| `report_type` | str | 否 | None | 报告类型：`Q1`/`H1`/`Q3`/`A`，None 表示不限 |

#### `macro_indicator`

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `indicator_code` | str | 是 | - | 宏观指标代码 |
| `region` | str | 否 | `CN` | 地区 |

## 5. 字段裁剪规则

- `fields=None`：返回实体 schema 中定义的全部业务字段（不含系统字段）
- `fields=[...]`：仅返回指定字段，字段名必须是实体 schema 中的标准字段名
- 系统字段（`batch_id`、`source_name` 等）默认不返回，除非显式请求
- 请求不存在的字段名时，抛出 `QueryContractError`（参数错误）

## 6. 排序规则

- 默认排序字段为实体的主时间字段：
  - `market_quote_daily` → `trade_date`
  - `financial_indicator` → `report_date`
  - `macro_indicator` → `observation_date`
- `sort_by` 必须是实体中定义的字段名
- 排序方向仅支持 `asc` 和 `desc`
- 对不存在的字段排序时，抛出 `QueryContractError`

## 7. 分页规则

- `limit` 最大值为 10000，超过时截断并记录 warning
- `offset` 必须 >= 0
- `limit` 和 `offset` 同时使用时，返回 `[offset, offset+limit)` 范围的行
- `limit=None` 且 `offset=0` 时不分页

## 8. 时间范围规则

- 日期格式统一为 `YYYY-MM-DD`
- `start_date` 和 `end_date` 均为闭区间
- 仅指定 `start_date` 时，`end_date` 默认为最新可用日期
- 仅指定 `end_date` 时，`start_date` 默认为最早可用日期
- 两者均未指定时，返回全部可用数据
- `start_date > end_date` 时抛出 `QueryContractError`

## 9. 版本选择规则

- `release_version=None`：读取最新稳定发布版本
- `release_version="latest"`：同上
- `release_version=<具体版本号>`：读取指定版本
- 指定版本不存在时，抛出 `ServiceError`（版本不存在）
- 数据集尚未有任何发布版本时，抛出 `ServiceError`（数据未发布）

## 10. 错误语义

| 错误类型 | 异常类 | 触发条件 | HTTP 类比 |
|----------|--------|----------|-----------|
| 参数错误 | `QueryContractError` | 参数格式错误、字段不存在、日期范围非法等 | 400 |
| 无数据 | `NoDataError` | 查询范围内确实没有数据记录 | 404 |
| 数据未发布 | `DataNotPublishedError` | 数据集尚未通过质量门禁发布到 Served 层 | 404 |
| 质量阻断 | `QualityBlockedError` | 最新批次质量门禁未通过，数据被阻断 | 503 |
| 版本不存在 | `VersionNotFoundError` | 指定的 release_version 不存在 | 404 |

## 11. 返回结构

查询结果统一包装为 `QueryResult`：

```python
@dataclass
class QueryResult:
    data: pd.DataFrame          # 查询数据
    dataset: str                # 数据集名
    total_rows: int             # 匹配总行数（分页前）
    returned_rows: int          # 实际返回行数（分页后）
    release_version: str        # 数据来源版本
    query_time: datetime        # 查询时间
    fields: list[str]           # 实际返回字段
```

## 12. 与实体 schema 的绑定关系

查询契约中的字段名、主键、时间字段全部继承自：

- `docs/design/30-standard-entities.md`
- `config/standards/entities/*.yaml`
- `config/standards/field_dictionary.yaml`

查询契约不自行定义任何字段名。

## 13. 禁止事项

- 对外契约中不得出现 `stock_daily`、`finance_indicator` 等历史名称
- 对外契约中不得出现源字段别名（如 `close`、`amount`、`pe`）
- 对外契约中不得出现 cache table 名（如 `STOCK_DAILY`、`FINANCE_INDICATOR`）
- 不在此契约中实现异步补数调度（属于任务 13）
- 不在此契约中实现质量检查器（属于任务 6）
