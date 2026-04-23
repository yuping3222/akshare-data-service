# 标准字段命名规范（T4-002）

> 最后更新：2026-04-23
> 适用范围：Raw → Standardized → Served 全链路

## 1. 目标

建立唯一字段命名体系，避免同义字段并存（例如 `code/symbol/security_id`）。

## 2. 命名总则

- 统一使用 `snake_case`
- 业务字段使用“语义名”，不使用源系统缩写
- 单位进入字段名（`_pct`, `_amount`, `_count`）
- 日期字段使用 `_date`，时间戳字段使用 `_time`
- 标识字段优先 `*_id`，代码字段优先 `*_code`

## 3. 禁止项

- 禁止将旧缓存表字段直接作为标准字段：`symbol`, `date`, `close`, `amount`, `pct_chg`
- 禁止中英文混合或拼音缩写字段
- 禁止出现同义多套字段（如 `trade_date` 与 `date` 并存）

## 4. 标准后缀

| 后缀 | 语义 | 示例 |
|---|---|---|
| `_id` | 稳定标识 | `security_id` |
| `_code` | 业务编码 | `exchange_code` |
| `_name` | 展示名称 | `security_name` |
| `_date` | 自然日/交易日 | `trade_date` |
| `_time` | 时间戳 | `ingest_time` |
| `_pct` | 百分比 | `roe_pct` |
| `_amount` | 金额 | `turnover_amount` |
| `_volume` | 数量 | `trade_volume` |
| `_flag` | 布尔/枚举标记 | `suspended_flag` |
| `_status` | 状态值 | `quality_status` |
| `_version` | 版本标识 | `normalize_version` |

## 5. 核心同义词归一

| 旧字段 | 标准字段 |
|---|---|
| symbol/code/ts_code | security_id |
| date/datetime | trade_date |
| open | open_price |
| high | high_price |
| low | low_price |
| close | close_price |
| vol/volume | trade_volume |
| amount/turnover | turnover_amount |
| pct_chg/pct_change | price_change_pct |

## 6. 层级要求

- Raw：保留源字段 + 系统字段，不创建新语义别名
- Standardized：必须使用本规范字段；旧字段只允许出现在 `alias_map`
- Served：不新增命名体系，仅继承标准字段

## 7. 治理要求

- 新字段必须先更新 `config/standards/field_dictionary.yaml`
- 若为破坏性改名，必须走弃用流程与版本发布流程
- Contract Test 必须阻止 legacy 字段进入 canonical keys
