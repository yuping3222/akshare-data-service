# 任务 18：定义查询契约、文档绑定与服务错误语义

## 目标

把 Service 从“能查到数据”升级到“有稳定查询契约、有字段字典绑定、有明确错误语义”的正式服务入口。

## 必读文档

- `docs/all.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/30-standard-entities.md`
- `docs/design/50-quality-rule-spec.md`

## 任务范围

- 新建 `docs/design/71-query-contract.md`
- 新建 `src/akshare_data/service/query_contract.py`
- 新建 `src/akshare_data/service/docgen.py`
- 新建 `src/akshare_data/service/error_mapper.py`
- 如有必要，补充 `docs/02-api-reference.md`

## 关键要求

- 查询契约要明确分页、排序、过滤、字段裁剪、时间范围、版本选择
- 对外字段说明必须来自字段字典和实体 schema，不再手写散落注释
- 错误语义至少区分：无数据、未发布、质量阻断、参数错误、版本不存在
- 对外契约中不得出现源字段别名和历史 cache table 名

## 协作边界

- 本任务优先拥有 `service/query_contract.py`、`service/docgen.py`、`service/error_mapper.py`
- 不重写 `service/data_service.py` 和 `api.py` 主逻辑
- 不实现异步补数调度，那属于任务 13

## 非目标

- 不直接改 source adapter
- 不实现质量检查器

## 验收标准

- 首批 3 个数据集有统一查询契约
- 服务文档可以绑定字段字典生成
- 服务层能清晰区分“没数据”和“数据不可发布”
