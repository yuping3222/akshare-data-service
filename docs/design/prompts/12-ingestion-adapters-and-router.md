# 任务 12：迁移 Source Adapters 与 Router 到 `ingestion/`

## 目标

把现有 `sources/` 中的源适配器、路由和健康路由职责迁移到 `ingestion/`，明确“抓取属于 ingestion，不属于 service”。

## 必读文档

- `docs/design/01-architecture-rfc.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/05-current-to-target-mapping.md`

## 补充参考

- `docs/design/DATA_SOURCE_REDESIGN.md`

## 任务范围

- 新建 `src/akshare_data/ingestion/base.py`
- 新建 `src/akshare_data/ingestion/router.py`
- 新建 `src/akshare_data/ingestion/adapters/akshare.py`
- 新建 `src/akshare_data/ingestion/adapters/lixinger.py`
- 新建 `src/akshare_data/ingestion/adapters/tushare.py`
- 新建 `src/akshare_data/ingestion/adapters/mock.py`
- 如有必要，将旧 `src/akshare_data/sources/*` 改为兼容转发层

## 关键要求

- `service` 不得直接 import 新 adapter
- `router` 只处理路由、降级、熔断、空数据策略，不再承担服务语义
- 旧 `sources/*` 若保留，只做 re-export 或薄兼容壳
- 数据集命名、字段命名仍遵守标准实体命名，不沿用旧 cache table 命名

## 协作边界

- 本任务优先拥有 `src/akshare_data/ingestion/adapters/` 和 `src/akshare_data/ingestion/router.py`
- 尽量不要改 `api.py`
- 不实现调度、限流配置和异步补数入口，那属于任务 13

## 非目标

- 不改 Service 主查询路径
- 不做 Raw/Standardized/Served 写入

## 验收标准

- 新架构下 adapter 和 router 可以从 `ingestion/` 被引用
- `sources/*` 不再承载新逻辑中心
- 后续任务可以在不依赖 `service` 的情况下直接使用 ingestion 层
