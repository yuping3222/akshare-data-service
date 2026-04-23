# 任务 08：改造 Service 为只读 Served

## 目标

把对外服务主路径改造成“只读 Served”，停止把实时抓取作为默认响应路径。

## 必读文档

- `docs/design/00-project-goal.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/05-current-to-target-mapping.md`

## 任务范围

- 重构 `src/akshare_data/api.py`
- 新建或完善 `src/akshare_data/service/data_service.py`
- 新建 `src/akshare_data/service/reader.py`
- 新建 `src/akshare_data/service/version_selector.py`
- 新建 `src/akshare_data/service/missing_data_policy.py`

## 关键要求

- 保留外部 `DataService` 入口兼容
- 主查询路径只读 `served`
- 缺数时可返回状态或触发异步补数请求，但不可同步直连源站
- 避免把 source adapter 放回 `service`
- 新逻辑尽量放进 `service/`，`api.py` 只保留 facade 和兼容转发
- 若异步补数入口尚未落地，只保留请求接口或占位协议，不偷偷回源兜底

## 协作边界

- 本任务优先拥有 `src/akshare_data/service/*.py` 和 `api.py` 中的 facade 改造
- 不修改 `src/akshare_data/ingestion/adapters/*`
- 不在 `service/` 中重新实现 `served` 的发布、回滚或质量逻辑

## 验收标准

- 关键查询入口不再直接回源
- 兼容层仍可被现有调用方导入
- 即使 Served 暂时无数据，也返回清晰状态，而不是静默同步抓取
