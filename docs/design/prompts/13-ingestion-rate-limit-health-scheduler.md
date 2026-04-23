# 任务 13：统一限流、源健康、调度与异步补数入口

## 目标

把限流、源健康追踪、任务调度和异步补数入口收口到 `ingestion/`，避免在线、离线、脚本各自为政。

## 必读文档

- `docs/all.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/05-current-to-target-mapping.md`

## 补充参考

- `docs/design/DATA_SOURCE_REDESIGN.md`

## 任务范围

- 新建 `src/akshare_data/ingestion/rate_limiter.py`
- 新建 `src/akshare_data/ingestion/source_health.py`
- 新建 `src/akshare_data/ingestion/scheduler.py`
- 如有必要，新建 `src/akshare_data/ingestion/backfill_request.py`
- 新建 `config/ingestion/rate_limits.yaml`
- 新建 `config/ingestion/schedules.yaml`

## 关键要求

- 限流粒度至少支持 `source + interface + domain`，不能只停留在 source 名称
- 调度输出必须对齐任务 01 的 `ExtractTask` / `BatchContext`
- 补数请求是显式、异步、受控能力，不是 service 同步回源
- 健康记录要包含失败原因、恢复时间、降级原因和熔断状态

## 协作边界

- 优先通过新文件扩展能力，不要求重写旧 downloader/prober
- 尽量不要改 `ingestion/router.py` 的核心路由逻辑
- 如果任务 01 还未落地，先对着文档字段实现兼容接口

## 非目标

- 不实现具体 source adapter
- 不实现 Raw/Served 读写

## 验收标准

- 可以按日历、优先级、分区生成任务
- 可以表达异步补数请求而不是同步服务回源
- 源级和域级限流、健康状态都有统一入口
