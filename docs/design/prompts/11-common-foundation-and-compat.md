# 任务 11：抽出 `common/` 基础层与兼容壳

## 目标

建立 `common/` 作为跨层共享基础能力目录，同时把旧 `core/` 收口为兼容壳，减少后续重构互相踩踏。

## 必读文档

- `docs/design/01-architecture-rfc.md`
- `docs/design/10-target-repo-layout.md`
- `docs/design/05-current-to-target-mapping.md`
- `docs/design/06-tech-debt-register.md`

## 任务范围

- 新建 `src/akshare_data/common/__init__.py`
- 新建 `src/akshare_data/common/config.py`
- 新建 `src/akshare_data/common/errors.py`
- 新建 `src/akshare_data/common/logging.py`
- 新建 `src/akshare_data/common/types.py`
- 仅在必要时更新 `src/akshare_data/core/__init__.py` 和少量 `core/*` 兼容转发

## 关键要求

- `common/` 只放跨层共享能力，不放 schema、normalizer、source adapter 等业务逻辑
- 旧 `core/*` 导入路径尽量继续可用，但内部应转向 `common/*`
- 兼容层必须显式、薄、可清理，必要时加 deprecation 提示
- 不把新的主实现继续堆进旧 `core/`

## 协作边界

- 本任务优先拥有 `src/akshare_data/common/`
- 不改 `service/`、`ingestion/`、`standardized/` 的业务实现
- 如果别的任务已创建 `common/`，在现有结构上增量补齐，不回退对方改动

## 非目标

- 不实现源适配器迁移
- 不实现服务主路径改造
- 不重写旧 `core/schema.py`

## 验收标准

- 新代码可以稳定从 `common/` 导入基础能力
- 旧 `core` 入口仍可作为兼容壳使用
- 后续任务不再必须依赖旧 `core/*` 才能开发
