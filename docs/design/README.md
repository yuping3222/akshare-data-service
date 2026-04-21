# 设计文档（已归档）

本目录包含架构设计、开发计划等历史文档。这些文档描述了**未实现或部分未实现**的设计方案，仅供历史参考。

> **注意：** 本文档中的方案均未在代码中完整实现。请勿将其描述的功能当作现有特性使用。

## 架构设计

| 文档 | 描述 | 状态 |
|------|------|------|
| [DATA_SOURCE_REDESIGN.md](DATA_SOURCE_REDESIGN.md) | 按域名粒度重新设计限速策略 | 未实现（限速通过 `rate_limits.yaml` 实现） |
| [11-config-redesign.md](11-config-redesign.md) | 拆分 akshare_registry.yaml 为模块化配置 | 已部分实现（`config/interfaces/` 已拆分） |
| [11-cache-policy-config.md](11-cache-policy-config.md) | 缓存策略配置化 + 离线分析工具 | 已部分实现 |
| [DESIGN_non_akshare_sources.md](DESIGN_non_akshare_sources.md) | 非 AkShare 数据源设计方案 | 部分实现（Lixinger/Tushare/Mock） |

## 开发计划

| 文档 | 描述 |
|------|------|
| [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) | 开发计划（历史文档） |
| [implementation_plan.md](implementation_plan.md) | 实现计划（历史文档） |
| [MIGRATION_PLAN.md](MIGRATION_PLAN.md) | 迁移计划（历史文档） |
| [TESTING_PLAN.md](TESTING_PLAN.md) | 测试计划（部分已实现） |

## 验证脚本

| 文件 | 描述 |
|------|------|
| [verify_service.py](../verify_service.py) | 服务验证脚本 |
| [test_completeness.py](../test_completeness.py) | 完整性测试脚本 |
| [imports.txt](../imports.txt) | 导入列表 |
