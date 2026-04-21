# 文档索引

## 用户文档

| 序号 | 文档 | 说明 |
|------|------|------|
| 1 | [01-项目概览](01-overview.md) | 项目简介、架构总览、核心特性、缓存表分类、数据源优先级 |
| 2 | [02-API 参考](02-api-reference.md) | 模块级函数、命名空间 API、代码格式、错误处理入口 |
| 3 | [03-数据源](03-data-sources.md) | Lixinger、AkShare、Tushare 等数据源配置与使用 |
| 4 | [04-存储层](04-storage-layer.md) | Parquet 分区存储、DuckDB 查询、内存缓存、聚合层 |
| 5 | [05-核心模块](05-core-modules.md) | 配置管理、Schema 注册、代码转换、字段映射、日志统计 |
| 6 | [06-离线工具](06-offline-tools.md) | 批量下载、接口探测、质量检查、报告生成等离线工具 |
| 7 | [07-Schema 注册表](07-schema-registry.md) | 69 张缓存表的完整字段定义、类型、分区、优先级 |
| 8 | [08-缓存策略](08-cache-strategy.md) | Cache-First 策略、多层缓存架构、增量更新、原子写入 |
| 9 | [09-Sources 测试报告](09-sources-test-report.md) | Sources 层测试失败分类分析、Bug 修复记录 |
| 10 | [10-错误处理](10-error-handling.md) | 177 个错误码枚举、异常类层次结构、使用模式 |
| 11 | [11-迁移指南](11-migration-guide.md) | 从 jk2bt/AkShare 迁移、安装配置、常见使用模式 |
| 12 | [12-配置参考](12-configuration-reference.md) | 所有 YAML 配置文件的完整参考 |
| 13 | [13-术语表](13-glossary.md) | 文档中使用的关键术语和定义 |
| 14 | [14-入门教程](14-getting-started.md) | 新用户分步教程（安装、配置、首次 API 调用） |
| -- | [CLI_REFERENCE.md](CLI_REFERENCE.md) | 离线工具命令行参考手册（download/probe/analyze/report） |
| -- | [CHANGELOG.md](../CHANGELOG.md) | 项目版本变更日志 |

## 内部/历史设计文档

位于 [docs/design/](design/) 目录：

| 文档 | 说明 | 状态 |
|------|------|------|
| [design/README.md](design/README.md) | 设计文档索引与归档说明 | 已归档 |
| [design/DATA_SOURCE_REDESIGN.md](design/DATA_SOURCE_REDESIGN.md) | 按域名粒度重新设计限速策略 | 未实现 |
| [design/11-config-redesign.md](design/11-config-redesign.md) | 拆分 akshare_registry.yaml 为模块化配置 | 部分实现 |
| [design/11-cache-policy-config.md](design/11-cache-policy-config.md) | 缓存策略配置化 + 离线分析工具 | 部分实现 |
| [design/DESIGN_non_akshare_sources.md](design/DESIGN_non_akshare_sources.md) | 非 AkShare 数据源设计方案 | 部分实现 |
| [design/DEVELOPMENT_PLAN.md](design/DEVELOPMENT_PLAN.md) | 开发计划（历史文档） | 已归档 |
| [design/implementation_plan.md](design/implementation_plan.md) | 实现计划（历史文档） | 已归档 |
| [design/MIGRATION_PLAN.md](design/MIGRATION_PLAN.md) | 迁移计划（历史文档） | 已归档 |
| [design/TESTING_PLAN.md](design/TESTING_PLAN.md) | 测试计划（部分已实现） | 部分实现 |

> **注意：** `docs/design/` 下的文档描述未实现或部分未实现的设计方案，仅供历史参考。
