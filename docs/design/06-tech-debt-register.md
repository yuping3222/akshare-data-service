# 技术债总表 (T0-007)

本文档登记已识别的技术债务，按严重度、模块、处理阶段分类。

**更新日期**: 2026-04-24  
**版本**: 0.3.0-dev

---

## 严重度定义

| 级别 | 定义 | 处理时限 |
|------|------|----------|
| P0 | 阻塞性问题，影响核心功能或数据正确性 | 立即处理 |
| P1 | 重要问题，影响可维护性或用户体验 | Phase 1 |
| P2 | 中等问题，影响代码质量或效率 | Phase 2-3 |
| P3 | 低优先级，优化建议或改进点 | Phase 4+ |

---

## 技术债清单

### 架构债务 (ARCH)

| ID | 描述 | 严重度 | 模块 | 影响范围 | 计划阶段 | 状态 |
|----|------|--------|------|----------|----------|------|
| ARCH-001 | `api.py` 文件过长（1500+行），包含20+个 Namespace API 类，职责混乱 | P1 | api | 全局 | Phase 1 | ✅ 已完成（2026-04-22，现仅 69 行） |
| ARCH-002 | `DataSource` 基类继承链过深：11个Mixin + ABC，增加理解难度 | P2 | core/base | 数据源层 | Phase 2 | ⏳ 待处理 |
| ARCH-003 | `router.py` 与 `api.py` 职责边界模糊：`MultiSourceRouter` 同时处理路由和健康监控 | P2 | sources/router | 数据源层 | Phase 2 | ⏳ 待处理 |
| ARCH-004 | `offline` 模块与核心模块耦合度高：`BatchDownloader` 直接依赖 `paths`、`yaml` 加载 | P2 | offline | 离线工具 | Phase 2 | ⏳ 待处理 |
| ARCH-005 | 缺少统一的事件/回调机制：`AccessLogger`、`ProgressTracker`、`StatsCollector` 各自独立 | P3 | 全局 | 监控层 | Phase 3 | ⏳ 待处理 |
| ARCH-006 | 主链路未端到端打通：raw→standardized→quality→served 无统一入口 | P0 | ingestion | 全链路 | P0 紧急 | 🔄 进行中（P0-1）|
| ARCH-007 | `LegacySourceAdapterMixin` 绕过架构禁令（service→source 直连） | P0 | service | 服务层 | P0 紧急 | 🔄 进行中（P0-2）|
| ARCH-008 | `core/schema.py` 双事实源：75 张硬编码表 vs 3 个 YAML 实体 | P1 | core/schema | 契约管理 | Phase 1 | 🔄 进行中（P1-1）|
| ARCH-009 | 三个独立收集器无统一路由：metrics/events/lineage 各自独立 | P1 | common | 可观测性 | Phase 1 | 🔄 进行中（P1-4）|

### 代码债务 (CODE)

| ID | 描述 | 严重度 | 模块 | 影响范围 | 计划阶段 | 状态 |
|----|------|--------|------|----------|----------|------|
| CODE-001 | 测试文件跳过18个测试用例（`test_offline_downloader.py`），缺乏维护 | P1 | tests | 测试覆盖 | Phase 1 | ✅ 已完成（2026-04-22，skip 已清零） |
| CODE-002 | `cached_fetch` 调用模式重复：`api.py` 中50+处相似代码块 | P1 | api | 代码重复 | Phase 1 | ⏳ 待处理 |
| CODE-003 | 硬编码常量散布：`DEFAULT_MAX_WORKERS=4`、`_ERROR_THRESHOLD=5`、`_DISABLE_DURATION=300` | P2 | 多模块 | 配置管理 | Phase 2 | ⏳ 待处理 |
| CODE-004 | 类型注解不统一：部分使用 `str | None`，部分使用 `Optional[str]` | P2 | 全局 | 类型安全 | Phase 2 | ⏳ 待处理 |
| CODE-005 | `__getattr__` 动态路由增加调试难度：`AkShareAdapter` 方法调用无法静态分析 | P2 | sources/akshare_source | 可维护性 | Phase 2 | ⏳ 待处理 |
| CODE-006 | 异常处理不一致：部分抛出 `DataSourceError`，部分返回空 DataFrame | P2 | 多模块 | 错误处理 | Phase 2 | ⏳ 待处理 |
| CODE-007 | 缺少统一的日志格式：各模块使用不同的日志模板和级别 | P3 | 全局 | 可观测性 | Phase 3 | ⏳ 待处理 |

### 数据债务 (DATA)

| ID | 描述 | 严重度 | 模块 | 影响范围 | 计划阶段 | 状态 |
|----|------|--------|------|----------|----------|------|
| DATA-001 | 缓存语义不清：`storage_layer` 四层（daily/meta/snapshot/minute）使用规则未文档化 | P1 | store | 数据存储 | Phase 1 | ⏳ 待处理 |
| DATA-002 | 缺少数据质量门禁：写入 Parquet 前无 schema 强制校验 | P1 | store/parquet | 数据质量 | Phase 1 | 🔄 进行中（P0-3，per-layer strict_schema 实现中）|
| DATA-003 | TTL 值硬编码在 `schema.py`：69个表的 TTL 散布在代码中，难以动态调整 | P2 | core/schema | 缓存管理 | Phase 2 | ⏳ 待处理 |
| DATA-004 | 增量拉取边界模糊：`find_missing_ranges` 依赖缓存的完整性假设 | P2 | store/strategies | 数据完整性 | Phase 2 | ⏳ 待处理 |
| DATA-005 | DuckDB 连接池管理不规范：thread-local 连接缺少清理机制 | P2 | store/duckdb | 资源管理 | Phase 2 | ⏳ 待处理 |
| DATA-006 | 缺少数据版本追踪：Parquet 文件无版本标记，无法识别 schema 变更 | P3 | store | 数据治理 | Phase 3 | ⏳ 待处理 |
| DATA-007 | 两套 `BackfillRegistry` 并存（service 占位 vs ingestion 真实实现） | P1 | service/ingestion | 补数链路 | Phase 1 | 🔄 进行中（P1-2）|
| DATA-008 | Lixinger `metricsList` 黑盒：默认指标列表无法验证有效性 | P2 | sources/lixinger | 数据完整性 | Phase 2 | ⏳ 待处理 |
| DATA-009 | AkShare 接口成功率 44%（129/291），一半接口不可用 | P2 | sources/akshare | 数据可用性 | Phase 2 | ⏳ 待处理 |

### 文档债务 (DOC)

| ID | 描述 | 严重度 | 模块 | 影响范围 | 计划阶段 | 状态 |
|----|------|--------|------|----------|----------|------|
| DOC-001 | 设计文档与实现不一致：`docs/design/README.md` 标记方案"未实现"，但部分已实现 | P1 | docs | 文档准确性 | Phase 1 | ⏳ 待处理 |
| DOC-002 | 缺少 API 文档：`DataService` 100+方法无统一文档入口 | P1 | docs | 用户文档 | Phase 1 | ⏳ 待处理 |
| DOC-003 | 测试跳过未记录原因：18个跳过测试仅有 TODO 注释，无详细分析 | P2 | tests | 测试维护 | Phase 2 | ⏳ 待处理 |
| DOC-004 | Schema Registry 文档缺失：69个表定义无说明文档 | P2 | docs | 数据模型 | Phase 2 | ⏳ 待处理 |
| DOC-005 | 错误码文档不全：177个错误码缺少使用场景说明 | P3 | docs | 错误处理 | Phase 3 | ⏳ 待处理 |

### 运维债务 (OPS)

| ID | 描述 | 严重度 | 模块 | 影响范围 | 计划阶段 | 状态 |
|----|------|--------|------|----------|----------|------|
| OPS-001 | Dockerfile 未固定 Python 版本，镜像 dtype 修复未 smoke 验证 | P2 | Dockerfile | 构建稳定性 | Phase 2 | 🔄 进行中（P2-2）|

---

## 按处理阶段汇总

### P0 紧急（立即处理）

| ID | 类型 | 描述 | 负责模块 |
|----|------|------|----------|
| ARCH-006 | 架构 | 主链路 raw→served 打通，建立统一入口 | ingestion |
| ARCH-007 | 架构 | 消除 LegacySourceAdapterMixin 跨层直连 | service |
| DATA-002 | 数据 | per-layer strict_schema 质量门禁 | store/parquet |

**P0 总计**: 3 项（均进行中）

### Phase 1（高优先级）

| ID | 类型 | 描述 | 负责模块 |
|----|------|------|----------|
| ARCH-001 | 架构 | api.py 职责拆分 | api |
| CODE-001 | 代码 | 恢复跳过测试 | tests |
| CODE-002 | 代码 | 消除 cached_fetch 重复 | api |
| DATA-001 | 数据 | 明确 storage_layer 语义 | store |
| DOC-001 | 文档 | 同步设计文档状态 | docs |
| DOC-002 | 文档 | 补充 API 文档 | docs |
| ARCH-008 | 架构 | 合并 schema.py 双事实源 | core/schema |
| ARCH-009 | 架构 | 统一 metrics/events/lineage 路由 | common |
| DATA-007 | 数据 | 合并两套 BackfillRegistry | service/ingestion |

**Phase 1 总计**: 9 项（2 已完成，4 进行中，3 待处理）

### Phase 2（中优先级）

| ID | 类型 | 描述 | 负责模块 |
|----|------|------|----------|
| ARCH-002 | 架构 | 简化 DataSource 继承链 | core/base |
| ARCH-003 | 架构 | 明确 router/api 边界 | sources/router |
| ARCH-004 | 架构 | 降低 offline 耦合 | offline |
| CODE-003 | 代码 | 集中管理硬编码常量 | 多模块 |
| CODE-004 | 代码 | 统一类型注解风格 | 全局 |
| CODE-005 | 代码 | AkShareAdapter 静态分析支持 | sources |
| CODE-006 | 代码 | 统一异常处理策略 | 多模块 |
| DATA-003 | 数据 | TTL 配置化 | core/schema |
| DATA-004 | 数据 | 完善增量拉取边界处理 | store/strategies |
| DATA-005 | 数据 | 规范 DuckDB 连接管理 | store/duckdb |
| DOC-003 | 文档 | 分析跳过测试原因 | tests |
| DOC-004 | 文档 | 编写 Schema Registry 文档 | docs |
| OPS-001 | 运维 | 固定 Dockerfile Python 版本，补 smoke 验证 | Dockerfile |
| DATA-008 | 数据 | 验证 Lixinger metricsList 有效性 | sources/lixinger |
| DATA-009 | 数据 | 提升 AkShare 接口可用率（当前 44%） | sources/akshare |

**Phase 2 总计**: 15 项（1 进行中，14 待处理）

### Phase 3（低优先级）

| ID | 类型 | 描述 | 负责模块 |
|----|------|------|----------|
| ARCH-005 | 架构 | 统一事件/回调机制 | 全局 |
| CODE-007 | 代码 | 统一日志格式 | 全局 |
| DATA-006 | 数据 | 数据版本追踪 | store |
| DOC-005 | 文档 | 错误码场景说明 | docs |

**Phase 3 总计**: 4 项

---

## 统计汇总

| 类别 | P0 | P1 | P2 | P3 | 已完成 | 合计 |
|------|----|----|----|----|--------|------|
| 架构债务 | 2 | 3 | 3 | 1 | 1 | **10** |
| 代码债务 | 0 | 2 | 4 | 1 | 1 | **8** |
| 数据债务 | 0 | 3 | 5 | 1 | 0 | **9** |
| 文档债务 | 0 | 2 | 2 | 1 | 0 | **5** |
| 运维债务 | 0 | 0 | 1 | 0 | 0 | **1** |
| **合计** | **2** | **10** | **15** | **4** | **2** | **33** |

---

## 处理进展

| 阶段 | 总计 | 已完成 | 进行中 | 待处理 |
|------|------|--------|--------|--------|
| P0 紧急 | 3 | 0 | 3 | 0 |
| Phase 1 | 9 | 2 | 4 | 3 |
| Phase 2 | 15 | 0 | 1 | 14 |
| Phase 3 | 4 | 0 | 0 | 4 |

---

## 附录：债务发现依据

### A. 架构债务发现方法

1. **代码行数分析**: `api.py` 1500+行，超过健康阈值（500行）
2. **继承链分析**: `DataSource` 继承图可视化后发现 11 层 Mixin
3. **依赖矩阵分析**: `offline` 模块依赖图显示跨层调用
4. **架构评审**: 发现 `LegacySourceAdapterMixin` 绕过分层禁令，主链路无统一入口

### B. 代码债务发现方法

1. **测试覆盖率分析**: `pytest -v --collect-only` 显示跳过测试
2. **代码重复检测**: `cached_fetch` 调用模式相似度分析
3. **grep TODO/FIXME**: 发现 19 处待处理标记

### C. 数据债务发现方法

1. **Schema Registry 审查**: 69 个表 TTL 值硬编码统计，发现双事实源问题
2. **缓存策略分析**: `storage_layer` 使用模式不一致
3. **数据质量检查**: Parquet 写入缺少 schema 校验
4. **接口可用性探测**: AkShare 291 个接口测试，成功率仅 44%（129/291）
5. **BackfillRegistry 审查**: service 层占位实现与 ingestion 真实实现并存

### D. 文档债务发现方法

1. **文档一致性检查**: `docs/design/README.md` 与代码实现对比
2. **API 文档覆盖检查**: `DataService` 方法文档缺失统计

### E. 运维债务发现方法

1. **Dockerfile 审查**: Python 基础镜像未固定版本，dtype 修复后缺乏 smoke 测试验证

---

## 下一步行动

1. **P0 优先**: 推进 ARCH-006（主链路打通）、ARCH-007（消除直连）、DATA-002（质量门禁）
2. **Phase 1 收尾**: 处理 CODE-002、DATA-001、DOC-001、DOC-002
3. **建立跟踪机制**: 每周更新技术债处理进展
4. **自动化检测**: 添加 CI 检查跳过测试数量阈值，接口可用率监控

---

## 更新历史

| 日期 | 版本 | 变更内容 |
|------|------|---------|
| 2026-04-24 | 0.3.0-dev | ARCH-001、CODE-001 标记完成；DATA-002 更新为进行中；新增 P0 ARCH-006/007，P1 DATA-007/ARCH-008/009，P2 DATA-008/OPS-001/DATA-009 |
| 2026-04-22 | 0.2.0 | 初始版本，7 项 Phase 1 债务 |
