# 测试策略 v2（T10-001）

> 最后更新：2026-04-23

## 1. 目标

将测试从“功能覆盖”升级为“契约 + 质量门禁 + 回归保护”三位一体。

## 2. 测试金字塔

- Unit：模块内逻辑（快速，默认执行）
- Contract：配置与数据契约（字段、规则、版本）
- Integration：跨层协作（standardized/served/api）
- System：端到端关键链路

## 3. v2 新增重点

1. 字段映射契约测试
   - 保证映射文件与字段字典、实体 schema 一致
2. 质量规则测试
   - 保证质量 DSL 完整、层级合法、无旧命名残留
3. 发布门禁回归测试
   - block 规则失败必须阻断发布
4. 服务只读边界测试
   - 防止 service 回源

## 4. 执行策略

- PR 必跑：unit + contract
- 夜间：integration + served + replay
- 周期：system + network

## 5. 质量门禁

- 任一 contract 失败即阻断合并
- 关键数据集（P0）要求规则配置与字段映射 100% 可解析
- Deprecated 字段新增必须带迁移说明

## 6. 目录约定

- `tests/contract/`：契约测试
- `tests/quality/`：质量 DSL 与规则执行测试
- `tests/served/`：发布门禁与版本测试
- `tests/replay/`：Raw 重放与样本回归
