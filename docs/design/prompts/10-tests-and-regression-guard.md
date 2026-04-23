# 任务 10：补齐测试与回归保护

## 目标

为新架构关键链路建立最低限度但有效的回归保护。

## 必读文档

- `docs/design/20-raw-spec.md`
- `docs/design/30-standard-entities.md`
- `docs/design/50-quality-rule-spec.md`
- `docs/design/01-architecture-rfc.md`
- `docs/design/06-tech-debt-register.md`

## 任务范围

- 新建或补充 `tests/contract/test_entity_schemas.py`
- 新建或补充 `tests/contract/test_field_dictionary.py`
- 新建或补充 `tests/replay/test_raw_replay.py`
- 新建或补充 `tests/served/test_publish_gate.py`
- 新建或补充 `tests/integration/test_service_reads_served_only.py`
- 如有必要，补充 `tests/fixtures/`

## 关键要求

- 至少覆盖：实体 schema 契约、Raw manifest、quality gate、Served 发布、Service 只读 Served
- 使用首批 3 个数据集的样例数据
- 测试命名和字段名必须与标准实体一致
- 测试默认不得访问真实网络或真实源站，优先使用 fixture、stub、fake manifest
- 如果实现尚未完全合入，优先写契约级和协议级测试，不用大量 `skip` 掩盖缺口
- 按现有仓库习惯补充合适的 pytest marker

## 协作边界

- 本任务优先拥有 `tests/contract/`、`tests/replay/`、`tests/served/`、`tests/integration/` 下的新测试文件
- 不重写生产实现，只允许为可测性增加小型、明确的 hook
- 不维护业务样本生成逻辑，那由 Raw/Standardized 相关任务提供

## 验收标准

- 关键契约漂移时测试会直接失败
- 可以验证 service 主路径不直接回源
- 即使首批实现未全量完成，测试结构也能作为后续回归保护骨架继续扩展
