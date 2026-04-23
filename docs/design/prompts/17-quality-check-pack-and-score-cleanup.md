# 任务 17：补齐质量检查器并移除硬编码评分

## 目标

在已有质量引擎和门禁骨架之外，补齐完整性、一致性、异常检测器，并把旧硬编码评分迁移成规则驱动结果。

## 必读文档

- `docs/all.md`
- `docs/design/50-quality-rule-spec.md`
- `docs/design/30-standard-entities.md`
- `docs/design/06-tech-debt-register.md`

## 任务范围

- 新建 `src/akshare_data/quality/checks/completeness.py`
- 新建 `src/akshare_data/quality/checks/consistency.py`
- 新建 `src/akshare_data/quality/checks/anomaly.py`
- 如有必要，补充 `src/akshare_data/quality/scoring.py`
- 按需更新 `config/quality/*.yaml`

## 关键要求

- 检查器只引用标准字段名，不引用源字段别名
- 至少覆盖：日期连续性、主键覆盖率、跨源偏差、价格/数值异常
- 评分结果如果保留，必须由规则权重和结果聚合产生，不得再硬编码固定分值
- 失败明细要能落到具体规则、字段、分区、批次

## 协作边界

- 不重写 `engine.py`、`gate.py` 的主干结构，优先通过插件式检查器接入
- 不修改标准实体定义
- 如果质量配置已由任务 06 创建，做增量补充，不另起第二套文件命名

## 非目标

- 不做 Served 发布逻辑
- 不做 Service 错误语义

## 验收标准

- 质量引擎可以调用完整性、一致性、异常检测器
- 首批 3 个数据集有可运行的关键规则集
- 旧硬编码评分逻辑不再是主路径
