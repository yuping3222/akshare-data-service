# 运行手册：数据重放 (Replay)

> 文档编号: `RUNBOOK-REPLAY`
> 最后更新: 2026-04-22

## 1. 适用场景

- 质量门禁失败后，需要从 Raw 层重新生成 Standardized 层
- 标准化规则变更后，需要重新执行历史批次
- 发现数据异常，需要重新跑某个批次
- 告警规则 P1-001、P1-005、P2-001 触发时

## 2. 前置条件

- Raw 层数据完整且未被清理
- 标准化规则已更新（如适用）
- 知道需要重放的 `batch_id` 或日期范围
- 有 `dataset` 名称

## 3. 操作步骤

### 3.1 确认重放范围

```bash
# 查看指定数据集的批次状态
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --batch-id 20260422_001

# 查看批次对应的 Raw 数据是否存在
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --layer raw \
    --batch-id 20260422_001
```

### 3.2 检查 Raw 数据完整性

```bash
# 确认 Raw 层有完整数据
python -c "
from akshare_data.raw.reader import RawReader
reader = RawReader()
manifest = reader.get_manifest(dataset='market_quote_daily', batch_id='20260422_001')
print(f'Records: {manifest.record_count}')
print(f'Schema fingerprint: {manifest.schema_fingerprint}')
"
```

### 3.3 执行重放

```bash
# 单批次重放
python -m akshare_data.offline.cli replay \
    --dataset market_quote_daily \
    --batch-id 20260422_001

# 日期范围重放
python -m akshare_data.offline.cli replay \
    --dataset market_quote_daily \
    --start-date 2026-04-01 \
    --end-date 2026-04-22

# 多数据集重放
python -m akshare_data.offline.cli replay \
    --dataset market_quote_daily,financial_indicator \
    --batch-id 20260422_001
```

### 3.4 验证重放结果

```bash
# 检查质量报告
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --batch-id 20260422_001

# 对比重放前后数据量
python -c "
from akshare_data.standardized.reader import StandardizedReader
reader = StandardizedReader()
df = reader.query(dataset='market_quote_daily', batch_id='20260422_001')
print(f'Replayed records: {len(df)}')
"
```

### 3.5 重新发布（如质量通过）

```bash
# 发布重放后的数据
python -m akshare_data.offline.cli publish \
    --dataset market_quote_daily \
    --batch-id 20260422_001
```

## 4. 回退方案

如果重放后质量仍然不通过：

1. 查看质量报告确认失败规则
2. 如果是源数据问题 → 转向 [Backfill](backfill.md) 流程
3. 如果是规则问题 → 调整规则后重新重放
4. 如果无法修复 → 考虑 [Rollback](rollback.md) 到上一版本

## 5. 注意事项

- 重放是幂等的，同一批次多次重放结果一致
- 重放不会删除已有 Standardized 数据，而是覆盖
- 重放期间该数据集的 Served 层不受影响（发布是独立的）
- 大批次重放可能耗时较长，建议在低峰期执行

## 6. 关联告警

| 告警规则 | 关联场景 |
|----------|----------|
| P1-001 | 核心数据质量门禁失败 |
| P1-005 | 隔离区积压过多 |
| P2-001 | 非核心数据质量波动 |
| P3-003 | 质量规则持续告警 |

## 7. 常见问题

### Q: 重放后数据量变少了？

A: 可能原因：
- 标准化规则更严格，部分记录被过滤
- Raw 数据本身不完整
- 检查质量报告中的 quarantine 记录

### Q: 重放耗时过长？

A: 可能原因：
- 批次数据量大
- 质量规则执行慢
- 考虑按日期范围拆分为多个小批次重放

### Q: Raw 数据已被清理？

A: 无法重放。需要从源站重新抓取，转向 [Backfill](backfill.md) 流程。
