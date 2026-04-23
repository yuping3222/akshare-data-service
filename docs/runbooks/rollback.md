# 运行手册：发布回滚 (Rollback)

> 文档编号: `RUNBOOK-ROLLBACK`
> 最后更新: 2026-04-22

## 1. 适用场景

- 新发布版本引入数据质量问题
- 发布后发现数据口径错误
- 新版本的标准化规则导致下游不兼容
- 告警规则 P0-004、P1-006 触发时

## 2. 前置条件

- 知道需要回滚的 `dataset` 和 `release_version`
- 存在可回滚的上一版本（至少有一个历史版本）
- Served 层保留了历史版本快照

## 3. 操作步骤

### 3.1 确认回滚目标

```bash
# 查看当前发布版本
python -m akshare_data.offline.cli report publish-status \
    --dataset market_quote_daily

# 查看可用历史版本
python -c "
from akshare_data.served.publisher import ServedPublisher
publisher = ServedPublisher()
versions = publisher.list_versions(dataset='market_quote_daily')
for v in versions:
    print(f'{v.release_version} | published_at={v.published_at} | status={v.status}')
"
```

### 3.2 评估回滚影响

```bash
# 查看当前版本的数据范围
python -c "
from akshare_data.served.publisher import ServedPublisher
publisher = ServedPublisher()
current = publisher.get_current_version(dataset='market_quote_daily')
print(f'Current version: {current.release_version}')
print(f'Date range: {current.start_date} - {current.end_date}')
print(f'Record count: {current.record_count}')
"

# 查看目标版本的数据范围
python -c "
from akshare_data.served.publisher import ServedPublisher
publisher = ServedPublisher()
target = publisher.get_version(dataset='market_quote_daily', version='v20260421_001')
print(f'Target version: {target.release_version}')
print(f'Date range: {target.start_date} - {target.end_date}')
print(f'Record count: {target.record_count}')
"
```

### 3.3 执行回滚

```bash
# 回滚到上一版本
python -m akshare_data.offline.cli rollback \
    --dataset market_quote_daily \
    --target-version v20260421_001

# 回滚到上一个可用版本（自动选择）
python -m akshare_data.offline.cli rollback \
    --dataset market_quote_daily \
    --target-version previous

# 多数据集回滚
python -m akshare_data.offline.cli rollback \
    --dataset market_quote_daily,financial_indicator \
    --target-version previous
```

### 3.4 验证回滚结果

```bash
# 确认当前版本已切换
python -m akshare_data.offline.cli report publish-status \
    --dataset market_quote_daily

# 验证服务读取正常
python -c "
from akshare_data.service.reader import ServiceReader
reader = ServiceReader()
df = reader.query(dataset='market_quote_daily', start_date='2026-04-20', end_date='2026-04-22')
print(f'Records after rollback: {len(df)}')
"

# 检查服务响应
python -m akshare_data.offline.cli probe --service --endpoint cn.stock.quote.daily
```

### 3.5 通知下游

回滚完成后：

1. 记录回滚原因和影响范围
2. 通知依赖该数据集的下游系统
3. 更新质量报告和发布日志

## 4. 回滚后处理

### 4.1 问题修复

回滚只是临时措施，需要：

1. 分析导致回滚的根本原因
2. 在 Raw/Standardized 层修复问题
3. 重新走完整链路（normalize → validate → publish）

### 4.2 废弃问题版本

```bash
# 标记问题版本为废弃
python -c "
from akshare_data.served.publisher import ServedPublisher
publisher = ServedPublisher()
publisher.deprecate_version(
    dataset='market_quote_daily',
    version='v20260422_001',
    reason='Quality gate failure: mq_daily_pk_unique'
)
"
```

## 5. 回退方案

如果回滚失败：

1. 目标版本不存在 → 检查存储层，确认版本快照是否完整
2. 回滚后服务异常 → 检查 Service Reader 是否正确指向新版本
3. 所有历史版本都不可用 → 紧急执行 [Backfill](backfill.md) + [Replay](replay.md)

## 6. 注意事项

- 回滚不会删除问题版本，只是切换活跃版本指针
- 回滚后问题版本仍可在历史版本列表中看到
- 回滚操作本身会记录到 `served.rollback_total` 指标
- 频繁回滚（P1-006 告警）说明上游质量或发布流程有问题

## 7. 关联告警

| 告警规则 | 关联场景 |
|----------|----------|
| P0-004 | 发布系统异常 |
| P1-006 | 发布回滚 |

## 8. 常见问题

### Q: 回滚后数据变旧了？

A: 是的，回滚到上一版本意味着使用之前的数据。
需要尽快修复问题并重新发布。

### Q: 可以回滚到任意历史版本吗？

A: 可以，只要该版本的快照仍然存在。
但建议只回滚到上一个已知良好的版本。

### Q: 回滚会影响正在进行的查询吗？

A: 取决于实现。如果 Service Reader 使用版本快照读取，
则正在进行的事务不受影响；如果是实时切换，则可能影响。
