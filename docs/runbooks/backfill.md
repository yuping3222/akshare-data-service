# 运行手册：数据回补 (Backfill)

> 文档编号: `RUNBOOK-BACKFILL`
> 最后更新: 2026-04-22

## 1. 适用场景

- Raw 层数据缺失或已被清理，需要从源站重新抓取
- 历史数据回填（新增数据集或新增字段）
- 源站修复后补拉之前失败的数据
- 告警规则 P0-002、P1-002、P1-003、P2-002、P2-006、P2-007 触发时

## 2. 前置条件

- 源站可用（lixinger / akshare / tushare）
- 知道需要回补的 `dataset`、日期范围和源站
- 有足够的 API 配额（注意限流）
- 如果是交易日回补，确认源站数据已更新

## 3. 操作步骤

### 3.1 确认回补范围

```bash
# 查看数据缺失情况
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --check completeness

# 查看缺失的日期范围
python -c "
from akshare_data.offline.analyzer import CacheAnalyzer
analyzer = CacheAnalyzer()
missing = analyzer.find_missing_dates(
    dataset='market_quote_daily',
    start_date='2026-04-01',
    end_date='2026-04-22'
)
print(f'Missing dates: {missing}')
"
```

### 3.2 检查源站可用性

```bash
# 探测源站健康状态
python -m akshare_data.offline.cli probe --source lixinger
python -m akshare_data.offline.cli probe --source akshare

# 查看源站熔断状态
python -c "
from akshare_data.ingestion.source_health import SourceHealthMonitor
monitor = SourceHealthMonitor()
status = monitor.get_source_status('lixinger')
print(f'Status: {status}')
"
```

### 3.3 执行回补

```bash
# 单数据集增量回补（补缺失日期）
python -m akshare_data.offline.cli download \
    --dataset market_quote_daily \
    --mode incremental \
    --days 30

# 指定日期范围回补
python -m akshare_data.offline.cli download \
    --dataset market_quote_daily \
    --start-date 2026-04-01 \
    --end-date 2026-04-22 \
    --mode full

# 多数据集并行回补
python -m akshare_data.offline.cli download \
    --dataset market_quote_daily,financial_indicator,macro_indicator \
    --mode incremental \
    --days 7 \
    --workers 3

# 全量回补（谨慎使用）
python -m akshare_data.offline.cli download \
    --all \
    --mode full \
    --workers 4
```

### 3.4 监控回补进度

```bash
# 查看下载进度
python -m akshare_data.offline.cli report download-status

# 查看源站限流情况
python -c "
from akshare_data.ingestion.source_health import SourceHealthMonitor
monitor = SourceHealthMonitor()
stats = monitor.get_rate_limit_stats('akshare', hours=1)
print(f'Rate limit hits: {stats}')
"
```

### 3.5 验证回补结果

```bash
# 检查数据完整性
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --check completeness

# 检查数据新鲜度
python -m akshare_data.offline.cli report quality \
    --table market_quote_daily \
    --check freshness
```

### 3.6 触发标准化和发布

回补完成后，数据进入 Raw 层，需要走完整链路：

```bash
# 执行标准化
python -m akshare_data.offline.cli normalize \
    --dataset market_quote_daily \
    --start-date 2026-04-01 \
    --end-date 2026-04-22

# 执行质量检查
python -m akshare_data.offline.cli quality-check \
    --dataset market_quote_daily \
    --start-date 2026-04-01 \
    --end-date 2026-04-22

# 发布（质量通过后自动或手动）
python -m akshare_data.offline.cli publish \
    --dataset market_quote_daily \
    --start-date 2026-04-01 \
    --end-date 2026-04-22
```

## 4. 限流处理

### 4.1 AkShare 限流

- AkShare 无官方限流策略，建议控制并发数 <= 4
- 如遇 IP 封禁，等待 30 分钟后重试
- 可配置 `config/rate_limits.yaml` 调整间隔

### 4.2 Lixinger 限流

- Lixinger 有 API 配额限制
- 使用 `--workers` 控制并发
- 监控 `ingestion.rate_limit_hits_total` 指标

## 5. 回退方案

如果回补失败：

1. 源站不可用 → 等待源站恢复，设置告警静默
2. 数据格式变化 → 检查 Schema 漂移告警，更新映射规则
3. 配额耗尽 → 等待配额重置或切换到备用源
4. 部分成功 → 记录失败范围，后续单独补拉

## 6. 注意事项

- 回补会产生新的 `batch_id`，与历史批次不同
- 回补期间注意不要与定时任务冲突
- 大批量回补建议分批执行，避免源站压力
- 回补后务必执行质量检查和发布流程

## 7. 关联告警

| 告警规则 | 关联场景 |
|----------|----------|
| P0-002 | 核心数据集断更 |
| P1-002 | 单源站持续失败 |
| P1-003 | 数据延迟超标 |
| P2-002 | 限流频繁触发 |
| P2-006 | 缺数响应增多 |
| P2-007 | 非核心数据延迟 |

## 8. 常见问题

### Q: 回补后数据重复了？

A: 标准链路使用幂等机制，同一日期范围的数据会被覆盖而非追加。
如果确实出现重复，检查 merge/upsert 规则是否正确。

### Q: 回补速度太慢？

A: 可能原因：
- 源站响应慢
- 并发数设置过低
- 数据量大
- 考虑增加 `--workers` 或拆分日期范围

### Q: 回补后质量检查失败？

A: 转向 [Replay](replay.md) 流程（如果 Raw 数据已存在）或
检查源数据质量。
