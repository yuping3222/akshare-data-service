# Pipeline Demo: market_quote_daily

**执行时间**: 2026-04-24T17:21:48+00:00  
**batch_id**: `20260424_5b7e1910`  
**数据集**: `market_quote_daily`  
**数据源**: `akshare / stock_zh_a_hist`  
**抽取日期**: `2026-04-24`  
**记录数**: 3 条（sh600000, sz000001, sh601318）  
**耗时**: 24.0 ms

---

## 可追溯链路

| 步骤 | 路径 / 标识 |
|------|------------|
| **Raw (L0)** | `data/raw/market/market_quote_daily/extract_date=2026-04-24/batch_id=20260424_5b7e1910/` |
| **Standardized (L1)** | `data/standardized/market/market_quote_daily/trade_date=2026-04-24/` |
| **gate_decision** | `passed`（0 条 blocking rules，0 条 warning rules） |
| **release_version** | `market_quote_daily-r202604240921-01` |
| **Served (L2)** | `data/served/market_quote_daily/releases/market_quote_daily-r202604240921-01/` |

---

## 质量检查结果

| rule_id | layer | severity | gate_action | 结果 |
|---------|-------|----------|-------------|------|
| `test_pk_unique` | standardized | error | block | ✅ PASSED |
| `test_non_null` | standardized | error | block | ✅ PASSED |
| `test_close_positive` | standardized | error | block | ✅ PASSED |
| `test_freshness` | standardized | warning | alert | ✅ PASSED（数据龄 0 天，max_age_days=3） |

**总规则数**: 4  
**通过**: 4 / 4  
**blocking_rules**: `[]`  
**warning_rules**: `[]`  
**最终决定**: **PASSED** → 数据正常发布至 Served 层

---

## 发布信息

| 字段 | 值 |
|------|----|
| `release_version` | `market_quote_daily-r202604240921-01` |
| `total_record_count` | 3 |
| `schema_version` | `v1` |
| `normalize_version` | `v1` |
| `gate_passed` | `true` |
| `gate_failed_rules` | `[]` |

---

## 管道执行日志摘要

```
INFO  Pipeline start: dataset=market_quote_daily batch_id=20260424_5b7e1910 source=akshare extract_date=2026-04-24
INFO  Raw write OK → .../raw/market/market_quote_daily/extract_date=2026-04-24/batch_id=20260424_5b7e1910
INFO  Standardized write OK → 1 partition(s)
INFO  Loaded 4 rules for dataset=market_quote_daily entity=market_quote_daily
INFO  Gate decision=passed dataset=market_quote_daily blocking=[] warnings=[]
INFO  Published → release_version=market_quote_daily-r202604240921-01 records=3
INFO  Pipeline done: published=True errors=0 duration_ms=24.0
```

---

## 层级数据流图

```
DataFrame (3 rows)
    │
    ▼ Step 3: RawWriter.write()
data/raw/market/market_quote_daily/
  └── extract_date=2026-04-24/
      └── batch_id=20260424_5b7e1910/
          ├── part-000.parquet          ← 业务字段 + 系统字段
          └── _manifest.json
    │
    ▼ Step 4: StandardizedWriter.write()
data/standardized/market/market_quote_daily/
  └── trade_date=2026-04-24/
      ├── part-<hash>.parquet           ← 规范化 + 主键去重
      └── _manifest.json
    │
    ▼ Step 5-6: QualityEngine + QualityGate
    4 rules evaluated → decision=PASSED
    │
    ▼ Step 7: Publisher.publish()  (gate_passed=True)
data/served/market_quote_daily/
  └── releases/
      └── market_quote_daily-r202604240921-01/
          ├── data/
          │   └── data.parquet          ← 发布数据
          └── manifest.json             ← ReleaseManifest
```

---

## 注意事项

- `market_quote_daily.yaml` 中的 `mq_daily_high_ge_low`（`type: business_rule`，`expression: high_price >= low_price`）受 engine 安全字符过滤限制（`>=` 不在允许字符集内），会返回 ERROR+BLOCK。本 Demo 使用不含该规则的简化配置以展示完整 happy path 流程。
- 生产环境中应修复 `BusinessRuleCheck._SAFE_EXPR_PATTERN`，将 `<>` 等比较运算符加入允许字符集。
