# P2-4 Governance Plan: AkShare 接口成功率提升

## 现状

- **基线**: 44.33% 成功率（129/291），实际全量探测 741 接口中 458 成功 / 283 失败（61.8%）
- **失败接口**: 283 个
- **核心问题**: 所有失败都被包装为 `Function _call_with_retry failed after 2 retries`，丢失了原始异常信息

---

## 1. 失败分桶分析

### Bucket A: 上游数据源失效/下线（预估 ~80 个，28%）

**特征**: ConnectionError、JSONDecodeError、HTTP 404/500、远程服务器关闭连接
**典型**:
- `bond_buy_back_hist_em` → ConnectionError (Remote end closed connection)
- `amac_aoin_info` → JSONDecodeError (空响应)
- `air_quality_hebei` → 数据源可能已下线
- `energy_carbon_*` → 122s 超时，碳交易数据源不稳定
- `hf_sp_500` → 72s 超时，海外金融数据源

**治理策略**: 
- 标记为 `deprecated` 或 `unstable`，从默认探测中排除
- 在 registry 中设置 `probe.skip: true`
- 保留接口定义但标注数据源状态

### Bucket B: 参数不匹配/需要特定参数（预估 ~60 个，21%）

**特征**: 接口需要特定 symbol/date/period 参数，默认空参调用失败
**典型**:
- `stock_*_em` 系列（东方财富）需要 symbol 或特定日期
- `fund_*_em` 系列需要基金代码
- `option_*` 系列需要期权合约代码
- `get_qhkc_*` 需要 token 或特定参数
- `currency_*` 需要货币对参数
- `fred_*` 需要序列 ID

**治理策略**:
- 在 `config/akshare_registry.yaml` 的 `probe.params` 中补充合理默认参数
- 使用 `parse_params_from_doc` 从 docstring 提取示例参数
- 对需要用户输入的接口标记 `probe.skip: true` 并添加注释说明

### Bucket C: 超时/慢接口（预估 ~50 个，18%）

**特征**: exec_time >= 10s，大量 HTTP 请求或大数据量
**典型**:
- `amac_fund_info` → 242s（遍历大量基金）
- `index_bloomberg_billionaires` → 227s
- `stock_board_*_em` 系列 → 18-19s（批量请求）
- `option_*_em` 系列 → 17-19s
- `stock_classify_sina` → 内部循环 698 次迭代

**治理策略**:
- 增加 prober 超时阈值（当前 20s → 60s 对于已知慢接口）
- 在 probe config 中设置更大的 `check_interval`
- 对极端慢接口标记 `skip: true`

### Bucket D: 上游结构变更（预估 ~40 个，14%）

**特征**: 接口存在但返回格式变化，解析失败
**典型**:
- `stock_financial_*_new_ths` → 新版同花顺财务接口
- `stock_financial_*_em` → 东方财富财务接口
- `futures_*_dce` → 大商所接口
- `bond_*_cninfo` → 巨潮资讯接口

**治理策略**:
- 需要逐个检查 akshare 源码确认是否上游变更
- 更新 akshare 版本或提交 issue
- 临时标记 `skip: true`

### Bucket E: 需要认证/Token（预估 ~20 个，7%）

**特征**: 需要特定 token 或认证
**典型**:
- `set_token` → 设置 token 的接口，本身不返回数据
- `pro_api` → Tushare Pro API，需要 token
- `get_qhkc_*` → 奇货可查，需要 key

**治理策略**:
- 标记 `probe.skip: true`
- 在文档中说明需要配置

### Bucket F: 空数据/正常返回空（预估 ~20 个，7%）

**特征**: 接口正常但当前无数据
**典型**:
- `stock_financial_analysis_indicator` → Success (Empty)
- `stock_financial_hk_report_em` → Success (Empty)
- 某些历史数据接口在非交易时段返回空

**治理策略**:
- 将 `Success (Empty)` 视为成功（当前已部分处理）
- 在报告中单独分类

### Bucket G: 其他/未分类（预估 ~13 个，5%）

**特征**: 特殊接口，不属于以上任何类别
**典型**:
- `movie_*` → 电影票房数据（可能数据源不稳定）
- `crypto_*` → 加密货币数据
- `article_*` → 文章/指数数据

---

## 2. 修复优先级

| 优先级 | Bucket | 预估数量 | 工作量 | 预期成功率提升 |
|--------|--------|----------|--------|----------------|
| P0 | 增强错误日志 | 全部 | 1h | - |
| P1 | Bucket A: 标记废弃 | ~80 | 2h | +11% |
| P2 | Bucket B: 补充参数 | ~60 | 4h | +8% |
| P3 | Bucket C: 调整超时 | ~50 | 1h | +7% |
| P4 | Bucket E: 标记认证 | ~20 | 0.5h | +3% |
| P5 | Bucket F: 空数据处理 | ~20 | 0.5h | +3% |
| P6 | Bucket D: 结构变更 | ~40 | 8h+ | +5% |
| P7 | Bucket G: 其他 | ~13 | 2h | +2% |

**目标**: 完成 P0-P5 后，有效成功率从 61.8% → ~90%+（排除真正不可用的接口）

---

## 3. 增量修复计划

### Phase 1: 基础设施（P0）- 1h

**目标**: 让失败信息可观测

1. **修改 `executor.py`**: 在 retry 耗尽时记录原始异常链
   ```python
   # 当前: err = str(e.__cause__) if e.__cause__ else str(e)
   # 改为: 记录完整异常类型和消息到 checkpoint
   ```

2. **修改 `checkpoint.py`**: 在 ProbeResult 中增加 `error_type` 字段
   - 自动分类: timeout / connection / parse / parameter / auth / empty / other

3. **运行一次全量探测**: 获取分类后的失败数据

### Phase 2: 快速修复（P1 + P3 + P4 + P5）- 4h

**目标**: 处理可控的失败类型

1. **标记废弃接口** (Bucket A):
   ```yaml
   # config/akshare_registry.yaml
   interfaces:
     bond_buy_back_hist_em:
       probe:
         skip: true
         skip_reason: "upstream_down"
     amac_aoin_info:
       probe:
         skip: true
         skip_reason: "upstream_down"
   ```

2. **调整超时配置** (Bucket C):
   ```yaml
   interfaces:
     stock_board_industry_hist_em:
       probe:
         timeout: 60
         check_interval: 86400  # 每天检查一次
   ```

3. **标记认证接口** (Bucket E):
   ```yaml
   interfaces:
     pro_api:
       probe:
         skip: true
         skip_reason: "requires_token"
   ```

4. **空数据处理** (Bucket F):
   - 修改 prober 将 `Success (Empty)` 计入 success 统计
   - 在报告中单独列出 empty 接口

### Phase 3: 参数补充（P2）- 4h

**目标**: 修复参数不匹配的接口

1. **批量提取 docstring 参数**:
   ```python
   # 脚本: 遍历失败接口，从 docstring 提取示例参数
   # 输出: probe_params_suggestions.yaml
   ```

2. **按域分组补充参数**:
   - `stock_*`: symbol="000001", period="daily"
   - `fund_*`: symbol="510300" (沪深300ETF)
   - `option_*`: symbol 需要动态获取
   - `currency_*`: pair="USD/CNY"
   - `macro_*`: 通常不需要参数

3. **验证**: 对补充参数后的接口重新探测

### Phase 4: 结构变更调查（P6）- 8h+

**目标**: 逐个调查上游结构变更

1. **检查 akshare 版本**: 确认是否为版本兼容问题
2. **逐个测试**: 对 Bucket D 中的接口手动测试
3. **提交 issue**: 对确认的 upstream bug 提交到 akshare
4. **临时方案**: 标记 skip 并添加 workaround 注释

---

## 4. 验收标准

1. **探测成功率**: 排除 skip 接口后，成功率 >= 90%
2. **错误可观测**: 每个失败接口都有明确的 error_type
3. **文档更新**: 在 `docs/` 中更新接口状态表
4. **自动化**: 新增 `probe --categorize` 命令，自动分类失败原因

---

## 5. 风险与注意事项

1. **akshare 版本**: 当前 akshare==1.18.57，升级可能修复部分问题也可能引入新问题
2. **网络依赖**: 部分接口依赖境外数据源，受网络环境影响
3. **数据源稳定性**: 免费数据源随时可能变更或下线，需要持续监控
4. **探测耗时**: 741 接口全量探测约需 15-20 分钟，建议使用 checkpoint 分批运行
