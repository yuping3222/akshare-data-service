# Reconciliation Baseline (AkShare)

生成时间: 2026-04-24 (UTC)
依赖版本: akshare==1.18.57

## 目标

建立当前依赖版本下的基线：
- 注册表生成/校验可运行
- 全量探测可启动并可持续落盘（支持长任务中断续跑）
- equity 分类字段分析可完整跑通并产出报告

## 本轮执行命令

1. `python -m akshare_data.offline.cli config generate`
2. `python -m akshare_data.offline.cli config validate`
3. `timeout 300 python -m akshare_data.offline.cli probe --all`
4. `python -m akshare_data.offline.cli probe --status`
5. `python -m akshare_data.offline.cli analyze fields --category equity`

## 结果摘要

### 1) config generate
- 扫描到 `1074` 个 akshare 函数
- 已导出 `config/akshare_registry.yaml`

### 2) config validate
- 校验通过（Validation passed）

### 3) probe --all（300 秒窗口）
- 命令按预期启动并执行
- 在 300 秒超时窗口内完成部分探测并写入 checkpoint
- `probe --status` 显示当前 checkpoint 共 `291` 个接口结果
- 当前摘要：`success=129`, `failed=162`, `rate=44.33%`

### 4) analyze fields --category equity
- 在 `field_mapper` 修复后可完整跑完
- 分类 `equity` 共 399 接口，本轮按默认 sample size 分析 50 个
- 结果：`21 success`, `28 failed`, `1 empty`
- 列映射统计：总列 `166`，已映射 `78 (47.0%)`，未映射 `88 (53.0%)`

## 本轮代码/配置调整

### A. 修复字段分析崩溃（已完成）
文件: `src/akshare_data/offline/field_mapper.py`

问题：
- 某些字段样本值为数组/对象时，`pd.notna(val)` 返回数组，触发
  `ValueError: The truth value of an array with more than one element is ambiguous`

修复：
- 将缺失判断改为安全分支，兼容标量/数组返回值
- 字段名统一转字符串后再做映射与格式判断，避免非字符串列名导致异常

### B. 增强长任务 checkpoint 落盘能力（已完成）
文件: `src/akshare_data/offline/prober/prober.py`

问题：
- 原逻辑仅在 run_check 结束时 save，长任务中断会丢失大量进度

修复：
- 每处理 10 条结果执行一次 checkpoint save
- 使用 `finally` 保证退出前尽量落盘

### C. 刷新注册表（已完成）
文件: `config/akshare_registry.yaml`

说明：
- 按当前依赖版本重新扫描并导出，保证 registry 与 akshare 版本一致。

## 下一步建议（按“配置和代码对齐”优先）

1. 按失败类型分桶治理（参数不匹配 / 站点限流 / 上游结构变更 / 空数据）
2. 先处理“参数不匹配”类失败（最可控），修正 `probe.params` 与接口签名
3. 以 `equity` 为第一批，补齐 `output_mapping` 高频未映射字段
4. 持续运行 `probe --all`（分批 + checkpoint），直到覆盖 100% 接口

