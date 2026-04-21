# 离线工具命令行参考手册

## 目录

- [快速开始](#快速开始)
- [1. 下载命令 `download`](#1-下载命令-download)
- [2. 探测命令 `probe`](#2-探测命令-probe)
- [3. 分析命令 `analyze`](#3-分析命令-analyze)
- [4. 报告命令 `report`](#4-报告命令-report)
- [5. 配置命令 `config`](#5-配置命令-config)
- [配置文件位置](#配置文件位置)
- [常见问题](#常见问题)

---

## 快速开始

```bash
# 查看帮助
python -m akshare_data.offline.cli --help
```

输出:
```
usage: offline [-h] {download,probe,analyze,report,config} ...

AkShare Data Service - Offline Tools

positional arguments:
  {download,probe,analyze,report,config}
                        Available commands
    download            Download data
    probe               Probe interface health
    analyze             Analyze data
    report              Generate reports
    config              Manage configuration
```

---

## 1. 下载命令 `download`

批量下载数据到共享缓存（DuckDB + Parquet）。

### 1.1 增量下载（默认）

下载最近 N 天的数据，自动跳过已缓存的日期范围。

```bash
# 下载最近 1 天数据（默认）
python -m akshare_data.offline.cli download

# 下载最近 7 天数据
python -m akshare_data.offline.cli download --mode incremental --days 7

# 指定并发线程数
python -m akshare_data.offline.cli download --workers 8 --days 1
```

**真实输出示例**:
```
2026-04-20 13:01:10 - Wrote table=bond_deal_summary_sse to cache/meta/bond_deal_summary_sse/part_50350_c03405de.parquet
2026-04-20 13:01:12 - Wrote table=bond_local_government_issue_cninfo to cache/meta/bond_local_government_issue_cninfo/part_50350_595efe2d.parquet
Progress: 10/10 (100.0%) - 1.8 tasks/s
Download completed: {'total': 10, 'completed': 7, 'failed': 3, 'elapsed': 5.67, 'success_count': 7, 'failed_count': 3, 'failed_stocks': [('air_quality_hist', "'result'"), ('bond_china_yield', 'Empty data'), ('bond_china_close_return', "'newDateValue'")]}
```

### 1.2 全量下载

下载指定日期范围的完整历史数据。

```bash
# 全量下载指定接口和日期范围
python -m akshare_data.offline.cli download --mode full --interface macro_china_gdp --start 2024-01-01 --end 2024-06-01 --workers 2
```

**真实输出示例**:
```
2026-04-20 13:01:14 - Wrote table=macro_china_gdp to cache/meta/macro_china_gdp/part_50397_d2b82ead.parquet
Progress: 1/1 (100.0%) - 4.1 tasks/s
Download completed: {'total': 1, 'completed': 1, 'failed': 0, 'elapsed': 0.25, 'success_count': 1, 'failed_count': 0, 'failed_stocks': []}
```

### 1.3 定时调度

启动后台调度器，按 `config/download/schedule.yaml` 配置自动执行下载任务。

```bash
# 启动调度器（持续运行，Ctrl+C 停止）
python -m akshare_data.offline.cli download --schedule
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--interface` | str | 所有接口 | 指定接口名称 |
| `--mode` | str | `incremental` | 下载模式：`incremental` / `full` |
| `--days` | int | 1 | 增量下载回溯天数 |
| `--start` | str | `2020-01-01` | 全量下载开始日期 |
| `--end` | str | 今天 | 全量下载结束日期 |
| `--workers` | int | 4 | 并发工作线程数 |
| `--schedule` | flag | false | 启动定时调度器 |

---

## 2. 探测命令 `probe`

并发审计 AkShare 接口可用性，生成健康报告。

### 2.1 运行探测

```bash
# 探测所有接口
python -m akshare_data.offline.cli probe --all
```

### 2.2 查看探测状态

```bash
# 查看上次探测结果摘要
python -m akshare_data.offline.cli probe --status
```

**真实输出示例**:
```
2026-04-20 13:00:59 - Loaded checkpoint: 1 entries
2026-04-20 13:00:59 - Loaded probe config: 1075 entries
Last probe results (1 interfaces):
  bond_cash_summary_sse: Success (0.09s, 10 rows)
Summary: {'total': 1, 'success': 1, 'failed': 0, 'rate': 100.0, 'elapsed': 0.0}
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--all` | flag | false | 探测所有接口 |
| `--interface` | str | - | 探测指定接口 |
| `--status` | flag | false | 显示上次探测状态 |

### 输出文件

| 文件 | 路径 | 说明 |
|------|------|------|
| 健康报告 | `reports/health/health_report_YYYYMMDD.md` | Markdown 格式健康报告 |
| 探测状态 | `config/prober/state.json` | 探测检查点（支持断点续跑） |
| 样本数据 | `config/prober/samples/*.csv` | 成功接口的样本数据 |

---

## 3. 分析命令 `analyze`

分析访问日志、缓存数据、接口字段。

### 3.1 日志分析 `logs`

分析访问日志，生成下载优先级配置。

```bash
# 分析最近 7 天日志（默认）
python -m akshare_data.offline.cli analyze logs --window 7

# 分析最近 30 天日志
python -m akshare_data.offline.cli analyze logs --window 30
```

**真实输出示例**:
```
2026-04-20 13:00:55 - Saved priority config to /path/to/config/download/priority.yaml
Analysis completed: 5 interfaces
```

**输出文件**: `config/download/priority.yaml`

### 3.2 缓存分析 `cache`

分析缓存数据完整性。

```bash
# 分析指定表的缓存完整性
python -m akshare_data.offline.cli analyze cache --table stock_daily
```

**真实输出示例**:
```
No data found for table: macro_china_gdp
```

### 3.3 字段分析 `fields`

分析接口返回字段，自动匹配中英文字段映射。

```bash
# 分析所有接口字段（默认 50 个样本）
python -m akshare_data.offline.cli analyze fields

# 只分析指定分类的接口
python -m akshare_data.offline.cli analyze fields --category equity
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `type` | str | **必填** | 分析类型：`logs` / `cache` / `fields` |
| `--window` | int | 7 | 日志分析窗口（天） |
| `--table` | str | - | 缓存分析表名 |
| `--category` | str | - | 字段分析接口分类 |

---

## 4. 报告命令 `report`

生成各类审计报告。

### 4.1 健康报告

```bash
# 生成接口健康审计报告
python -m akshare_data.offline.cli report health
```

**真实输出示例**:
```
2026-04-20 13:01:01 - Health report saved to /path/to/reports/health/health_report_20260420.md
Health report generated successfully
# AkShare Health Audit Report
- **Report Time**: 2026-04-20 13:01:01
- **Total APIs**: 1
- **Available APIs**: 1
- **Health Rate**: 100.0%
```

### 4.2 质量报告

```bash
# 生成指定表的数据质量报告
python -m akshare_data.offline.cli report quality --table stock_daily
```

**真实输出示例**:
```
2026-04-20 13:01:04 - Quality report saved to /path/to/reports/quality/quality_report_20260420.md
Quality report for stock_daily generated successfully
# Data Quality Report
- **Table**: stock_daily
- **total_records**: 100
- **completeness_ratio**: 1.0
- **is_complete**: True
- **Total Anomalies**: 44
```

### 4.3 仪表盘

```bash
# 生成综合数据仪表盘
python -m akshare_data.offline.cli report dashboard
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `type` | str | **必填** | 报告类型：`health` / `quality` / `dashboard` |
| `--table` | str | - | 质量报告表名 |

---

## 5. 配置命令 `config`

管理接口注册表配置。

### 5.1 生成配置

扫描 AkShare 模块，自动生成接口注册表。

```bash
# 生成完整配置
python -m akshare_data.offline.cli config generate
```

**真实输出示例**:
```
2026-04-20 - Scanned 1074 functions from akshare
Config generated: /path/to/config/akshare_registry.yaml
```

### 5.2 验证配置

检查注册表配置的完整性和一致性。

```bash
# 验证当前注册表
python -m akshare_data.offline.cli config validate
```

**真实输出示例**:
```
2026-04-20 - Registry validation passed
Validation passed
```

### 5.3 合并配置

将手工维护的接口定义合并到自动生成的注册表中。

```bash
# 合并手工配置
python -m akshare_data.offline.cli config merge
```

**真实输出示例**:
```
2026-04-20 - Scanned 1074 functions
Merged 0 manual interface configurations
Merged 6 manual rate limit configurations
Config merged: /path/to/config/akshare_registry.yaml
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `action` | str | **必填** | 操作：`generate` / `validate` / `merge` |

---

## 配置文件位置

```
config/
├── akshare_registry.yaml           # 主注册表文件
├── rate_limits.yaml                # 域名限速配置
├── schemas.yaml                    # Schema 注册表配置
├── system.yaml                     # 系统配置
├── interfaces/                     # 按分类的接口定义
│   ├── equity.yaml
│   ├── index.yaml
│   ├── fund.yaml
│   ├── bond.yaml
│   ├── options.yaml
│   ├── futures.yaml
│   └── macro.yaml
├── sources/
│   ├── domains.yaml                # 域名限速配置
│   ├── failover.yaml               # 切源策略
│   └── sources.yaml                # 数据源配置
├── download/
│   ├── priority.yaml               # 下载优先级（由 analyze logs 生成）
│   └── schedule.yaml               # 定时下载任务
├── prober/
│   ├── config.yaml                 # 探测配置
│   ├── state.json                  # 探测检查点
│   └── samples/                    # 探测样本
├── domains/
│   └── by_domain.yaml              # 域名分组配置
├── logging/
│   └── access.yaml                 # 日志配置
├── fields/
│   └── mappings/                   # 接口字段映射
├── field_mappings/                 # 字段映射配置
├── registry/                       # 注册表中间文件
├── generated/                      # 自动生成文件
│   ├── field_mappings/
│   ├── health_samples/
│   ├── registry_raw/
│   └── reports/
└── cache/                          # 配置缓存
```

---

## 常见问题

### Q: 下载任务全部失败怎么办？

1. 检查网络连接
2. 检查接口参数是否正确
3. 使用 `--interface` 指定单个接口测试
4. 查看错误日志中的具体失败原因

### Q: 探测状态显示全零？

首次运行时探测状态为空是正常的。运行 `probe --all` 后状态会更新。

### Q: 配置验证失败？

运行 `config validate` 查看具体错误信息，通常是 YAML 格式问题或缺少必需字段。

### Q: 字段分析超时？

字段分析会实际调用 AkShare 接口，耗时较长。建议先用 `--category` 指定分类缩小范围。

### Q: 报告文件为空？

报告命令需要依赖探测或分析的数据。先运行 `probe --all` 或 `analyze logs` 再生成报告。

### Q: Python 模块导入错误？

确保在项目根目录下运行命令，且已安装所有依赖：
```bash
pip install -e .
```

---

## 命令速查表

| 场景 | 命令 |
|------|------|
| 每日增量更新 | `offline download --days 1` |
| 全量历史数据 | `offline download --mode full --start 2020-01-01` |
| 启动定时调度 | `offline download --schedule` |
| 接口健康检查 | `offline probe --all` |
| 查看探测状态 | `offline probe --status` |
| 分析访问日志 | `offline analyze logs --window 7` |
| 分析缓存完整性 | `offline analyze cache --table stock_daily` |
| 分析接口字段 | `offline analyze fields --category equity` |
| 生成健康报告 | `offline report health` |
| 生成质量报告 | `offline report quality --table stock_daily` |
| 生成配置 | `offline config generate` |
| 验证配置 | `offline config validate` |
| 合并手工配置 | `offline config merge` |
