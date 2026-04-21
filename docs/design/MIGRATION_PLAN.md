# 迁移规划：stock-backtesting-system → akshare-data-service

## 执行摘要

**stock-backtesting-system** 是一个完整的量化交易系统，包含数据层、因子系统、回测引擎、实盘交易、ML研究等模块。

**akshare-data-service** 目前只实现了数据层能力（多源数据获取、三级缓存、增量更新、离线工具）。

本规划详细说明如何将 stock-backtesting-system 的所有核心能力迁移到 akshare-data-service，同时利用其更强大的数据基础设施。

---

## 一、能力对比矩阵

| 能力模块 | stock-backtesting-system | akshare-data-service | 迁移状态 |
|---------|------------------------|---------------------|---------|
| **数据获取** | AkShare + OSS | AkShare + Tushare + Lixinger + JSL | ✅ 已超越 |
| **缓存系统** | 内存 + OSS + Parquet | 内存 + DuckDB + Parquet | ✅ 已超越 |
| **多源备份** | Sina/EastMoney fallback | 4源备份 + 熔断器 | ✅ 已超越 |
| **增量更新** | 手动实现 | 自动增量引擎 | ✅ 已超越 |
| **数据质量检查** | 基础检查 | 7维度质量检查 | ✅ 已超越 |
| **批量下载** | 无 | BatchDownloader | ✅ 已有 |
| **API探测** | 无 | APIProber | ✅ 已有 |
| **回测引擎** | Backtrader集成 | ❌ 缺失 | ❌ 需迁移 |
| **因子计算** | TA-Lib 200+ / 内置 / 自定义 | ❌ 缺失 | ❌ 需迁移 |
| **因子分析** | Alphalens / IC/ICIR | ❌ 缺失 | ❌ 需迁移 |
| **因子工作流** | ML训练 / Qlib | ❌ 缺失 | ❌ 需迁移 |
| **策略库** | JoinQuant涨停基因等 | ❌ 缺失 | ❌ 需迁移 |
| **指标体系** | 30+绩效指标 | ❌ 缺失 | ❌ 需迁移 |
| **实盘交易** | 执行引擎/风控/KillSwitch | ❌ 缺失 | ❌ 需迁移 |
| **ML研究** | XGBoost/SHAP | ❌ 缺失 | ❌ 需迁移 |
| **Web UI** | Streamlit仪表板 | ❌ 缺失 | ❌ 需迁移 |
| **Serverless** | 阿里云函数计算 | ❌ 缺失 | ❌ 需迁移 |
| **LLM Skills** | MCP自主回测 | ❌ 缺失 | ❌ 需迁移 |

---

## 二、迁移架构设计

### 2.1 目标架构

```
akshare-data-service/
├── src/akshare_data/
│   ├── __init__.py              # 统一入口
│   ├── api.py                   # DataService (已有)
│   ├── core/                    # 核心模块 (已有)
│   ├── sources/                 # 数据源 (已有)
│   ├── store/                   # 存储层 (已有)
│   ├── offline/                 # 离线工具 (已有)
│   │
│   ├── backtest/                # [新增] 回测引擎
│   │   ├── __init__.py
│   │   ├── engine.py            # BacktestEngine
│   │   ├── strategies/          # 策略库
│   │   │   ├── __init__.py
│   │   │   ├── base.py          # 策略基类
│   │   │   ├── weighted_top_n.py
│   │   │   ├── equal_weight.py
│   │   │   ├── direct_execution.py
│   │   │   └── limit_up_gene.py # 涨停基因策略
│   │   ├── config.py            # 回测配置
│   │   ├── metrics.py           # 绩效指标
│   │   └── report.py            # 报告生成
│   │
│   ├── factor/                  # [新增] 因子系统
│   │   ├── __init__.py
│   │   ├── generator/           # 因子生成
│   │   │   ├── __init__.py
│   │   │   ├── base.py          # FactorCalculator ABC
│   │   │   ├── builtin.py       # 内置因子
│   │   │   ├── talib.py         # TA-Lib因子
│   │   │   └── custom.py        # 自定义因子
│   │   ├── analyzer/            # 因子分析
│   │   │   ├── __init__.py
│   │   │   ├── core.py          # Alphalens集成
│   │   │   ├── ic_analysis.py   # IC/ICIR分析
│   │   │   └── rolling.py       # 滚动监控
│   │   ├── quality.py           # 因子质量检查
│   │   └── merger.py            # 因子合并
│   │
│   ├── live_trading/            # [新增] 实盘交易
│   │   ├── __init__.py
│   │   ├── execution.py         # 执行引擎
│   │   ├── risk.py              # 风控引擎
│   │   ├── kill_switch.py       # 紧急停止
│   │   ├── broker.py            # 券商适配器
│   │   ├── portfolio.py         # 组合构建
│   │   ├── drift.py             # 模型漂移检测
│   │   └── state.py             # 状态持久化
│   │
│   ├── metrics/                 # [新增] 指标体系
│   │   ├── __init__.py
│   │   ├── performance.py       # 基础绩效指标
│   │   ├── risk.py              # 风险指标
│   │   ├── trade.py             # 交易指标
│   │   └── portfolio.py         # 组合结构指标
│   │
│   └── ml/                      # [新增] ML研究
│       ├── __init__.py
│       ├── pipeline.py          # ML训练流水线
│       ├── preprocessing.py     # 特征预处理
│       ├── models.py            # 模型实现
│       └── predictions.py       # 预测生成
│
├── scripts/                     # [新增] 工具脚本
│   ├── batch_backtest.py        # 批量回测
│   ├── factor_test.py           # 因子测试
│   └── live_trading_runner.py   # 实盘运行
│
├── web/                         # [新增] Web UI
│   └── app.py                   # Streamlit应用
│
└── skills/                      # [新增] LLM Skills
    ├── run_backtest/
    ├── compute_factor/
    └── evaluate_factor/
```

### 2.2 数据层适配设计

**核心原则**：所有迁移模块直接使用 akshare-data-service 的 DataService API，无需额外适配层。

| 原系统数据调用 | akshare-data-service 等价 | 说明 |
|--------------|-------------------------|------|
| `load_oss_stocks()` | `get_daily()` 批量调用 | 返回宽表格式 |
| `load_oss_complex_stocks()` | `get_daily()` + 字段选择 | 返回 OHLCV 字典 |
| `get_index_stocks()` | `get_index_stocks()` | 直接等价 |
| `get_industry_category()` | `get_industry_mapping()` | 直接等价 |
| `get_concept_categories()` | 需扩展 | 新增概念板块接口 |
| `get_trading_dates()` | `get_trading_days()` | 名称差异 |
| `code2name` | `get_security_info()` | 直接等价 |
| `get_st_stocks()` | `get_st_stocks()` | 直接等价 |
| `get_suspended_stocks()` | `get_suspended_stocks()` | 直接等价 |

---

## 三、分阶段迁移计划

### 阶段一：回测引擎 + 指标体系（优先级：P0）

**目标**：实现基础回测能力，支持预测驱动的策略回测。

**工作量**：约 5-7 天

**交付物**：
1. `src/akshare_data/backtest/engine.py` - BacktestEngine 核心
2. `src/akshare_data/backtest/strategies/` - 4种基础策略
3. `src/akshare_data/metrics/` - 30+ 绩效指标
4. `src/akshare_data/backtest/config.py` - 配置系统
5. `src/akshare_data/backtest/report.py` - 报告生成

**关键依赖**：
- `backtrader` (回测框架)
- `pandas`, `numpy` (已有)

**数据层依赖**：
- `get_daily()` - 日线数据
- `get_index()` - 指数数据
- `get_trading_days()` - 交易日历
- `get_security_info()` - 证券信息

**测试要求**：
- 单元测试：策略逻辑、指标计算
- 集成测试：完整回测流程
- 基准测试：与原版回测结果对比

---

### 阶段二：因子系统（优先级：P0）

**目标**：实现因子计算、分析、质量检查能力。

**工作量**：约 7-10 天

**交付物**：
1. `src/akshare_data/factor/generator/` - 因子生成器
2. `src/akshare_data/factor/analyzer/` - 因子分析器
3. `src/akshare_data/factor/quality.py` - 质量检查
4. `src/akshare_data/factor/merger.py` - 因子合并

**关键依赖**：
- `ta-lib` (TA-Lib 技术指标)
- `alphalens-reloaded` (因子分析)
- `scipy`, `statsmodels` (统计分析)

**数据层依赖**：
- `get_daily()` - OHLCV 数据
- `get_index_stocks()` - 股票池
- `get_industry_mapping()` - 行业分类

**测试要求**：
- 单元测试：因子计算准确性
- 集成测试：Alphalens 分析流程
- 验证：与原系统因子结果对比

---

### 阶段三：实盘交易系统（优先级：P1）

**目标**：实现实盘交易能力，包括执行、风控、监控。

**工作量**：约 5-7 天

**交付物**：
1. `src/akshare_data/live_trading/execution.py` - 执行引擎
2. `src/akshare_data/live_trading/risk.py` - 风控引擎
3. `src/akshare_data/live_trading/kill_switch.py` - 紧急停止
4. `src/akshare_data/live_trading/portfolio.py` - 组合构建
5. `src/akshare_data/live_trading/drift.py` - 漂移检测
6. `src/akshare_data/live_trading/state.py` - 状态持久化

**关键依赖**：
- 已有依赖（无新增）

**数据层依赖**：
- `get_realtime_data()` - 实时行情
- `get_daily()` - 历史数据
- `get_st_stocks()` / `get_suspended_stocks()` - 黑名单
- `get_industry_mapping()` - 行业分类

**测试要求**：
- 单元测试：风控规则、订单逻辑
- 模拟测试：Paper Trading 模式
- 集成测试：完整交易流水线

---

### 阶段四：Web UI + CLI 工具（优先级：P1）

**目标**：提供用户友好的交互界面。

**工作量**：约 3-5 天

**交付物**：
1. `web/app.py` - Streamlit Web UI
2. `scripts/batch_backtest.py` - 批量回测脚本
3. `scripts/factor_test.py` - 因子测试脚本
4. `scripts/live_trading_runner.py` - 实盘运行脚本

**关键依赖**：
- `streamlit` (Web UI)
- `streamlit-echarts` (图表)
- `argparse` (CLI)

**测试要求**：
- 手动测试：UI 交互
- E2E测试：完整工作流

---

### 阶段五：ML 研究 + Serverless（优先级：P2）

**目标**：实现 ML 模型训练和云端因子监控。

**工作量**：约 5-7 天

**交付物**：
1. `src/akshare_data/ml/pipeline.py` - ML 训练流水线
2. `src/akshare_data/ml/models.py` - 模型实现
3. `src/akshare_data/ml/predictions.py` - 预测生成
4. `scripts/serverless_factor_handler.py` - 阿里云函数计算

**关键依赖**：
- `xgboost` (ML 模型)
- `shap` (模型解释)
- `scikit-learn` (预处理)

**测试要求**：
- 单元测试：模型训练/预测
- 集成测试：完整 ML 流水线

---

### 阶段六：LLM Skills（优先级：P2）

**目标**：实现 MCP 协议集成，支持 LLM 自主执行任务。

**工作量**：约 2-3 天

**交付物**：
1. `skills/run_backtest/` - 回测技能
2. `skills/compute_factor/` - 因子计算技能
3. `skills/evaluate_factor/` - 因子评估技能

**测试要求**：
- 集成测试：LLM 调用技能

---

## 四、关键设计决策

### 4.1 回测框架选择

**决策**：继续使用 Backtrader

**理由**：
- 原系统已深度集成
- 社区活跃，文档完善
- 支持多种策略模式
- 可扩展性强

### 4.2 因子分析框架

**决策**：使用 alphalens-reloaded

**理由**：
- 原系统已使用
- 行业标准因子分析工具
- 提供完整的 IC/ICIR/分位数分析

### 4.3 数据层集成方式

**决策**：直接调用 DataService API，不创建适配层

**理由**：
- akshare-data-service 已提供所有必要接口
- 减少维护负担
- 自动获得缓存/增量更新能力

### 4.4 代码组织

**决策**：所有模块放在 `src/akshare_data/` 下，保持统一命名空间

**理由**：
- 单一包管理
- 统一导入路径
- 便于版本控制

---

## 五、风险与缓解

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| TA-Lib 安装困难 | 因子计算受阻 | 中 | 提供纯 Python 备选实现 |
| Alphalens 兼容性问题 | 因子分析失败 | 低 | 使用 alphalens-reloaded |
| Backtrader 版本差异 | 回测结果不一致 | 低 | 锁定版本 |
| 数据接口变更 | 部分功能失效 | 中 | 抽象数据层接口 |
| 性能瓶颈 | 大规模回测慢 | 中 | 使用 DuckDB 加速查询 |

---

## 六、测试策略

### 6.1 单元测试
- 每个模块独立测试
- 覆盖率目标：>80%
- 使用 pytest

### 6.2 集成测试
- 端到端工作流测试
- 数据层集成测试
- 使用 Mock 数据源

### 6.3 回归测试
- 与原系统回测结果对比
- 因子计算结果验证
- 误差容忍：<1%

### 6.4 性能测试
- 批量回测性能
- 因子计算吞吐量
- 缓存命中率

---

## 七、时间估算

| 阶段 | 工作量 | 累计时间 |
|------|--------|---------|
| 阶段一：回测引擎 + 指标 | 5-7 天 | 7 天 |
| 阶段二：因子系统 | 7-10 天 | 17 天 |
| 阶段三：实盘交易 | 5-7 天 | 24 天 |
| 阶段四：Web UI + CLI | 3-5 天 | 29 天 |
| 阶段五：ML + Serverless | 5-7 天 | 36 天 |
| 阶段六：LLM Skills | 2-3 天 | 39 天 |
| **总计** | **27-39 天** | **约 6 周** |

---

## 八、成功标准

1. **功能完整性**：所有原系统功能均已迁移
2. **性能提升**：由于缓存系统，数据获取速度提升 >50%
3. **代码质量**：测试覆盖率 >80%
4. **向后兼容**：支持原系统的预测 CSV 格式
5. **文档完善**：每个模块都有 API 文档和使用示例

---

## 九、下一步行动

1. **确认迁移范围**：与团队确认是否需要全部 6 个阶段
2. **环境准备**：安装必要依赖 (backtrader, ta-lib, alphalens-reloaded)
3. **开始阶段一**：实现回测引擎核心
