# 迁移指南

本文档涵盖从安装到常见使用模式的完整指南，以及从 jk2bt 和直接使用 AkShare 迁移的详细说明。

---

## 目录

1. [安装与配置](#1-安装与配置)
2. [快速开始](#2-快速开始)
3. [从 jk2bt 迁移](#3-从-jk2bt-迁移)
4. [从直接使用 AkShare 迁移](#4-从直接使用-akshare-迁移)
5. [常见使用模式](#5-常见使用模式)
6. [配置指南](#6-配置指南)
7. [离线工具使用](#7-离线工具使用)
8. [故障排除](#8-故障排除)
9. [项目结构参考](#9-项目结构参考)
10. [示例文件参考](#10-示例文件参考)

---

## 1. 安装与配置

### 1.1 安装

```bash
# 克隆仓库后进入项目目录
cd akshare-data-service

# 开发模式安装（推荐）
pip install -e .
```

### 1.2 依赖项

核心依赖在 `pyproject.toml` 中定义：

| 依赖 | 最低版本 | 说明 |
|------|----------|------|
| pandas | >=2.0 | 数据处理 |
| duckdb | >=0.9 | SQL 查询引擎 |
| pyarrow | >=14.0 | Parquet 文件读写 |
| cachetools | >=5.3 | 内存 LRU 缓存 |
| akshare | >=1.10 | 数据源 |
| pyyaml | >=6.0 | 配置文件解析 |
| croniter | >=1.3 | 定时任务调度 |

可选依赖：

```bash
# 安装 Tushare 支持
pip install -e ".[tushare]"

# 安装 BaoStock 支持
pip install -e ".[baostock]"

# 安装所有可选依赖
pip install -e ".[all]"
```

### 1.3 环境变量配置

通过环境变量覆盖默认配置：

| 环境变量 | 说明 | 示例 |
|----------|------|------|
| `AKSHARE_DATA_CACHE_DIR` | 缓存根目录 | `~/.akshare_data/cache` |
| `AKSHARE_DATA_CACHE_MAX_ITEMS` | 内存缓存最大条目数 | `5000` |
| `AKSHARE_DATA_CACHE_TTL_SECONDS` | 内存缓存默认 TTL（秒） | `3600` |
| `AKSHARE_DATA_CACHE_COMPRESSION` | Parquet 压缩算法 | `snappy` / `gzip` / `zstd` |
| `AKSHARE_DATA_CACHE_ROW_GROUP_SIZE` | Parquet 行组大小 | `100000` |
| `AKSHARE_DATA_CACHE_DUCKDB_THREADS` | DuckDB 线程数 | `4` |
| `AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT` | DuckDB 内存限制 | `4GB` |
| `AKSHARE_DATA_CACHE_LOG_LEVEL` | 日志级别 | `DEBUG` / `INFO` / `WARNING` |
| `AKSHARE_DATA_CACHE_RETENTION_HOURS` | 过期文件保留时间（小时） | `24` |
| `LIXINGER_TOKEN` | Lixinger API Token | `your_token_here` |

### 1.4 Lixinger Token 配置

Lixinger 是优先数据源，需要配置 Token 才能使用：

**方式一：环境变量**

```bash
export LIXINGER_TOKEN="your_token_here"
```

**方式二：token.cfg 文件**

在项目根目录或 `~/.akshare_data/` 下创建 `token.cfg` 文件：

```ini
[lixinger]
token = your_token_here
```

**验证配置：**

```python
from akshare_data import get_service

service = get_service()
print(service.lixinger.health_check())
# 输出: {'status': 'ok', 'message': 'Lixinger API reachable...', 'latency_ms': 123.45}
```

---

## 2. 快速开始

### 2.1 使用模块级函数（最简单）

```python
from akshare_data import get_daily, get_index, get_etf, get_minute

# 获取股票日线数据（自动缓存）
df = get_daily("000001", "2024-01-01", "2024-12-31")
print(df.head())

# 获取指数数据
df = get_index("000300", "2024-01-01", "2024-12-31")

# 获取ETF数据
df = get_etf("510300", "2024-01-01", "2024-12-31")

# 获取分钟线数据
df = get_minute("000001", freq="5min", start_date="2024-06-01", end_date="2024-06-05")
```

### 2.2 使用 DataService 类

```python
from akshare_data import DataService, get_service

# 方式一：创建新实例
service = DataService()
df = service.get_daily("000001", "2024-01-01", "2024-12-31")

# 方式二：使用单例（推荐）
service = get_service()
df = service.get_daily("000001", "2024-01-01", "2024-12-31")
```

### 2.3 使用命名空间 API

DataService 提供了按市场和功能分类的命名空间 API：

```python
from akshare_data import get_service

service = get_service()

# A股股票行情
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31")
df = service.cn.stock.quote.minute("000001", freq="5min")
df = service.cn.stock.quote.realtime("000001")
df = service.cn.stock.quote.call_auction("000001")

# A股财务数据
df = service.cn.stock.finance.balance_sheet("000001")
df = service.cn.stock.finance.income_statement("000001")
df = service.cn.stock.finance.cash_flow("000001")
df = service.cn.stock.finance.indicators("000001", start_date="2023-01-01", end_date="2024-12-31")

# A股资金流向
df = service.cn.stock.capital.money_flow("000001")
df = service.cn.stock.capital.northbound_holdings("000001", "2024-01-01", "2024-12-31")
df = service.cn.stock.capital.block_deal("000001", "2024-01-01", "2024-12-31")
df = service.cn.stock.capital.dragon_tiger("2024-06-01")
df = service.cn.stock.capital.margin("000001", "2024-01-01", "2024-12-31")
df = service.cn.stock.capital.north(start_date="2024-01-01", end_date="2024-12-31")  # 北向资金

# 指数
df = service.cn.index.quote.daily("000300", "2024-01-01", "2024-12-31")
df = service.cn.index.meta.components("000300")  # 指数成分股（含权重）

# ETF
df = service.cn.fund.quote.daily("510300", "2024-01-01", "2024-12-31")

# 事件数据
df = service.cn.stock.event.dividend("000001")
df = service.cn.stock.event.restricted_release("000001")

# 港股
df = service.hk.stock.quote.daily()

# 美股
df = service.us.stock.quote.daily()

# 宏观经济
df = service.macro.china.interest_rate("2024-01-01", "2024-12-31")
df = service.macro.china.gdp("2024-01-01", "2024-12-31")
df = service.macro.china.social_financing("2024-01-01", "2024-12-31")

# 交易日历
days = service.cn.trade_calendar(start_date="2024-01-01", end_date="2024-12-31")
```

### 2.4 指定数据源

```python
# 使用特定数据源
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source="lixinger")
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source="akshare")

# 按优先级尝试多个数据源
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source=["lixinger", "akshare"])
```

---

## 3. 从 jk2bt 迁移

### 3.1 兼容别名对照表

akshare_data 提供了与 jk2bt 兼容的函数别名，可直接替换：

| jk2bt 函数 | akshare_data 别名 | 推荐新函数 |
|------------|-------------------|------------|
| `get_stock_daily(symbol, start, end)` | `get_stock_daily()` | `get_daily()` |
| `get_stock_price(symbol)` | `get_stock_price()` | `get_daily()` |
| `get_etf_daily(symbol, start, end)` | `get_etf_daily()` | `get_etf()` |
| `get_index_daily(symbol, start, end)` | `get_index_daily()` | `get_index()` |
| `get_stock_minute(symbol, start, end, period)` | `get_stock_minute()` | `get_minute()` |
| `get_money_flow(symbol)` | `get_money_flow_alias()` | `get_money_flow()` |
| `get_north_money_flow()` | `get_north_money_flow_alias()` | `get_north_money_flow()` |
| `get_company_info(symbol)` | `get_company_info()` | `get_basic_info()` |
| `get_shareholders(symbol)` | `get_shareholders()` | `get_top_shareholders()` |
| `get_income(symbol)` | `get_income()` | `get_income_statement()` |
| `get_cashflow(symbol)` | `get_cashflow()` | `get_cash_flow()` |
| `get_valuation(symbol)` | `get_valuation()` | `get_stock_valuation()` |
| `get_index_valuation(index_code)` | `get_index_valuation_alias()` | `get_index_valuation()` |

### 3.2 迁移示例

**旧代码（jk2bt）：**

```python
from jk2bt import get_stock_daily, get_company_info, get_stock_minute

df = get_stock_daily("000001.XSHE", "2024-01-01", "2024-12-31")
info = get_company_info("000001.XSHE")
minute = get_stock_minute("000001.XSHE", "2024-06-01", "2024-06-05", period="1m")
```

**新代码（akshare_data）- 推荐方式：**

```python
from akshare_data import get_daily, get_basic_info, get_minute

# 代码格式自动转换，无需修改
df = get_daily("000001", "2024-01-01", "2024-12-31")
info = get_basic_info("000001")
minute = get_minute("000001", freq="1min", start_date="2024-06-01", end_date="2024-06-05")
```

**过渡期（使用兼容别名）：**

```python
from akshare_data import get_stock_daily, get_company_info, get_stock_minute

# 完全兼容的调用方式
df = get_stock_daily("000001.XSHE", "2024-01-01", "2024-12-31")
info = get_company_info("000001.XSHE")
minute = get_stock_minute("000001.XSHE", "2024-06-01", "2024-06-05", period="1m")
```

### 3.3 证券代码格式

akshare_data 支持多种代码格式，自动标准化：

| 格式 | 示例 | 说明 |
|------|------|------|
| 纯数字 | `"000001"` | 自动识别市场 |
| 交易所前缀 | `"sh600000"`, `"sz000001"` | 沪市 sh / 深市 sz |
| JoinQuant 格式 | `"600000.XSHG"`, `"000001.XSHE"` | 自动转换为标准格式 |

### 3.4 缓存目录迁移

如果之前使用 jk2bt 的缓存数据，可以设置环境变量指向旧缓存目录：

```bash
# 指向旧的缓存目录
export AKSHARE_DATA_CACHE_DIR="/path/to/old/jk2bt/cache"
```

或者在代码中指定：

```python
from akshare_data import CacheManager

cache = CacheManager(base_dir="/path/to/old/jk2bt/cache")
```

---

## 4. 从直接使用 AkShare 迁移

### 4.1 对比示例

**旧代码（直接调用 AkShare）：**

```python
import akshare as ak

# 每次调用都请求网络，无缓存
df = ak.stock_zh_a_hist(symbol="000001", period="daily",
                        start_date="20240101", end_date="20241231", adjust="qfq")

# 需要手动处理日期格式转换
df = ak.stock_zh_index_daily(symbol="sh000300")

# 需要手动处理字段映射
df = ak.stock_individual_fund_flow(stock="000001", market="sz")
```

**新代码（使用 akshare_data）：**

```python
from akshare_data import get_daily, get_index, get_money_flow

# 自动缓存，重复调用直接返回缓存
df = get_daily("000001", "2024-01-01", "2024-12-31", adjust="qfq")

# 统一的代码格式，自动处理
df = get_index("000300", "2024-01-01", "2024-12-31")

# 统一的字段名
df = get_money_flow("000001", "2024-01-01", "2024-12-31")
```

### 4.2 迁移优势

| 特性 | 直接使用 AkShare | akshare_data |
|------|-----------------|--------------|
| 缓存 | 无，每次请求网络 | 多级缓存（内存 → Parquet → DuckDB） |
| 增量更新 | 手动处理 | 自动检测缺失日期，只拉取增量 |
| 多数据源 | 单源 | Lixinger + AkShare 自动切换 |
| 字段标准化 | 各接口字段名不一致 | 统一字段名 |
| 代码格式 | 各接口格式要求不同 | 统一支持多种格式 |
| 容错 | 无 | 多源备份 + 熔断器 |
| 离线工具 | 无 | 批量下载、接口探测、质量检查 |

### 4.3 常用 AkShare 接口映射

| AkShare 接口 | akshare_data 函数 |
|-------------|-------------------|
| `stock_zh_a_hist()` | `get_daily()` |
| `stock_zh_a_minute()` | `get_minute()` |
| `stock_zh_index_daily()` | `get_index()` |
| `fund_etf_hist_em()` | `get_etf()` |
| `stock_individual_fund_flow()` | `get_money_flow()` |
| `stock_zh_index_spot_em()` | `get_index_stocks()` |
| `stock_financial_analysis_indicator()` | `get_finance_indicator()` |
| `stock_profile_em()` | `get_basic_info()` |

---

## 5. 常见使用模式

### 5.1 获取股票日线数据

```python
from akshare_data import get_daily

# 基本用法
df = get_daily("000001", "2024-01-01", "2024-12-31")

# 后复权
df = get_daily("600519", "2024-01-01", "2024-12-31", adjust="hfq")

# 不复权
df = get_daily("000001", "2024-01-01", "2024-12-31", adjust="none")

# 指定数据源
df = get_daily("000001", "2024-01-01", "2024-12-31", source="lixinger")
```

返回字段：`symbol`, `date`, `open`, `high`, `low`, `close`, `volume`, `amount`, `adjust`

### 5.2 获取指数成分股

```python
from akshare_data import get_index_stocks, get_index_components

# 获取成分股代码列表
stocks = get_index_stocks("000300")  # 沪深300成分股
print(f"成分股数量: {len(stocks)}")

# 获取成分股及权重
df = get_index_components("000300", include_weights=True)
print(df.head())
```

### 5.3 获取财务数据

```python
from akshare_data import (
    get_finance_indicator,
    get_basic_info,
    get_balance_sheet,
    get_income_statement,
    get_cash_flow,
    get_stock_valuation,
)

# 财务指标（PE/PB/PS/ROE等）
df = get_finance_indicator("600519")

# 基本信息
df = get_basic_info("000001")

# 资产负债表
df = get_balance_sheet("000001")

# 利润表
df = get_income_statement("000001")

# 现金流量表
df = get_cash_flow("000001")

# 估值数据
df = get_stock_valuation("000001")
```

### 5.4 使用特定数据源

```python
from akshare_data import get_service

service = get_service()

# 通过命名空间 API 指定数据源
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source="lixinger")
df = service.cn.stock.quote.daily("000001", "2024-01-01", "2024-12-31", source="akshare")

# 使用模块级函数时通过 DataService 指定
df = service.get_daily("000001", "2024-01-01", "2024-12-31", source="lixinger")
```

### 5.5 批量下载

```python
from akshare_data import BatchDownloader

downloader = BatchDownloader(max_workers=4)

# 增量下载（默认最近 1 天）
result = downloader.download_incremental(days_back=1)
print(f"成功: {result['success_count']}, 失败: {result['failed_count']}")

# 全量下载（默认取注册表前 20 个接口）
result = downloader.download_full(
    interfaces=["equity_daily"],
    start="2024-01-01",
    end="2024-12-31",
)

# 带进度回调
def on_progress(current, total, result):
    print(f"进度: {current}/{total} ({current/total*100:.1f}%)")

result = downloader.download_incremental(days_back=1, progress_callback=on_progress)
```

### 5.6 数据质量检查

```python
from akshare_data.offline.quality import DataQualityChecker

checker = DataQualityChecker()

# 完整性检查
report = checker.check_completeness(
    table="stock_daily",
    symbol="000001",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
print(f"完整度: {report['completeness_ratio']:.2%}")
print(f"缺失日期数: {report['missing_dates_count']}")

# 异常值检测
df = checker.cache_manager.read("stock_daily", where={"symbol": "000001"})
anomalies = checker.check_anomalies(df)
print(f"异常数量: {anomalies['anomaly_count']}")

# 一致性检查（跨表比对）
report = checker.check_consistency("stock_daily", "index_daily", "000001")

# 综合质量报告
report = checker.generate_report(
    table="stock_daily",
    symbol="000001",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
print(f"总体评分: {report['summary']['overall_score']}")
```

---

## 6. 配置指南

### 6.1 CacheConfig 选项

```python
from akshare_data.core.config import CacheConfig, TableConfig

config = CacheConfig(
    base_dir="./cache",              # 缓存根目录
    daily_dir="daily",               # 日线数据子目录
    minute_dir="minute",             # 分钟线数据子目录
    snapshot_dir="snapshot",         # 快照数据子目录
    meta_dir="meta",                 # 元数据子目录
    source_priority=[              # 数据源优先级（索引越小优先级越高）
        "akshare", "sina", "eastmoney", "tushare", "baostock"
    ],
    tushare_token="",               # Tushare API Token
    memory_cache_max_items=5000,    # 内存缓存最大条目数
    memory_cache_default_ttl_seconds=3600,  # 内存缓存 TTL
    compression="snappy",           # Parquet 压缩算法
    row_group_size=100_000,         # Parquet 行组大小
    aggregation_enabled=True,       # 是否启用聚合存储
    aggregation_schedule="daily",   # 聚合任务调度周期
    duckdb_read_only=True,          # DuckDB 只读模式
    duckdb_threads=4,               # DuckDB 线程数
    duckdb_memory_limit="4GB",      # DuckDB 内存限制
    cleanup_retention_hours=24,     # 过期文件保留时间
    log_level="INFO",               # 日志级别
    log_format="json",              # 日志格式
)
```

### 6.2 按表配置

```python
from akshare_data.core.config import CacheConfig, TableConfig

config = CacheConfig(
    base_dir="./cache",
    tables={
        "stock_daily": TableConfig(
            partition_by="date",
            ttl_hours=0,               # 0 = 永不过期
            compaction_threshold=20,   # 文件数超过此值触发压缩
            aggregation_enabled=True,
        ),
        "finance_indicator": TableConfig(
            partition_by="report_date",
            ttl_hours=2160,            # 90天过期
            compaction_threshold=5,
            aggregation_enabled=True,
        ),
        "securities": TableConfig(
            partition_by=None,
            ttl_hours=0,
            compaction_threshold=0,
        ),
    },
)
```

### 6.3 使用自定义配置

```python
from akshare_data import DataService, CacheManager
from akshare_data.core.config import CacheConfig

# 方式一：通过环境变量
import os
os.environ["AKSHARE_DATA_CACHE_DIR"] = "/data/akshare_cache"

# 方式二：通过配置对象
config = CacheConfig.from_env()
config.base_dir = "/data/akshare_cache"
config.duckdb_threads = 8

cache = CacheManager(config=config)
service = DataService(cache_manager=cache)

# 方式三：从 JSON 文件加载
config = CacheConfig.from_json("config/cache.json")
service = DataService(cache_manager=CacheManager(config=config))
```

### 6.4 限速配置

限速配置从 `config/akshare_registry.yaml` 中自动加载。每个域名有独立的限速器：

```python
from akshare_data.offline.downloader import BatchDownloader

downloader = BatchDownloader(
    max_workers=4,
    rate_limiter_config={
        "sina": (10, 1.0),       # 每秒最多10次调用
        "eastmoney": (10, 1.0),
        "tushare": (10, 1.0),
        "default": (10, 1.0),
    },
)
```

---

## 7. 离线工具使用

### 7.1 BatchDownloader（批量下载器）

批量下载器直接从 AkShare 原始函数下载数据并写入共享缓存（DuckDB + Parquet）：

```python
from akshare_data import BatchDownloader

# 创建下载器
downloader = BatchDownloader(
    max_workers=4,       # 并发线程数
    batch_size=50,       # 批次大小
)

# 模式一：增量下载
result = downloader.download_incremental(
    days_back=1,           # 回溯天数
)

# 模式二：全量下载
result = downloader.download_full(
    interfaces=["equity_daily", "index_daily"],  # 指定接口
    start="2020-01-01",
    end="2024-12-31",
    force=False,           # False=跳过已有数据
)

# 查看结果
print(f"成功: {result['success_count']}")
print(f"失败: {result['failed_count']}")
if result.get('failed_stocks'):
    print(f"失败接口: {result['failed_stocks'][:10]}")
```

**带进度回调：**

```python
def on_progress(current, total, result):
    print(f"进度: {current}/{total} ({current/total*100:.1f}%)")

downloader.download_incremental(days_back=1, progress_callback=on_progress)
```

### 7.2 APIProber（接口探测器）

APIProber 用于并发审计 AkShare 接口的可用性：

```python
from akshare_data import APIProber

# 创建探测器
prober = APIProber()

# 运行健康检查
prober.run_check()

# 生成配置
prober.generate_full_config()
```

**命令行使用：**

```bash
# 运行健康检查
python -m akshare_data.offline.prober

# 生成探测配置
python -m akshare_data.offline.prober --generate-config
```

探测结果会保存到：
- `config/health_state.json` - 检查点文件
- `reports/health_report.md` - 健康报告
- `config/health_samples/` - 样本数据

### 7.3 DataQualityChecker（数据质量检查器）

```python
from akshare_data.offline.quality import DataQualityChecker

checker = DataQualityChecker()

# 完整性检查
report = checker.check_completeness(
    table="stock_daily",
    symbol="000001",
    start_date="2024-01-01",
    end_date="2024-12-31",
)

# 异常值检测
anomalies = checker.check_anomalies(df, price_change_threshold=20.0)

# 一致性检查
report = checker.check_consistency("table1", "table2", "000001")

# 综合质量报告
report = checker.generate_report("stock_daily", "000001")
```

### 7.4 Reporter（报告生成器）

```python
from akshare_data.offline.reporter import Reporter

reporter = Reporter()

# 生成健康报告
content = reporter.generate_health_report(results)

# 生成质量报告
content = reporter.generate_quality_report(classification_df)

# 生成数据量报告
content = reporter.generate_volume_report(volume_df)

# 保存为 JSON
Reporter.save_json(results, "output/report.json")

# 生成摘要
summary = Reporter.generate_summary(probe_results)
```

---

## 8. 故障排除

### 8.1 常见错误及解决方案

**错误：Lixinger token not configured**

```python
# 解决方案：设置 LIXINGER_TOKEN 环境变量
import os
os.environ["LIXINGER_TOKEN"] = "your_token"

# 或创建 token.cfg 文件
```

**错误：No data available for the requested query**

```python
# 可能原因：
# 1. 日期范围内无交易数据（节假日/停牌）
# 2. 股票代码有误
# 3. 数据源暂时不可用

# 解决方案：
# 1. 检查代码格式是否正确
# 2. 扩大日期范围
# 3. 尝试指定其他数据源
df = get_daily("000001", "2024-01-01", "2024-12-31", source="akshare")
```

**错误：Connection refused / Timeout**

```python
# 可能原因：网络问题或数据源限流

# 解决方案：
# 1. 检查网络连接
# 2. 降低并发数
# 3. 增加重试间隔
downloader = BatchDownloader(max_workers=2)
```

**错误：Invalid symbol format**

```python
# 支持的代码格式：
# "000001"       - 纯数字
# "sh600000"     - 交易所前缀
# "600000.XSHG"  - JoinQuant 格式

# 确保使用正确的代码格式
df = get_daily("000001", "2024-01-01", "2024-12-31")  # 推荐
```

### 8.2 错误码参考

错误码定义在 `core/errors.py`，按类别分组：

| 前缀 | 类别 | 示例 |
|------|------|------|
| `1xxx` | 数据源错误 | `1001_SOURCE_UNAVAILABLE`, `1002_SOURCE_TIMEOUT` |
| `2xxx` | 缓存错误 | `2001_CACHE_MISS`, `2003_CACHE_WRITE_FAILED` |
| `3xxx` | 参数/验证错误 | `3001_INVALID_SYMBOL`, `3002_INVALID_DATE_RANGE` |
| `4xxx` | 网络错误 | `4001_NETWORK_TIMEOUT`, `4003_NETWORK_DNS_FAILURE` |
| `5xxx` | 数据质量错误 | `5001_NO_DATA`, `5002_INVALID_DATA` |
| `6xxx` | 系统/内部错误 | `6001_INTERNAL_ERROR`, `6003_CONFIGURATION_ERROR` |
| `7xxx` | 存储/文件错误 | `7001_FILE_NOT_FOUND`, `7013_DATABASE_LOCKED` |
| `8xxx` | 认证/授权错误 | `8001_AUTH_TOKEN_MISSING`, `8002_AUTH_TOKEN_EXPIRED` |
| `9xxx` | 并发/限流错误 | `9001_RATE_LIMIT_GLOBAL`, `9010_CIRCUIT_BREAKER_OPEN` |

**捕获和处理错误：**

```python
from akshare_data.core.errors import (
    DataAccessException,
    SourceUnavailableError,
    NoDataError,
    TimeoutError,
    RateLimitError,
    CacheError,
    ValidationError,
)

try:
    df = get_daily("000001", "2024-01-01", "2024-12-31")
except NoDataError as e:
    print(f"无数据: {e}")
except TimeoutError as e:
    print(f"超时: {e}")
except SourceUnavailableError as e:
    print(f"数据源不可用: {e}")
except DataAccessException as e:
    print(f"数据访问错误: {e.error_code} - {e.message}")
    print(f"错误类别: {e.to_dict()['category']}")
```

### 8.3 调试日志

```python
import logging

# 启用调试日志
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# 或者只启用 akshare_data 的日志
logging.getLogger("akshare_data").setLevel(logging.DEBUG)
```

**查看缓存状态：**

```python
from akshare_data import get_service

service = get_service()
stats = service.cache.get_stats()
print(f"内存缓存大小: {stats['memory_cache_size']}")
print(f"缓存命中率: {stats['memory_cache_hit_rate']:.2%}")
print(f"表列表: {list(stats['tables'].keys())}")

# 查看特定表的信息
info = service.cache.table_info("stock_daily", "daily")
print(f"文件数: {info['file_count']}")
print(f"总大小: {info['total_size_mb']} MB")
print(f"最后更新: {info['last_updated']}")
```

---

## 9. 项目结构参考

### 9.1 完整目录树

```
akshare-data-service/
├── pyproject.toml                 # 项目配置和依赖
├── README.md                      # 项目说明
├── DESIGN_non_akshare_sources.md  # 非 AkShare 数据源设计文档
├── DEVELOPMENT_PLAN.md            # 开发计划
├── implementation_plan.md         # 实现计划
├── MIGRATION_PLAN.md              # 迁移计划
├── verify_service.py              # 服务验证脚本
├── imports.txt                    # 导入列表
│
├── config/                        # 配置文件
│   ├── akshare_registry.yaml      # AkShare 接口注册表
│   ├── download_priority.yaml     # 下载优先级配置
│   ├── prober_config.json         # 探测器配置
│   └── health_samples/            # 健康检查样本数据
│
├── cache/                         # 缓存数据目录（运行时生成）
│   ├── daily/                     # 日线数据
│   │   └── stock_daily/
│   │       └── symbol=000001/
│   │           └── *.parquet
│   ├── minute/                    # 分钟线数据
│   ├── meta/                      # 元数据
│   └── snapshot/                  # 快照数据
│
├── src/akshare_data/              # 源代码
│   ├── __init__.py                # 入口，导出所有公共 API
│   ├── api.py                     # DataService 类，核心编排器
│   │
│   ├── core/                      # 核心基座
│   │   ├── __init__.py
│   │   ├── base.py                # DataSource 抽象基类
│   │   ├── config.py              # CacheConfig / TableConfig 配置管理
│   │   ├── config_dir.py          # 配置目录解析
│   │   ├── errors.py              # 错误码体系（177 个错误码）
│   │   ├── fields.py              # 字段映射（CN_TO_EN, FIELD_MAPS）
│   │   ├── logging.py             # 结构化日志 + StatsCollector
│   │   ├── normalize.py           # DataFrame 标准化
│   │   ├── options.py             # 期权工具
│   │   ├── schema.py              # Schema 注册表（69 表）
│   │   ├── stats.py               # 统计收集器
│   │   ├── symbols.py             # 证券代码格式转换
│   │   └── tokens.py              # Token 管理
│   │
│   ├── sources/                   # 数据源层
│   │   ├── __init__.py
│   │   ├── akshare_source.py      # AkShare 适配器
│   │   ├── lixinger_source.py     # Lixinger 适配器
│   │   ├── lixinger_client.py     # Lixinger HTTP 客户端
│   │   ├── tushare_source.py      # Tushare 适配器
│   │   ├── router.py              # 多源路由 + 熔断器
│   │   ├── mock.py                # Mock 数据源（测试用）
│   │   └── akshare/
│   │       ├── __init__.py
│   │       └── fetcher.py         # AkShare 数据获取器
│   │
│   ├── store/                     # 存储层
│   │   ├── __init__.py
│   │   ├── manager.py             # CacheManager 统一入口
│   │   ├── duckdb.py              # DuckDB 查询引擎
│   │   ├── parquet.py             # Parquet 原子写入
│   │   ├── memory.py              # 内存 TTL/LRU 缓存
│   │   ├── fetcher.py             # CachedFetcher 缓存执行器
│   │   ├── validator.py           # 数据验证
│   │   ├── aggregator.py          # 数据聚合器
│   │   ├── missing_ranges.py      # 缺失区间检测
│   │   └── strategies/            # 缓存策略
│   │
│   └── offline/                   # 离线工具
│       ├── __init__.py            # 懒加载入口（45 个导出名）
│       ├── core/                  # 基础设施
│       │   ├── paths.py           # 路径管理
│       │   ├── config_loader.py   # 配置加载
│       │   ├── errors.py          # 离线工具错误
│       │   └── retry.py           # 重试装饰器
│       ├── scanner/               # AkShare 接口扫描
│       ├── registry/              # 注册表管理
│       ├── prober/                # 接口健康探测
│       ├── downloader/            # 批量数据下载
│       ├── analyzer/              # 数据分析
│       │   ├── access_log/        # 访问日志 & 统计
│       │   ├── cache_analysis/    # 缓存完整性 & 异常检测
│       │   └── interface_analysis/ # 接口分析
│       ├── scheduler/             # 定时任务调度
│       ├── source_manager/        # 数据源健康管理
│       ├── report/                # 报告生成
│       └── cli/                   # 命令行入口
│
├── examples/                      # 示例代码
│   ├── __init__.py
│   ├── example_daily.py           # 日线数据示例
│   ├── example_minute.py          # 分钟线数据示例
│   ├── example_index.py           # 指数数据示例
│   ├── example_etf.py             # ETF 数据示例
│   ├── example_finance_indicator.py  # 财务指标示例
│   ├── example_money_flow.py      # 资金流向示例
│   ├── example_north_money_flow.py   # 北向资金示例
│   ├── example_margin.py          # 融资融券示例
│   ├── example_block_deal.py      # 大宗交易示例
│   ├── example_dragon_tiger.py    # 龙虎榜示例
│   ├── example_limit_pool.py      # 涨跌停池示例
│   ├── example_macro.py           # 宏观经济示例
│   ├── example_call_auction.py    # 集合竞价示例
│   ├── example_convertible_bond.py   # 可转债示例
│   ├── example_fund.py            # 基金示例
│   ├── example_industry.py        # 行业板块示例
│   ├── example_index_stocks.py    # 指数成分股示例
│   ├── example_realtime.py        # 实时行情示例
│   ├── example_security_info.py   # 证券信息示例
│   ├── example_securities_list.py # 证券列表示例
│   ├── example_sector_fund.py     # 板块资金示例
│   ├── example_st_stocks.py       # ST 股票示例
│   ├── example_suspended_stocks.py   # 停牌股票示例
│   ├── example_trading_days.py    # 交易日示例
│   └── example_futures.py         # 期货示例
│
├── tests/                         # 测试代码
│   ├── test_api.py
│   ├── test_api_basic.py
│   ├── test_api_comprehensive.py
│   ├── test_core_base.py
│   ├── test_core_config.py
│   ├── test_core_errors.py
│   ├── test_core_fields.py
│   ├── test_core_logging.py
│   ├── test_core_metrics.py
│   ├── test_core_normalize.py
│   ├── test_core_schema.py
│   ├── test_core_symbols.py
│   ├── test_fetcher.py
│   ├── test_offline.py
│   ├── test_offline_downloader.py
│   ├── test_offline_prober.py
│   ├── test_offline_quality.py
│   ├── test_offline_reporter.py
│   ├── test_sources_akshare.py
│   ├── test_sources_lixinger.py
│   ├── test_sources_lixinger_client.py
│   ├── test_sources_mock.py
│   ├── test_sources_router.py
│   ├── test_sources_tushare.py
│   ├── test_store.py
│   ├── test_store_duckdb.py
│   ├── test_store_manager.py
│   ├── test_store_memory.py
│   └── test_store_parquet.py
│
└── docs/                          # 文档
    ├── 01-overview.md
    ├── DATA_SOURCE_REDESIGN.md
    └── 11-migration-guide.md      # 本文档
```

### 9.2 关键文件说明

| 文件 | 说明 |
|------|------|
| `src/akshare_data/__init__.py` | 公共 API 入口，导出所有函数和类 |
| `src/akshare_data/api.py` | DataService 核心类，编排缓存优先策略 |
| `src/akshare_data/core/config.py` | CacheConfig 和 TableConfig 配置类 |
| `src/akshare_data/core/errors.py` | ErrorCode 枚举和异常类层次结构 |
| `src/akshare_data/core/symbols.py` | 证券代码格式标准化（normalize_symbol） |
| `src/akshare_data/core/base.py` | DataSource 抽象基类 |
| `src/akshare_data/sources/router.py` | 多源路由和熔断器 |
| `src/akshare_data/store/manager.py` | CacheManager 单例，统一读写入口 |
| `src/akshare_data/store/fetcher.py` | CachedFetcher + FetchConfig |
| `src/akshare_data/store/strategies/` | 缓存策略（FullCacheStrategy / IncrementalStrategy） |
| `src/akshare_data/offline/downloader/downloader.py` | BatchDownloader 批量下载器 |
| `src/akshare_data/offline/prober/prober.py` | APIProber 接口探测器 |
| `src/akshare_data/offline/analyzer/cache_analysis/completeness.py` | DataQualityChecker 数据质量检查 |
| `src/akshare_data/offline/report/renderer.py` | ReportRenderer 报告生成器 |

---

## 10. 示例文件参考

`examples/` 目录包含所有主要 API 的使用示例，可直接运行学习：

| 文件 | 说明 | 主要接口 |
|------|------|----------|
| `example_daily.py` | 股票日线数据获取 | `get_daily()` - 多种复权类型、代码格式、数据分析 |
| `example_minute.py` | 股票分钟线数据获取 | `get_minute()` - 1/5/15/30/60 分钟线 |
| `example_index.py` | 指数日线数据获取 | `get_index()` - 主要指数对比、年度涨跌幅 |
| `example_etf.py` | ETF 日线数据获取 | `get_etf()` - 多只 ETF 对比、统计分析 |
| `example_finance_indicator.py` | 财务指标数据获取 | `get_finance_indicator()` - PE/PB/PS/ROE |
| `example_money_flow.py` | 个股资金流向数据获取 | `get_money_flow()` - 主力/超大/大/中/小单 |
| `example_north_money_flow.py` | 北向资金流向数据获取 | `get_north_money_flow()` - 净流入分析、趋势分析 |
| `example_margin.py` | 融资融券数据获取 | `get_margin_data()` |
| `example_block_deal.py` | 大宗交易数据获取 | `get_block_deal()` |
| `example_dragon_tiger.py` | 龙虎榜数据获取 | `get_dragon_tiger_list()` |
| `example_limit_pool.py` | 涨跌停池数据获取 | `get_limit_up_pool()`, `get_limit_down_pool()` |
| `example_macro.py` | 宏观经济数据获取 | `get_lpr_rate()`, `get_pmi_index()`, `get_cpi_data()`, `get_ppi_data()`, `get_m2_supply()` |
| `example_call_auction.py` | 集合竞价数据获取 | `get_call_auction()` |
| `example_convertible_bond.py` | 可转债数据获取 | 可转债相关接口 |
| `example_fund.py` | 基金数据获取 | 基金相关接口 |
| `example_industry.py` | 行业板块数据获取 | `get_industry_stocks()`, `get_industry_mapping()`, `get_industry_list()` |
| `example_index_stocks.py` | 指数成分股数据获取 | `get_index_stocks()`, `get_index_components()` |
| `example_realtime.py` | 实时行情数据获取 | `get_realtime_data()` |
| `example_security_info.py` | 证券基本信息获取 | `get_security_info()`, `get_basic_info()` |
| `example_securities_list.py` | 证券列表数据获取 | `get_securities_list()` |
| `example_sector_fund.py` | 板块资金流向数据获取 | 板块资金相关接口 |
| `example_st_stocks.py` | ST 股票列表获取 | `get_st_stocks()` |
| `example_suspended_stocks.py` | 停牌股票列表获取 | `get_suspended_stocks()` |
| `example_trading_days.py` | 交易日历数据获取 | `get_trading_days()` |
| `example_futures.py` | 期货数据获取 | 期货相关接口 |

### 运行示例

```bash
# 运行单个示例
python examples/example_daily.py
python examples/example_minute.py
python examples/example_index.py
python examples/example_etf.py
python examples/example_finance_indicator.py
python examples/example_money_flow.py
python examples/example_north_money_flow.py
python examples/example_macro.py

# 运行所有示例（需要网络连接）
for f in examples/example_*.py; do
    echo "Running $f..."
    python "$f"
done
```

---

## 附录：DataService 方法列表

以下方法均可通过 `service.method_name()` 或模块级 `get_method_name()` 调用。

### 基础行情

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_daily(symbol, start_date, end_date, adjust)` | symbol, 起止日期, 复权类型 | DataFrame |
| `get_minute(symbol, freq, start_date, end_date)` | symbol, 频率, 起止日期 | DataFrame |
| `get_index(index_code, start_date, end_date)` | 指数代码, 起止日期 | DataFrame |
| `get_etf(symbol, start_date, end_date)` | ETF代码, 起止日期 | DataFrame |
| `get_realtime_data(symbol)` | symbol | DataFrame |
| `get_call_auction(symbol, date)` | symbol, 日期 | DataFrame |

### 指数相关

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_index_stocks(index_code)` | 指数代码 | List[str] |
| `get_index_components(index_code, include_weights)` | 指数代码, 是否含权重 | DataFrame |
| `get_index_valuation(index_code)` | 指数代码 | DataFrame |

### 证券列表

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_securities_list(security_type, date)` | 类型, 日期 | DataFrame |
| `get_security_info(symbol)` | symbol | Dict |
| `get_trading_days(start_date, end_date)` | 起止日期 | List[str] |
| `get_suspended_stocks()` | 无 | DataFrame |
| `get_st_stocks()` | 无 | DataFrame |

### 行业/概念

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_industry_stocks(industry_code, level)` | 行业代码, 级别 | List[str] |
| `get_industry_mapping(symbol, level)` | symbol, 级别 | str |
| `get_concept_list()` | 无 | DataFrame |
| `get_concept_stocks(concept_code)` | 概念代码 | List[str] |
| `get_stock_concepts(symbol)` | symbol | List[str] |
| `get_sw_industry_list()` | 无 | DataFrame |
| `get_sw_industry_daily(index_code, start_date, end_date)` | 行业代码, 起止日期 | DataFrame |

### 资金流向

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_money_flow(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_north_money_flow(start_date, end_date)` | 起止日期 | DataFrame |
| `get_northbound_holdings(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |

### 财务数据

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_finance_indicator(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_balance_sheet(symbol)` | symbol | DataFrame |
| `get_income_statement(symbol)` | symbol | DataFrame |
| `get_cash_flow(symbol)` | symbol | DataFrame |
| `get_financial_metrics(symbol)` | symbol | DataFrame |
| `get_stock_valuation(symbol)` | symbol | DataFrame |

### 股东数据

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_shareholder_changes(symbol)` | symbol | DataFrame |
| `get_top_shareholders(symbol)` | symbol | DataFrame |
| `get_institution_holdings(symbol)` | symbol | DataFrame |
| `get_latest_holder_number(symbol)` | symbol | DataFrame |
| `get_insider_trading(symbol)` | symbol | DataFrame |
| `get_equity_freeze(symbol)` | symbol | DataFrame |
| `get_capital_change(symbol)` | symbol | DataFrame |

### 事件数据

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_dividend_data(symbol)` | symbol | DataFrame |
| `get_restricted_release(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_restricted_release_detail(start_date, end_date)` | 起止日期 | DataFrame |
| `get_restricted_release_calendar(start_date, end_date)` | 起止日期 | DataFrame |
| `get_equity_pledge(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_equity_pledge_rank(date, top_n)` | 日期, 排名数 | DataFrame |

### 龙虎榜/大宗交易/融资融券

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_dragon_tiger_list(date)` | 日期 | DataFrame |
| `get_block_deal(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_margin_data(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |

### 宏观数据

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_shibor_rate(start_date, end_date)` | 起止日期 | DataFrame |
| `get_social_financing(start_date, end_date)` | 起止日期 | DataFrame |
| `get_macro_gdp(start_date, end_date)` | 起止日期 | DataFrame |
| `get_macro_exchange_rate(start_date, end_date)` | 起止日期 | DataFrame |

> 更多宏观数据（LPR/PMI/CPI/PPI/M2）通过命名空间访问：`service.macro.china.interest_rate(...)` 等

### 基金/ETF/LOF

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_fund_open_daily()` | 无 | DataFrame |
| `get_fund_open_nav(fund_code, start_date, end_date)` | 基金代码, 起止日期 | DataFrame |
| `get_fund_open_info(fund_code)` | 基金代码 | Dict |
| `get_lof_spot()` | 无 | DataFrame |
| `get_lof_nav()` | 无 | DataFrame |
| `get_fof_list()` | 无 | DataFrame |
| `get_fof_nav(fund_code)` | 基金代码 | DataFrame |

### 可转债

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_convert_bond_premium()` | 无 | DataFrame |
| `get_convert_bond_spot()` | 无 | DataFrame |
| `get_conversion_bond_list()` | 无 | DataFrame |
| `get_conversion_bond_daily()` | 无 | DataFrame |

### 期货

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_futures_daily()` | 无 | DataFrame |
| `get_futures_spot()` | 无 | DataFrame |

### 期权

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_option_list(symbol)` | 标的代码 | DataFrame |
| `get_option_daily(option_code, start_date, end_date)` | 期权代码, 起止日期 | DataFrame |

### 港美股/IPO

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_hk_stocks()` | 无 | DataFrame |
| `get_us_stocks()` | 无 | DataFrame |
| `get_new_stocks()` | 无 | DataFrame |
| `get_ipo_info()` | 无 | DataFrame |

### 其他

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_hot_rank()` | 无 | DataFrame |
| `get_earnings_forecast(symbol)` | symbol | DataFrame |
| `get_performance_forecast(symbol)` | symbol | DataFrame |
| `get_performance_express(symbol)` | symbol | DataFrame |
| `get_analyst_rank(symbol)` | symbol | DataFrame |
| `get_research_report(symbol)` | symbol | DataFrame |
| `get_chip_distribution(symbol)` | symbol | DataFrame |
| `get_stock_bonus(symbol)` | symbol | DataFrame |
| `get_rights_issue(symbol)` | symbol | DataFrame |
| `get_dividend_by_date(date)` | 日期 | DataFrame |
| `get_management_info(symbol)` | symbol | DataFrame |
| `get_name_history(symbol)` | symbol | DataFrame |
| `get_goodwill_data(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_goodwill_impairment(date)` | 日期 | DataFrame |
| `get_goodwill_by_industry(date)` | 日期 | DataFrame |
| `get_repurchase_data(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_esg_rating(symbol)` | symbol | DataFrame |
| `get_esg_rank(date)` | 日期 | DataFrame |
| `get_industry_performance()` | 无 | DataFrame |
| `get_concept_performance()` | 无 | DataFrame |
| `get_stock_industry(symbol)` | symbol | DataFrame |
| `get_spot_em()` | 无 | DataFrame |
| `get_stock_hist(symbol, start_date, end_date)` | symbol, 起止日期 | DataFrame |
| `get_lof_daily()` | 无 | DataFrame |
