# AkShare Data Service

**Cache-First 统一金融数据服务**，整合 Lixinger + AkShare 多数据源，提供股票、ETF、指数、宏观等数据的获取、缓存与查询能力。

---

## 快速开始

```bash
pip install -e .
```

```python
from akshare_data import get_daily, DataService

# 便捷函数（自动缓存）
df = get_daily("000001", "2024-01-01", "2024-12-31")

# 命名空间 API
service = DataService()
df = service.cn.stock.quote.daily("600519", "2024-01-01", "2024-12-31")
df = service.cn.index.quote.daily("000300", "2024-01-01", "2024-12-31")
df = service.cn.fund.quote.daily("510300", "2024-01-01", "2024-12-31")
df = service.macro.china.gdp("2024-01-01", "2024-12-31")
```

---

## 核心特性

- **Cache-First 策略** — 内存 → Parquet → DuckDB 三级缓存，自动增量更新
- **多数据源** — Lixinger（主）+ AkShare（备），自动故障转移与熔断
- **统一 API** — 95+ 便捷函数 + 命名空间分类访问
- **69 张缓存表** — 覆盖日线、分钟线、财务、资金、宏观等全品类
- **离线工具** — 批量下载、接口探测、数据质量检查、CLI 命令行

---

## 文档导航

| 文档 | 说明 |
|------|------|
| [01-项目概览](docs/01-overview.md) | 架构设计、目录结构、缓存表分类 |
| [02-API 参考](docs/02-api-reference.md) | 完整 API 列表与使用方式 |
| [03-数据源](docs/03-data-sources.md) | Lixinger/AkShare 适配器、多源路由、熔断器 |
| [04-存储层](docs/04-storage-layer.md) | CacheManager、DuckDB、Parquet、内存缓存、增量引擎 |
| [05-核心模块](docs/05-core-modules.md) | Config、Schema Registry、Symbols、Normalize、Fields、Logging |
| [06-离线工具](docs/06-offline-tools.md) | BatchDownloader、APIProber、DataQualityChecker |
| [07-Schema 注册表](docs/07-schema-registry.md) | 69 张缓存表的完整 Schema |
| [08-缓存策略](docs/08-cache-strategy.md) | Cache-First 流程、增量更新、自适应刷新 |
| [09-错误处理](docs/09-error-handling.md) | 177 错误码、异常层次体系 |
| [10-迁移指南](docs/10-migration-guide.md) | 安装、快速开始、从 jk2bt/AkShare 迁移 |
| [CLI 参考](docs/CLI_REFERENCE.md) | 离线工具命令行用法 |

---

## 项目结构

```
src/akshare_data/
├── __init__.py          # 入口，__getattr__ 动态转发到 DataService
├── api.py               # DataService + 命名空间 API 类（95+ 方法）
├── core/                # 核心模块（15 个文件）
│   ├── base.py          # DataSource 抽象基类
│   ├── config.py        # CacheConfig / TableConfig
│   ├── config_cache.py  # 配置缓存机制
│   ├── config_dir.py    # 配置目录发现
│   ├── errors.py        # 177 错误码 + 异常层次
│   ├── fields.py        # 字段映射（CN→EN，多源）
│   ├── logging.py       # 结构化日志 + StatsCollector
│   ├── normalize.py     # DataFrame 标准化
│   ├── options.py       # 期权相关工具
│   ├── schema.py        # Schema 注册表（69 张表）
│   ├── stats.py         # 轻量指标收集
│   ├── symbols.py       # 代码格式转换
│   └── tokens.py        # Token 管理
├── sources/             # 数据源层（8 个文件 + akshare/ 子包）
│   ├── lixinger_source.py   # Lixinger 适配器（主）
│   ├── lixinger_client.py   # Lixinger HTTP 客户端
│   ├── akshare_source.py    # AkShare 适配器（配置驱动）
│   ├── akshare/fetcher.py   # 配置驱动的数据获取
│   ├── router.py            # 多源路由 + 熔断器 + 限速
│   ├── tushare_source.py    # Tushare 适配器（可选）
│   └── mock.py              # Mock 数据源
├── store/               # 存储层（10 个文件 + strategies/ 子包）
│   ├── manager.py           # CacheManager 统一入口
│   ├── memory.py            # TTL 内存缓存
│   ├── parquet.py           # Parquet 原子写入 + 分区
│   ├── duckdb.py            # DuckDB SQL 查询引擎
│   ├── incremental.py       # 增量缓存引擎
│   ├── fetcher.py           # CachedFetcher
│   ├── validator.py         # 数据验证
│   ├── aggregator.py        # 数据聚合器
│   ├── missing_ranges.py    # 缺失区间检测
│   └── strategies/          # 存储策略实现
└── offline/             # 离线工具（15 个目录，54 个文件）
    ├── core/                  # 基础设施（路径、配置、重试）
    ├── downloader/            # 批量下载器
    ├── prober/                # 接口探测器
    ├── analyzer/              # 缓存分析、访问日志分析
    ├── registry/              # 接口注册表管理
    ├── report/                # 报告生成
    ├── scanner/               # AkShare 接口扫描
    ├── scheduler/             # 下载调度器
    ├── source_manager/        # 数据源健康管理
    └── cli/                   # CLI 入口
```

---

## 配置

通过环境变量覆盖默认配置：

| 环境变量 | 说明 | 默认值 |
|----------|------|--------|
| `AKSHARE_DATA_CACHE_DIR` | 缓存根目录 | `./cache` |
| `AKSHARE_DATA_CACHE_MAX_ITEMS` | 内存缓存最大条目 | `5000` |
| `AKSHARE_DATA_CACHE_TTL_SECONDS` | 内存缓存 TTL | `3600` |
| `AKSHARE_DATA_CACHE_DUCKDB_THREADS` | DuckDB 线程数 | `4` |
| `AKSHARE_DATA_CACHE_DUCKDB_MEMORY_LIMIT` | DuckDB 内存限制 | `4GB` |
| `LIXINGER_TOKEN` | Lixinger API Token | 无 |

---

## 版本

- **当前版本**: 0.2.0
- **Python**: >= 3.10
- **License**: MIT
