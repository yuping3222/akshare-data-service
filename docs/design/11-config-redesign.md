# 配置文件重构设计文档

## 1. 现状分析

### 1.1 文件大小统计

| 文件 | 行数 | 大小 | 问题 |
|------|------|------|------|
| `akshare_registry.yaml` | 27,349 | 651K | **太大**，包含 1074 个接口定义 |
| `akshare_registry_backup.yaml` | 27,933 | 664K | 备份文件，不应在 config/ 根目录 |
| `interfaces/equity.yaml` | 233 | 6.2K | 合理 |
| `interfaces/index.yaml` | 76 | 1.8K | 合理 |
| `interfaces/macro.yaml` | 74 | 1.5K | 合理 |
| `interfaces/fund.yaml` | 53 | 1.3K | 合理 |
| `interfaces/futures.yaml` | 52 | 1.3K | 合理 |
| `interfaces/options.yaml` | 43 | 1.3K | 合理 |
| `interfaces/bond.yaml` | 29 | 751B | 合理 |
| `rate_limits.yaml` | 34 | 748B | 合理 |
| `health_state.json` | 0 | 2B | 空文件 |

### 1.2 akshare_registry.yaml 内部结构

| 区块 | 行数 | 内容 |
|------|------|------|
| `interfaces:` | 25,124 | 1074 个接口定义（按 category 分布） |
| `domains:` | 2,181 | 域名 -> 接口映射关系 |
| `rate_limits:` | 43 | 限流配置 |

### 1.3 接口按类别分布

| Category | 数量 | 占比 |
|----------|------|------|
| equity | 412 | 38.4% |
| macro | 227 | 21.1% |
| other | 95 | 8.8% |
| fund | 89 | 8.3% |
| index | 77 | 7.2% |
| futures | 67 | 6.2% |
| options | 44 | 4.1% |
| bond | 35 | 3.3% |
| market | 26 | 2.4% |
| 其他 | 2 | 0.2% |

---

## 2. 重构目标

1. **单文件不超过 5000 行** - 便于人工审查和 Git diff
2. **按功能模块拆分** - 职责清晰，独立维护
3. **静态配置与运行时数据分离** - config/ 只放静态配置
4. **保留离线生成能力** - 生成器输出到子目录，人工确认后合并

---

## 3. 新目录结构设计

```
config/
│
├── system.yaml                    # 系统配置（新增）
├── schemas.yaml                   # 缓存表 Schema（新增，从 schema.py 外置）
├── rate_limits.yaml               # 限流配置（保留，34 行合理）
├── health_state.json              # 数据源健康状态（保留，运行时数据）
│
├── interfaces/                    # 接口定义（按类别拆分）
│   ├── equity.yaml                # 412 个接口
│   ├── macro.yaml                 # 227 个接口
│   ├── fund.yaml                  # 89 个接口
│   ├── index.yaml                 # 77 个接口
│   ├── futures.yaml               # 67 个接口
│   ├── options.yaml               # 44 个接口
│   ├── bond.yaml                  # 35 个接口
│   ├── market.yaml                # 26 个接口
│   └── other.yaml                 # 97 个接口（other + meta + corporate）
│
├── domains/                       # 域名映射（从 registry 拆分）
│   ├── by_domain.yaml             # 域名 -> 接口列表映射
│   └── by_interface.yaml          # 接口 -> 域名列表映射（可选，反向索引）
│
└── generated/                     # 离线工具生成的中间文件
    ├── registry_raw/              # config_generator.py 原始输出
    │   └── akshare_registry_YYYYMMDD_HHMMSS.yaml
    ├── health_samples/            # prober.py 采样数据
    ├── field_mappings/            # field_mapper.py 输出
    └── reports/                   # 各类分析报告
```

---

## 4. 拆分策略

### 4.1 interfaces/ 目录

**原则**: 按 category 拆分，每个文件 500-2500 行

```yaml
# config/interfaces/equity.yaml (示例结构)
# 412 个接口，预计 ~10000 行
version: '1.0'
category: equity
description: "股票相关接口"
interfaces:
  stock_zh_a_hist:
    name: stock_zh_a_hist
    description: "..."
    signature: [symbol, period, start_date, end_date, adjust]
    domains: [push2his.eastmoney.com]
    rate_limit_key: em_push2his
    sources:
      - name: akshare_em
        func: stock_zh_a_hist
        enabled: true
        input_mapping: {...}
        output_mapping: {...}
    probe:
      params: {...}
      skip: false
      check_interval: 2592000
  # ... 其他 411 个接口
```

### 4.2 domains/ 目录

**原则**: 域名映射独立，便于限流和故障排查

```yaml
# config/domains/by_domain.yaml
version: '1.0'
domains:
  push2his.eastmoney.com:
    rate_limit_key: em_push2his
    interfaces:
      - stock_zh_a_hist
      - stock_zh_a_hist_min_em
      - ...
  www.zq12369.com:
    rate_limit_key: default
    interfaces:
      - air_city_table
      - air_quality_hist
      - ...
```

### 4.3 system.yaml（新增）

```yaml
# config/system.yaml
version: '1.0'

# 缓存配置
cache:
  base_dir: "./cache"
  memory:
    max_items: 1000
    default_ttl_seconds: 300
  duckdb:
    threads: 4
    memory_limit: "2GB"
  parquet:
    compression: snappy
    row_group_size: 100000

# 日志配置
logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# 数据源配置
sources:
  default_sources:
    - lixinger
    - akshare
    - tushare
  max_retries: 3
  retry_delay: 2.0
  timeout: 30
```

### 4.4 schemas.yaml（新增，从 schema.py 外置）

```yaml
# config/schemas.yaml
version: '1.0'
tables:
  stock_daily:
    partition_by: symbol
    ttl_hours: 0
    primary_key: [symbol, date]
    aggregation_enabled: true
    compaction_threshold: 20
    priority: P0
    storage_layer: daily
    schema:
      symbol: string
      date: date
      open: float
      high: float
      low: float
      close: float
      volume: float
  # ... 其他 62 张表
```

---

## 5. 离线生成流程

### 5.1 生成器输出路径

```
config_generator.py  -->  config/generated/registry_raw/akshare_registry_YYYYMMDD_HHMMSS.yaml
prober.py           -->  config/generated/health_samples/*.csv
field_mapper.py     -->  config/generated/field_mappings/*.json
```

### 5.2 人工确认流程

```
1. 运行 config_generator.py
   └─> 输出到 config/generated/registry_raw/akshare_registry_20260420_100538.yaml

2. 运行 split_registry.py（新增脚本）
   └─> 拆分为：
       - config/interfaces/*.yaml (按 category)
       - config/domains/by_domain.yaml

3. 人工审查 diff
   └─> git diff config/interfaces/ config/domains/

4. 确认后应用到线上
   └─> 删除时间戳，保留正式配置文件
```

### 5.3 split_registry.py 脚本功能

```python
"""将原始注册表拆分为 interfaces/ 和 domains/ 子文件"""

def split_registry(input_path: str, output_dir: str):
    """
    输入: config/generated/registry_raw/akshare_registry_YYYYMMDD_HHMMSS.yaml
    输出:
      - config/interfaces/{category}.yaml
      - config/domains/by_domain.yaml
      - config/domains/by_interface.yaml (可选)
    """
    # 1. 读取原始 YAML
    # 2. 按 category 分组 interfaces
    # 3. 写入 config/interfaces/{category}.yaml
    # 4. 提取 domains 映射，写入 config/domains/by_domain.yaml
    # 5. 保留 rate_limits 到 config/rate_limits.yaml（如与现有冲突则合并）
```

---

## 6. 代码适配

### 6.1 加载器变更

```python
# 原: 加载单个大文件
registry = load_yaml("config/akshare_registry.yaml")

# 新: 按需加载
interfaces = load_interfaces_by_category("config/interfaces/")
domains = load_yaml("config/domains/by_domain.yaml")
rate_limits = load_yaml("config/rate_limits.yaml")
```

### 6.2 Schema 加载器

```python
# 原: 硬编码在 schema.py
STOCK_DAILY = CacheTable(name="stock_daily", ...)

# 新: 从 YAML 加载
schemas = load_yaml("config/schemas.yaml")
SCHEMA_REGISTRY = TableRegistry.from_yaml(schemas)
```

### 6.3 系统配置加载器

```python
# 原: CacheConfig 数据类 + 环境变量
config = CacheConfig.from_env()

# 新: 从 system.yaml 加载
config = SystemConfig.from_yaml("config/system.yaml")
# 环境变量仍可覆盖
config = config.with_env_overrides()
```

---

## 7. 迁移步骤

| 步骤 | 操作 | 影响 |
|------|------|------|
| 1 | 创建新目录结构 | 无 |
| 2 | 编写 split_registry.py | 无 |
| 3 | 运行拆分脚本，生成 interfaces/ 和 domains/ | 无 |
| 4 | 编写 schemas.yaml（从 schema.py 提取） | 无 |
| 5 | 编写 system.yaml | 无 |
| 6 | 修改加载器代码，支持新结构 | 需测试 |
| 7 | 保留旧加载器作为 fallback | 向后兼容 |
| 8 | 全量测试 | 需验证 |
| 9 | 删除 akshare_registry.yaml 和旧备份 | 清理 |

---

## 8. 文件行数预估

| 文件 | 预估行数 | 说明 |
|------|----------|------|
| interfaces/equity.yaml | ~10,000 | 412 个接口 |
| interfaces/macro.yaml | ~5,500 | 227 个接口 |
| interfaces/fund.yaml | ~2,200 | 89 个接口 |
| interfaces/index.yaml | ~1,900 | 77 个接口 |
| interfaces/futures.yaml | ~1,700 | 67 个接口 |
| interfaces/options.yaml | ~1,100 | 44 个接口 |
| interfaces/bond.yaml | ~900 | 35 个接口 |
| interfaces/market.yaml | ~650 | 26 个接口 |
| interfaces/other.yaml | ~2,400 | 97 个接口 |
| domains/by_domain.yaml | ~2,200 | 域名映射 |
| schemas.yaml | ~1,500 | 63 张表 |
| system.yaml | ~50 | 系统配置 |
| rate_limits.yaml | ~34 | 限流（不变） |

**最大文件 10,000 行**，可接受。如 equity 仍太大，可进一步拆分为 `equity/daily.yaml`、`equity/minute.yaml` 等。
