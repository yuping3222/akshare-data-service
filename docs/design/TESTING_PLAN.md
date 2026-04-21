# 系统测试与集成测试设计方案

## 现状概述

项目已有 48 个测试文件、约 24K 行测试代码，覆盖了核心模块的单元测试。但存在以下关键缺口：

- **无集成测试**：所有测试都使用 mock，没有验证全链路（DataService → Router → Store → DataSource）协作
- **无 conftest.py**：每个测试文件重复定义 fixture，缺少共享测试基础设施
- **无 pytest 配置**：没有 marker 分类、无覆盖率阈值、无测试路径配置
- **缺少模块测试**：`core/tokens.py`、`store/strategies/` 增量策略等无专门测试
- **API 方法覆盖不全**：DataService 有 130+ 方法，部分冷门方法未覆盖
- **pytest-asyncio 未使用**：作为 dev 依赖但无任何 async test

---

## 一、集成测试设计

### 1.1 测试策略

采用 **分层金字塔** 策略，在现有单元测试基础上增加中间层的集成测试：

```
        ┌─────────┐
        │ E2E/系统 │  ← 少量关键路径
       ┌┴──────────┴┐
       │  集成测试   │  ← 模块间协作（本次重点）
      ┌┴────────────┴┐
      │   单元测试    │  ← 已有，保持并补充
      └──────────────┘
```

### 1.2 集成测试范围

| 集成路径 | 测试内容 | Mock 程度 |
|---|---|---|
| DataService → CacheManager → MemoryCache → Parquet → DuckDB | 缓存三级读写链路 | 仅 mock 外部数据源 |
| DataService → MultiSourceRouter → Source → Store | 路由+故障转移+缓存回写 | mock HTTP 响应（使用 responses/httpx mock） |
| DataSource → Schema Registry → Validator → Writer | 数据写入全链路 | 不 mock，使用真实 parquet/duckdb |
| Namespace API → DataService → 底层链路 | API 命名空间代理正确性 | 仅 mock 数据源返回 |

### 1.3 新增测试文件

```
tests/integration/
├── __init__.py
├── conftest.py              # 集成测试共享 fixture
├── test_cache_pipeline.py   # 缓存三级读写集成测试
├── test_router_failover.py  # 多源路由+故障转移集成测试
├── test_data_service.py     # DataService 全链路集成测试
├── test_namespace_api.py    # 命名空间 API 集成测试
├── test_write_pipeline.py   # 数据写入链路（schema→validate→parquet→duckdb）
└── test_query_engine.py     # DuckDB 查询引擎集成测试
```

### 1.4 关键测试用例

#### test_cache_pipeline.py
- 首次读取：memory miss → parquet miss → DuckDB miss → fetch → write parquet → write memory → return
- 二次读取：memory hit → return（不调用下层）
- 过期读取：memory expire → parquet hit → DuckDB query → return
- 全缓存未命中后回填：fetch 后 parquet 和 memory 均有数据

#### test_router_failover.py
- 主源成功：Lixinger 返回数据，不调用备用源
- 主源失败 → 备用源成功：Lixinger 抛异常 → AKShare 返回数据
- 全部源失败：所有源抛异常 → 正确聚合错误信息
- 熔断器触发：连续失败 N 次 → 源被熔断 → 请求跳过该源
- 熔断恢复：熔断间隔后 → 探测请求成功 → 源恢复

#### test_data_service.py
- `get_daily()` 完整链路：参数校验 → 缓存检查 → 数据获取 → 缓存回填 → 返回 DataFrame
- 字段映射：不同数据源返回不同字段名 → 统一为英文字段
- 符号转换：`sh600000` → `600000.XSHG` 等格式转换在链路中正确传递
- 增量更新：已有部分缓存 → 检测缺失区间 → 只获取缺失数据 → 合并

#### test_write_pipeline.py
- Schema 注册表：64 个缓存表的 schema 定义与实际写入一致
- 数据验证：非法数据被 Validator 拦截
- Parquet 原子写入：写入中断不会产生损坏文件
- 分区策略：按日期/代码分区正确

---

## 二、系统测试设计

### 2.1 测试策略

系统测试关注 **端到端关键业务路径**，使用最小 mock（仅外部 HTTP），验证整个库作为一个系统的行为。

### 2.2 新增测试文件

```
tests/system/
├── __init__.py
├── conftest.py              # 系统测试共享 fixture
├── test_stock_data_flow.py  # A 股数据端到端流程
├── test_index_data_flow.py  # 指数数据端到端流程
├── test_etf_data_flow.py    # ETF 数据端到端流程
├── test_macro_data_flow.py  # 宏观数据端到端流程
├── test_concurrent_access.py # 并发访问场景
├── test_error_scenarios.py  # 异常场景系统行为
└── test_docker_integration.py # Docker 部署集成测试
```

### 2.3 关键测试用例

#### test_stock_data_flow.py
- 完整日线查询：`DataService().cn.stock.quote.daily()` 从创建到返回数据的完整流程
- 分钟线查询：含数据量大、分区多的场景
- 财务数据查询：资产负债表/利润表/现金流量表

#### test_concurrent_access.py
- 多线程并发读取同一缓存：无数据竞争
- 多线程并发写入：Parquet 原子写入不损坏
- CacheManager 单例在多线程下的正确性

#### test_error_scenarios.py
- 所有数据源不可用时的降级行为
- 磁盘空间不足时的写入失败处理
- 畸形数据源返回时的验证和错误码
- 配置缺失/错误时的启动行为

#### test_docker_integration.py
- Docker 容器内可以 import 并执行基本查询
- docker-compose 启动后健康检查通过
- 卷挂载后缓存数据可持久化

---

## 三、测试基础设施改进

### 3.1 创建根级 conftest.py

```
tests/
├── conftest.py              # ← 新增：全局共享 fixture
├── integration/
│   └── conftest.py          # 集成测试专用 fixture
├── system/
│   └── conftest.py          # 系统测试专用 fixture
└── (existing test files)
```

根级 conftest.py 应提供：
- `temp_cache_dir`：创建临时缓存目录，测试后自动清理
- `data_service`：预配置的 DataService 实例
- `cache_manager`：独立的 CacheManager 实例
- `sample_stock_data`：标准测试数据生成 fixture
- `mock_lixinger` / `mock_akshare`：可复用的 HTTP mock

### 3.2 配置 pytest（pyproject.toml）

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "unit: 单元测试（无外部依赖）",
    "integration: 集成测试（模块间协作）",
    "system: 系统测试（端到端关键路径）",
    "slow: 耗时较长的测试（>5s）",
    "network: 需要真实网络访问的测试",
]
addopts = "-v --tb=short --strict-markers"
filterwarnings = [
    "ignore::DeprecationWarning",
    "ignore::PendingDeprecationWarning",
]
```

### 3.3 补充缺失的单元测试

```
tests/test_core_tokens.py        # ← 新增：tokens.py 测试
tests/test_store_strategies_incremental.py  # ← 新增：增量策略测试
```

---

## 四、CI 集成

### 4.1 更新 CI 工作流

在 `.github/workflows/ci.yml` 中增加：

1. **覆盖率门禁**：`--cov-fail-under=80`（或当前基线）
2. **标记分组**：CI 默认跳过 `slow` 和 `network` 标记
3. **集成测试 job**：独立运行 `pytest -m integration`
4. **Docker 内测试 job**：在构建的容器中运行测试

```yaml
# 新增 job
integration-test:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - run: pip install -e ".[all,dev]"
    - run: pytest -m integration -v --cov=akshare_data

coverage:
  runs-on: ubuntu-latest
  steps:
    - run: pytest --cov=akshare_data --cov-report=xml --cov-fail-under=80
```

### 4.2 Makefile 新增目标

```makefile
test-integration:
	python -m pytest tests/integration/ -v -m integration

test-system:
	python -m pytest tests/system/ -v -m system

test-all:
	python -m pytest tests/ -v --cov=akshare_data --cov-fail-under=80

test-unit:
	python -m pytest tests/ -v -m unit --ignore=tests/integration --ignore=tests/system
```

---

## 五、实施计划

### Phase 1：基础设施（1-2 天）

- [ ] 创建 `tests/conftest.py`，提取共享 fixture
- [ ] 在 `pyproject.toml` 中配置 pytest markers 和 addopts
- [ ] 创建 `tests/integration/` 和 `tests/system/` 目录
- [ ] 更新 `Makefile` 增加新测试目标
- [ ] 补充 `test_core_tokens.py`

### Phase 2：集成测试（3-5 天）

- [ ] `test_cache_pipeline.py` — 缓存三级读写链路
- [ ] `test_router_failover.py` — 多源路由+故障转移
- [ ] `test_data_service.py` — DataService 全链路
- [ ] `test_namespace_api.py` — 命名空间 API
- [ ] `test_write_pipeline.py` — 数据写入链路
- [ ] `test_query_engine.py` — DuckDB 查询引擎

### Phase 3：系统测试（2-3 天）

- [ ] `test_stock_data_flow.py` — A 股端到端
- [ ] `test_index_data_flow.py` — 指数端到端
- [ ] `test_etf_data_flow.py` — ETF 端到端
- [ ] `test_macro_data_flow.py` — 宏观端到端
- [ ] `test_concurrent_access.py` — 并发场景
- [ ] `test_error_scenarios.py` — 异常场景

### Phase 4：CI 与质量门禁（1 天）

- [ ] 更新 `.github/workflows/ci.yml`
- [ ] 设定覆盖率基线和门禁
- [ ] 添加 Docker 内测试 job
- [ ] 验证 CI 全绿色通过

---

## 六、Mock 策略规范

| 层级 | Mock 范围 | 不 Mock 范围 |
|---|---|---|
| 单元测试 | 所有外部依赖、网络、文件系统 | 被测函数本身的逻辑 |
| 集成测试 | 外部 HTTP 服务（用 `responses` 库 mock） | 模块间调用、文件系统、DuckDB |
| 系统测试 | 仅外部 HTTP（可选用 `responses` 或真实网络） | 整个内部链路 |

推荐使用 `responses` 库替代部分 `unittest.mock.patch`，因为它可以 mock 到 HTTP 层，允许真实的数据流通过。
