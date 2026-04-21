# 缓存策略配置化方案

## 1. 背景与目标

### 1.1 现状问题

当前 `api.py` 中约 44 个 `get_*` 方法绕过缓存直接调用 source，包括：
- 财务报表（balance_sheet、income_statement、cash_flow）
- 估值数据（stock_valuation、index_valuation）
- 股东数据（shareholder_changes、top_shareholders、institution_holdings）
- IPO/新股、基金、行业概念等

这些方法不走缓存的原因可能是：
1. 使用频率较低，开发时未实现
2. 遗漏（BUG）
3. 数据更新频繁或实时性要求高

### 1.2 目标

- **配置驱动**：API 层只依赖配置文件决定缓存行为，不硬编码
- **离线分析**：工具自动扫描代码特征，生成配置建议
- **人工校验**：配置可手工编辑，工具不修改业务代码
- **运行时生效**：API 启动时加载配置，动态决定缓存策略

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    离线分析工具                           │
│  cache_policy_analyzer.py                               │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │ AST 扫描     │→│ 特征分析      │→│ 生成配置建议    │ │
│  │ api.py 方法  │  │ 参数/返回值   │  │ cache_policy.yaml│ │
│  └─────────────┘  └──────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────┘
                            ↓ 人工编辑校验
┌─────────────────────────────────────────────────────────┐
│              配置文件 (config/cache_policy.yaml)          │
│  每个 get_* 方法一条策略：                                │
│  - cached: true/false                                   │
│  - table, storage_layer, partition_by, ttl_hours...     │
│  - reason: 人工备注                                      │
└─────────────────────────────────────────────────────────┘
                            ↓ 运行时加载
┌─────────────────────────────────────────────────────────┐
│                    API 运行时                             │
│  DataService                                             │
│  ┌──────────────────┐  ┌──────────────────────────────┐ │
│  │ CachePolicyLoader│→│ 方法执行时查询策略              │ │
│  │ 加载配置          │  │ cached=true → cached_fetch()  │ │
│  └──────────────────┘  │ cached=false→ source直调       │ │
│                        └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 3. 配置文件设计

### 3.1 文件位置

```
config/cache_policy.yaml
```

### 3.2 配置结构

```yaml
# 版本标识，用于未来格式升级
version: 1

# 全局默认值
defaults:
  storage_layer: "daily"
  ttl_hours: 0          # 0 = 永不过期
  compaction_threshold: 20
  aggregation_enabled: true

# 方法级策略
policies:
  # ── 已缓存的方法（示例） ──
  get_daily:
    cached: true
    table: "stock_daily"
    storage_layer: "daily"
    partition_by: "symbol"
    date_col: "date"
    ttl_hours: 0
    strategy: "incremental"
    reason: "核心行情数据，高频访问"

  get_index_components:
    cached: true
    table: "index_components"
    storage_layer: "meta"
    partition_by: "index_code"
    ttl_hours: 720
    strategy: "full"
    reason: "元数据，低频变更"

  # ── 绕过缓存的方法（示例） ──
  get_realtime_data:
    cached: false
    reason: "实时行情，缓存无意义"

  get_concept_stocks:
    cached: false
    reason: "返回 List[str]，非 DataFrame"

  # ── 待评估的方法（工具自动生成） ──
  get_balance_sheet:
    cached: null          # null = 待人工决定
    suggested:
      cached: true
      table: "balance_sheet"
      storage_layer: "daily"
      partition_by: "report_date"
      date_col: "report_date"
      ttl_hours: 2160
      strategy: "full"
    reason: "季度更新，数据量大，建议缓存"

  get_stock_valuation:
    cached: null
    suggested:
      cached: true
      table: "valuation"
      storage_layer: "daily"
      partition_by: "date"
      date_col: "date"
      ttl_hours: 0
      strategy: "full"
    reason: "日频更新，历史数据常用"
```

### 3.3 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cached` | `bool/null` | 是 | `true`=走缓存，`false`=直调 source，`null`=待决定 |
| `table` | `string` | 否 | 缓存表名（cached=true 时必填） |
| `storage_layer` | `string` | 否 | 存储层：daily/minute/snapshot/meta |
| `partition_by` | `string/null` | 否 | 分区字段名 |
| `date_col` | `string` | 否 | 日期列名，默认 "date" |
| `ttl_hours` | `int` | 否 | 过期时间（小时），0=永不过期 |
| `strategy` | `string` | 否 | 缓存策略：incremental/full |
| `reason` | `string` | 否 | 人工备注，说明缓存决策原因 |
| `suggested` | `object` | 否 | 工具生成的建议值（cached=null 时有效） |

## 4. 离线分析工具

### 4.1 工具位置

```
src/akshare_data/tools/cache_policy_analyzer.py
```

### 4.2 功能

#### 4.2.1 扫描（scan）

解析 `api.py` AST，提取所有 `get_*` 方法：
- 方法名、参数列表、返回类型注解
- 方法体中是否调用 `cached_fetch` / `cache.read`
- 方法体中是否直接调用 `self._get_source(source).get_xxx()`

#### 4.2.2 分析（analyze）

根据方法特征推断缓存建议：

| 特征 | 推断结果 |
|------|---------|
| 方法名含 `realtime`/`spot` | `cached: false` |
| 返回类型 `List[str]`/`Dict` | `cached: false` |
| 有 `start_date` + `end_date` 参数 | `strategy: incremental` |
| 只有 `symbol` 参数 | `strategy: full` |
| 无参数（全局数据） | `strategy: full`, `partition_by: null` |
| 方法名含 `rank`/`calendar` | `storage_layer: snapshot`, 短 TTL |
| 方法名含 `finance`/`balance`/`income`/`cash` | `storage_layer: daily`, `ttl_hours: 2160` |

#### 4.2.3 生成（generate）

输出 `cache_policy.yaml`：
- 已缓存的方法：从代码中提取现有配置
- 未缓存的方法：生成 `cached: null` + `suggested` 建议
- 保留人工编辑的 `reason` 字段

#### 4.2.4 差异检查（diff）

对比代码与配置：
- 代码中新增的方法 → 配置中缺失
- 配置中存在的方法 → 代码中已删除
- 配置 `cached: null` 的方法 → 待人工决定

### 4.3 使用方式

```bash
# 扫描并生成配置（不修改代码）
python -m akshare_data.tools.cache_policy_analyzer generate \
    --output config/cache_policy.yaml

# 检查代码与配置差异
python -m akshare_data.tools.cache_policy_analyzer diff \
    --config config/cache_policy.yaml

# 列出所有待人工决定的方法
python -m akshare_data.tools.cache_policy_analyzer pending \
    --config config/cache_policy.yaml
```

## 5. 运行时集成

### 5.1 CachePolicyLoader

```python
# src/akshare_data/core/cache_policy.py

@dataclass
class CachePolicy:
    """单个方法的缓存策略"""
    cached: bool | None
    table: str | None = None
    storage_layer: str = "daily"
    partition_by: str | None = None
    date_col: str = "date"
    ttl_hours: int = 0
    strategy: str = "full"       # incremental | full
    reason: str = ""

class CachePolicyLoader:
    """加载并解析 cache_policy.yaml"""

    def __init__(self, config_path: str, defaults: dict | None = None):
        self._policies: dict[str, CachePolicy] = {}
        self._load(config_path, defaults or {})

    def get(self, method_name: str) -> CachePolicy:
        """获取方法的缓存策略"""
        return self._policies.get(method_name, CachePolicy(cached=None))

    def list_pending(self) -> list[str]:
        """列出 cached=null 的方法名"""
        return [k for k, v in self._policies.items() if v.cached is None]
```

### 5.2 DataService 集成

```python
class DataService:
    def __init__(self, ..., policy_config: str | None = None):
        self.cache = cache_manager or get_cache_manager()
        self.fetcher = CachedFetcher(self.cache)

        # 加载缓存策略配置
        if policy_config:
            self.policy = CachePolicyLoader(policy_config)
        else:
            self.policy = CachePolicyLoader("config/cache_policy.yaml")

    def _execute_with_policy(self, method_name: str, fetch_fn, **params):
        """根据配置决定是否走缓存"""
        policy = self.policy.get(method_name)

        # cached=null 时降级为直调 source
        if policy.cached is not True:
            return fetch_fn()

        return self.cached_fetch(
            table=policy.table,
            storage_layer=policy.storage_layer,
            partition_by=policy.partition_by,
            date_col=policy.date_col,
            fetch_fn=fetch_fn,
            **params,
        )

    # ── 示例：财务报表方法 ──
    def get_balance_sheet(self, symbol, source=None):
        return self._execute_with_policy(
            "get_balance_sheet",
            fetch_fn=lambda: self._get_source(source).get_balance_sheet(symbol),
            symbol=symbol,
        )

    def get_income_statement(self, symbol, source=None):
        return self._execute_with_policy(
            "get_income_statement",
            fetch_fn=lambda: self._get_source(source).get_income_statement(symbol),
            symbol=symbol,
        )
```

### 5.3 统一装饰器（可选）

为减少重复代码，可使用装饰器模式：

```python
def cached(method_name: str):
    """装饰器：根据配置自动决定缓存行为"""
    def decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            policy = self.policy.get(method_name)
            if policy.cached is not True:
                return fn(self, *args, **kwargs)

            # 从参数中提取 symbol/start_date/end_date 等
            params = self._extract_params(fn, args, kwargs)
            return self.cached_fetch(
                table=policy.table,
                storage_layer=policy.storage_layer,
                partition_by=policy.partition_by,
                date_col=policy.date_col,
                fetch_fn=lambda: fn(self, *args, **kwargs),
                **params,
            )
        return wrapper
    return decorator

# 使用示例
class DataService:
    @cached("get_balance_sheet")
    def get_balance_sheet(self, symbol, source=None):
        return self._get_source(source).get_balance_sheet(symbol)
```

## 6. 工作流

### 6.1 初始化流程

```
1. 运行工具生成初始配置
   python -m akshare_data.tools.cache_policy_analyzer generate

2. 人工编辑 config/cache_policy.yaml
   - 将 cached: null 改为 true 或 false
   - 调整 table/partition_by/ttl_hours 等参数
   - 填写 reason 说明决策原因

3. 运行 diff 检查完整性
   python -m akshare_data.tools.cache_policy_analyzer diff

4. 启动服务，配置自动生效
```

### 6.2 日常维护流程

```
1. 代码新增 get_xxx 方法

2. 运行工具检测差异
   python -m akshare_data.tools.cache_policy_analyzer diff
   → 输出：新增方法 get_xxx，配置中缺失

3. 运行工具生成建议
   python -m akshare_data.tools.cache_policy_analyzer generate
   → 自动在配置中添加 get_xxx 的 suggested 条目

4. 人工编辑配置，设置 cached: true/false

5. 无需修改代码，重启服务即生效
```

### 6.3 配置校验流程

```
1. 运行 pending 检查未决项
   python -m akshare_data.tools.cache_policy_analyzer pending
   → 输出：以下方法 cached=null，待人工决定

2. 逐一评估每个方法：
   - 数据更新频率？
   - 访问频率？
   - 数据量大小？
   - 返回类型是否为 DataFrame？

3. 编辑配置，设置最终值
```

## 7. 评估规则参考

人工编辑 `cached: null` 时的决策参考：

| 评估维度 | 建议缓存 | 建议不缓存 |
|---------|---------|-----------|
| 数据更新频率 | 季度/年度/低频 | 实时/分钟级 |
| 访问频率 | 高频 | 极低频 |
| 数据量 | 大（>1000行） | 小（<10行） |
| 返回类型 | DataFrame | List/Dict/标量 |
| 历史数据价值 | 需要历史对比 | 只需当前值 |
| Source 响应速度 | 慢（>2s） | 快（<200ms） |

## 8. 文件清单

| 文件 | 说明 |
|------|------|
| `config/cache_policy.yaml` | 缓存策略配置文件（人工编辑） |
| `src/akshare_data/tools/cache_policy_analyzer.py` | 离线分析工具 |
| `src/akshare_data/core/cache_policy.py` | 运行时配置加载器 |
| `src/akshare_data/api.py` | API 层（依赖配置，不硬编码缓存逻辑） |

## 9. 优势

1. **单一真相源**：缓存策略集中在一个配置文件，易于审查和维护
2. **代码简洁**：API 方法无需重复编写缓存逻辑
3. **安全可控**：工具不改代码，人工最终决策
4. **可追溯**：每个方法的缓存决策都有 `reason` 说明
5. **可扩展**：未来可添加更多策略维度（如按环境、按用户分级）
