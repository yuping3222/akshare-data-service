# 数据源架构重新设计文档 v4

## 核心问题

### 问题 1: 限速粒度过粗

AkShare 是一个聚合了 **1093 个函数**的 Python 包，底层对接了数十个不同网站。当前限速按"数据源名称"（如 `akshare`、`eastmoney`）限制，但同一个名称下实际调用多个不同域名。

**示例：**
- `stock_zh_a_hist()` → `push2his.eastmoney.com`
- `stock_zh_a_spot_em()` → `push2.eastmoney.com`
- `stock_zh_a_daily()` → `vip.stock.finance.sina.com.cn`
- `stock_zh_a_spot()` → `hq.sinajs.cn`

这四个函数都"属于 akshare"，但底层是 3 个不同域名，限速应该分别控制。

### 问题 2: 同类接口不可切换

AkShare 内部对同一类数据有多个实现：

| 数据类型 | akshare 函数 | 底层来源 |
|---------|-------------|---------|
| A股日线 | `stock_zh_a_hist()` | 东方财富 |
| A股日线 | `stock_zh_a_daily()` | 新浪 |
| A股分钟线 | `stock_zh_a_hist_min_em()` | 东方财富 |
| 指数日线 | `index_zh_a_hist()` | 新浪 |
| 指数日线 | `index_hist_sw()` | 申万 |

当前架构中 `akshare` 被视为单一数据源，无法在这些接口之间切换。

### 问题 3: AkShareAdapter 代码臃肿

`akshare_source.py` 有 **1774 行、107 个方法**，其中 ~98% 都是同一种模式：

```python
def get_xxx(self, param1, param2, **kwargs) -> pd.DataFrame:
    from akshare_data.sources.akshare.fetcher import fetch_xxx
    if not self._akshare_available:
        raise DataSourceError("akshare 不可用", source=self.name)
    return fetch_xxx(self._akshare, param1, param2, **kwargs)
```

### 问题 4: 输入输出字段不统一

不同数据源对同一类数据返回的字段名不同：

| 数据源 | 日期字段 | 代码字段 | 开盘价 | 收盘价 |
|-------|---------|---------|-------|-------|
| 东方财富 | `date` | `code` | `open` | `close` |
| 新浪 | `day` | `symbol` | `open` | `close` |
| Tushare | `trade_date` | `ts_code` | `open` | `close` |
| 理杏仁 | `date` | `code` | `open` | `close` |

调用方需要知道每个数据源的字段名，无法统一处理。

## 设计原则

### 原则 1: 输入输出字段统一

**所有同类接口使用统一的输入参数名和输出列名。**

- 输入：`symbol`, `start_date`, `end_date`, `adjust` 等
- 输出：`date`, `open`, `high`, `low`, `close`, `volume` 等
- 数据源差异通过配置层的 `input_mapping` 和 `output_mapping` 屏蔽

### 原则 2: 配置驱动

**所有接口定义、限速、字段映射、数据源优先级都通过 YAML 配置。**

- 不写重复的 `import → check → call` 代码
- 新增接口只需改配置，不改代码
- 配置即文档

### 原则 3: 多数据源统一接口

**同类数据（如日线）只有一个接口定义，底层可绑定多个数据源。**

- 调用方只关心 `get_daily(symbol, start_date, end_date)`
- 不关心底层是东方财富、新浪还是 Tushare
- 数据源切换通过配置完成，对调用方透明

## 架构概览

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Online Layer (api.py)                          │
│                                                                       │
│  DataService:                                                         │
│  - Cache-First 策略                                                   │
│  - 所有数据获取委托给 OfflineEngine                                   │
│  - 所有计算逻辑在此层实现（Greeks、BS定价、隐含波动率等）               │
│  - 调用方只看到统一接口：get_daily(), get_minute() 等                   │
└────────────────────────────┬──────────────────────────────────────────┘
                             │ 调用统一接口
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       Offline Module (offline/)                       │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    OfflineEngine                                │  │
│  │                                                                 │  │
│  │  输入: get_daily(symbol="000001", start_date="2024-01-01", ...) │  │
│  │                                                                 │  │
│  │  1. 查找接口定义 → equity_daily                                 │  │
│  │  2. 按优先级尝试数据源 → [em, sina, tushare]                    │  │
│  │  3. 应用限速 → em_push2his: 0.5s                                │  │
│  │  4. 转换输入参数 → symbol → code (如果需要)                     │  │
│  │  5. 调用 Provider → provider.call("stock_zh_a_hist", ...)       │  │
│  │  6. 转换输出字段 → day → date, symbol → code (如果需要)         │  │
│  │  7. 返回统一格式的 DataFrame                                    │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐    │
│  │ AkProvider  │ │ EmProvider  │ │ SinaProvider│ │ TsProvider  │    │
│  │ (akshare    │ │ (东方财富直连│ │ (新浪直连)  │ │ (Tushare)   │    │
│  │  wrapper)   │ │  HTTP)      │ │             │ │             │    │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘    │
│  ┌─────────────┐ ┌─────────────┐                                     │
│  │ LxProvider  │ │ JsProvider  │                                     │
│  │ (理杏仁)    │ │ (集思录)    │                                     │
│  └─────────────┘ └─────────────┘                                     │
└──────────────────────────────────────────────────────────────────────┘
                             ▲
                             │ 读取
┌──────────────────────────────────────────────────────────────────────┐
│                        Config Files (config/)                         │
│                                                                       │
│  interfaces.yaml            - 统一接口定义（输入/输出/数据源/限速）    │
│  rate_limits.yaml           - 全局限速配置                           │
│  default.yaml               - 其他系统配置                           │
└──────────────────────────────────────────────────────────────────────┘
```

## 核心设计：统一接口

### 接口定义模型

每个"业务接口"（如日线、分钟线、实时行情）只有一个定义，但可以绑定多个数据源：

```yaml
# config/interfaces.yaml
#
# 统一接口定义
# 每个接口：
#   - name: 接口名称（调用方使用）
#   - category: 接口分类
#   - description: 说明
#   - input: 输入参数定义（统一字段名）
#   - output: 输出字段定义（统一列名）
#   - rate_limit_key: 限速键
#   - sources: 数据源列表（按优先级排序）

# ─── 日线数据 ───
equity_daily:
  name: "equity_daily"
  category: "equity"
  description: "股票日线数据"
  
  # 统一输入参数
  input:
    - {name: "symbol", type: "str", required: true, desc: "股票代码，如 000001"}
    - {name: "start_date", type: "date", required: true, desc: "开始日期 YYYY-MM-DD"}
    - {name: "end_date", type: "date", required: true, desc: "结束日期 YYYY-MM-DD"}
    - {name: "adjust", type: "str", required: false, default: "qfq", desc: "复权类型: qfq/hfq/none"}
  
  # 统一输出字段
  output:
    - {name: "date", type: "date", desc: "交易日期"}
    - {name: "open", type: "float", desc: "开盘价"}
    - {name: "high", type: "float", desc: "最高价"}
    - {name: "low", type: "float", desc: "最低价"}
    - {name: "close", type: "float", desc: "收盘价"}
    - {name: "volume", type: "float", desc: "成交量"}
    - {name: "amount", type: "float", desc: "成交额"}
  
  rate_limit_key: "default"
  
  # 数据源列表（按优先级排序）
  sources:
    - name: "akshare_em"
      provider: "akshare"
      func: "stock_zh_a_hist"
      # 输入映射：将统一参数映射到 akshare 函数参数
      input_mapping:
        symbol: "symbol"
        start_date: "start_date"
        end_date: "end_date"
        adjust: "adjust"
      # 参数转换：日期格式转换等
      param_transforms:
        start_date: "YYYYMMDD"   # 2024-01-01 → 20240101
        end_date: "YYYYMMDD"
      # 输出映射：将 akshare 返回的列名映射为统一列名
      output_mapping:
        日期: "date"
        开盘: "open"
        最高: "high"
        最低: "low"
        收盘: "close"
        成交量: "volume"
        成交额: "amount"
      # 列类型转换
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
        amount: "float"
      enabled: true
    
    - name: "akshare_sina"
      provider: "akshare"
      func: "stock_zh_a_daily"
      input_mapping:
        symbol: "symbol"
      output_mapping:
        day: "date"
        open: "open"
        high: "high"
        low: "low"
        close: "close"
        volume: "volume"
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
      enabled: true
    
    - name: "tushare"
      provider: "tushare"
      func: "get_daily"
      input_mapping:
        symbol: "ts_code"
        start_date: "start_date"
        end_date: "end_date"
      param_transforms:
        symbol: "to_ts_code"    # 000001 → 000001.SZ
        start_date: "YYYYMMDD"
        end_date: "YYYYMMDD"
      output_mapping:
        trade_date: "date"
        open: "open"
        high: "high"
        low: "low"
        close: "close"
        vol: "volume"
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
      enabled: false  # 需要 tushare token

# ─── 分钟线数据 ───
equity_minute:
  name: "equity_minute"
  category: "equity"
  description: "股票分钟线数据"
  
  input:
    - {name: "symbol", type: "str", required: true}
    - {name: "start_date", type: "datetime", required: false}
    - {name: "end_date", type: "datetime", required: false}
    - {name: "period", type: "str", required: false, default: "5", desc: "分钟周期: 1/5/15/30/60"}
    - {name: "adjust", type: "str", required: false, default: ""}
  
  output:
    - {name: "datetime", type: "datetime", desc: "时间戳"}
    - {name: "open", type: "float"}
    - {name: "high", type: "float"}
    - {name: "low", type: "float"}
    - {name: "close", type: "float"}
    - {name: "volume", type: "float"}
    - {name: "amount", type: "float"}
  
  rate_limit_key: "em_push2his"
  
  sources:
    - name: "akshare_em"
      provider: "akshare"
      func: "stock_zh_a_hist_min_em"
      input_mapping:
        symbol: "symbol"
        start_date: "start_date"
        end_date: "end_date"
        period: "period"
        adjust: "adjust"
      output_mapping:
        时间: "datetime"
        开盘: "open"
        最高: "high"
        最低: "low"
        收盘: "close"
        成交量: "volume"
        成交额: "amount"
      column_types:
        datetime: "datetime"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
        amount: "float"
      enabled: true

# ─── 实时行情 ───
equity_realtime:
  name: "equity_realtime"
  category: "equity"
  description: "股票实时行情"
  
  input:
    - {name: "symbol", type: "str", required: false, desc: "股票代码，不传返回全部"}
  
  output:
    - {name: "symbol", type: "str", desc: "股票代码"}
    - {name: "name", type: "str", desc: "股票名称"}
    - {name: "price", type: "float", desc: "最新价"}
    - {name: "change", type: "float", desc: "涨跌额"}
    - {name: "pct_change", type: "float", desc: "涨跌幅%"}
    - {name: "volume", type: "float", desc: "成交量"}
    - {name: "amount", type: "float", desc: "成交额"}
    - {name: "open", type: "float", desc: "今开"}
    - {name: "high", type: "float", desc: "最高"}
    - {name: "low", type: "float", desc: "最低"}
    - {name: "prev_close", type: "float", desc: "昨收"}
  
  rate_limit_key: "em_push2"
  
  sources:
    - name: "akshare_em"
      provider: "akshare"
      func: "stock_zh_a_spot_em"
      output_mapping:
        代码: "symbol"
        名称: "name"
        最新价: "price"
        涨跌额: "change"
        涨跌幅: "pct_change"
        成交量: "volume"
        成交额: "amount"
        今开: "open"
        最高: "high"
        最低: "low"
        昨收: "prev_close"
      column_types:
        price: "float"
        change: "float"
        pct_change: "float"
        volume: "float"
        amount: "float"
        open: "float"
        high: "float"
        low: "float"
        prev_close: "float"
      enabled: true
    
    - name: "akshare_sina"
      provider: "akshare"
      func: "stock_zh_a_spot"
      output_mapping:
        code: "symbol"
        name: "name"
        price: "price"
        change: "change"
        pct_change: "pct_change"
        volume: "volume"
        amount: "amount"
        open: "open"
        high: "high"
        low: "low"
        prev_close: "prev_close"
      column_types:
        price: "float"
        change: "float"
        pct_change: "float"
        volume: "float"
        amount: "float"
        open: "float"
        high: "float"
        low: "float"
        prev_close: "float"
      enabled: true

# ─── 指数日线 ───
index_daily:
  name: "index_daily"
  category: "index"
  description: "指数日线数据"
  
  input:
    - {name: "symbol", type: "str", required: true, desc: "指数代码"}
    - {name: "start_date", type: "date", required: true}
    - {name: "end_date", type: "date", required: true}
  
  output:
    - {name: "date", type: "date"}
    - {name: "open", type: "float"}
    - {name: "high", type: "float"}
    - {name: "low", type: "float"}
    - {name: "close", type: "float"}
    - {name: "volume", type: "float"}
    - {name: "amount", type: "float"}
  
  rate_limit_key: "sina_vip"
  
  sources:
    - name: "akshare_sina"
      provider: "akshare"
      func: "index_zh_a_hist"
      input_mapping:
        symbol: "symbol"
        start_date: "start_date"
        end_date: "end_date"
      param_transforms:
        start_date: "YYYYMMDD"
        end_date: "YYYYMMDD"
      output_mapping:
        date: "date"
        open: "open"
        high: "high"
        low: "low"
        close: "close"
        volume: "volume"
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
      enabled: true
    
    - name: "akshare_sw"
      provider: "akshare"
      func: "index_hist_sw"
      input_mapping:
        symbol: "symbol"
      param_transforms:
        period: "day"
      output_mapping:
        date: "date"
        open: "open"
        high: "high"
        low: "low"
        close: "close"
        volume: "volume"
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
      enabled: true

# ─── ETF日线 ───
etf_daily:
  name: "etf_daily"
  category: "fund"
  description: "ETF日线数据"
  
  input:
    - {name: "symbol", type: "str", required: true}
    - {name: "start_date", type: "date", required: true}
    - {name: "end_date", type: "date", required: true}
    - {name: "adjust", type: "str", required: false, default: "qfq"}
  
  output:
    - {name: "date", type: "date"}
    - {name: "open", type: "float"}
    - {name: "high", type: "float"}
    - {name: "low", type: "float"}
    - {name: "close", type: "float"}
    - {name: "volume", type: "float"}
    - {name: "amount", type: "float"}
  
  rate_limit_key: "em_push2his"
  
  sources:
    - name: "akshare_em"
      provider: "akshare"
      func: "fund_etf_hist_em"
      input_mapping:
        symbol: "symbol"
        start_date: "start_date"
        end_date: "end_date"
        adjust: "adjust"
      param_transforms:
        start_date: "YYYYMMDD"
        end_date: "YYYYMMDD"
      output_mapping:
        日期: "date"
        开盘: "open"
        最高: "high"
        最低: "low"
        收盘: "close"
        成交量: "volume"
        成交额: "amount"
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
        amount: "float"
      enabled: true

# ─── 期货日线 ───
futures_daily:
  name: "futures_daily"
  category: "futures"
  description: "期货日线数据"
  
  input:
    - {name: "symbol", type: "str", required: true, desc: "期货代码，如 AG0"}
    - {name: "start_date", type: "date", required: true}
    - {name: "end_date", type: "date", required: true}
  
  output:
    - {name: "date", type: "date"}
    - {name: "open", type: "float"}
    - {name: "high", type: "float"}
    - {name: "low", type: "float"}
    - {name: "close", type: "float"}
    - {name: "volume", type: "float"}
    - {name: "open_interest", type: "float", desc: "持仓量"}
  
  rate_limit_key: "em_push2his"
  
  sources:
    - name: "akshare_em"
      provider: "akshare"
      func: "futures_hist_em"
      input_mapping:
        symbol: "symbol"
        start_date: "start_date"
        end_date: "end_date"
      param_transforms:
        start_date: "YYYYMMDD"
        end_date: "YYYYMMDD"
        period: "daily"
      output_mapping:
        date: "date"
        open: "open"
        high: "high"
        low: "low"
        close: "close"
        volume: "volume"
        open_interest: "open_interest"
      column_types:
        date: "date"
        open: "float"
        high: "float"
        low: "float"
        close: "float"
        volume: "float"
        open_interest: "float"
      enabled: true

# ─── 期权实时行情 ───
options_realtime:
  name: "options_realtime"
  category: "options"
  description: "期权实时行情"
  
  input:
    - {name: "symbol", type: "str", required: false, desc: "期权代码"}
    - {name: "underlying", type: "str", required: false, desc: "标的代码"}
  
  output:
    - {name: "symbol", type: "str", desc: "期权代码"}
    - {name: "underlying", type: "str", desc: "标的代码"}
    - {name: "option_type", type: "str", desc: "期权类型: C/P"}
    - {name: "strike", type: "float", desc: "行权价"}
    - {name: "expiration", type: "date", desc: "到期日"}
    - {name: "price", type: "float", desc: "最新价"}
    - {name: "volume", type: "float", desc: "成交量"}
    - {name: "open_interest", type: "float", desc: "持仓量"}
  
  rate_limit_key: "em_push2"
  
  sources:
    - name: "akshare_em"
      provider: "akshare"
      func: "option_current_em"
      output_mapping:
        期权代码: "symbol"
        标的代码: "underlying"
        期权类型: "option_type"
        行权价: "strike"
        到期日: "expiration"
        最新价: "price"
        成交量: "volume"
        持仓量: "open_interest"
      column_types:
        strike: "float"
        expiration: "date"
        price: "float"
        volume: "float"
        open_interest: "float"
      enabled: true

# ─── 宏观经济 ───
macro_china:
  name: "macro_china"
  category: "macro"
  description: "中国宏观经济数据"
  
  input:
    - {name: "start_date", type: "date", required: false}
    - {name: "end_date", type: "date", required: false}
  
  output:
    - {name: "date", type: "date"}
    # 其他字段因指标不同而异，不强制映射
  
  rate_limit_key: "em_datacenter"
  
  sources:
    - name: "akshare_cpi"
      provider: "akshare"
      func: "macro_china_cpi"
      output_mapping:
        月份: "date"
      column_types:
        date: "date"
      enabled: true
    
    - name: "akshare_gdp"
      provider: "akshare"
      func: "macro_china_gdp"
      output_mapping:
        季度: "date"
      column_types:
        date: "date"
      enabled: true
    
    - name: "akshare_pmi"
      provider: "akshare"
      func: "macro_china_pmi"
      output_mapping:
        月份: "date"
      column_types:
        date: "date"
      enabled: true
    
    - name: "akshare_lpr"
      provider: "akshare"
      func: "macro_china_lpr"
      output_mapping:
        日期: "date"
      column_types:
        date: "date"
      enabled: true

# ... 更多接口按同样模式定义
```

### 限速配置

```yaml
# config/rate_limits.yaml

em_push2his:
  interval: 0.5
  description: "东方财富历史数据API (push2his.eastmoney.com)"

em_push2:
  interval: 0.5
  description: "东方财富实时行情API (push2.eastmoney.com)"

em_datacenter:
  interval: 1.0
  description: "东方财富数据中心 (datacenter-web.eastmoney.com)"

sina_vip:
  interval: 1.0
  description: "新浪财经VIP接口 (vip.stock.finance.sina.com.cn)"

sina_hq:
  interval: 0.3
  description: "新浪行情接口 (hq.sinajs.cn)"

tushare:
  interval: 0.2
  description: "Tushare Pro (tushare.pro)"

lixinger:
  interval: 2.0
  description: "理杏仁 OpenAPI (open.lixinger.com)"

default:
  interval: 0.5
  description: "默认限速"
```

## 核心代码实现

### 目录结构

```
src/akshare_data/offline/
├── __init__.py
├── engine.py              # OfflineEngine: 核心调度器
├── registry.py            # InterfaceRegistry: 加载接口定义
├── rate_limiter.py        # RateLimiter: 限速控制
├── normalizer.py          # FieldNormalizer: 输入输出字段标准化
├── providers/             # 数据提供者
│   ├── __init__.py
│   ├── base.py            # BaseProvider 抽象接口
│   ├── akshare_provider.py
│   ├── tushare_provider.py
│   ├── lixinger_provider.py
│   └── mock_provider.py
├── downloader.py          # BatchDownloader (已有)
├── prober.py              # APIProber (已有)
├── quality.py             # DataQualityChecker (已有)
└── reporter.py            # Reporter (已有)
```

### 1. InterfaceRegistry

```python
# src/akshare_data/offline/registry.py

import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

@dataclass
class FieldDef:
    """字段定义"""
    name: str
    type: str = "str"
    required: bool = False
    default: Any = None
    desc: str = ""

@dataclass
class SourceDef:
    """数据源定义"""
    name: str
    provider: str
    func: str
    input_mapping: Dict[str, str] = field(default_factory=dict)
    output_mapping: Dict[str, str] = field(default_factory=dict)
    column_types: Dict[str, str] = field(default_factory=dict)
    param_transforms: Dict[str, str] = field(default_factory=dict)
    enabled: bool = True

@dataclass
class InterfaceDef:
    """统一接口定义"""
    name: str
    category: str
    description: str = ""
    input: List[FieldDef] = field(default_factory=list)
    output: List[FieldDef] = field(default_factory=list)
    rate_limit_key: str = "default"
    sources: List[SourceDef] = field(default_factory=list)

class InterfaceRegistry:
    """接口注册表。
    
    从 YAML 配置文件加载统一接口定义。
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self._interfaces: Dict[str, InterfaceDef] = {}
        self._by_category: Dict[str, List[str]] = {}
        self._config_path = config_path or self._find_config()
        if self._config_path:
            self.load(self._config_path)
    
    def _find_config(self) -> Optional[str]:
        candidates = [
            Path(__file__).parent.parent.parent / "config" / "interfaces.yaml",
            Path.cwd() / "config" / "interfaces.yaml",
        ]
        for p in candidates:
            if p.exists():
                return str(p)
        return None
    
    def load(self, config_path: str) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        
        for iface_name, defn in raw.items():
            input_fields = [
                FieldDef(**f) for f in defn.get("input", [])
            ]
            output_fields = [
                FieldDef(**f) for f in defn.get("output", [])
            ]
            sources = []
            for s in defn.get("sources", []):
                sources.append(SourceDef(
                    name=s["name"],
                    provider=s["provider"],
                    func=s["func"],
                    input_mapping=s.get("input_mapping", {}),
                    output_mapping=s.get("output_mapping", {}),
                    column_types=s.get("column_types", {}),
                    param_transforms=s.get("param_transforms", {}),
                    enabled=s.get("enabled", True),
                ))
            
            interface = InterfaceDef(
                name=defn.get("name", iface_name),
                category=defn["category"],
                description=defn.get("description", ""),
                input=input_fields,
                output=output_fields,
                rate_limit_key=defn.get("rate_limit_key", "default"),
                sources=sources,
            )
            
            self._interfaces[iface_name] = interface
            
            cat = interface.category
            if cat not in self._by_category:
                self._by_category[cat] = []
            self._by_category[cat].append(iface_name)
    
    def get_interface(self, name: str) -> Optional[InterfaceDef]:
        return self._interfaces.get(name)
    
    def get_by_category(self, category: str) -> List[str]:
        return self._by_category.get(category, [])
    
    def get_enabled_sources(self, name: str) -> List[SourceDef]:
        iface = self._interfaces.get(name)
        if not iface:
            return []
        return [s for s in iface.sources if s.enabled]
    
    def list_all(self) -> List[str]:
        return list(self._interfaces.keys())
    
    def list_categories(self) -> List[str]:
        return list(self._by_category.keys())
```

### 2. FieldNormalizer（输入输出标准化）

```python
# src/akshare_data/offline/normalizer.py

import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, date

class FieldNormalizer:
    """输入输出字段标准化器。
    
    职责：
    1. 输入参数映射：将统一参数名映射到数据源参数名
    2. 参数转换：日期格式转换、代码格式转换等
    3. 输出字段映射：将数据源列名映射为统一列名
    4. 列类型转换：确保输出列类型一致
    """
    
    def map_input(
        self,
        kwargs: Dict[str, Any],
        input_mapping: Dict[str, str],
        param_transforms: Dict[str, str],
    ) -> Dict[str, Any]:
        """映射并转换输入参数。
        
        Args:
            kwargs: 统一参数
            input_mapping: {统一参数名: 数据源参数名}
            param_transforms: {参数名: 转换规则}
        
        Returns:
            转换后的参数字典
        """
        result = {}
        
        for unified_name, source_name in input_mapping.items():
            if unified_name not in kwargs:
                continue
            
            value = kwargs[unified_name]
            
            # 应用参数转换
            transform = param_transforms.get(unified_name)
            if transform:
                value = self._transform_param(value, transform)
            
            result[source_name] = value
        
        # 传递未映射的参数
        for key, value in kwargs.items():
            if key not in input_mapping:
                result[key] = value
        
        return result
    
    def normalize_output(
        self,
        df: pd.DataFrame,
        output_mapping: Dict[str, str],
        column_types: Dict[str, str],
    ) -> pd.DataFrame:
        """标准化输出 DataFrame。
        
        Args:
            df: 原始 DataFrame
            output_mapping: {原始列名: 统一列名}
            column_types: {统一列名: 类型}
        
        Returns:
            标准化后的 DataFrame
        """
        if df.empty:
            return df
        
        # 重命名列
        rename_map = {}
        for orig_col, unified_col in output_mapping.items():
            if orig_col in df.columns:
                rename_map[orig_col] = unified_col
        
        if rename_map:
            df = df.rename(columns=rename_map)
        
        # 转换列类型
        for col, type_name in column_types.items():
            if col not in df.columns:
                continue
            
            pandas_type = self._to_pandas_type(type_name)
            try:
                if pandas_type == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col])
                else:
                    df[col] = df[col].astype(pandas_type)
            except (ValueError, TypeError):
                pass  # 类型转换失败时跳过
        
        return df
    
    def _transform_param(self, value: Any, transform: str) -> Any:
        """转换参数值。
        
        支持的转换规则：
        - YYYYMMDD: datetime/date → "20240101"
        - to_ts_code: "000001" → "000001.SZ"
        """
        if value is None:
            return None
        
        if transform == "YYYYMMDD":
            if isinstance(value, (date, datetime)):
                return value.strftime("%Y%m%d")
            if isinstance(value, str):
                return value.replace("-", "").replace("/", "")
            return str(value)
        
        if transform == "to_ts_code":
            code = str(value).zfill(6)
            if code.startswith("6"):
                return f"{code}.SH"
            return f"{code}.SZ"
        
        if transform == "to_ak_code":
            # JoinQuant → AkShare
            code = str(value)
            if ".XSHG" in code:
                return code.replace(".XSHG", "")
            if ".XSHE" in code:
                return code.replace(".XSHE", "")
            return code
        
        return value
    
    def _to_pandas_type(self, type_name: str) -> str:
        """将配置类型转换为 pandas 类型。"""
        type_map = {
            "str": "str",
            "float": "float64",
            "int": "int64",
            "date": "datetime64[ns]",
            "datetime": "datetime64[ns]",
            "bool": "bool",
        }
        return type_map.get(type_name, "str")
```

### 3. RateLimiter

```python
# src/akshare_data/offline/rate_limiter.py

import time
import threading
import yaml
from pathlib import Path
from typing import Dict, Optional

class RateLimiter:
    """按限速键控制请求频率。"""
    
    def __init__(self, config_path: Optional[str] = None):
        self._intervals: Dict[str, float] = {"default": 0.5}
        self._last_request: Dict[str, float] = {}
        self._lock = threading.Lock()
        
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "rate_limits.yaml"
        
        if Path(config_path).exists():
            self._load_config(config_path)
    
    def _load_config(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        for key, cfg in raw.items():
            if isinstance(cfg, dict) and "interval" in cfg:
                self._intervals[key] = float(cfg["interval"])
    
    def wait(self, key: str) -> None:
        interval = self._intervals.get(key, self._intervals.get("default", 0.5))
        with self._lock:
            last = self._last_request.get(key, 0)
            elapsed = time.time() - last
            if elapsed < interval:
                time.sleep(interval - elapsed)
            self._last_request[key] = time.time()
    
    def set_interval(self, key: str, interval: float) -> None:
        with self._lock:
            self._intervals[key] = interval
```

### 4. OfflineEngine（核心调度器）

```python
# src/akshare_data/offline/engine.py

import logging
from typing import Any, Dict, List, Optional
import pandas as pd

from .registry import InterfaceRegistry, InterfaceDef, SourceDef
from .rate_limiter import RateLimiter
from .normalizer import FieldNormalizer
from .providers.base import BaseProvider
from .providers.akshare_provider import AkShareProvider

logger = logging.getLogger(__name__)

class OfflineEngine:
    """离线引擎核心调度器。
    
    调用方使用统一接口：
        engine.call("equity_daily", symbol="000001", start_date="2024-01-01", ...)
    
    引擎内部：
    1. 查找接口定义 → equity_daily
    2. 按优先级尝试数据源 → [akshare_em, akshare_sina, tushare]
    3. 应用限速
    4. 输入参数映射 + 转换
    5. 调用 Provider
    6. 输出字段映射 + 类型转换
    7. 返回统一格式的 DataFrame
    """
    
    def __init__(
        self,
        registry: Optional[InterfaceRegistry] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.registry = registry or InterfaceRegistry()
        self.rate_limiter = rate_limiter or RateLimiter()
        self.normalizer = FieldNormalizer()
        self._providers: Dict[str, BaseProvider] = {}
        self._register_default_providers()
    
    def _register_default_providers(self) -> None:
        self._providers["akshare"] = AkShareProvider()
    
    def register_provider(self, name: str, provider: BaseProvider) -> None:
        self._providers[name] = provider
    
    def call(
        self,
        interface_name: str,
        sources: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """调用统一接口。
        
        Args:
            interface_name: 接口名称（如 "equity_daily"）
            sources: 可选，指定数据源列表
            **kwargs: 统一输入参数
        
        Returns:
            统一格式的 DataFrame
        """
        iface = self.registry.get_interface(interface_name)
        if iface is None:
            raise ValueError(f"接口 {interface_name} 未定义")
        
        # 确定数据源列表
        if sources:
            source_defs = [s for s in iface.sources if s.name in sources and s.enabled]
        else:
            source_defs = self.registry.get_enabled_sources(interface_name)
        
        if not source_defs:
            raise RuntimeError(f"接口 {interface_name} 没有可用的数据源")
        
        errors = []
        for source_def in source_defs:
            provider = self._providers.get(source_def.provider)
            if provider is None:
                errors.append((source_def.name, "provider not registered"))
                continue
            
            # 应用限速
            self.rate_limiter.wait(iface.rate_limit_key)
            
            # 输入参数映射 + 转换
            call_kwargs = self.normalizer.map_input(
                kwargs,
                source_def.input_mapping,
                source_def.param_transforms,
            )
            
            try:
                logger.debug(
                    "Calling %s via %s.%s",
                    interface_name, source_def.provider, source_def.func,
                )
                result = provider.call(source_def.func, **call_kwargs)
                
                # 输出字段映射 + 类型转换
                result = self.normalizer.normalize_output(
                    result,
                    source_def.output_mapping,
                    source_def.column_types,
                )
                
                # 标注来源
                result.attrs["source"] = source_def.name
                result.attrs["interface"] = interface_name
                
                return result
                
            except Exception as e:
                logger.warning("数据源 %s 调用失败: %s", source_def.name, e)
                errors.append((source_def.name, str(e)))
                continue
        
        raise RuntimeError(
            f"所有数据源都失败: {interface_name}\n"
            + "\n".join(f"  {name}: {err}" for name, err in errors)
        )
    
    def list_interfaces(self) -> List[str]:
        return self.registry.list_all()
    
    def list_categories(self) -> List[str]:
        return self.registry.list_categories()
```

### 5. Provider 抽象

```python
# src/akshare_data/offline/providers/base.py

from abc import ABC, abstractmethod
import pandas as pd

class BaseProvider(ABC):
    """数据提供者抽象基类。"""
    
    name: str
    
    @abstractmethod
    def call(self, func_name: str, **kwargs) -> pd.DataFrame:
        ...

# src/akshare_data/offline/providers/akshare_provider.py

import logging
import pandas as pd
from .base import BaseProvider

logger = logging.getLogger(__name__)

class AkShareProvider(BaseProvider):
    """AkShare 数据提供者。"""
    
    name = "akshare"
    
    def __init__(self):
        import akshare
        self._ak = akshare
    
    def call(self, func_name: str, **kwargs) -> pd.DataFrame:
        func = getattr(self._ak, func_name, None)
        if func is None:
            raise AttributeError(f"akshare 中不存在函数: {func_name}")
        
        result = func(**kwargs)
        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"akshare.{func_name} 返回类型不是 DataFrame")
        
        return result
```

### 6. 在线层（api.py）

```python
# src/akshare_data/api.py

class DataService:
    """Unified data service.
    
    调用方只看到统一接口，不关心底层数据源。
    """
    
    def __init__(self, cache_manager=None, offline_engine=None):
        self.cache = cache_manager or get_cache_manager()
        self.engine = offline_engine or get_offline_engine()
    
    def get_daily(self, symbol, start_date, end_date, adjust="qfq"):
        """获取日线数据。统一接口。"""
        symbol = normalize_symbol(symbol)
        cached = self._read_range("stock_daily", symbol, start_date, end_date)
        
        if self._is_complete(cached, start_date, end_date):
            return cached
        
        for m_start, m_end in self._find_gaps(...):
            # 调用统一接口，不关心底层是东方财富还是新浪
            df = self.engine.call(
                "equity_daily",
                symbol=symbol,
                start_date=m_start,
                end_date=m_end,
                adjust=adjust,
            )
            if df is not None and not df.empty:
                self._write("stock_daily", symbol, df)
        
        return self._read_range("stock_daily", symbol, start_date, end_date)
    
    def get_minute(self, symbol, freq="1min", start_date=None, end_date=None):
        """获取分钟线数据。统一接口。"""
        df = self.engine.call(
            "equity_minute",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            period=freq.replace("min", ""),
        )
        # ... 缓存逻辑
    
    def get_realtime(self, symbol=None):
        """获取实时行情。统一接口。"""
        return self.engine.call("equity_realtime", symbol=symbol)
    
    def get_index_daily(self, symbol, start_date=None, end_date=None):
        """获取指数日线。统一接口。"""
        return self.engine.call(
            "index_daily",
            symbol=symbol,
            start_date=start_date or "1990-01-01",
            end_date=end_date or datetime.now().strftime("%Y-%m-%d"),
        )
    
    def get_etf_daily(self, symbol, start_date, end_date):
        """获取ETF日线。统一接口。"""
        return self.engine.call(
            "etf_daily",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
    
    def get_futures_daily(self, symbol, start_date, end_date):
        """获取期货日线。统一接口。"""
        return self.engine.call(
            "futures_daily",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
    
    def get_options_realtime(self, symbol=None, underlying=None):
        """获取期权实时行情。统一接口。"""
        return self.engine.call(
            "options_realtime",
            symbol=symbol,
            underlying=underlying,
        )
    
    # ─── 计算方法（在 DataService 层） ───
    
    def get_option_greeks(self, symbol, date, **kwargs):
        """计算期权 Greeks。从 engine 取数据后计算。"""
        chain = self.engine.call("options_realtime", symbol=symbol)
        # ... 计算逻辑
    
    def calculate_option_implied_vol(self, symbol, price, strike, expiry, **kwargs):
        """计算隐含波动率。"""
        # ... 计算逻辑
    
    def black_scholes_price(self, S, K, T, r, sigma, option_type):
        """BS 定价。纯计算。"""
        # ... 计算逻辑
```

### 7. 改造后的 AkShareAdapter

```python
# src/akshare_data/sources/akshare_source.py
# 从 1774 行 → 不再需要
# 
# AkShareAdapter 被 OfflineEngine + AkShareProvider 替代。
# 旧的 AkShareAdapter 可以保留为兼容层，内部委托给 OfflineEngine。

class AkShareAdapter(DataSource):
    """向后兼容层。内部委托给 OfflineEngine。"""
    
    name = "akshare"
    source_type = "real"
    
    def __init__(self, engine=None, ...):
        self.engine = engine or get_offline_engine()
        # ... 其他初始化
    
    def get_daily_data(self, symbol, start_date, end_date, adjust="qfq", **kwargs):
        return self.engine.call("equity_daily", symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
    
    def get_minute_data(self, symbol, freq="1min", **kwargs):
        return self.engine.call("equity_minute", symbol=symbol, period=freq.replace("min", ""), **kwargs)
    
    def get_realtime_data(self, symbol, **kwargs):
        return self.engine.call("equity_realtime", symbol=symbol)
    
    def get_index_daily(self, symbol, start_date, end_date, **kwargs):
        return self.engine.call("index_daily", symbol=symbol, start_date=start_date, end_date=end_date)
    
    # ... 其他方法都委托给 engine.call()
```

## 输入输出字段统一规范

### 通用输入参数

| 参数名 | 类型 | 必填 | 说明 | 示例 |
|-------|------|------|------|------|
| `symbol` | str | 视接口 | 代码 | `000001` |
| `start_date` | date | 视接口 | 开始日期 | `2024-01-01` |
| `end_date` | date | 视接口 | 结束日期 | `2024-12-31` |
| `adjust` | str | 否 | 复权类型 | `qfq`/`hfq`/`none` |
| `period` | str | 否 | 周期 | `1`/`5`/`15`/`daily` |

### 通用输出字段

#### OHLCV 类（日线/分钟线/指数/ETF/期货）

| 字段名 | 类型 | 说明 |
|-------|------|------|
| `date` | datetime | 日期（日线/指数） |
| `datetime` | datetime | 时间戳（分钟线） |
| `open` | float64 | 开盘价 |
| `high` | float64 | 最高价 |
| `low` | float64 | 最低价 |
| `close` | float64 | 收盘价 |
| `volume` | float64 | 成交量 |
| `amount` | float64 | 成交额 |

#### 实时行情类

| 字段名 | 类型 | 说明 |
|-------|------|------|
| `symbol` | str | 代码 |
| `name` | str | 名称 |
| `price` | float64 | 最新价 |
| `change` | float64 | 涨跌额 |
| `pct_change` | float64 | 涨跌幅% |
| `volume` | float64 | 成交量 |
| `amount` | float64 | 成交额 |
| `open` | float64 | 今开 |
| `high` | float64 | 最高 |
| `low` | float64 | 最低 |
| `prev_close` | float64 | 昨收 |

#### 期权类

| 字段名 | 类型 | 说明 |
|-------|------|------|
| `symbol` | str | 期权代码 |
| `underlying` | str | 标的代码 |
| `option_type` | str | C/P |
| `strike` | float64 | 行权价 |
| `expiration` | date | 到期日 |
| `price` | float64 | 最新价 |
| `volume` | float64 | 成交量 |
| `open_interest` | float64 | 持仓量 |

## 数据源切换示例

```python
# 默认使用配置中优先级最高的数据源
df = engine.call("equity_daily", symbol="000001", start_date="2024-01-01", end_date="2024-12-31")
# 底层走的是 akshare_em → stock_zh_a_hist
# 返回统一字段：date, open, high, low, close, volume, amount

# 强制使用新浪源
df = engine.call("equity_daily", sources=["akshare_sina"], symbol="000001", ...)
# 底层走的是 akshare_sina → stock_zh_a_daily
# 输出字段自动映射为统一的 date, open, high, low, close, volume

# 强制使用 Tushare
df = engine.call("equity_daily", sources=["tushare"], symbol="000001", ...)
# 底层走的是 tushare → get_daily
# symbol 自动转换为 000001.SZ，日期自动转换为 20240101
# 输出字段 vol → volume, trade_date → date
```

## 配置文件结构

```
config/
├── default.yaml              # 系统配置（缓存、DuckDB、日志等）
├── interfaces.yaml           - 统一接口定义（输入/输出/数据源/限速/字段映射）
├── rate_limits.yaml          - 全局限速配置
└── health_state.json         - 健康状态（已有）
```

## 迁移策略

### Phase 1: 基础设施（1-2周）

1. 创建 `offline/registry.py` - 接口注册表
2. 创建 `offline/normalizer.py` - 字段标准化器
3. 创建 `offline/rate_limiter.py` - 限速器
4. 创建 `offline/engine.py` - 核心调度器
5. 创建 `offline/providers/base.py` + `akshare_provider.py`
6. 创建 `config/interfaces.yaml` - 核心 10 个接口定义
7. 创建 `config/rate_limits.yaml` - 限速配置

### Phase 2: AkShareAdapter 改造（1周）

1. AkShareAdapter 改为兼容层，内部委托给 OfflineEngine
2. 所有方法改为 `self.engine.call(interface_name, ...)`
3. 移除所有计算逻辑，上移到 DataService
4. 单元测试验证行为不变

### Phase 3: DataService 改造（1周）

1. 数据获取改为 `self.engine.call(...)`
2. 计算逻辑从 AkShareAdapter 迁移到 DataService
3. 测试验证

### Phase 4: 补充接口定义（持续）

1. 按优先级补充接口到 `interfaces.yaml`
2. 日线 → 分钟线 → 实时 → 财务 → 宏观 → 其他

### Phase 5: 添加更多 Provider（按需）

1. EastMoneyProvider（直连）
2. SinaProvider（直连）
3. 在接口配置中启用

## 关键优势

| 维度 | 改造前 | 改造后 |
|-----|-------|-------|
| 接口定义 | 每个数据源独立方法 | 统一接口，多数据源绑定 |
| 输入字段 | 各数据源不同 | 统一参数名 |
| 输出字段 | 各数据源不同 | 统一列名 |
| 字段映射 | 代码中硬编码 | 配置中声明 |
| 限速粒度 | 按数据源名称 | 按底层域名/端点 |
| 数据源切换 | 改代码 | 改配置或传 sources 参数 |
| 新增接口 | 写方法 + 测试 | 写 YAML 配置 |
| 代码量 | 1774 行 | ~80 行兼容层 + 配置 |
| 计算逻辑 | 混在 Adapter | 集中在 DataService |
| 可测试性 | 难 | 易（MockProvider） |

## 与 akshare-one-enhanced 的对应关系

| akshare-one-enhanced | akshare-data-service (新设计) |
|---------------------|-------------------------------|
| `modules/historical/eastmoney.py` | `interfaces.yaml` 中 equity_daily 的一个 source |
| `modules/historical/sina.py` | `interfaces.yaml` 中 equity_daily 的另一个 source |
| `modules/factory_base.py` | `offline/registry.py` |
| `modules/multi_source.py` | `offline/engine.py` |
| 各模块的列名映射 | `interfaces.yaml` 中的 output_mapping |
| 各模块的参数转换 | `interfaces.yaml` 中的 input_mapping + param_transforms |
| N/A | `offline/normalizer.py` (统一字段标准化) |

### 关键差异

1. **接口统一方式**：
   - akshare-one-enhanced: 每个 Provider 独立定义输入输出
   - akshare-data-service: 统一接口定义，数据源通过 mapping 适配

2. **字段标准化**：
   - akshare-one-enhanced: 各 Provider 自己处理
   - akshare-data-service: FieldNormalizer 统一处理，配置驱动

3. **配置驱动程度**：
   - akshare-one-enhanced: 部分配置
   - akshare-data-service: 100% 配置驱动，零手写业务方法

4. **复杂度**：
   - akshare-one-enhanced: 80+ 模块
   - akshare-data-service: 核心 5 个模块 + YAML 配置
