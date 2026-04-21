# 错误处理系统

本文档介绍 akshare-data-service 的完整错误处理系统，包括错误码枚举、异常类层次结构以及使用模式。

## 概述

错误处理系统位于 `src/akshare_data/core/errors.py`，提供结构化的错误分类和报告机制。系统由以下核心组件构成：

- **ErrorCode 枚举**：标准化的错误码，按类别分组
- **异常类层次结构**：从 `DataAccessException` 基类派生的专用异常类
- **工具方法**：错误码分类和人类可读消息获取

## ErrorCode 枚举

`ErrorCode` 枚举定义了所有标准化错误码，格式为 `"编号_名称"`，按类别划分为 9 大类。

### 1xxx: 数据源错误 (Data Source Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 1001 | SOURCE_UNAVAILABLE | 数据源当前不可用 |
| 1002 | SOURCE_TIMEOUT | 数据源请求超时 |
| 1003 | SOURCE_RATE_LIMITED | 数据源频率限制已超出 |
| 1004 | SOURCE_AUTH_FAILED | 数据源认证失败 |
| 1005 | SOURCE_CONNECTION_REFUSED | 数据源拒绝连接 |
| 1006 | SOURCE_DNS_ERROR | 数据源 DNS 解析失败 |
| 1007 | SOURCE_SSL_ERROR | 连接数据源时 SSL/TLS 错误 |
| 1008 | SOURCE_HTTP_ERROR | 数据源 HTTP 错误 |
| 1009 | SOURCE_HTTP_400 | 数据源请求错误 (HTTP 400) |
| 1010 | SOURCE_HTTP_401 | 数据源未授权访问 (HTTP 401) |
| 1011 | SOURCE_HTTP_403 | 数据源禁止访问 (HTTP 403) |
| 1012 | SOURCE_HTTP_404 | 数据源资源未找到 (HTTP 404) |
| 1013 | SOURCE_HTTP_429 | 数据源请求过多 (HTTP 429) |
| 1014 | SOURCE_HTTP_500 | 数据源内部服务器错误 (HTTP 500) |
| 1015 | SOURCE_HTTP_502 | 数据源网关错误 (HTTP 502) |
| 1016 | SOURCE_HTTP_503 | 数据源服务不可用 (HTTP 503) |
| 1017 | SOURCE_HTTP_504 | 数据源网关超时 (HTTP 504) |
| 1018 | SOURCE_DEPRECATED | 数据源 API 已弃用 |
| 1019 | SOURCE_MAINTENANCE | 数据源维护中 |
| 1020 | SOURCE_QUOTA_EXCEEDED | 数据源配额已超出 |
| 1021 | SOURCE_IP_BLOCKED | IP 地址被数据源封禁 |
| 1022 | SOURCE_ACCOUNT_SUSPENDED | 账户被数据源暂停 |
| 1023 | SOURCE_API_VERSION_ERROR | 数据源不支持该 API 版本 |
| 1024 | SOURCE_RESPONSE_EMPTY | 数据源返回空响应 |
| 1025 | SOURCE_RESPONSE_TRUNCATED | 数据源响应被截断 |

### 2xxx: 缓存错误 (Cache Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 2001 | CACHE_MISS | 缓存中未找到请求的数据 |
| 2002 | CACHE_CORRUPTED | 缓存数据已损坏 |
| 2003 | CACHE_WRITE_FAILED | 写入缓存失败 |
| 2004 | CACHE_READ_FAILED | 读取缓存失败 |
| 2005 | CACHE_KEY_NOT_FOUND | 缓存键未找到 |
| 2006 | CACHE_EXPIRED | 缓存数据已过期 |
| 2007 | CACHE_INVALIDATION_FAILED | 缓存失效操作失败 |
| 2008 | CACHE_SERIALIZATION_ERROR | 缓存序列化失败 |
| 2009 | CACHE_DESERIALIZATION_ERROR | 缓存反序列化失败 |
| 2010 | CACHE_MEMORY_FULL | 缓存内存限制已达 |
| 2011 | CACHE_DISK_FULL | 缓存磁盘空间耗尽 |
| 2012 | CACHE_LOCK_TIMEOUT | 获取缓存锁超时 |
| 2013 | CACHE_LOCK_ACQUISITION_FAILED | 获取缓存锁失败 |
| 2014 | CACHE_COMPACT_FAILED | 缓存压缩失败 |
| 2015 | CACHE_PARTITION_ERROR | 缓存分区错误 |
| 2016 | CACHE_STALE_DATA | 缓存数据可能已过时 |
| 2017 | CACHE_VERSION_MISMATCH | 缓存版本不匹配 |
| 2018 | CACHE_SCHEMA_MISMATCH | 缓存数据模式不匹配 |

### 3xxx: 参数/验证错误 (Parameter/Validation Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 3001 | INVALID_SYMBOL | 股票符号格式无效 |
| 3002 | INVALID_DATE_RANGE | 日期范围无效 |
| 3003 | INVALID_PARAMETER | 参数值无效 |
| 3004 | MISSING_PARAMETER | 缺少必需参数 |
| 3005 | INVALID_DATE_FORMAT | 日期格式无效 |
| 3006 | INVALID_FREQUENCY | 数据频率无效 |
| 3007 | INVALID_ADJUST_TYPE | 复权类型无效 |
| 3008 | INVALID_DATA_SOURCE | 数据源指定无效 |
| 3009 | INVALID_TABLE_NAME | 表名无效 |
| 3010 | INVALID_COLUMN_NAME | 列名无效 |
| 3011 | INVALID_SORT_ORDER | 排序顺序无效 |
| 3012 | INVALID_LIMIT_VALUE | 限制值无效 |
| 3013 | INVALID_TIMEZONE | 时区无效 |
| 3014 | INVALID_CURRENCY | 货币代码无效 |
| 3015 | INVALID_EXCHANGE | 交易所代码无效 |
| 3016 | INVALID_ASSET_TYPE | 资产类型无效 |
| 3017 | INVALID_FIELD_TYPE | 字段类型无效 |
| 3018 | INVALID_CONFIG | 配置无效 |
| 3019 | INVALID_JSON | JSON 格式无效 |
| 3020 | INVALID_CSV_FORMAT | CSV 格式无效 |
| 3021 | INVALID_PARQUET_FILE | Parquet 文件无效 |
| 3022 | DUPLICATE_PARAMETER | 参数重复指定 |
| 3023 | PARAMETER_OUT_OF_RANGE | 参数值超出范围 |
| 3024 | PARAMETER_TYPE_MISMATCH | 参数类型不匹配 |
| 3025 | SYMBOL_NOT_IN_UNIVERSE | 符号不在交易范围内 |
| 3026 | DATE_IN_FUTURE | 日期在未来 |
| 3027 | DATE_BEFORE_MARKET_OPEN | 日期在市场开盘之前 |
| 3028 | START_AFTER_END | 开始日期晚于结束日期 |

### 4xxx: 网络错误 (Network Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 4001 | NETWORK_TIMEOUT | 网络请求超时 |
| 4002 | NETWORK_CONNECTION_LOST | 网络连接丢失 |
| 4003 | NETWORK_DNS_FAILURE | DNS 解析失败 |
| 4004 | NETWORK_PROXY_ERROR | 代理配置错误 |
| 4005 | NETWORK_SOCKET_ERROR | 套接字错误 |
| 4006 | NETWORK_RESET_BY_PEER | 连接被对端重置 |
| 4007 | NETWORK_NO_ROUTE_TO_HOST | 没有到主机的路由 |
| 4008 | NETWORK_UNREACHABLE | 网络不可达 |
| 4009 | NETWORK_TOO_MANY_REDIRECTS | 重定向次数过多 |
| 4010 | NETWORK_BANDWIDTH_EXCEEDED | 网络带宽已超出 |
| 4011 | NETWORK_CONGESTION | 检测到网络拥塞 |
| 4012 | NETWORK_TLS_HANDSHAKE_FAILED | TLS 握手失败 |
| 4013 | NETWORK_CERTIFICATE_EXPIRED | SSL 证书已过期 |
| 4014 | NETWORK_CERTIFICATE_UNTRUSTED | SSL 证书不受信任 |

### 5xxx: 数据质量错误 (Data Quality Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 5001 | NO_DATA | 请求的查询无可用数据 |
| 5002 | INVALID_DATA | 数据包含无效值 |
| 5003 | DATA_FORMAT_ERROR | 数据格式不正确 |
| 5004 | MISSING_COLUMNS | 缺少必需的列 |
| 5005 | DUPLICATE_ROWS | 检测到重复行 |
| 5006 | NULL_VALUES_DETECTED | 检测到意外的空值 |
| 5007 | OUTLIER_DETECTED | 检测到统计异常值 |
| 5008 | DATA_INCONSISTENCY | 检测到数据不一致 |
| 5009 | DATA_TRUNCATION | 数据被截断 |
| 5010 | DATA_ENCODING_ERROR | 数据编码错误 |
| 5011 | SCHEMA_MISMATCH | 数据模式与预期不匹配 |
| 5012 | COLUMN_TYPE_MISMATCH | 列类型不匹配 |
| 5013 | MISSING_REQUIRED_COLUMN | 缺少必需的列 |
| 5014 | DATA_RANGE_ERROR | 数据值超出预期范围 |
| 5015 | NEGATIVE_VOLUME | 检测到负的成交量 |
| 5016 | NEGATIVE_PRICE | 检测到负的价格 |
| 5017 | ZERO_PRICE | 检测到零价格 |
| 5018 | PRICE_ANOMALY | 检测到价格异常 |
| 5019 | VOLUME_ANOMALY | 检测到成交量异常 |
| 5020 | TIMESTAMP_ERROR | 数据中的时间戳错误 |
| 5021 | FUTURE_TIMESTAMP | 检测到未来的时间戳 |
| 5022 | DUPLICATE_TIMESTAMP | 检测到重复的时间戳 |
| 5023 | GAP_IN_TIMESERIES | 时间序列数据中存在间隔 |
| 5024 | INCOMPLETE_TRADING_DAY | 交易日数据不完整 |
| 5025 | SPLIT_ADJUSTMENT_ERROR | 股票拆分调整错误 |
| 5026 | DIVIDEND_ADJUSTMENT_ERROR | 股息调整错误 |
| 5027 | SUSPENSION_NOT_HANDLED | 未处理交易暂停 |
| 5028 | DELISTED_SYMBOL_DATA | 已退市符号的数据 |

### 6xxx: 系统/内部错误 (System/Internal Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 6001 | INTERNAL_ERROR | 内部系统错误 |
| 6002 | NOT_IMPLEMENTED | 功能未实现 |
| 6003 | CONFIGURATION_ERROR | 配置错误 |
| 6004 | DEPENDENCY_ERROR | 依赖错误 |
| 6005 | IMPORT_ERROR | 模块导入错误 |
| 6006 | VERSION_MISMATCH | 版本不匹配 |
| 6007 | MEMORY_ERROR | 内存不足 |
| 6008 | STACK_OVERFLOW | 栈溢出 |
| 6009 | RECURSION_LIMIT | 递归限制已超出 |
| 6010 | THREAD_ERROR | 线程错误 |
| 6011 | PROCESS_ERROR | 进程错误 |
| 6012 | SIGNAL_INTERRUPTED | 操作被信号中断 |
| 6013 | GRACEFUL_SHUTDOWN | 系统正在优雅关闭 |
| 6014 | UNEXPECTED_STATE | 意外的系统状态 |
| 6015 | RACE_CONDITION | 检测到竞态条件 |
| 6016 | DEADLOCK_DETECTED | 检测到死锁 |
| 6017 | RESOURCE_LEAK | 检测到资源泄漏 |
| 6018 | PLUGIN_ERROR | 插件错误 |
| 6019 | EXTENSION_ERROR | 扩展错误 |
| 6020 | MIGRATION_ERROR | 数据迁移错误 |

### 7xxx: 存储/文件错误 (Storage/File Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 7001 | FILE_NOT_FOUND | 文件未找到 |
| 7002 | FILE_PERMISSION_DENIED | 文件权限被拒绝 |
| 7003 | FILE_ALREADY_EXISTS | 文件已存在 |
| 7004 | FILE_CORRUPTED | 文件已损坏 |
| 7005 | FILE_TOO_LARGE | 文件超出大小限制 |
| 7006 | DIRECTORY_NOT_FOUND | 目录未找到 |
| 7007 | DIRECTORY_NOT_WRITABLE | 目录不可写 |
| 7008 | DISK_SPACE_INSUFFICIENT | 磁盘空间不足 |
| 7009 | INODE_EXHAUSTED | 文件系统 inode 限制已达 |
| 7010 | SYMLINK_LOOP | 检测到符号链接循环 |
| 7011 | PATH_TOO_LONG | 文件路径过长 |
| 7012 | INVALID_PATH | 文件路径无效 |
| 7013 | DATABASE_LOCKED | 数据库已锁定 |
| 7014 | DATABASE_CORRUPTED | 数据库已损坏 |
| 7015 | DATABASE_MIGRATION_FAILED | 数据库迁移失败 |
| 7016 | PARQUET_WRITE_ERROR | Parquet 写入错误 |
| 7017 | PARQUET_READ_ERROR | Parquet 读取错误 |
| 7018 | PARQUET_SCHEMA_ERROR | Parquet 模式错误 |
| 7019 | CSV_PARSE_ERROR | CSV 解析错误 |
| 7020 | JSON_PARSE_ERROR | JSON 解析错误 |

### 8xxx: 认证/授权错误 (Authentication/Authorization Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 8001 | AUTH_TOKEN_MISSING | 认证令牌缺失 |
| 8002 | AUTH_TOKEN_EXPIRED | 认证令牌已过期 |
| 8003 | AUTH_TOKEN_INVALID | 认证令牌无效 |
| 8004 | AUTH_TOKEN_REVOKED | 认证令牌已被撤销 |
| 8005 | AUTH_INSUFFICIENT_PERMISSIONS | 权限不足 |
| 8006 | AUTH_ACCOUNT_LOCKED | 账户已被锁定 |
| 8007 | AUTH_RATE_LIMIT_EXCEEDED | 认证频率限制已超出 |
| 8008 | AUTH_IP_NOT_ALLOWED | IP 地址不允许 |
| 8009 | AUTH_2FA_REQUIRED | 需要双因素认证 |
| 8010 | AUTH_SESSION_EXPIRED | 会话已过期 |
| 8011 | AUTH_CREDENTIALS_INVALID | 凭证无效 |
| 8012 | AUTH_API_KEY_INVALID | API 密钥无效 |
| 8013 | AUTH_SUBSCRIPTION_EXPIRED | 订阅已过期 |
| 8014 | AUTH_PLAN_LIMIT_REACHED | 计划限制已达 |

### 9xxx: 并发/频率限制错误 (Concurrency/Rate Limiting Errors)

| 错误码 | 常量名 | 描述 |
|--------|--------|------|
| 9001 | RATE_LIMIT_GLOBAL | 全局频率限制已超出 |
| 9002 | RATE_LIMIT_PER_SOURCE | 单数据源频率限制已超出 |
| 9003 | RATE_LIMIT_PER_MINUTE | 每分钟频率限制已超出 |
| 9004 | RATE_LIMIT_PER_HOUR | 每小时频率限制已超出 |
| 9005 | RATE_LIMIT_PER_DAY | 每天频率限制已超出 |
| 9006 | CONCURRENT_REQUEST_LIMIT | 并发请求限制已超出 |
| 9007 | QUEUE_FULL | 请求队列已满 |
| 9008 | QUEUE_TIMEOUT | 请求队列超时 |
| 9009 | BACKPRESSURE_TRIGGERED | 触发背压机制 |
| 9010 | CIRCUIT_BREAKER_OPEN | 熔断器已打开 |

## ErrorCode 工具方法

### get_category()

获取错误码所属的类别名称。

```python
from akshare_data.core.errors import ErrorCode

category = ErrorCode.get_category(ErrorCode.SOURCE_TIMEOUT)
# 返回: "data_source"

category = ErrorCode.get_category(ErrorCode.CACHE_MISS)
# 返回: "cache"
```

**类别映射表：**

| 错误码前缀 | 类别名称 |
|------------|----------|
| 1xxx | data_source |
| 2xxx | cache |
| 3xxx | parameter |
| 4xxx | network |
| 5xxx | data_quality |
| 6xxx | system |
| 7xxx | storage |
| 8xxx | authentication |
| 9xxx | rate_limit |

### get_message()

获取错误码的人类可读消息。

```python
from akshare_data.core.errors import ErrorCode

message = ErrorCode.get_message(ErrorCode.SOURCE_TIMEOUT)
# 返回: "Request to data source timed out"

message = ErrorCode.get_message(ErrorCode.INVALID_SYMBOL)
# 返回: "Invalid stock symbol format"
```

## 异常类层次结构

所有异常类继承自 `DataAccessException` 基类，提供统一的错误上下文信息。

```
Exception
└── DataAccessException (基类)
    ├── DataSourceError (数据源错误)
    │   ├── SourceUnavailableError (数据源不可用)
    │   ├── NoDataError (无可用数据)
    │   ├── TimeoutError (请求超时)
    │   └── RateLimitError (频率限制)
    ├── CacheError (缓存错误)
    ├── ValidationError (验证错误)
    ├── DataQualityError (数据质量错误)
    ├── StorageError (存储错误)
    ├── AuthError (认证错误)
    ├── NetworkError (网络错误)
    └── SystemError (系统错误)
```

### DataAccessException (基类)

所有数据访问异常的基类。

**属性：**

| 属性 | 类型 | 描述 |
|------|------|------|
| message | str | 错误消息 |
| error_code | ErrorCode | 错误码枚举值 |
| source | str | 数据源名称（可选） |
| symbol | str | 股票符号（可选） |

**方法：**

| 方法 | 返回类型 | 描述 |
|------|----------|------|
| to_dict() | dict | 将异常转换为字典，用于日志记录/序列化 |

**to_dict() 返回结构：**

```python
{
    "error_code": "1002_SOURCE_TIMEOUT",
    "message": "Request timed out after 30s",
    "source": "akshare",
    "symbol": "600519",
    "category": "data_source",
    "human_message": "Request to data source timed out"
}
```

### DataSourceError

通用数据源错误，向后兼容的别名。

```python
raise DataSourceError(
    message="Failed to fetch data",
    error_code=ErrorCode.SOURCE_UNAVAILABLE,
    source="akshare",
    symbol="600519"
)
```

### SourceUnavailableError

数据源不可用或无法连接时抛出。

**默认错误码：** `ErrorCode.SOURCE_UNAVAILABLE`

```python
raise SourceUnavailableError(
    message="Akshare API is down",
    source="akshare"
)
```

### NoDataError

请求的查询无可用数据时抛出。

**默认错误码：** `ErrorCode.NO_DATA`

```python
raise NoDataError(
    message="No trading data for delisted stock",
    symbol="000001",
    source="akshare"
)
```

### TimeoutError

数据源请求超时时抛出。

**默认错误码：** `ErrorCode.SOURCE_TIMEOUT`

```python
raise TimeoutError(
    message="Request timed out after 30s",
    source="akshare",
    symbol="600519"
)
```

### RateLimitError

数据源频率限制被触发时抛出。

**默认错误码：** `ErrorCode.SOURCE_RATE_LIMITED`

```python
raise RateLimitError(
    message="Rate limit: 100 requests/minute exceeded",
    source="akshare"
)
```

### CacheError

缓存操作失败时抛出。

**默认错误码：** `ErrorCode.CACHE_MISS`

```python
raise CacheError(
    message="Failed to write to cache",
    error_code=ErrorCode.CACHE_WRITE_FAILED,
    source="redis"
)
```

### ValidationError

输入验证失败时抛出。

**默认错误码：** `ErrorCode.INVALID_PARAMETER`

```python
raise ValidationError(
    message="Date format must be YYYY-MM-DD",
    error_code=ErrorCode.INVALID_DATE_FORMAT
)
```

### DataQualityError

检测到数据质量问题时抛出。

**默认错误码：** `ErrorCode.INVALID_DATA`

```python
raise DataQualityError(
    message="Negative volume detected in trading data",
    error_code=ErrorCode.NEGATIVE_VOLUME,
    symbol="600519"
)
```

### StorageError

存储操作失败时抛出。

**默认错误码：** `ErrorCode.FILE_NOT_FOUND`

```python
raise StorageError(
    message="Parquet file corrupted",
    error_code=ErrorCode.PARQUET_READ_ERROR,
    source="local_storage"
)
```

### AuthError

认证或授权失败时抛出。

**默认错误码：** `ErrorCode.AUTH_TOKEN_INVALID`

```python
raise AuthError(
    message="API key has expired",
    error_code=ErrorCode.AUTH_API_KEY_INVALID
)
```

### NetworkError

网络操作失败时抛出。

**默认错误码：** `ErrorCode.NETWORK_TIMEOUT`

```python
raise NetworkError(
    message="Connection reset by peer",
    error_code=ErrorCode.NETWORK_RESET_BY_PEER
)
```

### SystemError

系统级错误时抛出。

**默认错误码：** `ErrorCode.INTERNAL_ERROR`

```python
raise SystemError(
    message="Unexpected null reference",
    error_code=ErrorCode.UNEXPECTED_STATE
)
```

## 错误使用模式

### 数据源中的错误抛出

数据源实现应使用专用异常类，附带完整的上下文信息：

```python
from akshare_data.core.errors import (
    DataSourceError,
    SourceUnavailableError,
    TimeoutError,
    RateLimitError,
    NoDataError,
    ErrorCode,
)

class AkshareSource:
    def fetch_daily(self, symbol: str, start_date: str, end_date: str):
        try:
            response = self._request(symbol, start_date, end_date)
        except requests.Timeout:
            raise TimeoutError(
                message=f"Request timed out for {symbol}",
                source="akshare",
                symbol=symbol
            )
        except requests.ConnectionError:
            raise SourceUnavailableError(
                message=f"Cannot connect to akshare for {symbol}",
                source="akshare",
                symbol=symbol
            )

        if response.status_code == 429:
            raise RateLimitError(
                message="Akshare rate limit exceeded",
                source="akshare"
            )

        if response.status_code == 404:
            raise NoDataError(
                message=f"No data found for {symbol}",
                error_code=ErrorCode.SOURCE_HTTP_404,
                source="akshare",
                symbol=symbol
            )

        if not response.text:
            raise DataSourceError(
                message="Empty response from akshare",
                error_code=ErrorCode.SOURCE_RESPONSE_EMPTY,
                source="akshare",
                symbol=symbol
            )

        return self._parse_response(response)
```

### 路由器的错误处理

数据路由器实现故障转移和熔断机制：

```python
from akshare_data.core.errors import (
    DataAccessException,
    SourceUnavailableError,
    TimeoutError,
    ErrorCode,
)

class DataRouter:
    def fetch_with_failover(self, symbol: str, **kwargs):
        errors = []

        for source in self.available_sources:
            try:
                return source.fetch(symbol, **kwargs)
            except (SourceUnavailableError, TimeoutError) as e:
                errors.append(e.to_dict())
                self.circuit_breaker.record_failure(source.name)
                continue
            except DataAccessException as e:
                # 非可恢复错误，直接抛出
                raise

        # 所有数据源都失败
        raise SourceUnavailableError(
            message=f"All data sources failed for {symbol}",
            error_code=ErrorCode.SOURCE_UNAVAILABLE,
            symbol=symbol,
        )

    def is_circuit_open(self, source_name: str) -> bool:
        """检查熔断器是否已打开"""
        failures = self.circuit_breaker.get_failure_count(source_name)
        return failures >= self.circuit_breaker.threshold
```

### 错误上下文附加

所有异常都支持附加 `source` 和 `symbol` 上下文，便于问题诊断：

```python
# 完整上下文示例
try:
    data = fetcher.get_daily_kline("600519", "2024-01-01", "2024-12-31")
except DataAccessException as e:
    error_info = e.to_dict()
    logger.error(
        f"Data fetch failed: {error_info['message']}",
        extra={
            "error_code": error_info["error_code"],
            "category": error_info["category"],
            "source": error_info["source"],
            "symbol": error_info["symbol"],
        }
    )
    # 根据错误类别采取不同策略
    if error_info["category"] == "data_source":
        # 尝试备用数据源
        self.failover(error_info["symbol"])
    elif error_info["category"] == "parameter":
        # 参数错误，通知调用方
        raise ValidationError(error_info["message"])
    elif error_info["category"] == "rate_limit":
        # 频率限制，等待后重试
        self.wait_and_retry()
```

### 异常捕获最佳实践

```python
from akshare_data.core.errors import (
    DataAccessException,
    ValidationError,
    DataSourceError,
    CacheError,
)

def process_stock_data(symbol: str):
    try:
        # 参数验证错误 - 不应重试
        validate_symbol(symbol)

        # 尝试从缓存获取
        try:
            return cache.get(symbol)
        except CacheError:
            # 缓存未命中，从数据源获取
            pass

        # 从数据源获取
        data = source.fetch(symbol)

    except ValidationError as e:
        # 验证错误，直接返回给调用方
        raise

    except DataSourceError as e:
        # 数据源错误，记录并尝试恢复
        logger.warning(f"Source error: {e.to_dict()}")
        raise

    except DataAccessException as e:
        # 捕获所有其他数据访问异常
        logger.error(f"Unexpected error: {e.to_dict()}")
        raise
```

## 错误码统计

| 类别 | 错误码范围 | 数量 |
|------|------------|------|
| 数据源错误 | 1001-1025 | 25 |
| 缓存错误 | 2001-2018 | 18 |
| 参数/验证错误 | 3001-3028 | 28 |
| 网络错误 | 4001-4014 | 14 |
| 数据质量错误 | 5001-5028 | 28 |
| 系统/内部错误 | 6001-6020 | 20 |
| 存储/文件错误 | 7001-7020 | 20 |
| 认证/授权错误 | 8001-8014 | 14 |
| 并发/频率限制错误 | 9001-9010 | 10 |
| **总计** | | **177** |

## 扩展

### 添加新错误码

在 `ErrorCode` 枚举的相应类别中添加新常量，`get_category()` 和 `get_message()` 方法中会自动通过枚举值解析。

### 添加新异常类

继承 `DataAccessException` 并提供默认错误码即可。
