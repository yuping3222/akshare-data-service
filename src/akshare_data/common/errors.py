"""Error codes and exception hierarchy for data access layer.

Provides structured error handling with error codes for better
error classification and reporting.

This is the canonical location for error definitions.
The old `akshare_data.core.errors` is a compatibility shell.
"""

from enum import Enum
from typing import Optional


class ErrorCode(Enum):
    """Standardized error codes for data access operations.

    Categories:
    - 1xxx: Data source errors
    - 2xxx: Cache errors
    - 3xxx: Parameter/validation errors
    - 4xxx: Network errors
    - 5xxx: Data quality errors
    - 6xxx: System/internal errors
    - 7xxx: Storage/file errors
    - 8xxx: Authentication/authorization errors
    - 9xxx: Concurrency/rate limiting errors
    """

    # ── 1xxx: Data Source Errors ──────────────────────────────────
    SOURCE_UNAVAILABLE = "1001_SOURCE_UNAVAILABLE"
    SOURCE_TIMEOUT = "1002_SOURCE_TIMEOUT"
    SOURCE_RATE_LIMITED = "1003_SOURCE_RATE_LIMITED"
    SOURCE_AUTH_FAILED = "1004_SOURCE_AUTH_FAILED"
    SOURCE_CONNECTION_REFUSED = "1005_SOURCE_CONNECTION_REFUSED"
    SOURCE_DNS_ERROR = "1006_SOURCE_DNS_ERROR"
    SOURCE_SSL_ERROR = "1007_SOURCE_SSL_ERROR"
    SOURCE_HTTP_ERROR = "1008_SOURCE_HTTP_ERROR"
    SOURCE_HTTP_400 = "1009_SOURCE_HTTP_400"
    SOURCE_HTTP_401 = "1010_SOURCE_HTTP_401"
    SOURCE_HTTP_403 = "1011_SOURCE_HTTP_403"
    SOURCE_HTTP_404 = "1012_SOURCE_HTTP_404"
    SOURCE_HTTP_429 = "1013_SOURCE_HTTP_429"
    SOURCE_HTTP_500 = "1014_SOURCE_HTTP_500"
    SOURCE_HTTP_502 = "1015_SOURCE_HTTP_502"
    SOURCE_HTTP_503 = "1016_SOURCE_HTTP_503"
    SOURCE_HTTP_504 = "1017_SOURCE_HTTP_504"
    SOURCE_DEPRECATED = "1018_SOURCE_DEPRECATED"
    SOURCE_MAINTENANCE = "1019_SOURCE_MAINTENANCE"
    SOURCE_QUOTA_EXCEEDED = "1020_SOURCE_QUOTA_EXCEEDED"
    SOURCE_IP_BLOCKED = "1021_SOURCE_IP_BLOCKED"
    SOURCE_ACCOUNT_SUSPENDED = "1022_SOURCE_ACCOUNT_SUSPENDED"
    SOURCE_API_VERSION_ERROR = "1023_SOURCE_API_VERSION_ERROR"
    SOURCE_RESPONSE_EMPTY = "1024_SOURCE_RESPONSE_EMPTY"
    SOURCE_RESPONSE_TRUNCATED = "1025_SOURCE_RESPONSE_TRUNCATED"

    # ── 2xxx: Cache Errors ────────────────────────────────────────
    CACHE_MISS = "2001_CACHE_MISS"
    CACHE_CORRUPTED = "2002_CACHE_CORRUPTED"
    CACHE_WRITE_FAILED = "2003_CACHE_WRITE_FAILED"
    CACHE_READ_FAILED = "2004_CACHE_READ_FAILED"
    CACHE_KEY_NOT_FOUND = "2005_CACHE_KEY_NOT_FOUND"
    CACHE_EXPIRED = "2006_CACHE_EXPIRED"
    CACHE_INVALIDATION_FAILED = "2007_CACHE_INVALIDATION_FAILED"
    CACHE_SERIALIZATION_ERROR = "2008_CACHE_SERIALIZATION_ERROR"
    CACHE_DESERIALIZATION_ERROR = "2009_CACHE_DESERIALIZATION_ERROR"
    CACHE_MEMORY_FULL = "2010_CACHE_MEMORY_FULL"
    CACHE_DISK_FULL = "2011_CACHE_DISK_FULL"
    CACHE_LOCK_TIMEOUT = "2012_CACHE_LOCK_TIMEOUT"
    CACHE_LOCK_ACQUISITION_FAILED = "2013_CACHE_LOCK_ACQUISITION_FAILED"
    CACHE_COMPACT_FAILED = "2014_CACHE_COMPACT_FAILED"
    CACHE_PARTITION_ERROR = "2015_CACHE_PARTITION_ERROR"
    CACHE_STALE_DATA = "2016_CACHE_STALE_DATA"
    CACHE_VERSION_MISMATCH = "2017_CACHE_VERSION_MISMATCH"
    CACHE_SCHEMA_MISMATCH = "2018_CACHE_SCHEMA_MISMATCH"

    # ── 3xxx: Parameter/Validation Errors ─────────────────────────
    INVALID_SYMBOL = "3001_INVALID_SYMBOL"
    INVALID_DATE_RANGE = "3002_INVALID_DATE_RANGE"
    INVALID_PARAMETER = "3003_INVALID_PARAMETER"
    MISSING_PARAMETER = "3004_MISSING_PARAMETER"
    INVALID_DATE_FORMAT = "3005_INVALID_DATE_FORMAT"
    INVALID_FREQUENCY = "3006_INVALID_FREQUENCY"
    INVALID_ADJUST_TYPE = "3007_INVALID_ADJUST_TYPE"
    INVALID_DATA_SOURCE = "3008_INVALID_DATA_SOURCE"
    INVALID_TABLE_NAME = "3009_INVALID_TABLE_NAME"
    INVALID_COLUMN_NAME = "3010_INVALID_COLUMN_NAME"
    INVALID_SORT_ORDER = "3011_INVALID_SORT_ORDER"
    INVALID_LIMIT_VALUE = "3012_INVALID_LIMIT_VALUE"
    INVALID_TIMEZONE = "3013_INVALID_TIMEZONE"
    INVALID_CURRENCY = "3014_INVALID_CURRENCY"
    INVALID_EXCHANGE = "3015_INVALID_EXCHANGE"
    INVALID_ASSET_TYPE = "3016_INVALID_ASSET_TYPE"
    INVALID_FIELD_TYPE = "3017_INVALID_FIELD_TYPE"
    INVALID_CONFIG = "3018_INVALID_CONFIG"
    INVALID_JSON = "3019_INVALID_JSON"
    INVALID_CSV_FORMAT = "3020_INVALID_CSV_FORMAT"
    INVALID_PARQUET_FILE = "3021_INVALID_PARQUET_FILE"
    DUPLICATE_PARAMETER = "3022_DUPLICATE_PARAMETER"
    PARAMETER_OUT_OF_RANGE = "3023_PARAMETER_OUT_OF_RANGE"
    PARAMETER_TYPE_MISMATCH = "3024_PARAMETER_TYPE_MISMATCH"
    SYMBOL_NOT_IN_UNIVERSE = "3025_SYMBOL_NOT_IN_UNIVERSE"
    DATE_IN_FUTURE = "3026_DATE_IN_FUTURE"
    DATE_BEFORE_MARKET_OPEN = "3027_DATE_BEFORE_MARKET_OPEN"
    START_AFTER_END = "3028_START_AFTER_END"

    # ── 4xxx: Network Errors ──────────────────────────────────────
    NETWORK_TIMEOUT = "4001_NETWORK_TIMEOUT"
    NETWORK_CONNECTION_LOST = "4002_NETWORK_CONNECTION_LOST"
    NETWORK_DNS_FAILURE = "4003_NETWORK_DNS_FAILURE"
    NETWORK_PROXY_ERROR = "4004_NETWORK_PROXY_ERROR"
    NETWORK_SOCKET_ERROR = "4005_NETWORK_SOCKET_ERROR"
    NETWORK_RESET_BY_PEER = "4006_NETWORK_RESET_BY_PEER"
    NETWORK_NO_ROUTE_TO_HOST = "4007_NETWORK_NO_ROUTE_TO_HOST"
    NETWORK_UNREACHABLE = "4008_NETWORK_UNREACHABLE"
    NETWORK_TOO_MANY_REDIRECTS = "4009_NETWORK_TOO_MANY_REDIRECTS"
    NETWORK_BANDWIDTH_EXCEEDED = "4010_NETWORK_BANDWIDTH_EXCEEDED"
    NETWORK_CONGESTION = "4011_NETWORK_CONGESTION"
    NETWORK_TLS_HANDSHAKE_FAILED = "4012_NETWORK_TLS_HANDSHAKE_FAILED"
    NETWORK_CERTIFICATE_EXPIRED = "4013_NETWORK_CERTIFICATE_EXPIRED"
    NETWORK_CERTIFICATE_UNTRUSTED = "4014_NETWORK_CERTIFICATE_UNTRUSTED"

    # ── 5xxx: Data Quality Errors ─────────────────────────────────
    NO_DATA = "5001_NO_DATA"
    INVALID_DATA = "5002_INVALID_DATA"
    DATA_FORMAT_ERROR = "5003_DATA_FORMAT_ERROR"
    MISSING_COLUMNS = "5004_MISSING_COLUMNS"
    DUPLICATE_ROWS = "5005_DUPLICATE_ROWS"
    NULL_VALUES_DETECTED = "5006_NULL_VALUES_DETECTED"
    OUTLIER_DETECTED = "5007_OUTLIER_DETECTED"
    DATA_INCONSISTENCY = "5008_DATA_INCONSISTENCY"
    DATA_TRUNCATION = "5009_DATA_TRUNCATION"
    DATA_ENCODING_ERROR = "5010_DATA_ENCODING_ERROR"
    SCHEMA_MISMATCH = "5011_SCHEMA_MISMATCH"
    COLUMN_TYPE_MISMATCH = "5012_COLUMN_TYPE_MISMATCH"
    MISSING_REQUIRED_COLUMN = "5013_MISSING_REQUIRED_COLUMN"
    DATA_RANGE_ERROR = "5014_DATA_RANGE_ERROR"
    NEGATIVE_VOLUME = "5015_NEGATIVE_VOLUME"
    NEGATIVE_PRICE = "5016_NEGATIVE_PRICE"
    ZERO_PRICE = "5017_ZERO_PRICE"
    PRICE_ANOMALY = "5018_PRICE_ANOMALY"
    VOLUME_ANOMALY = "5019_VOLUME_ANOMALY"
    TIMESTAMP_ERROR = "5020_TIMESTAMP_ERROR"
    FUTURE_TIMESTAMP = "5021_FUTURE_TIMESTAMP"
    DUPLICATE_TIMESTAMP = "5022_DUPLICATE_TIMESTAMP"
    GAP_IN_TIMESERIES = "5023_GAP_IN_TIMESERIES"
    INCOMPLETE_TRADING_DAY = "5024_INCOMPLETE_TRADING_DAY"
    SPLIT_ADJUSTMENT_ERROR = "5025_SPLIT_ADJUSTMENT_ERROR"
    DIVIDEND_ADJUSTMENT_ERROR = "5026_DIVIDEND_ADJUSTMENT_ERROR"
    SUSPENSION_NOT_HANDLED = "5027_SUSPENSION_NOT_HANDLED"
    DELISTED_SYMBOL_DATA = "5028_DELISTED_SYMBOL_DATA"

    # ── 6xxx: System/Internal Errors ──────────────────────────────
    INTERNAL_ERROR = "6001_INTERNAL_ERROR"
    NOT_IMPLEMENTED = "6002_NOT_IMPLEMENTED"
    CONFIGURATION_ERROR = "6003_CONFIGURATION_ERROR"
    DEPENDENCY_ERROR = "6004_DEPENDENCY_ERROR"
    IMPORT_ERROR = "6005_IMPORT_ERROR"
    VERSION_MISMATCH = "6006_VERSION_MISMATCH"
    MEMORY_ERROR = "6007_MEMORY_ERROR"
    STACK_OVERFLOW = "6008_STACK_OVERFLOW"
    RECURSION_LIMIT = "6009_RECURSION_LIMIT"
    THREAD_ERROR = "6010_THREAD_ERROR"
    PROCESS_ERROR = "6011_PROCESS_ERROR"
    SIGNAL_INTERRUPTED = "6012_SIGNAL_INTERRUPTED"
    GRACEFUL_SHUTDOWN = "6013_GRACEFUL_SHUTDOWN"
    UNEXPECTED_STATE = "6014_UNEXPECTED_STATE"
    RACE_CONDITION = "6015_RACE_CONDITION"
    DEADLOCK_DETECTED = "6016_DEADLOCK_DETECTED"
    RESOURCE_LEAK = "6017_RESOURCE_LEAK"
    PLUGIN_ERROR = "6018_PLUGIN_ERROR"
    EXTENSION_ERROR = "6019_EXTENSION_ERROR"
    MIGRATION_ERROR = "6020_MIGRATION_ERROR"

    # ── 7xxx: Storage/File Errors ─────────────────────────────────
    FILE_NOT_FOUND = "7001_FILE_NOT_FOUND"
    FILE_PERMISSION_DENIED = "7002_FILE_PERMISSION_DENIED"
    FILE_ALREADY_EXISTS = "7003_FILE_ALREADY_EXISTS"
    FILE_CORRUPTED = "7004_FILE_CORRUPTED"
    FILE_TOO_LARGE = "7005_FILE_TOO_LARGE"
    DIRECTORY_NOT_FOUND = "7006_DIRECTORY_NOT_FOUND"
    DIRECTORY_NOT_WRITABLE = "7007_DIRECTORY_NOT_WRITABLE"
    DISK_SPACE_INSUFFICIENT = "7008_DISK_SPACE_INSUFFICIENT"
    INODE_EXHAUSTED = "7009_INODE_EXHAUSTED"
    SYMLINK_LOOP = "7010_SYMLINK_LOOP"
    PATH_TOO_LONG = "7011_PATH_TOO_LONG"
    INVALID_PATH = "7012_INVALID_PATH"
    DATABASE_LOCKED = "7013_DATABASE_LOCKED"
    DATABASE_CORRUPTED = "7014_DATABASE_CORRUPTED"
    DATABASE_MIGRATION_FAILED = "7015_DATABASE_MIGRATION_FAILED"
    PARQUET_WRITE_ERROR = "7016_PARQUET_WRITE_ERROR"
    PARQUET_READ_ERROR = "7017_PARQUET_READ_ERROR"
    PARQUET_SCHEMA_ERROR = "7018_PARQUET_SCHEMA_ERROR"
    CSV_PARSE_ERROR = "7019_CSV_PARSE_ERROR"
    JSON_PARSE_ERROR = "7020_JSON_PARSE_ERROR"

    # ── 8xxx: Authentication/Authorization Errors ─────────────────
    AUTH_TOKEN_MISSING = "8001_AUTH_TOKEN_MISSING"
    AUTH_TOKEN_EXPIRED = "8002_AUTH_TOKEN_EXPIRED"
    AUTH_TOKEN_INVALID = "8003_AUTH_TOKEN_INVALID"
    AUTH_TOKEN_REVOKED = "8004_AUTH_TOKEN_REVOKED"
    AUTH_INSUFFICIENT_PERMISSIONS = "8005_AUTH_INSUFFICIENT_PERMISSIONS"
    AUTH_ACCOUNT_LOCKED = "8006_AUTH_ACCOUNT_LOCKED"
    AUTH_RATE_LIMIT_EXCEEDED = "8007_AUTH_RATE_LIMIT_EXCEEDED"
    AUTH_IP_NOT_ALLOWED = "8008_AUTH_IP_NOT_ALLOWED"
    AUTH_2FA_REQUIRED = "8009_AUTH_2FA_REQUIRED"
    AUTH_SESSION_EXPIRED = "8010_AUTH_SESSION_EXPIRED"
    AUTH_CREDENTIALS_INVALID = "8011_AUTH_CREDENTIALS_INVALID"
    AUTH_API_KEY_INVALID = "8012_AUTH_API_KEY_INVALID"
    AUTH_SUBSCRIPTION_EXPIRED = "8013_AUTH_SUBSCRIPTION_EXPIRED"
    AUTH_PLAN_LIMIT_REACHED = "8014_AUTH_PLAN_LIMIT_REACHED"

    # ── 9xxx: Concurrency/Rate Limiting Errors ────────────────────
    RATE_LIMIT_GLOBAL = "9001_RATE_LIMIT_GLOBAL"
    RATE_LIMIT_PER_SOURCE = "9002_RATE_LIMIT_PER_SOURCE"
    RATE_LIMIT_PER_MINUTE = "9003_RATE_LIMIT_PER_MINUTE"
    RATE_LIMIT_PER_HOUR = "9004_RATE_LIMIT_PER_HOUR"
    RATE_LIMIT_PER_DAY = "9005_RATE_LIMIT_PER_DAY"
    CONCURRENT_REQUEST_LIMIT = "9006_CONCURRENT_REQUEST_LIMIT"
    QUEUE_FULL = "9007_QUEUE_FULL"
    QUEUE_TIMEOUT = "9008_QUEUE_TIMEOUT"
    BACKPRESSURE_TRIGGERED = "9009_BACKPRESSURE_TRIGGERED"
    CIRCUIT_BREAKER_OPEN = "9010_CIRCUIT_BREAKER_OPEN"

    @classmethod
    def get_category(cls, code: "ErrorCode") -> str:
        """Get the category name for an error code."""
        prefix = code.value.split("_")[0]
        categories = {
            "1001": "data_source",
            "1002": "data_source",
            "1003": "data_source",
            "1004": "data_source",
            "1005": "data_source",
            "1006": "data_source",
            "1007": "data_source",
            "1008": "data_source",
            "1009": "data_source",
            "1010": "data_source",
            "1011": "data_source",
            "1012": "data_source",
            "1013": "data_source",
            "1014": "data_source",
            "1015": "data_source",
            "1016": "data_source",
            "1017": "data_source",
            "1018": "data_source",
            "1019": "data_source",
            "1020": "data_source",
            "1021": "data_source",
            "1022": "data_source",
            "1023": "data_source",
            "1024": "data_source",
            "1025": "data_source",
            "2001": "cache",
            "2002": "cache",
            "2003": "cache",
            "2004": "cache",
            "2005": "cache",
            "2006": "cache",
            "2007": "cache",
            "2008": "cache",
            "2009": "cache",
            "2010": "cache",
            "2011": "cache",
            "2012": "cache",
            "2013": "cache",
            "2014": "cache",
            "2015": "cache",
            "2016": "cache",
            "2017": "cache",
            "2018": "cache",
            "3001": "parameter",
            "3002": "parameter",
            "3003": "parameter",
            "3004": "parameter",
            "3005": "parameter",
            "3006": "parameter",
            "3007": "parameter",
            "3008": "parameter",
            "3009": "parameter",
            "3010": "parameter",
            "3011": "parameter",
            "3012": "parameter",
            "3013": "parameter",
            "3014": "parameter",
            "3015": "parameter",
            "3016": "parameter",
            "3017": "parameter",
            "3018": "parameter",
            "3019": "parameter",
            "3020": "parameter",
            "3021": "parameter",
            "3022": "parameter",
            "3023": "parameter",
            "3024": "parameter",
            "3025": "parameter",
            "3026": "parameter",
            "3027": "parameter",
            "3028": "parameter",
            "4001": "network",
            "4002": "network",
            "4003": "network",
            "4004": "network",
            "4005": "network",
            "4006": "network",
            "4007": "network",
            "4008": "network",
            "4009": "network",
            "4010": "network",
            "4011": "network",
            "4012": "network",
            "4013": "network",
            "4014": "network",
            "5001": "data_quality",
            "5002": "data_quality",
            "5003": "data_quality",
            "5004": "data_quality",
            "5005": "data_quality",
            "5006": "data_quality",
            "5007": "data_quality",
            "5008": "data_quality",
            "5009": "data_quality",
            "5010": "data_quality",
            "5011": "data_quality",
            "5012": "data_quality",
            "5013": "data_quality",
            "5014": "data_quality",
            "5015": "data_quality",
            "5016": "data_quality",
            "5017": "data_quality",
            "5018": "data_quality",
            "5019": "data_quality",
            "5020": "data_quality",
            "5021": "data_quality",
            "5022": "data_quality",
            "5023": "data_quality",
            "5024": "data_quality",
            "5025": "data_quality",
            "5026": "data_quality",
            "5027": "data_quality",
            "5028": "data_quality",
            "6001": "system",
            "6002": "system",
            "6003": "system",
            "6004": "system",
            "6005": "system",
            "6006": "system",
            "6007": "system",
            "6008": "system",
            "6009": "system",
            "6010": "system",
            "6011": "system",
            "6012": "system",
            "6013": "system",
            "6014": "system",
            "6015": "system",
            "6016": "system",
            "6017": "system",
            "6018": "system",
            "6019": "system",
            "6020": "system",
            "7001": "storage",
            "7002": "storage",
            "7003": "storage",
            "7004": "storage",
            "7005": "storage",
            "7006": "storage",
            "7007": "storage",
            "7008": "storage",
            "7009": "storage",
            "7010": "storage",
            "7011": "storage",
            "7012": "storage",
            "7013": "storage",
            "7014": "storage",
            "7015": "storage",
            "7016": "storage",
            "7017": "storage",
            "7018": "storage",
            "7019": "storage",
            "7020": "storage",
            "8001": "authentication",
            "8002": "authentication",
            "8003": "authentication",
            "8004": "authentication",
            "8005": "authentication",
            "8006": "authentication",
            "8007": "authentication",
            "8008": "authentication",
            "8009": "authentication",
            "8010": "authentication",
            "8011": "authentication",
            "8012": "authentication",
            "8013": "authentication",
            "8014": "authentication",
            "9001": "rate_limit",
            "9002": "rate_limit",
            "9003": "rate_limit",
            "9004": "rate_limit",
            "9005": "rate_limit",
            "9006": "rate_limit",
            "9007": "rate_limit",
            "9008": "rate_limit",
            "9009": "rate_limit",
            "9010": "rate_limit",
        }
        return categories.get(prefix, "unknown")

    @classmethod
    def get_message(cls, code: "ErrorCode") -> str:
        """Get a human-readable message for an error code."""
        messages = {
            cls.SOURCE_UNAVAILABLE: "Data source is currently unavailable",
            cls.SOURCE_TIMEOUT: "Request to data source timed out",
            cls.SOURCE_RATE_LIMITED: "Data source rate limit exceeded",
            cls.SOURCE_AUTH_FAILED: "Authentication failed for data source",
            cls.SOURCE_CONNECTION_REFUSED: "Connection refused by data source",
            cls.SOURCE_DNS_ERROR: "DNS resolution failed for data source",
            cls.SOURCE_SSL_ERROR: "SSL/TLS error connecting to data source",
            cls.SOURCE_HTTP_ERROR: "HTTP error from data source",
            cls.SOURCE_HTTP_400: "Bad request to data source (HTTP 400)",
            cls.SOURCE_HTTP_401: "Unauthorized access to data source (HTTP 401)",
            cls.SOURCE_HTTP_403: "Forbidden access to data source (HTTP 403)",
            cls.SOURCE_HTTP_404: "Resource not found on data source (HTTP 404)",
            cls.SOURCE_HTTP_429: "Too many requests to data source (HTTP 429)",
            cls.SOURCE_HTTP_500: "Internal server error from data source (HTTP 500)",
            cls.SOURCE_HTTP_502: "Bad gateway from data source (HTTP 502)",
            cls.SOURCE_HTTP_503: "Service unavailable from data source (HTTP 503)",
            cls.SOURCE_HTTP_504: "Gateway timeout from data source (HTTP 504)",
            cls.SOURCE_DEPRECATED: "Data source API has been deprecated",
            cls.SOURCE_MAINTENANCE: "Data source is under maintenance",
            cls.SOURCE_QUOTA_EXCEEDED: "Data source quota exceeded",
            cls.SOURCE_IP_BLOCKED: "IP address blocked by data source",
            cls.SOURCE_ACCOUNT_SUSPENDED: "Account suspended by data source",
            cls.SOURCE_API_VERSION_ERROR: "API version not supported by data source",
            cls.SOURCE_RESPONSE_EMPTY: "Data source returned empty response",
            cls.SOURCE_RESPONSE_TRUNCATED: "Data source response was truncated",
            cls.CACHE_MISS: "Requested data not found in cache",
            cls.CACHE_CORRUPTED: "Cache data is corrupted",
            cls.CACHE_WRITE_FAILED: "Failed to write data to cache",
            cls.CACHE_READ_FAILED: "Failed to read data from cache",
            cls.CACHE_KEY_NOT_FOUND: "Cache key not found",
            cls.CACHE_EXPIRED: "Cached data has expired",
            cls.CACHE_INVALIDATION_FAILED: "Failed to invalidate cache entry",
            cls.CACHE_SERIALIZATION_ERROR: "Failed to serialize data for cache",
            cls.CACHE_DESERIALIZATION_ERROR: "Failed to deserialize data from cache",
            cls.CACHE_MEMORY_FULL: "Cache memory limit reached",
            cls.CACHE_DISK_FULL: "Cache disk space exhausted",
            cls.CACHE_LOCK_TIMEOUT: "Timeout acquiring cache lock",
            cls.CACHE_LOCK_ACQUISITION_FAILED: "Failed to acquire cache lock",
            cls.CACHE_COMPACT_FAILED: "Cache compaction failed",
            cls.CACHE_PARTITION_ERROR: "Cache partition error",
            cls.CACHE_STALE_DATA: "Cached data may be stale",
            cls.CACHE_VERSION_MISMATCH: "Cache version mismatch",
            cls.CACHE_SCHEMA_MISMATCH: "Cached data schema mismatch",
            cls.INVALID_SYMBOL: "Invalid stock symbol format",
            cls.INVALID_DATE_RANGE: "Invalid date range specified",
            cls.INVALID_PARAMETER: "Invalid parameter value",
            cls.MISSING_PARAMETER: "Required parameter is missing",
            cls.INVALID_DATE_FORMAT: "Invalid date format",
            cls.INVALID_FREQUENCY: "Invalid data frequency",
            cls.INVALID_ADJUST_TYPE: "Invalid adjustment type",
            cls.INVALID_DATA_SOURCE: "Invalid data source specified",
            cls.INVALID_TABLE_NAME: "Invalid table name",
            cls.INVALID_COLUMN_NAME: "Invalid column name",
            cls.INVALID_SORT_ORDER: "Invalid sort order",
            cls.INVALID_LIMIT_VALUE: "Invalid limit value",
            cls.INVALID_TIMEZONE: "Invalid timezone",
            cls.INVALID_CURRENCY: "Invalid currency code",
            cls.INVALID_EXCHANGE: "Invalid exchange code",
            cls.INVALID_ASSET_TYPE: "Invalid asset type",
            cls.INVALID_FIELD_TYPE: "Invalid field type",
            cls.INVALID_CONFIG: "Invalid configuration",
            cls.INVALID_JSON: "Invalid JSON format",
            cls.INVALID_CSV_FORMAT: "Invalid CSV format",
            cls.INVALID_PARQUET_FILE: "Invalid Parquet file",
            cls.DUPLICATE_PARAMETER: "Duplicate parameter specified",
            cls.PARAMETER_OUT_OF_RANGE: "Parameter value out of range",
            cls.PARAMETER_TYPE_MISMATCH: "Parameter type mismatch",
            cls.SYMBOL_NOT_IN_UNIVERSE: "Symbol not in trading universe",
            cls.DATE_IN_FUTURE: "Date is in the future",
            cls.DATE_BEFORE_MARKET_OPEN: "Date is before market opening",
            cls.START_AFTER_END: "Start date is after end date",
            cls.NETWORK_TIMEOUT: "Network request timed out",
            cls.NETWORK_CONNECTION_LOST: "Network connection lost",
            cls.NETWORK_DNS_FAILURE: "DNS resolution failed",
            cls.NETWORK_PROXY_ERROR: "Proxy configuration error",
            cls.NETWORK_SOCKET_ERROR: "Socket error occurred",
            cls.NETWORK_RESET_BY_PEER: "Connection reset by peer",
            cls.NETWORK_NO_ROUTE_TO_HOST: "No route to host",
            cls.NETWORK_UNREACHABLE: "Network unreachable",
            cls.NETWORK_TOO_MANY_REDIRECTS: "Too many redirects",
            cls.NETWORK_BANDWIDTH_EXCEEDED: "Network bandwidth exceeded",
            cls.NETWORK_CONGESTION: "Network congestion detected",
            cls.NETWORK_TLS_HANDSHAKE_FAILED: "TLS handshake failed",
            cls.NETWORK_CERTIFICATE_EXPIRED: "SSL certificate expired",
            cls.NETWORK_CERTIFICATE_UNTRUSTED: "SSL certificate untrusted",
            cls.NO_DATA: "No data available for the requested query",
            cls.INVALID_DATA: "Data contains invalid values",
            cls.DATA_FORMAT_ERROR: "Data format is incorrect",
            cls.MISSING_COLUMNS: "Required columns are missing from data",
            cls.DUPLICATE_ROWS: "Duplicate rows detected in data",
            cls.NULL_VALUES_DETECTED: "Unexpected null values detected",
            cls.OUTLIER_DETECTED: "Statistical outlier detected in data",
            cls.DATA_INCONSISTENCY: "Data inconsistency detected",
            cls.DATA_TRUNCATION: "Data was truncated",
            cls.DATA_ENCODING_ERROR: "Data encoding error",
            cls.SCHEMA_MISMATCH: "Data schema does not match expected",
            cls.COLUMN_TYPE_MISMATCH: "Column type mismatch",
            cls.MISSING_REQUIRED_COLUMN: "Required column is missing",
            cls.DATA_RANGE_ERROR: "Data value out of expected range",
            cls.NEGATIVE_VOLUME: "Negative volume value detected",
            cls.NEGATIVE_PRICE: "Negative price value detected",
            cls.ZERO_PRICE: "Zero price value detected",
            cls.PRICE_ANOMALY: "Price anomaly detected",
            cls.VOLUME_ANOMALY: "Volume anomaly detected",
            cls.TIMESTAMP_ERROR: "Timestamp error in data",
            cls.FUTURE_TIMESTAMP: "Future timestamp detected",
            cls.DUPLICATE_TIMESTAMP: "Duplicate timestamp detected",
            cls.GAP_IN_TIMESERIES: "Gap detected in time series data",
            cls.INCOMPLETE_TRADING_DAY: "Incomplete trading day data",
            cls.SPLIT_ADJUSTMENT_ERROR: "Stock split adjustment error",
            cls.DIVIDEND_ADJUSTMENT_ERROR: "Dividend adjustment error",
            cls.SUSPENSION_NOT_HANDLED: "Trading suspension not handled",
            cls.DELISTED_SYMBOL_DATA: "Data for delisted symbol",
            cls.INTERNAL_ERROR: "Internal system error occurred",
            cls.NOT_IMPLEMENTED: "Feature not implemented",
            cls.CONFIGURATION_ERROR: "Configuration error",
            cls.DEPENDENCY_ERROR: "Dependency error",
            cls.IMPORT_ERROR: "Module import error",
            cls.VERSION_MISMATCH: "Version mismatch detected",
            cls.MEMORY_ERROR: "Out of memory",
            cls.STACK_OVERFLOW: "Stack overflow",
            cls.RECURSION_LIMIT: "Recursion limit exceeded",
            cls.THREAD_ERROR: "Thread error",
            cls.PROCESS_ERROR: "Process error",
            cls.SIGNAL_INTERRUPTED: "Operation interrupted by signal",
            cls.GRACEFUL_SHUTDOWN: "System shutting down gracefully",
            cls.UNEXPECTED_STATE: "Unexpected system state",
            cls.RACE_CONDITION: "Race condition detected",
            cls.DEADLOCK_DETECTED: "Deadlock detected",
            cls.RESOURCE_LEAK: "Resource leak detected",
            cls.PLUGIN_ERROR: "Plugin error",
            cls.EXTENSION_ERROR: "Extension error",
            cls.MIGRATION_ERROR: "Data migration error",
            cls.FILE_NOT_FOUND: "File not found",
            cls.FILE_PERMISSION_DENIED: "File permission denied",
            cls.FILE_ALREADY_EXISTS: "File already exists",
            cls.FILE_CORRUPTED: "File is corrupted",
            cls.FILE_TOO_LARGE: "File exceeds size limit",
            cls.DIRECTORY_NOT_FOUND: "Directory not found",
            cls.DIRECTORY_NOT_WRITABLE: "Directory is not writable",
            cls.DISK_SPACE_INSUFFICIENT: "Insufficient disk space",
            cls.INODE_EXHAUSTED: "File system inode limit reached",
            cls.SYMLINK_LOOP: "Symbolic link loop detected",
            cls.PATH_TOO_LONG: "File path too long",
            cls.INVALID_PATH: "Invalid file path",
            cls.DATABASE_LOCKED: "Database is locked",
            cls.DATABASE_CORRUPTED: "Database is corrupted",
            cls.DATABASE_MIGRATION_FAILED: "Database migration failed",
            cls.PARQUET_WRITE_ERROR: "Parquet write error",
            cls.PARQUET_READ_ERROR: "Parquet read error",
            cls.PARQUET_SCHEMA_ERROR: "Parquet schema error",
            cls.CSV_PARSE_ERROR: "CSV parse error",
            cls.JSON_PARSE_ERROR: "JSON parse error",
            cls.AUTH_TOKEN_MISSING: "Authentication token is missing",
            cls.AUTH_TOKEN_EXPIRED: "Authentication token has expired",
            cls.AUTH_TOKEN_INVALID: "Authentication token is invalid",
            cls.AUTH_TOKEN_REVOKED: "Authentication token has been revoked",
            cls.AUTH_INSUFFICIENT_PERMISSIONS: "Insufficient permissions",
            cls.AUTH_ACCOUNT_LOCKED: "Account has been locked",
            cls.AUTH_RATE_LIMIT_EXCEEDED: "Authentication rate limit exceeded",
            cls.AUTH_IP_NOT_ALLOWED: "IP address not allowed",
            cls.AUTH_2FA_REQUIRED: "Two-factor authentication required",
            cls.AUTH_SESSION_EXPIRED: "Session has expired",
            cls.AUTH_CREDENTIALS_INVALID: "Invalid credentials",
            cls.AUTH_API_KEY_INVALID: "Invalid API key",
            cls.AUTH_SUBSCRIPTION_EXPIRED: "Subscription has expired",
            cls.AUTH_PLAN_LIMIT_REACHED: "Plan limit reached",
            cls.RATE_LIMIT_GLOBAL: "Global rate limit exceeded",
            cls.RATE_LIMIT_PER_SOURCE: "Per-source rate limit exceeded",
            cls.RATE_LIMIT_PER_MINUTE: "Per-minute rate limit exceeded",
            cls.RATE_LIMIT_PER_HOUR: "Per-hour rate limit exceeded",
            cls.RATE_LIMIT_PER_DAY: "Per-day rate limit exceeded",
            cls.CONCURRENT_REQUEST_LIMIT: "Concurrent request limit exceeded",
            cls.QUEUE_FULL: "Request queue is full",
            cls.QUEUE_TIMEOUT: "Request queue timeout",
            cls.BACKPRESSURE_TRIGGERED: "Backpressure triggered",
            cls.CIRCUIT_BREAKER_OPEN: "Circuit breaker is open",
        }
        return messages.get(code, f"Unknown error: {code.value}")


class DataAccessException(Exception):
    """Base exception for all data access errors."""

    def __init__(
        self,
        message: str,
        error_code: Optional[ErrorCode] = None,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message)
        self.error_code = error_code
        self.source = source
        self.symbol = symbol

    def to_dict(self) -> dict:
        """Convert exception to dictionary for logging/serialization."""
        return {
            "error_code": self.error_code.value if self.error_code else None,
            "message": str(self),
            "source": self.source,
            "symbol": self.symbol,
            "category": ErrorCode.get_category(self.error_code)
            if self.error_code
            else None,
            "human_message": ErrorCode.get_message(self.error_code)
            if self.error_code
            else None,
        }


class DataSourceError(DataAccessException):
    """Generic data source error (backward compatible alias)."""

    def __init__(
        self,
        message: str,
        error_code: Optional[ErrorCode] = None,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class SourceUnavailableError(DataSourceError):
    """Raised when a data source is unavailable or unreachable."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.SOURCE_UNAVAILABLE,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class NoDataError(DataSourceError):
    """Raised when no data is available for the requested query."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.NO_DATA,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class TimeoutError(DataSourceError):
    """Raised when a data source request times out."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.SOURCE_TIMEOUT,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class RateLimitError(DataSourceError):
    """Raised when a data source rate limit is exceeded."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.SOURCE_RATE_LIMITED,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class CacheError(DataAccessException):
    """Raised when a cache operation fails."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.CACHE_MISS,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class ValidationError(DataAccessException):
    """Raised when input validation fails."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INVALID_PARAMETER,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class DataQualityError(DataAccessException):
    """Raised when data quality issues are detected."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INVALID_DATA,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class StorageError(DataAccessException):
    """Raised when a storage operation fails."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.FILE_NOT_FOUND,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class AuthError(DataAccessException):
    """Raised when authentication or authorization fails."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.AUTH_TOKEN_INVALID,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class NetworkError(DataAccessException):
    """Raised when a network operation fails."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.NETWORK_TIMEOUT,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class SystemError(DataAccessException):
    """Raised when a system-level error occurs."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


__all__ = [
    "ErrorCode",
    "DataAccessException",
    "DataSourceError",
    "SourceUnavailableError",
    "NoDataError",
    "TimeoutError",
    "RateLimitError",
    "CacheError",
    "ValidationError",
    "DataQualityError",
    "StorageError",
    "AuthError",
    "NetworkError",
    "SystemError",
]
