"""离线工具错误定义"""

from typing import Optional

from akshare_data.core.errors import DataAccessException, ErrorCode


class OfflineError(DataAccessException):
    """离线工具基础错误"""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, error_code, source, symbol)


class ConfigError(OfflineError):
    """配置错误"""

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, ErrorCode.CONFIGURATION_ERROR, source, symbol)


class DownloadError(OfflineError):
    """下载错误"""

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, ErrorCode.DEPENDENCY_ERROR, source, symbol)


class ProbeError(OfflineError):
    """探测错误"""

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, ErrorCode.SOURCE_UNAVAILABLE, source, symbol)


class AnalysisError(OfflineError):
    """分析错误"""

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, ErrorCode.INTERNAL_ERROR, source, symbol)


class SourceError(OfflineError):
    """数据源错误"""

    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, ErrorCode.INVALID_DATA_SOURCE, source, symbol)


class RetryExhaustedError(OfflineError):
    """重试耗尽错误"""

    def __init__(
        self,
        message: str,
        last_error: Exception,
        source: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        super().__init__(message, ErrorCode.SOURCE_UNAVAILABLE, source, symbol)
        self.last_error = last_error
