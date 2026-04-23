"""Tests for akshare_data.core.errors module.

Covers:
- ErrorCode enum values and categories
- ErrorCode.get_category() method
- ErrorCode.get_message() method
- DataAccessException and all subclasses
- Exception to_dict() serialization
"""

from akshare_data.core.errors import (
    ErrorCode,
    DataAccessException,
    DataSourceError,
    SourceUnavailableError,
    NoDataError,
    TimeoutError,
    RateLimitError,
    CacheError,
    ValidationError,
    DataQualityError,
    StorageError,
    AuthError,
    NetworkError,
    SystemError,
)


class TestErrorCodeValues:
    """Test ErrorCode enum values exist and have correct format."""

    def test_error_code_format(self):
        """All error codes should follow XXXX_NAME format."""
        for code in ErrorCode:
            parts = code.value.split("_")
            assert len(parts) >= 2, (
                f"Error code {code.value} should have numeric prefix"
            )
            assert parts[0].isdigit(), f"First part of {code.value} should be numeric"
            assert len(parts[0]) == 4, (
                f"Numeric prefix should be 4 digits: {code.value}"
            )

    def test_data_source_errors_exist(self):
        """1xxx series should exist."""
        assert ErrorCode.SOURCE_UNAVAILABLE.value.startswith("1001")
        assert ErrorCode.SOURCE_TIMEOUT.value.startswith("1002")
        assert ErrorCode.SOURCE_RATE_LIMITED.value.startswith("1003")

    def test_cache_errors_exist(self):
        """2xxx series should exist."""
        assert ErrorCode.CACHE_MISS.value.startswith("2001")
        assert ErrorCode.CACHE_CORRUPTED.value.startswith("2002")
        assert ErrorCode.CACHE_WRITE_FAILED.value.startswith("2003")

    def test_parameter_errors_exist(self):
        """3xxx series should exist."""
        assert ErrorCode.INVALID_SYMBOL.value.startswith("3001")
        assert ErrorCode.INVALID_DATE_RANGE.value.startswith("3002")
        assert ErrorCode.INVALID_PARAMETER.value.startswith("3003")

    def test_network_errors_exist(self):
        """4xxx series should exist."""
        assert ErrorCode.NETWORK_TIMEOUT.value.startswith("4001")
        assert ErrorCode.NETWORK_CONNECTION_LOST.value.startswith("4002")

    def test_data_quality_errors_exist(self):
        """5xxx series should exist."""
        assert ErrorCode.NO_DATA.value.startswith("5001")
        assert ErrorCode.INVALID_DATA.value.startswith("5002")

    def test_system_errors_exist(self):
        """6xxx series should exist."""
        assert ErrorCode.INTERNAL_ERROR.value.startswith("6001")
        assert ErrorCode.NOT_IMPLEMENTED.value.startswith("6002")

    def test_storage_errors_exist(self):
        """7xxx series should exist."""
        assert ErrorCode.FILE_NOT_FOUND.value.startswith("7001")
        assert ErrorCode.FILE_PERMISSION_DENIED.value.startswith("7002")

    def test_auth_errors_exist(self):
        """8xxx series should exist."""
        assert ErrorCode.AUTH_TOKEN_MISSING.value.startswith("8001")
        assert ErrorCode.AUTH_TOKEN_EXPIRED.value.startswith("8002")

    def test_rate_limit_errors_exist(self):
        """9xxx series should exist."""
        assert ErrorCode.RATE_LIMIT_GLOBAL.value.startswith("9001")
        assert ErrorCode.CONCURRENT_REQUEST_LIMIT.value.startswith("9006")


class TestErrorCodeGetCategory:
    """Test ErrorCode.get_category() method."""

    def test_data_source_category(self):
        """1xxx codes should return 'data_source'."""
        assert ErrorCode.get_category(ErrorCode.SOURCE_UNAVAILABLE) == "data_source"
        assert ErrorCode.get_category(ErrorCode.SOURCE_TIMEOUT) == "data_source"
        assert ErrorCode.get_category(ErrorCode.SOURCE_HTTP_500) == "data_source"

    def test_cache_category(self):
        """2xxx codes should return 'cache'."""
        assert ErrorCode.get_category(ErrorCode.CACHE_MISS) == "cache"
        assert ErrorCode.get_category(ErrorCode.CACHE_CORRUPTED) == "cache"
        assert ErrorCode.get_category(ErrorCode.CACHE_WRITE_FAILED) == "cache"

    def test_parameter_category(self):
        """3xxx codes should return 'parameter'."""
        assert ErrorCode.get_category(ErrorCode.INVALID_SYMBOL) == "parameter"
        assert ErrorCode.get_category(ErrorCode.INVALID_DATE_RANGE) == "parameter"

    def test_network_category(self):
        """4xxx codes should return 'network'."""
        assert ErrorCode.get_category(ErrorCode.NETWORK_TIMEOUT) == "network"
        assert ErrorCode.get_category(ErrorCode.NETWORK_DNS_FAILURE) == "network"

    def test_data_quality_category(self):
        """5xxx codes should return 'data_quality'."""
        assert ErrorCode.get_category(ErrorCode.NO_DATA) == "data_quality"
        assert ErrorCode.get_category(ErrorCode.INVALID_DATA) == "data_quality"

    def test_system_category(self):
        """6xxx codes should return 'system'."""
        assert ErrorCode.get_category(ErrorCode.INTERNAL_ERROR) == "system"
        assert ErrorCode.get_category(ErrorCode.MEMORY_ERROR) == "system"

    def test_storage_category(self):
        """7xxx codes should return 'storage'."""
        assert ErrorCode.get_category(ErrorCode.FILE_NOT_FOUND) == "storage"
        assert ErrorCode.get_category(ErrorCode.DATABASE_CORRUPTED) == "storage"

    def test_authentication_category(self):
        """8xxx codes should return 'authentication'."""
        assert ErrorCode.get_category(ErrorCode.AUTH_TOKEN_MISSING) == "authentication"
        assert ErrorCode.get_category(ErrorCode.AUTH_TOKEN_EXPIRED) == "authentication"

    def test_rate_limit_category(self):
        """9xxx codes should return 'rate_limit'."""
        assert ErrorCode.get_category(ErrorCode.RATE_LIMIT_GLOBAL) == "rate_limit"
        assert (
            ErrorCode.get_category(ErrorCode.CONCURRENT_REQUEST_LIMIT) == "rate_limit"
        )


class TestErrorCodeGetMessage:
    """Test ErrorCode.get_message() method."""

    def test_data_source_messages(self):
        """Data source error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.SOURCE_UNAVAILABLE)
        assert isinstance(msg, str)
        assert len(msg) > 0
        assert "unavailable" in msg.lower() or "source" in msg.lower()

    def test_cache_messages(self):
        """Cache error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.CACHE_MISS)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_parameter_messages(self):
        """Parameter error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.INVALID_SYMBOL)
        assert isinstance(msg, str)
        assert len(msg) > 0
        assert "symbol" in msg.lower()

    def test_network_messages(self):
        """Network error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.NETWORK_TIMEOUT)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_data_quality_messages(self):
        """Data quality error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.NO_DATA)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_system_messages(self):
        """System error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.INTERNAL_ERROR)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_storage_messages(self):
        """Storage error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.FILE_NOT_FOUND)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_auth_messages(self):
        """Authentication error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.AUTH_TOKEN_MISSING)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_rate_limit_messages(self):
        """Rate limit error codes should return human-readable messages."""
        msg = ErrorCode.get_message(ErrorCode.RATE_LIMIT_GLOBAL)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_unknown_code_returns_fallback(self):
        """Unknown error code should return fallback message."""

        class MockCode:
            value = "9999_UNKNOWN"

        msg = ErrorCode.get_message(MockCode())
        assert "Unknown error" in msg


class TestDataAccessException:
    """Test DataAccessException base class."""

    def test_basic_instantiation(self):
        """Should create exception with message."""
        exc = DataAccessException("Test error")
        assert str(exc) == "Test error"
        assert exc.error_code is None
        assert exc.source is None
        assert exc.symbol is None

    def test_instantiation_with_error_code(self):
        """Should store error code."""
        exc = DataAccessException("Test error", error_code=ErrorCode.SOURCE_UNAVAILABLE)
        assert exc.error_code == ErrorCode.SOURCE_UNAVAILABLE

    def test_instantiation_with_source(self):
        """Should store source."""
        exc = DataAccessException("Test error", source="test_source")
        assert exc.source == "test_source"

    def test_instantiation_with_symbol(self):
        """Should store symbol."""
        exc = DataAccessException("Test error", symbol="600519")
        assert exc.symbol == "600519"

    def test_instantiation_with_all_params(self):
        """Should store all parameters."""
        exc = DataAccessException(
            "Test error",
            error_code=ErrorCode.SOURCE_UNAVAILABLE,
            source="test_source",
            symbol="600519",
        )
        assert exc.error_code == ErrorCode.SOURCE_UNAVAILABLE
        assert exc.source == "test_source"
        assert exc.symbol == "600519"

    def test_to_dict_without_error_code(self):
        """to_dict should work without error_code."""
        exc = DataAccessException("Test error", source="src")
        d = exc.to_dict()
        assert d["message"] == "Test error"
        assert d["source"] == "src"
        assert d["error_code"] is None
        assert d["category"] is None
        assert d["human_message"] is None

    def test_to_dict_with_error_code(self):
        """to_dict should include error code info."""
        exc = DataAccessException(
            "Test error",
            error_code=ErrorCode.SOURCE_UNAVAILABLE,
            source="test_source",
            symbol="600519",
        )
        d = exc.to_dict()
        assert d["error_code"] == "1001_SOURCE_UNAVAILABLE"
        assert d["category"] == "data_source"
        assert "unavailable" in d["human_message"].lower()


class TestDataSourceError:
    """Test DataSourceError exception class."""

    def test_basic_instantiation(self):
        """Should create with message."""
        exc = DataSourceError("Data source error occurred")
        assert str(exc) == "Data source error occurred"

    def test_inherits_from_data_access_exception(self):
        """Should be a DataAccessException."""
        exc = DataSourceError("test")
        assert isinstance(exc, DataAccessException)


class TestSourceUnavailableError:
    """Test SourceUnavailableError exception class."""

    def test_default_error_code(self):
        """Should use SOURCE_UNAVAILABLE as default."""
        exc = SourceUnavailableError("Source down")
        assert exc.error_code == ErrorCode.SOURCE_UNAVAILABLE

    def test_custom_error_code(self):
        """Should accept custom error code."""
        exc = SourceUnavailableError(
            "Connection refused", error_code=ErrorCode.SOURCE_CONNECTION_REFUSED
        )
        assert exc.error_code == ErrorCode.SOURCE_CONNECTION_REFUSED


class TestNoDataError:
    """Test NoDataError exception class."""

    def test_default_error_code(self):
        """Should use NO_DATA as default."""
        exc = NoDataError("No data found")
        assert exc.error_code == ErrorCode.NO_DATA


class TestTimeoutError:
    """Test TimeoutError exception class."""

    def test_default_error_code(self):
        """Should use SOURCE_TIMEOUT as default."""
        exc = TimeoutError("Request timed out")
        assert exc.error_code == ErrorCode.SOURCE_TIMEOUT


class TestRateLimitError:
    """Test RateLimitError exception class."""

    def test_default_error_code(self):
        """Should use SOURCE_RATE_LIMITED as default."""
        exc = RateLimitError("Rate limit exceeded")
        assert exc.error_code == ErrorCode.SOURCE_RATE_LIMITED


class TestCacheError:
    """Test CacheError exception class."""

    def test_default_error_code(self):
        """Should use CACHE_MISS as default."""
        exc = CacheError("Cache miss")
        assert exc.error_code == ErrorCode.CACHE_MISS

    def test_custom_error_code(self):
        """Should accept custom cache error code."""
        exc = CacheError("Cache write failed", error_code=ErrorCode.CACHE_WRITE_FAILED)
        assert exc.error_code == ErrorCode.CACHE_WRITE_FAILED


class TestValidationError:
    """Test ValidationError exception class."""

    def test_default_error_code(self):
        """Should use INVALID_PARAMETER as default."""
        exc = ValidationError("Invalid parameter")
        assert exc.error_code == ErrorCode.INVALID_PARAMETER


class TestDataQualityError:
    """Test DataQualityError exception class."""

    def test_default_error_code(self):
        """Should use INVALID_DATA as default."""
        exc = DataQualityError("Data quality issue")
        assert exc.error_code == ErrorCode.INVALID_DATA


class TestStorageError:
    """Test StorageError exception class."""

    def test_default_error_code(self):
        """Should use FILE_NOT_FOUND as default."""
        exc = StorageError("File not found")
        assert exc.error_code == ErrorCode.FILE_NOT_FOUND


class TestAuthError:
    """Test AuthError exception class."""

    def test_default_error_code(self):
        """Should use AUTH_TOKEN_INVALID as default."""
        exc = AuthError("Auth failed")
        assert exc.error_code == ErrorCode.AUTH_TOKEN_INVALID


class TestNetworkError:
    """Test NetworkError exception class."""

    def test_default_error_code(self):
        """Should use NETWORK_TIMEOUT as default."""
        exc = NetworkError("Network error")
        assert exc.error_code == ErrorCode.NETWORK_TIMEOUT


class TestSystemError:
    """Test SystemError exception class."""

    def test_default_error_code(self):
        """Should use INTERNAL_ERROR as default."""
        exc = SystemError("System error")
        assert exc.error_code == ErrorCode.INTERNAL_ERROR


class TestExceptionInheritance:
    """Test exception class hierarchy."""

    def test_all_exceptions_inherit_from_data_access_exception(self):
        """All custom exceptions should be DataAccessException subclasses."""
        exceptions = [
            DataSourceError,
            SourceUnavailableError,
            NoDataError,
            TimeoutError,
            RateLimitError,
            CacheError,
            ValidationError,
            DataQualityError,
            StorageError,
            AuthError,
            NetworkError,
            SystemError,
        ]
        for exc_class in exceptions:
            assert issubclass(exc_class, DataAccessException)

    def test_data_source_error_inherits_correctly(self):
        """DataSourceError should inherit from DataAccessException."""
        assert issubclass(DataSourceError, DataAccessException)
