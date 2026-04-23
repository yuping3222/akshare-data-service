"""Error mapper for the service layer.

Maps internal error codes and exceptions to service-level error responses
with clear semantic distinctions:

- No data: query returned empty, but the dataset exists and is published
- Data not published: dataset has not passed quality gate to Served layer
- Quality blocked: latest batch failed quality gate, publishing blocked
- Parameter error: query parameters violate the contract
- Version not found: requested release_version does not exist

This module does NOT implement quality checkers (task 6) or async
backfill scheduling (task 13).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from akshare_data.core.errors import (
    DataAccessException,
    DataQualityError,
    NoDataError,
    ValidationError,
)
from akshare_data.service.query_contract import (
    DataNotPublishedError,
    QualityBlockedError,
    QueryContractError,
    VersionNotFoundError,
)


# ---------------------------------------------------------------------------
# Service error categories
# ---------------------------------------------------------------------------


class ServiceErrorCategory(str, Enum):
    PARAM_ERROR = "param_error"
    NO_DATA = "no_data"
    NOT_PUBLISHED = "not_published"
    QUALITY_BLOCKED = "quality_blocked"
    VERSION_NOT_FOUND = "version_not_found"
    SOURCE_UNAVAILABLE = "source_unavailable"
    INTERNAL_ERROR = "internal_error"


# ---------------------------------------------------------------------------
# Error response model
# ---------------------------------------------------------------------------


@dataclass
class ServiceErrorResponse:
    category: ServiceErrorCategory
    code: str
    message: str
    dataset: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    http_status: int = 500

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "category": self.category.value,
            "code": self.code,
            "message": self.message,
            "http_status": self.http_status,
        }
        if self.dataset:
            result["dataset"] = self.dataset
        if self.details:
            result["details"] = self.details
        return result


# ---------------------------------------------------------------------------
# Error mapper
# ---------------------------------------------------------------------------

_ERROR_MAP: Dict[type, ServiceErrorCategory] = {
    QueryContractError: ServiceErrorCategory.PARAM_ERROR,
    ValidationError: ServiceErrorCategory.PARAM_ERROR,
    NoDataError: ServiceErrorCategory.NO_DATA,
    DataNotPublishedError: ServiceErrorCategory.NOT_PUBLISHED,
    QualityBlockedError: ServiceErrorCategory.QUALITY_BLOCKED,
    VersionNotFoundError: ServiceErrorCategory.VERSION_NOT_FOUND,
    DataQualityError: ServiceErrorCategory.QUALITY_BLOCKED,
}

_HTTP_STATUS_MAP: Dict[ServiceErrorCategory, int] = {
    ServiceErrorCategory.PARAM_ERROR: 400,
    ServiceErrorCategory.NO_DATA: 404,
    ServiceErrorCategory.NOT_PUBLISHED: 404,
    ServiceErrorCategory.QUALITY_BLOCKED: 503,
    ServiceErrorCategory.VERSION_NOT_FOUND: 404,
    ServiceErrorCategory.SOURCE_UNAVAILABLE: 503,
    ServiceErrorCategory.INTERNAL_ERROR: 500,
}

_DEFAULT_MESSAGES: Dict[ServiceErrorCategory, str] = {
    ServiceErrorCategory.PARAM_ERROR: "Invalid query parameters.",
    ServiceErrorCategory.NO_DATA: "No data available for the requested query.",
    ServiceErrorCategory.NOT_PUBLISHED: "Data has not been published to the served layer.",
    ServiceErrorCategory.QUALITY_BLOCKED: "Data is blocked by quality gate.",
    ServiceErrorCategory.VERSION_NOT_FOUND: "The requested version does not exist.",
    ServiceErrorCategory.SOURCE_UNAVAILABLE: "Data source is unavailable.",
    ServiceErrorCategory.INTERNAL_ERROR: "An internal error occurred.",
}


class ErrorMapper:
    """Maps exceptions to structured service error responses."""

    @staticmethod
    def map_error(
        exc: Exception, dataset: Optional[str] = None
    ) -> ServiceErrorResponse:
        category = _classify_error(exc)
        http_status = _HTTP_STATUS_MAP.get(category, 500)
        message = _extract_message(exc, category)
        code = _extract_code(exc, category)
        details = _extract_details(exc)

        return ServiceErrorResponse(
            category=category,
            code=code,
            message=message,
            dataset=dataset or _extract_dataset(exc),
            details=details,
            http_status=http_status,
        )


def _classify_error(exc: Exception) -> ServiceErrorCategory:
    if isinstance(exc, DataAccessException):
        return _ERROR_MAP.get(type(exc), ServiceErrorCategory.INTERNAL_ERROR)

    for exc_type, category in _ERROR_MAP.items():
        if isinstance(exc, exc_type):
            return category

    return ServiceErrorCategory.INTERNAL_ERROR


def _extract_message(exc: Exception, category: ServiceErrorCategory) -> str:
    msg = str(exc)
    if msg:
        return msg
    return _DEFAULT_MESSAGES.get(category, "Unknown error.")


def _extract_code(exc: Exception, category: ServiceErrorCategory) -> str:
    if isinstance(exc, DataAccessException) and exc.error_code:
        return exc.error_code.value
    category_codes = {
        ServiceErrorCategory.PARAM_ERROR: "SVC_PARAM_ERROR",
        ServiceErrorCategory.NO_DATA: "SVC_NO_DATA",
        ServiceErrorCategory.NOT_PUBLISHED: "SVC_NOT_PUBLISHED",
        ServiceErrorCategory.QUALITY_BLOCKED: "SVC_QUALITY_BLOCKED",
        ServiceErrorCategory.VERSION_NOT_FOUND: "SVC_VERSION_NOT_FOUND",
        ServiceErrorCategory.SOURCE_UNAVAILABLE: "SVC_SOURCE_UNAVAILABLE",
        ServiceErrorCategory.INTERNAL_ERROR: "SVC_INTERNAL_ERROR",
    }
    return category_codes.get(category, "SVC_UNKNOWN")


def _extract_dataset(exc: Exception) -> Optional[str]:
    return getattr(exc, "dataset", None)


def _extract_details(exc: Exception) -> Dict[str, Any]:
    details: Dict[str, Any] = {}

    if isinstance(exc, QualityBlockedError):
        if exc.failed_rules:
            details["failed_rules"] = exc.failed_rules

    if isinstance(exc, VersionNotFoundError):
        if exc.version:
            details["requested_version"] = exc.version

    if isinstance(exc, DataAccessException):
        if exc.source:
            details["source"] = exc.source
        if exc.symbol:
            details["symbol"] = exc.symbol

    return details


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------


def create_param_error(
    message: str, dataset: Optional[str] = None
) -> ServiceErrorResponse:
    return ServiceErrorResponse(
        category=ServiceErrorCategory.PARAM_ERROR,
        code="SVC_PARAM_ERROR",
        message=message,
        dataset=dataset,
        http_status=400,
    )


def create_no_data_error(
    dataset: Optional[str] = None, details: Optional[Dict[str, Any]] = None
) -> ServiceErrorResponse:
    return ServiceErrorResponse(
        category=ServiceErrorCategory.NO_DATA,
        code="SVC_NO_DATA",
        message="No data available for the requested query.",
        dataset=dataset,
        details=details or {},
        http_status=404,
    )


def create_not_published_error(dataset: Optional[str] = None) -> ServiceErrorResponse:
    return ServiceErrorResponse(
        category=ServiceErrorCategory.NOT_PUBLISHED,
        code="SVC_NOT_PUBLISHED",
        message=f"Dataset {dataset!r} has not been published to the served layer.",
        dataset=dataset,
        http_status=404,
    )


def create_quality_blocked_error(
    dataset: Optional[str] = None,
    failed_rules: Optional[List[str]] = None,
) -> ServiceErrorResponse:
    details: Dict[str, Any] = {}
    if failed_rules:
        details["failed_rules"] = failed_rules
    return ServiceErrorResponse(
        category=ServiceErrorCategory.QUALITY_BLOCKED,
        code="SVC_QUALITY_BLOCKED",
        message=f"Dataset {dataset!r} is blocked by quality gate.",
        dataset=dataset,
        details=details,
        http_status=503,
    )


def create_version_not_found_error(
    dataset: Optional[str] = None,
    version: Optional[str] = None,
) -> ServiceErrorResponse:
    details: Dict[str, Any] = {}
    if version:
        details["requested_version"] = version
    return ServiceErrorResponse(
        category=ServiceErrorCategory.VERSION_NOT_FOUND,
        code="SVC_VERSION_NOT_FOUND",
        message=f"Version {version!r} not found for dataset {dataset!r}.",
        dataset=dataset,
        details=details,
        http_status=404,
    )
