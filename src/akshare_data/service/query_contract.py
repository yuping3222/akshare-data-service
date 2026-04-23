"""Query contract definitions for the service layer.

Provides typed query parameter models, validation, and result wrappers
for the first 3 P0 datasets: market_quote_daily, financial_indicator,
macro_indicator.

Design reference: docs/design/71-query-contract.md
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd

from akshare_data.core.errors import DataAccessException, ErrorCode, NoDataError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_LIMIT = 10000
DEFAULT_SORT_ORDER = "asc"


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


# ---------------------------------------------------------------------------
# System field names (excluded from default field projection)
# ---------------------------------------------------------------------------

SYSTEM_FIELDS: frozenset[str] = frozenset(
    {
        "batch_id",
        "source_name",
        "interface_name",
        "ingest_time",
        "normalize_version",
        "schema_version",
        "quality_status",
        "publish_time",
        "release_version",
    }
)


# ---------------------------------------------------------------------------
# Entity schema metadata (loaded from config/standards/entities/*.yaml)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldDef:
    name: str
    field_type: str
    description: str
    unit: Optional[str] = None
    required: bool = False


@dataclass(frozen=True)
class EntitySchema:
    entity: str
    description: str
    primary_key: List[str]
    partition_by: List[str]
    time_fields: Dict[str, str]
    required_fields: List[str]
    fields: Dict[str, FieldDef]

    @property
    def business_fields(self) -> List[str]:
        return [n for n in self.fields if n not in SYSTEM_FIELDS]

    @property
    def default_sort_field(self) -> str:
        for f in self.primary_key:
            if f in self.time_fields:
                return f
        return self.primary_key[0] if self.primary_key else ""


# ---------------------------------------------------------------------------
# Entity schema loader
# ---------------------------------------------------------------------------


def _load_entity_schemas() -> Dict[str, EntitySchema]:
    import os

    import yaml

    schemas: Dict[str, EntitySchema] = {}
    entity_files = [
        "market_quote_daily.yaml",
        "financial_indicator.yaml",
        "macro_indicator.yaml",
    ]

    pkg_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    project_root = os.path.dirname(pkg_dir)
    entities_dir = os.path.join(project_root, "config", "standards", "entities")

    for filename in entity_files:
        filepath = os.path.join(entities_dir, filename)
        if not os.path.exists(filepath):
            continue

        with open(filepath, encoding="utf-8") as f:
            doc = yaml.safe_load(f)

        fields: Dict[str, FieldDef] = {}
        for fname, fdef in (doc.get("fields") or {}).items():
            fields[fname] = FieldDef(
                name=fname,
                field_type=fdef.get("type", "string"),
                description=fdef.get("description", ""),
                unit=fdef.get("unit"),
                required=fdef.get("required", False),
            )

        time_fields: Dict[str, str] = {}
        for tf, tdef in (doc.get("time_fields") or {}).items():
            time_fields[tf] = tdef.get("role", "business")

        schemas[doc["entity"]] = EntitySchema(
            entity=doc["entity"],
            description=doc.get("description", ""),
            primary_key=doc.get("primary_key", []),
            partition_by=doc.get("partition_by", []),
            time_fields=time_fields,
            required_fields=doc.get("required_fields", []),
            fields=fields,
        )

    return schemas


ENTITY_SCHEMAS: Dict[str, EntitySchema] = _load_entity_schemas()


# ---------------------------------------------------------------------------
# Query parameter models
# ---------------------------------------------------------------------------


@dataclass
class BaseQueryParams:
    """Shared query parameters across all datasets."""

    start_date: Optional[str] = None
    end_date: Optional[str] = None
    fields: Optional[List[str]] = None
    sort_by: Optional[str] = None
    sort_order: str = DEFAULT_SORT_ORDER
    limit: Optional[int] = None
    offset: int = 0
    release_version: Optional[str] = None

    def validate_dates(self) -> None:
        if self.start_date and not DATE_PATTERN.match(self.start_date):
            raise QueryContractError(
                f"Invalid start_date format: {self.start_date!r}. Expected YYYY-MM-DD.",
                error_code=ErrorCode.INVALID_DATE_FORMAT,
            )
        if self.end_date and not DATE_PATTERN.match(self.end_date):
            raise QueryContractError(
                f"Invalid end_date format: {self.end_date!r}. Expected YYYY-MM-DD.",
                error_code=ErrorCode.INVALID_DATE_FORMAT,
            )
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise QueryContractError(
                f"start_date ({self.start_date}) is after end_date ({self.end_date}).",
                error_code=ErrorCode.START_AFTER_END,
            )

    def validate_pagination(self) -> None:
        if self.offset < 0:
            raise QueryContractError(
                f"offset must be >= 0, got {self.offset}.",
                error_code=ErrorCode.INVALID_LIMIT_VALUE,
            )
        if self.limit is not None:
            if self.limit <= 0:
                raise QueryContractError(
                    f"limit must be > 0, got {self.limit}.",
                    error_code=ErrorCode.INVALID_LIMIT_VALUE,
                )
            if self.limit > MAX_LIMIT:
                self.limit = MAX_LIMIT

    def validate_sort(self, schema: EntitySchema) -> None:
        if self.sort_order not in ("asc", "desc"):
            raise QueryContractError(
                f"Invalid sort_order: {self.sort_order!r}. Must be 'asc' or 'desc'.",
                error_code=ErrorCode.INVALID_SORT_ORDER,
            )
        if self.sort_by is not None and self.sort_by not in schema.fields:
            raise QueryContractError(
                f"Field {self.sort_by!r} is not defined in entity {schema.entity!r}.",
                error_code=ErrorCode.INVALID_COLUMN_NAME,
            )

    def validate_fields(self, schema: EntitySchema) -> None:
        if self.fields is None:
            return
        for f in self.fields:
            if f not in schema.fields and f not in SYSTEM_FIELDS:
                raise QueryContractError(
                    f"Field {f!r} is not defined in entity {schema.entity!r}.",
                    error_code=ErrorCode.INVALID_COLUMN_NAME,
                )

    def validate(self, schema: EntitySchema) -> None:
        self.validate_dates()
        self.validate_pagination()
        self.validate_sort(schema)
        self.validate_fields(schema)


@dataclass
class MarketQuoteDailyParams(BaseQueryParams):
    security_id: Optional[str] = None
    adjust_type: str = "qfq"

    def validate(self, schema: EntitySchema) -> None:
        super().validate(schema)
        if not self.security_id:
            raise QueryContractError(
                "security_id is required for market_quote_daily.",
                error_code=ErrorCode.MISSING_PARAMETER,
            )
        if self.adjust_type not in ("qfq", "hfq", "none"):
            raise QueryContractError(
                f"Invalid adjust_type: {self.adjust_type!r}. Must be qfq/hfq/none.",
                error_code=ErrorCode.INVALID_ADJUST_TYPE,
            )


@dataclass
class FinancialIndicatorParams(BaseQueryParams):
    security_id: Optional[str] = None
    report_type: Optional[str] = None

    def validate(self, schema: EntitySchema) -> None:
        super().validate(schema)
        if not self.security_id:
            raise QueryContractError(
                "security_id is required for financial_indicator.",
                error_code=ErrorCode.MISSING_PARAMETER,
            )
        if self.report_type is not None and self.report_type not in (
            "Q1",
            "H1",
            "Q3",
            "A",
        ):
            raise QueryContractError(
                f"Invalid report_type: {self.report_type!r}. Must be Q1/H1/Q3/A.",
                error_code=ErrorCode.INVALID_PARAMETER,
            )


@dataclass
class MacroIndicatorParams(BaseQueryParams):
    indicator_code: Optional[str] = None
    region: str = "CN"

    def validate(self, schema: EntitySchema) -> None:
        super().validate(schema)
        if not self.indicator_code:
            raise QueryContractError(
                "indicator_code is required for macro_indicator.",
                error_code=ErrorCode.MISSING_PARAMETER,
            )


# ---------------------------------------------------------------------------
# Query result
# ---------------------------------------------------------------------------


@dataclass
class QueryResult:
    data: pd.DataFrame
    dataset: str
    total_rows: int
    returned_rows: int
    release_version: Optional[str]
    query_time: datetime
    fields: List[str]


# ---------------------------------------------------------------------------
# Query executor
# ---------------------------------------------------------------------------


class QueryExecutor:
    """Applies query contract operations to a DataFrame.

    Operations: field projection, sorting, pagination.
    """

    def __init__(self, schema: EntitySchema):
        self._schema = schema

    def execute(
        self,
        df: pd.DataFrame,
        params: BaseQueryParams,
        dataset: str,
        release_version: Optional[str] = None,
    ) -> QueryResult:
        if df.empty:
            raise NoDataError(
                f"No data available for dataset {dataset!r} with the given parameters.",
                error_code=ErrorCode.NO_DATA,
            )

        total_rows = len(df)

        df = self._apply_field_projection(df, params)
        df = self._apply_sorting(df, params)
        df = self._apply_pagination(df, params)

        return QueryResult(
            data=df.reset_index(drop=True),
            dataset=dataset,
            total_rows=total_rows,
            returned_rows=len(df),
            release_version=release_version,
            query_time=datetime.now(),
            fields=list(df.columns),
        )

    def _apply_field_projection(
        self, df: pd.DataFrame, params: BaseQueryParams
    ) -> pd.DataFrame:
        if params.fields is None:
            cols = [f for f in self._schema.business_fields if f in df.columns]
            return df[cols]

        requested = [f for f in params.fields if f in df.columns]
        if not requested:
            return df.iloc[:0]
        return df[requested]

    def _apply_sorting(self, df: pd.DataFrame, params: BaseQueryParams) -> pd.DataFrame:
        sort_field = params.sort_by or self._schema.default_sort_field
        if sort_field not in df.columns:
            return df
        ascending = params.sort_order == "asc"
        return df.sort_values(by=sort_field, ascending=ascending).reset_index(drop=True)

    def _apply_pagination(
        self, df: pd.DataFrame, params: BaseQueryParams
    ) -> pd.DataFrame:
        if params.limit is None and params.offset == 0:
            return df
        start = params.offset
        end = start + params.limit if params.limit is not None else None
        return df.iloc[start:end].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Service-level exceptions
# ---------------------------------------------------------------------------


class QueryContractError(DataAccessException):
    """Raised when query parameters violate the contract."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INVALID_PARAMETER,
        dataset: Optional[str] = None,
    ):
        super().__init__(message, error_code=error_code)
        self.dataset = dataset


class DataNotPublishedError(DataAccessException):
    """Raised when the dataset has not been published to Served layer."""

    def __init__(self, message: str, dataset: Optional[str] = None):
        super().__init__(message, error_code=ErrorCode.NO_DATA)
        self.dataset = dataset


class QualityBlockedError(DataAccessException):
    """Raised when the latest batch is blocked by quality gate."""

    def __init__(
        self,
        message: str,
        dataset: Optional[str] = None,
        failed_rules: Optional[List[str]] = None,
    ):
        super().__init__(message, error_code=ErrorCode.INVALID_DATA)
        self.dataset = dataset
        self.failed_rules = failed_rules or []


class VersionNotFoundError(DataAccessException):
    """Raised when the requested release version does not exist."""

    def __init__(
        self, message: str, dataset: Optional[str] = None, version: Optional[str] = None
    ):
        super().__init__(message, error_code=ErrorCode.CACHE_KEY_NOT_FOUND)
        self.dataset = dataset
        self.version = version


# ---------------------------------------------------------------------------
# Contract registry
# ---------------------------------------------------------------------------


@dataclass
class DatasetContract:
    dataset: str
    entity: str
    schema: EntitySchema
    params_class: type
    required_params: List[str]
    optional_params: List[str]


def get_contract(dataset: str) -> DatasetContract:
    """Retrieve the query contract for a dataset.

    Args:
        dataset: Standard dataset name (e.g. "market_quote_daily").

    Returns:
        DatasetContract instance.

    Raises:
        QueryContractError: If the dataset is not registered.
    """
    registry = _build_registry()
    if dataset not in registry:
        raise QueryContractError(
            f"Dataset {dataset!r} is not registered in the query contract.",
            error_code=ErrorCode.INVALID_TABLE_NAME,
            dataset=dataset,
        )
    return registry[dataset]


def list_contracts() -> List[str]:
    """Return all registered dataset names."""
    return list(_build_registry().keys())


def _build_registry() -> Dict[str, DatasetContract]:
    registry: Dict[str, DatasetContract] = {}

    mappings = [
        (
            "market_quote_daily",
            "market_quote_daily",
            MarketQuoteDailyParams,
            ["security_id"],
        ),
        (
            "financial_indicator",
            "financial_indicator",
            FinancialIndicatorParams,
            ["security_id"],
        ),
        (
            "macro_indicator",
            "macro_indicator",
            MacroIndicatorParams,
            ["indicator_code"],
        ),
    ]

    for dataset, entity, params_cls, required in mappings:
        schema = ENTITY_SCHEMAS.get(entity)
        if schema is None:
            continue
        all_params = [f.name for f in params_cls.__dataclass_fields__.values()]
        optional = [p for p in all_params if p not in required]
        registry[dataset] = DatasetContract(
            dataset=dataset,
            entity=entity,
            schema=schema,
            params_class=params_cls,
            required_params=required,
            optional_params=optional,
        )

    return registry
