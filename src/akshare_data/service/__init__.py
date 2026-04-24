"""Service layer for akshare-data-service.

Read-only access to Served data with missing data handling and version selection.
"""


def __getattr__(name: str):
    """Lazy imports to avoid circular dependency with api.py."""
    if name == "DataService":
        from akshare_data.service.data_service import DataService

        return DataService
    if name == "QueryResult":
        from akshare_data.service.data_service import QueryResult

        return QueryResult
    if name == "ServedReader":
        from akshare_data.service.reader import ServedReader

        return ServedReader
    if name == "VersionSelector":
        from akshare_data.service.version_selector import VersionSelector

        return VersionSelector
    if name == "VersionInfo":
        from akshare_data.service.version_selector import VersionInfo

        return VersionInfo
    if name == "MissingDataPolicy":
        from akshare_data.service.missing_data_policy import MissingDataPolicy

        return MissingDataPolicy
    if name == "MissingAction":
        from akshare_data.service.missing_data_policy import MissingAction

        return MissingAction
    if name == "MissingDataReport":
        from akshare_data.service.missing_data_policy import MissingDataReport

        return MissingDataReport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return [
        "DataService",
        "QueryResult",
        "ServedReader",
        "VersionSelector",
        "VersionInfo",
        "MissingDataPolicy",
        "MissingAction",
        "MissingDataReport",
    ]


__all__ = __dir__()
