"""Offline tools core infrastructure."""

import importlib

_LAZY_IMPORTS = {
    "Paths": ".paths",
    "ConfigLoader": ".config_loader",
    "OfflineError": ".errors",
    "ConfigError": ".errors",
    "DownloadError": ".errors",
    "ProbeError": ".errors",
    "AnalysisError": ".errors",
    "retry": ".retry",
    "RetryConfig": ".retry",
    "load_table": ".data_loader",
    "get_cache_manager_instance": ".data_loader",
}

__all__ = [
    "Paths",
    "ConfigLoader",
    "OfflineError",
    "ConfigError",
    "DownloadError",
    "ProbeError",
    "AnalysisError",
    "retry",
    "RetryConfig",
    "load_table",
    "get_cache_manager_instance",
]


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module = importlib.import_module(_LAZY_IMPORTS[name], __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
