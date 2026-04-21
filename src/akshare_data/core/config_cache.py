"""Shared configuration cache for akshare-data-service.

A thread-safe singleton that loads and caches the four config files
shared between fetcher.py and ConfigLoader:
  - akshare_registry.yaml
  - interfaces/  (or interfaces.yaml)
  - rate_limits.yaml
  - sources/sources.yaml

All callers read from a single cache, so runtime config changes are
visible to everyone after invalidate() is called.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from akshare_data.core.config_dir import get_config_dir

logger = logging.getLogger(__name__)

# Cache keys
_REGISTRY_KEY = "registry"
_INTERFACES_KEY = "interfaces"
_RATE_LIMITS_KEY = "rate_limits"
_SOURCES_KEY = "sources"

# Map cache keys to their file paths (for invalidate-by-path)
_KEY_TO_PATH: Dict[str, Path] = {}


def _init_key_to_path():
    cfg = get_config_dir()
    _KEY_TO_PATH[_REGISTRY_KEY] = cfg / "akshare_registry.yaml"
    _KEY_TO_PATH[_INTERFACES_KEY] = cfg / "interfaces"
    _KEY_TO_PATH[_RATE_LIMITS_KEY] = cfg / "rate_limits.yaml"
    _KEY_TO_PATH[_SOURCES_KEY] = cfg / "sources" / "sources.yaml"


_init_key_to_path()


class _ConfigCache:
    """Thread-safe singleton configuration cache."""

    _instance: Optional["_ConfigCache"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "_ConfigCache":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._cache: Dict[str, Any] = {}
                    cls._instance._dir = get_config_dir()
        return cls._instance

    # ── Public load methods ──────────────────────────────────────────

    def load_registry(self) -> Dict[str, Any]:
        """Load akshare_registry.yaml (auto-generated metadata).

        Returns {"interfaces": {}, "rate_limits": {}} if file missing.
        """
        key = _REGISTRY_KEY
        if key in self._cache:
            return self._cache[key]

        path = self._dir / "akshare_registry.yaml"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            self._cache[key] = data
            logger.debug("Loaded registry from %s", path)
        else:
            data = {"interfaces": {}, "rate_limits": {}}
            self._cache[key] = data
            logger.warning("Registry not found: %s", path)
        return self._cache[key]

    def load_interfaces(self) -> Dict[str, Any]:
        """Load interface definitions from interfaces/ directory or interfaces.yaml.

        This returns the raw data only. The merge logic that combines
        manual interfaces with registry metadata lives in fetcher.py.
        """
        key = _INTERFACES_KEY
        if key in self._cache:
            return self._cache[key]

        interfaces: Dict[str, Any] = {}
        iface_dir = self._dir / "interfaces"

        # Multi-file mode (preferred)
        if iface_dir.is_dir():
            for yaml_file in sorted(iface_dir.glob("*.yaml")):
                with open(yaml_file, "r", encoding="utf-8") as f:
                    content = yaml.safe_load(f) or {}
                interfaces.update(content)
                logger.debug("Loaded %d interfaces from %s", len(content), yaml_file.name)
        else:
            # Single-file fallback
            path = self._dir / "interfaces.yaml"
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    interfaces = yaml.safe_load(f) or {}
                logger.debug("Loaded interfaces from %s", path)

        self._cache[key] = interfaces
        logger.debug("Total %d interfaces loaded", len(interfaces))
        return self._cache[key]

    def load_rate_limits(self) -> Dict[str, Any]:
        """Load rate_limits.yaml, falling back to registry's rate_limits section."""
        key = _RATE_LIMITS_KEY
        if key in self._cache:
            return self._cache[key]

        path = self._dir / "rate_limits.yaml"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            self._cache[key] = data
            logger.debug("Loaded rate limits from %s", path)
        else:
            registry = self.load_registry()
            data = registry.get("rate_limits", {"default": {"interval": 0.5}})
            self._cache[key] = data
            logger.debug("Loaded rate limits from registry")

        return self._cache[key]

    def load_sources(self) -> Dict[str, Any]:
        """Load sources/sources.yaml, returning the 'sources' dict."""
        key = _SOURCES_KEY
        if key in self._cache:
            return self._cache[key]

        path = self._dir / "sources" / "sources.yaml"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            result = data.get("sources", {})
            self._cache[key] = result
            logger.debug("Loaded source registry: %s", list(result.keys()))
        else:
            self._cache[key] = {}
            logger.debug("sources.yaml not found, adapter routing disabled")

        return self._cache[key]

    # ── Invalidation ─────────────────────────────────────────────────

    def invalidate(self, path: str | Path | None = None) -> None:
        """Clear cache entries.

        Args:
            path: If given, clear the cache entry matching this file path.
                  If None, clear all cache entries.
        """
        if path is None:
            self._cache.clear()
            logger.debug("ConfigCache: cleared all entries")
            return

        path = Path(path)
        for key, key_path in _KEY_TO_PATH.items():
            # Exact match, or path is inside the key directory (for interfaces/)
            if key_path == path or (key_path.is_dir() and path.is_relative_to(key_path)):
                self._cache.pop(key, None)
                logger.debug("ConfigCache: cleared %s", key)
                return
        logger.debug("ConfigCache: no entry for path %s", path)


# Module-level singleton
ConfigCache = _ConfigCache()
