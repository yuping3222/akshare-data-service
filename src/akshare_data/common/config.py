"""Configuration and token management for akshare-data-service.

Provides:
- get_config_dir: Resolve the config directory
- get_project_root: Get the project root when running from source
- ConfigCache: Thread-safe singleton for cached config loading
- TokenManager: Thread-safe singleton for API token management

This is the canonical location for config and token management.
The old `akshare_data.core.config_dir`, `core.config_cache`, and `core.tokens`
are compatibility shells.
"""

from __future__ import annotations

import configparser
import logging
import os
import shutil
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)


# ============================================================================
# Config Directory Resolution
# ============================================================================


@lru_cache(maxsize=1)
def get_config_dir() -> Path:
    """Return the config directory path (cached singleton).

    Resolution order:
    1. $AKSHARE_DATA_CONFIG_DIR
    2. Walk up from this file looking for config/rate_limits.yaml
    3. Bundled config inside the wheel (copied to ~/.config/akshare-data/ on first use)
    4. Five levels up + "config" (legacy fallback)
    5. ~/.config/akshare-data/  (created if missing)
    """
    # 1. Environment variable
    env = os.environ.get("AKSHARE_DATA_CONFIG_DIR")
    if env:
        return Path(env)

    # 2. Walk up from this file looking for config/rate_limits.yaml
    candidate = Path(__file__).resolve().parent  # common/
    for _ in range(12):
        sentinel = candidate / "config" / "rate_limits.yaml"
        if sentinel.exists():
            return candidate / "config"
        parent = candidate.parent
        if parent == candidate:
            break
        candidate = parent

    # 3. Bundled config inside the wheel
    bundled = _get_bundled_config_dir()
    if bundled:
        return bundled

    # 4. Legacy fallback: five levels up from this file
    legacy = Path(__file__).parent.parent.parent.parent.parent / "config"
    if legacy.exists():
        return legacy

    # 5. pip-installed default
    default = Path.home() / ".config" / "akshare-data"
    default.mkdir(parents=True, exist_ok=True)
    return default


def _get_bundled_config_dir() -> Optional[Path]:
    """Check if config files are bundled inside the wheel."""
    try:
        from importlib.resources import files as _resource_files

        pkg_files = _resource_files("akshare_data._bundled_config")
        sentinel = pkg_files / "rate_limits.yaml"
        try:
            sentinel.read_text()
        except Exception:
            return None

        default = Path.home() / ".config" / "akshare-data"
        sentinel_user = default / "rate_limits.yaml"

        if not sentinel_user.exists():
            default.mkdir(parents=True, exist_ok=True)
            _copy_bundled_config(pkg_files, default)

        return default
    except Exception:
        return None


def _copy_bundled_config(src: Path, dst: Path) -> None:
    """Copy bundled config files to the user config directory."""
    try:
        for item in src.iterdir():
            if item.is_dir():
                shutil.copytree(item, dst / item.name, dirs_exist_ok=True)
            else:
                shutil.copy2(item, dst / item.name)
    except Exception:
        pass


def get_project_root() -> Optional[Path]:
    """Return the project root (parent of config dir) when running from source.

    Returns None when the config dir comes from $AKSHARE_DATA_CONFIG_DIR,
    bundled wheel config, or the pip-installed default.
    """
    config = get_config_dir()
    sentinel = config / "rate_limits.yaml"
    if sentinel.exists() and (config.parent / "pyproject.toml").exists():
        return config.parent
    return None


# ============================================================================
# Config Cache
# ============================================================================

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

    def load_registry(self) -> Dict[str, Any]:
        """Load akshare_registry.yaml."""
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
        """Load interface definitions from interfaces/ directory or interfaces.yaml."""
        key = _INTERFACES_KEY
        if key in self._cache:
            return self._cache[key]

        interfaces: Dict[str, Any] = {}
        iface_dir = self._dir / "interfaces"

        if iface_dir.is_dir():
            for yaml_file in sorted(iface_dir.glob("*.yaml")):
                with open(yaml_file, "r", encoding="utf-8") as f:
                    content = yaml.safe_load(f) or {}
                interfaces.update(content)
                logger.debug(
                    "Loaded %d interfaces from %s", len(content), yaml_file.name
                )
        else:
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

    def invalidate(self, path: str | Path | None = None) -> None:
        """Clear cache entries."""
        if path is None:
            self._cache.clear()
            logger.debug("ConfigCache: cleared all entries")
            return

        path = Path(path)
        for key, key_path in _KEY_TO_PATH.items():
            if key_path == path or (
                key_path.is_dir() and path.is_relative_to(key_path)
            ):
                self._cache.pop(key, None)
                logger.debug("ConfigCache: cleared %s", key)
                return
        logger.debug("ConfigCache: no entry for path %s", path)


# Module-level singleton
ConfigCache = _ConfigCache()


# ============================================================================
# Token Manager
# ============================================================================

# Map of source name -> environment variable name
_SOURCE_ENV_MAP = {
    "tushare": "TUSHARE_TOKEN",
    "lixinger": "LIXINGER_TOKEN",
}

# Map of source name -> section/key name in token.cfg
_SOURCE_CFG_KEYS = {
    "tushare": ("tushare", "token"),
    "lixinger": ("lixinger", "token"),
}


class TokenManager:
    """Thread-safe singleton for managing API tokens.

    Token resolution order:
        1. Programmatically set token (set_token)
        2. Environment variable (e.g. TUSHARE_TOKEN, LIXINGER_TOKEN)
        3. token.cfg file
    """

    _instance: Optional["TokenManager"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._tokens: dict[str, str] = {}
        self._token_cfg_cache: Optional[dict[str, str]] = None
        self._cfg_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "TokenManager":
        """Get or create the singleton TokenManager."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (useful for testing)."""
        with cls._lock:
            cls._instance = None

    def set_token(self, source: str, token: str) -> None:
        """Programmatically set a token for a given source."""
        self._tokens[source] = token

    def get_token(self, source: str) -> Optional[str]:
        """Resolve a token for the given source."""
        # 1. Check programmatic token
        if source in self._tokens:
            return self._tokens[source]

        # 2. Check environment variable
        env_name = _SOURCE_ENV_MAP.get(source)
        if env_name:
            env_val = os.environ.get(env_name)
            if env_val:
                return env_val

        # 3. Check token.cfg file
        cfg_tokens = self._load_token_cfg()
        if cfg_tokens and source in cfg_tokens:
            return cfg_tokens[source]

        return None

    def _load_token_cfg(self) -> dict[str, str]:
        """Load tokens from token.cfg, caching the result."""
        with self._cfg_lock:
            if self._token_cfg_cache is not None:
                return self._token_cfg_cache

            cfg_path = self._find_token_cfg()
            if cfg_path is None:
                self._token_cfg_cache = {}
                return self._token_cfg_cache

            tokens = self._parse_token_cfg(cfg_path)
            self._token_cfg_cache = tokens
            return tokens

    def _find_token_cfg(self) -> Optional[Path]:
        """Find the token.cfg file path."""
        # 1. Check $AKSHARE_DATA_CONFIG_DIR/token.cfg
        config_dir = os.environ.get("AKSHARE_DATA_CONFIG_DIR")
        if config_dir:
            p = Path(config_dir) / "token.cfg"
            if p.exists():
                return p

        # 2. Walk up from this file's directory to find config/token.cfg or token.cfg
        current = Path(__file__).resolve().parent
        for _ in range(10):
            candidate = current / "config" / "token.cfg"
            if candidate.exists():
                return candidate
            direct = current / "token.cfg"
            if direct.exists():
                return direct
            parent = current.parent
            if parent == current:
                break
            current = parent

        # 3. Check ~/.config/akshare-data/token.cfg
        home_cfg = Path.home() / ".config" / "akshare-data" / "token.cfg"
        if home_cfg.exists():
            return home_cfg

        return None

    def _parse_token_cfg(self, cfg_path: Path) -> dict[str, str]:
        """Parse token.cfg file.

        Supports two formats:
        - INI style: [tushare]\ntoken=xxx\n\n[lixinger]\ntoken=xxx
        - Key-value style: TUSHARE_TOKEN=xxx\nLIXINGER_TOKEN=xxx
        """
        tokens: dict[str, str] = {}
        try:
            content = cfg_path.read_text(encoding="utf-8").strip()
        except Exception as e:
            logger.warning(f"Failed to read token.cfg at {cfg_path}: {e}")
            return tokens

        # Try INI format first
        if content.startswith("["):
            try:
                config = configparser.ConfigParser()
                config.read_string(content)
                for source, (section, key) in _SOURCE_CFG_KEYS.items():
                    val = config.get(section, key, fallback="")
                    if val:
                        tokens[source] = val
                return tokens
            except Exception:
                pass

        # Key-value format: KEY=VALUE per line
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip().upper()
            value = value.strip()
            if not value:
                continue

            for source, env_name in _SOURCE_ENV_MAP.items():
                if key == env_name or key == f"{source.upper()}_TOKEN":
                    tokens[source] = value
                    break

        # Fallback: if no tokens extracted, treat entire content as a bare lixinger token
        if not tokens and content:
            tokens["lixinger"] = content

        return tokens


# Convenience module-level functions
def get_token(source: str) -> Optional[str]:
    """Get a token for the given source."""
    return TokenManager.get_instance().get_token(source)


def set_token(source: str, token: str) -> None:
    """Set a token for the given source."""
    TokenManager.get_instance().set_token(source, token)


__all__ = [
    "get_config_dir",
    "get_project_root",
    "ConfigCache",
    "TokenManager",
    "get_token",
    "set_token",
]
