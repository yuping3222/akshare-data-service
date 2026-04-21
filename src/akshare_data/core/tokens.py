"""Centralized token management for all data sources.

Provides a thread-safe singleton TokenManager that resolves tokens from
multiple sources in a consistent priority order:
1. Programmatically set token (via set_token)
2. Environment variable
3. token.cfg file

Usage:
    from akshare_data.core.tokens import TokenManager

    tm = TokenManager.get_instance()
    tm.set_token("tushare", "my-token")
    token = tm.get_token("tushare")
"""

from __future__ import annotations

import configparser
import logging
import os
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


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
        3. token.cfg file (format: TUSHARE_TOKEN=xxx or LIXINGER_TOKEN=xxx per line)

    token.cfg search order:
        1. $AKSHARE_DATA_CONFIG_DIR/token.cfg (if env var set)
        2. Walk up from this file's directory to find a config/ dir, then config/token.cfg
        3. ~/.config/akshare-data/token.cfg
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
        """Programmatically set a token for a given source.

        Args:
            source: Source name ("tushare", "lixinger", etc.)
            token: The token value.
        """
        self._tokens[source] = token

    def get_token(self, source: str) -> Optional[str]:
        """Resolve a token for the given source.

        Checks in order: programmatic -> env var -> token.cfg file.

        Args:
            source: Source name ("tushare", "lixinger", etc.)

        Returns:
            Token string or None if not found.
        """
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
        for _ in range(10):  # limit walk depth
            candidate = current / "config" / "token.cfg"
            if candidate.exists():
                return candidate
            # Also check for token.cfg directly at this level (e.g. project root)
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
                pass  # fall through to key-value format

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
