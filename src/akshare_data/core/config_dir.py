"""Resolve the config directory for akshare-data-service.

Works correctly both from a git checkout and from a pip-installed wheel.

Priority:
1. $AKSHARE_DATA_CONFIG_DIR (user-specified config directory)
2. Walk up from this file to find a directory containing config/rate_limits.yaml
   (works from git checkout / editable install)
3. Bundled config inside the wheel via importlib.resources
   (files copied to ~/.config/akshare-data/ on first use)
4. Path(__file__).parent.parent.parent.parent.parent / "config" (legacy fallback)
5. ~/.config/akshare-data/ (pip-installed default; created on demand)
"""

from __future__ import annotations

import os
import shutil
from functools import lru_cache
from pathlib import Path
from typing import Optional


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
    candidate = Path(__file__).resolve().parent  # core/
    for _ in range(12):  # generous limit
        sentinel = candidate / "config" / "rate_limits.yaml"
        if sentinel.exists():
            return candidate / "config"
        parent = candidate.parent
        if parent == candidate:  # reached filesystem root
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
    """Check if config files are bundled inside the wheel.

    When installed via pip, config files are included under
    akshare_data/_bundled_config/.  On first access they are copied
    to ~/.config/akshare-data/ so the user can edit them.
    """
    try:
        from importlib.resources import files as _resource_files

        pkg_files = _resource_files("akshare_data._bundled_config")
        # Check for the sentinel file
        sentinel = pkg_files / "rate_limits.yaml"
        # For Python < 3.11, files() on a directory may not support .exists()
        # Try to read the sentinel to verify it works
        try:
            sentinel.read_text()  # will raise if not found
        except Exception:
            return None

        default = Path.home() / ".config" / "akshare-data"
        sentinel_user = default / "rate_limits.yaml"

        # Only copy if the user config doesn't already exist
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
        pass  # non-failure; user config dir will just be empty


def get_project_root() -> Optional[Path]:
    """Return the project root (parent of config dir) when running from source.

    Returns None when the config dir comes from $AKSHARE_DATA_CONFIG_DIR,
    bundled wheel config, or the pip-installed default, since in those
    cases there is no meaningful project root.
    """
    config = get_config_dir()
    sentinel = config / "rate_limits.yaml"
    # Only treat config.parent as project root when the sentinel exists
    # AND we can detect it's a git checkout (look for pyproject.toml).
    if sentinel.exists() and (config.parent / "pyproject.toml").exists():
        return config.parent
    return None
