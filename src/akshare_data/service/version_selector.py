"""Version selector for Served data.

Selects which version/release of Served data to read.
In the current phase, version selection is simple: always read the latest
published version. The infrastructure supports future version pinning.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class VersionInfo:
    """Metadata about a served data version."""

    version: str
    publish_time: Optional[str] = None
    status: str = "active"
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class VersionSelector:
    """Selects which version of Served data to serve.

    Current behavior:
    - Default: latest active version
    - Supports explicit version pinning via query param
    - Falls back to latest if pinned version not found

    Future: integrate with served/publisher.py for real version manifests.
    """

    LATEST = "latest"

    def __init__(self, default_version: str = LATEST):
        self._default_version = default_version
        self._version_registry: Dict[str, VersionInfo] = {}

    def resolve(self, requested: Optional[str] = None) -> str:
        if requested is None or requested == self.LATEST:
            return self._get_latest_version()

        if requested in self._version_registry:
            info = self._version_registry[requested]
            if info.status == "active":
                return requested
            logger.warning(
                "Requested version %s is not active (status=%s), falling back to latest",
                requested,
                info.status,
            )
            return self._get_latest_version()

        logger.warning(
            "Version %s not found in registry, falling back to latest",
            requested,
        )
        return self._get_latest_version()

    def register_version(self, version: str, info: VersionInfo) -> None:
        self._version_registry[version] = info

    def get_version_info(self, version: Optional[str] = None) -> VersionInfo:
        resolved = self.resolve(version)
        if resolved in self._version_registry:
            return self._version_registry[resolved]
        return VersionInfo(
            version=resolved,
            status="unknown",
            metadata={"note": "Version info not yet registered"},
        )

    def _get_latest_version(self) -> str:
        active_versions = [
            v for v, info in self._version_registry.items() if info.status == "active"
        ]
        if active_versions:
            return active_versions[-1]
        return self.LATEST
