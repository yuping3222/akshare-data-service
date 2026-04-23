"""Served (L2) layer — versioned, gate-approved data for service consumption.

Modules:
    manifest   — ReleaseManifest model and persistence.
    publisher  — Publish standardized data to served after quality gate.
    reader     — Read served data by dataset, defaulting to latest stable version.
    rollback   — Roll back a dataset to its previous release version.
"""

from __future__ import annotations

from .manifest import GateDecision, ReleaseManifest, ReleaseStatus
from .publisher import Publisher, PublishError
from .reader import Reader
from .rollback import RollbackError, RollbackManager
from .versioning import ReleaseVersion, ReleaseVersionError, next_release_version

__all__ = [
    "GateDecision",
    "ReleaseManifest",
    "ReleaseStatus",
    "ReleaseVersion",
    "ReleaseVersionError",
    "next_release_version",
    "Publisher",
    "PublishError",
    "Reader",
    "RollbackError",
    "RollbackManager",
]
