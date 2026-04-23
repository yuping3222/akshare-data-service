"""Ownership registry for data domains, datasets, and quality rules.

Provides structured owner assignment and permission verification across
the governance layer. Owners are loaded from YAML config and can be
queried by domain, dataset, or quality rule scope.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "governance" / "owners.yaml"
)


@dataclass(frozen=True)
class Owner:
    """Represents a responsible party for a governance scope.

    Attributes:
        owner_id: Unique identifier (e.g. GitHub username).
        owner_name: Display name.
        owner_type: One of 'domain', 'dataset', 'quality_rule'.
        scope: The resource this owner is responsible for.
        since: Date when ownership began.
        backup_owner_id: Optional backup owner identifier.
    """

    owner_id: str
    owner_name: str
    owner_type: str
    scope: str
    since: date
    backup_owner_id: str | None = None

    def __post_init__(self) -> None:
        valid_types = {"domain", "dataset", "quality_rule"}
        if self.owner_type not in valid_types:
            raise ValueError(
                f"owner_type must be one of {valid_types}, got '{self.owner_type}'"
            )


@dataclass
class OwnershipRecord:
    """Mutable record for an ownership assignment with audit trail.

    Attributes:
        owner: The assigned Owner.
        created_at: When this record was created.
        updated_at: When this record was last updated.
        metadata: Additional key-value metadata.
    """

    owner: Owner
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)


class OwnershipRegistry:
    """Registry for ownership assignments across domains, datasets, and rules.

    Loads owner definitions from YAML config and provides lookup and
    permission verification operations.

    Integration points:
    - SchemaChange: verifies owner_id before allowing changes
    - DeprecationRecord: links deprecation to responsible owner
    - DatasetCatalog: Dataset.metadata can reference owner_id
    """

    def __init__(self, config_path: Path | None = None) -> None:
        self._config_path = config_path or _DEFAULT_CONFIG_PATH
        self._records: dict[str, OwnershipRecord] = {}
        self._domain_index: dict[str, str] = {}
        self._dataset_index: dict[str, str] = {}
        self._quality_rule_index: dict[str, str] = {}

    def load(self) -> int:
        """Load owner definitions from YAML config.

        Returns:
            Number of ownership records loaded.
        """
        config_path = Path(self._config_path)
        if not config_path.exists():
            logger.warning("Owner config not found: %s", config_path)
            return 0

        with open(config_path, encoding="utf-8") as fh:
            raw: dict[str, Any] = yaml.safe_load(fh) or {}

        count = 0
        for section, items in raw.items():
            if not isinstance(items, dict):
                continue

            owner_type = self._section_to_owner_type(section)
            for scope, info in items.items():
                record = self._parse_record(scope, owner_type, info)
                if record is not None:
                    self._register(record)
                    count += 1

        logger.info("Loaded %d ownership records from %s", count, config_path)
        return count

    def _section_to_owner_type(self, section: str) -> str:
        """Map YAML section name to owner_type string."""
        mapping = {
            "domains": "domain",
            "datasets": "dataset",
            "quality_rules": "quality_rule",
        }
        return mapping.get(section, section)

    def _parse_record(
        self, scope: str, owner_type: str, info: dict[str, Any]
    ) -> OwnershipRecord | None:
        """Parse a single owner entry from YAML into an OwnershipRecord."""
        owner_id = info.get("owner_id")
        owner_name = info.get("owner_name", owner_id)
        if not owner_id:
            logger.warning("Missing owner_id for scope %s", scope)
            return None

        since_str = info.get("since", date.today().isoformat())
        if isinstance(since_str, str):
            since = date.fromisoformat(since_str)
        else:
            since = date.today()

        owner = Owner(
            owner_id=owner_id,
            owner_name=owner_name,
            owner_type=owner_type,
            scope=scope,
            since=since,
            backup_owner_id=info.get("backup_owner_id"),
        )

        metadata: dict[str, Any] = {}
        domain = info.get("domain")
        if domain:
            metadata["domain"] = domain
        dataset = info.get("dataset")
        if dataset:
            metadata["dataset"] = dataset

        return OwnershipRecord(owner=owner, metadata=metadata)

    def _register(self, record: OwnershipRecord) -> None:
        """Register an ownership record and update indexes."""
        key = self._make_key(record.owner.owner_type, record.owner.scope)
        self._records[key] = record

        if record.owner.owner_type == "domain":
            self._domain_index[record.owner.scope] = record.owner.owner_id
        elif record.owner.owner_type == "dataset":
            self._dataset_index[record.owner.scope] = record.owner.owner_id
        elif record.owner.owner_type == "quality_rule":
            self._quality_rule_index[record.owner.scope] = record.owner.owner_id

    @staticmethod
    def _make_key(owner_type: str, scope: str) -> str:
        return f"{owner_type}:{scope}"

    def get_owner_for_dataset(self, dataset_name: str) -> Owner | None:
        """Get the owner for a dataset.

        Falls back to domain owner if no direct dataset owner is found.

        Args:
            dataset_name: Canonical dataset name.

        Returns:
            Owner for the dataset, or None if not found.
        """
        key = self._make_key("dataset", dataset_name)
        record = self._records.get(key)
        if record is not None:
            return record.owner

        return None

    def get_owner_for_domain(self, domain: str) -> Owner | None:
        """Get the owner for a data domain.

        Args:
            domain: Domain name (e.g. 'quote', 'finance', 'macro').

        Returns:
            Owner for the domain, or None if not found.
        """
        key = self._make_key("domain", domain)
        record = self._records.get(key)
        if record is not None:
            return record.owner
        return None

    def get_owner_for_quality_rule(self, rule_name: str) -> Owner | None:
        """Get the owner for a quality rule package.

        Args:
            rule_name: Quality rule package name.

        Returns:
            Owner for the quality rule, or None if not found.
        """
        key = self._make_key("quality_rule", rule_name)
        record = self._records.get(key)
        if record is not None:
            return record.owner
        return None

    def get_domain_for_dataset(self, dataset_name: str) -> str | None:
        """Get the domain that a dataset belongs to.

        Args:
            dataset_name: Canonical dataset name.

        Returns:
            Domain name, or None if not found.
        """
        key = self._make_key("dataset", dataset_name)
        record = self._records.get(key)
        if record is not None:
            return record.metadata.get("domain")
        return None

    def verify_permission(self, owner_id: str, owner_type: str, scope: str) -> bool:
        """Verify that an owner has permission for a given scope.

        Args:
            owner_id: The owner identifier to verify.
            owner_type: One of 'domain', 'dataset', 'quality_rule'.
            scope: The resource scope.

        Returns:
            True if the owner has permission, False otherwise.
        """
        key = self._make_key(owner_type, scope)
        record = self._records.get(key)
        if record is None:
            return False
        return record.owner.owner_id == owner_id

    def verify_dataset_change_permission(
        self, owner_id: str, dataset_name: str
    ) -> bool:
        """Verify permission for a dataset change.

        Checks dataset owner first, then falls back to domain owner.

        Args:
            owner_id: The owner identifier to verify.
            dataset_name: Canonical dataset name.

        Returns:
            True if the owner can make changes to the dataset.
        """
        if self.verify_permission(owner_id, "dataset", dataset_name):
            return True

        record = self._records.get(self._make_key("dataset", dataset_name))
        if record is not None:
            domain = record.metadata.get("domain")
            if domain and self.verify_permission(owner_id, "domain", domain):
                return True

        return False

    def list_owners_by_type(self, owner_type: str) -> list[Owner]:
        """List all owners of a given type.

        Args:
            owner_type: One of 'domain', 'dataset', 'quality_rule'.

        Returns:
            List of Owner objects.
        """
        return [
            record.owner
            for record in self._records.values()
            if record.owner.owner_type == owner_type
        ]

    def list_all(self) -> dict[str, OwnershipRecord]:
        """Return a copy of all ownership records."""
        return dict(self._records)

    def assign_owner(self, record: OwnershipRecord) -> None:
        """Assign or update an owner for a scope.

        Args:
            record: OwnershipRecord to register.
        """
        record.updated_at = datetime.now(timezone.utc)
        self._register(record)
        logger.info(
            "Assigned owner %s (%s) to %s:%s",
            record.owner.owner_id,
            record.owner.owner_name,
            record.owner.owner_type,
            record.owner.scope,
        )
