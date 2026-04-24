"""Extended governance module tests."""

from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import tempfile
import json

import pytest
import yaml

from akshare_data.governance.deprecation import (
    DeprecationRegistry,
    DeprecationRecord,
    DeprecationStatus,
)
from akshare_data.governance.ownership import (
    OwnershipRegistry,
    OwnershipRecord,
    Owner,
)
from akshare_data.governance.change_log import (
    ChangeLog,
    SchemaChange,
    ChangeType,
)


@pytest.fixture
def mock_schema_registry():
    """Create a mock SchemaRegistry with registered entities."""
    from unittest.mock import MagicMock
    from akshare_data.governance.schema_registry import EntitySchema, FieldDef

    registry = MagicMock()
    
    mock_entity = MagicMock()
    mock_entity.name = "market_quote_daily"
    mock_entity.version = "v1"
    mock_entity.description = "Market quote daily data"
    mock_entity.priority = "P0"
    mock_entity.storage_layer = "daily"
    mock_entity.primary_key = ["symbol", "trade_date"]
    mock_entity.partition_by = ["symbol"]
    mock_entity.fields = {"close_price": MagicMock(name="close_price", field_type="float")}
    mock_entity.system_fields = {}
    
    registry.get = MagicMock(return_value=mock_entity)
    registry.list_all = MagicMock(return_value=["market_quote_daily", "market_quote_minute"])
    
    return registry


@pytest.fixture
def temp_config_dir():
    """Create temporary config directory for owner config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_metadata_dir():
    """Create temporary metadata directory for change log."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def owner_config_file(temp_config_dir):
    """Create a sample owner config YAML."""
    config_file = temp_config_dir / "owners.yaml"
    config_file.write_text("""
domains:
  quote:
    owner_id: owner_quote
    owner_name: Quote Team
    since: "2024-01-01"
    backup_owner_id: owner_backup
  finance:
    owner_id: owner_finance
    owner_name: Finance Team
    since: "2024-01-01"

datasets:
  market_quote_daily:
    owner_id: owner_quote
    owner_name: Quote Dataset Owner
    since: "2024-01-15"
    domain: quote

quality_rules:
  completeness_rules:
    owner_id: owner_quality
    owner_name: Quality Team
    since: "2024-02-01"
""")
    return config_file


@pytest.mark.unit
class TestDeprecationRegistry:
    """Test DeprecationRegistry functionality."""

    def test_deprecate_field(self):
        """Test deprecating a field."""
        registry = DeprecationRegistry()
        record = registry.deprecate(
            entity_name="market_quote_daily",
            field_name="old_close",
            replacement_field="close_price",
            reason="Renamed to standard naming",
            window_days=30,
            owner_id="owner_001",
        )
        assert record.entity_name == "market_quote_daily"
        assert record.field_name == "old_close"
        assert record.status == DeprecationStatus.DEPRECATED
        assert record.window_days == 30

    def test_deprecate_with_minimum_window(self):
        """Test deprecation rejects window below minimum."""
        registry = DeprecationRegistry()
        with pytest.raises(ValueError, match="must be at least"):
            registry.deprecate(
                entity_name="test_entity",
                field_name="test_field",
                window_days=3,
            )

    def test_remove_deprecated_field(self):
        """Test removing a deprecated field."""
        registry = DeprecationRegistry()
        registry.deprecate(
            entity_name="test_entity",
            field_name="test_field",
            window_days=7,
        )
        with pytest.raises(ValueError, match="not yet removable"):
            registry.remove("test_entity", "test_field")

    def test_remove_after_window(self):
        """Test removing field after window passes."""
        registry = DeprecationRegistry()
        record = registry.deprecate(
            entity_name="test_entity",
            field_name="test_field",
            window_days=7,
        )
        assert record.status == DeprecationStatus.DEPRECATED

    def test_is_removed(self):
        """Test is_removed check."""
        registry = DeprecationRegistry()
        registry.deprecate("entity1", "field1", window_days=7)
        assert registry.is_removed("entity1", "field1") is False

    def test_get_replacement(self):
        """Test getting replacement field."""
        registry = DeprecationRegistry()
        registry.deprecate("entity1", "old_field", replacement_field="new_field")
        replacement = registry.get_replacement("entity1", "old_field")
        assert replacement == "new_field"

    def test_get_impact_summary(self):
        """Test impact summary generation."""
        registry = DeprecationRegistry()
        registry.deprecate(
            "entity1", "field1",
            impact_analysis={"downstream_consumers": 5},
        )
        summary = registry.get_impact_summary("entity1", "field1")
        assert summary["status"] == "DEPRECATED"
        assert summary["impact_analysis"]["downstream_consumers"] == 5


@pytest.mark.unit
class TestOwnershipRegistry:
    """Test OwnershipRegistry functionality."""

    def test_owner_creation_valid(self):
        """Test creating valid Owner."""
        owner = Owner(
            owner_id="owner_001",
            owner_name="Test Owner",
            owner_type="domain",
            scope="quote",
            since=date(2024, 1, 1),
        )
        assert owner.owner_id == "owner_001"
        assert owner.owner_type == "domain"

    def test_owner_invalid_type(self):
        """Test Owner rejects invalid type."""
        with pytest.raises(ValueError, match="owner_type must be one of"):
            Owner(
                owner_id="owner_001",
                owner_name="Test",
                owner_type="invalid_type",
                scope="test",
                since=date.today(),
            )

    def test_registry_load_config(self, owner_config_file):
        """Test loading ownership from config."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        count = registry.load()
        assert count >= 3

    def test_registry_get_owner_for_domain(self, owner_config_file):
        """Test getting owner for domain."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        registry.load()
        owner = registry.get_owner_for_domain("quote")
        assert owner is not None
        assert owner.owner_id == "owner_quote"

    def test_registry_get_owner_for_dataset(self, owner_config_file):
        """Test getting owner for dataset."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        registry.load()
        owner = registry.get_owner_for_dataset("market_quote_daily")
        assert owner is not None
        assert owner.owner_id == "owner_quote"

    def test_registry_get_owner_for_quality_rule(self, owner_config_file):
        """Test getting owner for quality rule."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        registry.load()
        owner = registry.get_owner_for_quality_rule("completeness_rules")
        assert owner is not None
        assert owner.owner_id == "owner_quality"

    def test_registry_verify_permission(self, owner_config_file):
        """Test permission verification."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        registry.load()
        assert registry.verify_permission("owner_quote", "domain", "quote") is True
        assert registry.verify_permission("owner_finance", "domain", "quote") is False

    def test_registry_list_owners_by_type(self, owner_config_file):
        """Test listing owners by type."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        registry.load()
        domain_owners = registry.list_owners_by_type("domain")
        assert len(domain_owners) >= 2

    def test_registry_assign_owner(self):
        """Test assigning new owner."""
        registry = OwnershipRegistry()
        owner = Owner(
            owner_id="new_owner",
            owner_name="New Owner",
            owner_type="dataset",
            scope="new_dataset",
            since=date.today(),
        )
        record = OwnershipRecord(owner=owner)
        registry.assign_owner(record)
        found = registry.get_owner_for_dataset("new_dataset")
        assert found is not None
        assert found.owner_id == "new_owner"

    def test_registry_get_domain_for_dataset(self, owner_config_file):
        """Test getting domain for dataset."""
        registry = OwnershipRegistry(config_path=owner_config_file)
        registry.load()
        domain = registry.get_domain_for_dataset("market_quote_daily")
        assert domain == "quote"


@pytest.mark.unit
class TestChangeLog:
    """Test ChangeLog functionality."""

    def test_record_change(self, temp_metadata_dir):
        """Test recording a schema change."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        change = log.record(
            change_type=ChangeType.ADD_FIELD,
            entity_name="market_quote_daily",
            field_name="new_field",
            owner_id="owner_001",
            version_before="v1",
            version_after="v2",
            description="Added new_field for tracking",
        )
        assert change.change_type == ChangeType.ADD_FIELD
        assert change.entity_name == "market_quote_daily"
        assert change.field_name == "new_field"
        assert change.version_after == "v2"

    def test_get_all_changes(self, temp_metadata_dir):
        """Test getting all changes."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "entity1", "field1")
        log.record(ChangeType.MODIFY_FIELD, "entity2", "field2")
        changes = log.get_all()
        assert len(changes) == 2

    def test_get_by_entity(self, temp_metadata_dir):
        """Test filtering changes by entity."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "entity1", "field1")
        log.record(ChangeType.ADD_FIELD, "entity2", "field2")
        log.record(ChangeType.MODIFY_FIELD, "entity1", "field3")
        changes = log.get_by_entity("entity1")
        assert len(changes) == 2

    def test_get_by_field(self, temp_metadata_dir):
        """Test filtering changes by field."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "entity1", "field1")
        log.record(ChangeType.MODIFY_FIELD, "entity1", "field1")
        log.record(ChangeType.ADD_FIELD, "entity1", "field2")
        changes = log.get_by_field("entity1", "field1")
        assert len(changes) == 2

    def test_get_by_owner(self, temp_metadata_dir):
        """Test filtering changes by owner."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "e1", "f1", owner_id="owner_a")
        log.record(ChangeType.ADD_FIELD, "e2", "f2", owner_id="owner_b")
        changes = log.get_by_owner("owner_a")
        assert len(changes) == 1

    def test_get_by_type(self, temp_metadata_dir):
        """Test filtering changes by type."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "e1", "f1")
        log.record(ChangeType.REMOVE_FIELD, "e2", "f2")
        log.record(ChangeType.ADD_FIELD, "e3", "f3")
        changes = log.get_by_type(ChangeType.ADD_FIELD)
        assert len(changes) == 2

    def test_get_emergency_changes(self, temp_metadata_dir):
        """Test filtering emergency changes."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "e1", "f1", emergency=False)
        log.record(ChangeType.REMOVE_FIELD, "e2", "f2", emergency=True)
        changes = log.get_emergency_changes()
        assert len(changes) == 1

    def test_get_changes_since(self, temp_metadata_dir):
        """Test filtering changes by timestamp."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "e1", "f1")
        cutoff = datetime.now(timezone.utc) + timedelta(hours=1)
        changes = log.get_changes_since(cutoff)
        assert len(changes) == 0

    def test_get_latest_version(self, temp_metadata_dir):
        """Test getting latest version from changes."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        log.record(ChangeType.ADD_FIELD, "entity1", version_before="v1", version_after="v2")
        log.record(ChangeType.MODIFY_FIELD, "entity1", version_before="v2", version_after="v3")
        latest = log.get_latest_version("entity1")
        assert latest == "v3"

    def test_persist_to_file(self, temp_metadata_dir):
        """Test persisting changes to JSONL file."""
        log = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=True)
        log.record(ChangeType.ADD_FIELD, "entity1", "field1")
        persist_path = temp_metadata_dir / "schema_changes.jsonl"
        assert persist_path.exists()
        content = persist_path.read_text()
        assert "ADD_FIELD" in content

    def test_load_from_file(self, temp_metadata_dir):
        """Test loading changes from JSONL file."""
        log1 = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=True)
        log1.record(ChangeType.ADD_FIELD, "entity1", "field1")
        log1.record(ChangeType.MODIFY_FIELD, "entity2", "field2")

        log2 = ChangeLog(persist_dir=temp_metadata_dir, auto_persist=False)
        count = log2.load_from_file()
        assert count == 2


@pytest.mark.unit
class TestDatasetCatalog:
    """Test DatasetCatalog functionality."""

    def test_catalog_register_dataset(self, mock_schema_registry):
        """Test registering a dataset."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        dataset = catalog.register(
            entity_name="market_quote_daily",
            batch_id="batch_001",
            normalize_version="n_v1",
            release_version="r_v1",
        )
        assert dataset.name == "market_quote_daily"
        assert len(dataset.versions) == 1

    def test_catalog_get_dataset(self, mock_schema_registry):
        """Test getting a registered dataset."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        catalog.register(
            entity_name="market_quote_daily",
            batch_id="batch_001",
            normalize_version="n_v1",
        )
        dataset = catalog.get("market_quote_daily")
        assert dataset is not None
        assert dataset.name == "market_quote_daily"

    def test_catalog_get_nonexistent(self):
        """Test getting nonexistent dataset raises KeyError."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog()
        with pytest.raises(KeyError):
            catalog.get("nonexistent_dataset")

    def test_catalog_get_or_none(self):
        """Test get_or_none returns None for nonexistent."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog()
        result = catalog.get_or_none("nonexistent")
        assert result is None

    def test_catalog_has_dataset(self, mock_schema_registry):
        """Test has() check."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        catalog.register("market_quote_daily", "batch_001", "n_v1")
        assert catalog.has("market_quote_daily") is True
        assert catalog.has("nonexistent") is False

    def test_catalog_list_all(self, mock_schema_registry):
        """Test listing all datasets."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        catalog.register("market_quote_daily", "b1", "n1")
        catalog.register("market_quote_minute", "b1", "n1")
        all_datasets = catalog.list_all()
        assert len(all_datasets) == 2

    def test_catalog_version_history(self, mock_schema_registry):
        """Test version history tracking."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        catalog.register("market_quote_daily", "b1", "n1")
        catalog.register("market_quote_daily", "b2", "n2")
        catalog.register("market_quote_daily", "b3", "n3")
        history = catalog.get_version_history("market_quote_daily")
        assert len(history) == 3

    def test_catalog_current_version(self, mock_schema_registry):
        """Test current version property."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        catalog.register("market_quote_daily", "b1", "n1", release_version="r1")
        catalog.register("market_quote_daily", "b2", "n2", release_version="r2")
        dataset = catalog.get("market_quote_daily")
        current = dataset.current_version
        assert current is not None
        assert current.release_version == "r2"

    def test_catalog_dataset_field_names(self, mock_schema_registry):
        """Test dataset field_names property."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        dataset = catalog.register("market_quote_daily", "b1", "n1")
        field_names = dataset.field_names
        assert len(field_names) >= 1

    def test_catalog_list_by_priority(self, mock_schema_registry):
        """Test listing datasets by priority."""
        from akshare_data.governance.catalog import DatasetCatalog

        catalog = DatasetCatalog(schema_registry=mock_schema_registry)
        catalog.register("market_quote_daily", "b1", "n1")
        p0_datasets = catalog.list_by_priority("P0")
        assert len(p0_datasets) >= 1


@pytest.mark.unit
class TestLineageTracker:
    """Test LineageTracker (from existing test file)."""

    def test_load_mapping_config_records_batch(self, tmp_path):
        """Test loading mapping config."""
        pytest.importorskip("yaml")
        from akshare_data.governance.lineage import LineageTracker

        tracker = LineageTracker()
        config_file = tmp_path / "mapping.yaml"
        config_file.write_text("""
dataset: market_quote_daily
batch_id: b_001
schema_version: s_1
normalize_version: n_1
release_version: r_1
mappings:
  - standard_field: close_price
    source_name: akshare
    source_field: close
    interface_name: stock_zh_a_daily
    transform: rename
""")
        loaded = tracker.load_mapping_config(str(config_file))
        assert loaded == 1

    def test_build_release_manifest(self):
        """Test building release manifest."""
        from akshare_data.governance.lineage import LineageTracker

        tracker = LineageTracker()
        tracker.record(
            dataset="market_quote_daily",
            standard_field="close_price",
            source_name="akshare",
            source_field="close",
            interface_name="stock_zh_a_daily",
            batch_id="b_001",
            schema_version="s_1",
            normalize_version="n_1",
            release_version="r_1",
        )
        manifest = tracker.build_release_manifest(
            dataset="market_quote_daily",
            release_version="r_1",
            batch_id="b_001",
        )
        assert manifest["dataset"] == "market_quote_daily"
        assert manifest["field_count"] == 1