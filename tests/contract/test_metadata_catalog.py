"""Contract tests for governance metadata catalog (T9-001 / T8-007)."""

from __future__ import annotations

from pathlib import Path

import pytest

from akshare_data.common.errors import ErrorCode
from akshare_data.governance.catalog import DatasetCatalog
from akshare_data.governance.metadata_catalog import MetadataCatalog
from akshare_data.governance.schema_registry import SchemaRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTITY_CONFIG_DIR = PROJECT_ROOT / "config" / "standards" / "entities"
FIELD_DICT_PATH = PROJECT_ROOT / "config" / "standards" / "field_dictionary.yaml"
QUALITY_CONFIG_DIR = PROJECT_ROOT / "config" / "quality"


@pytest.fixture(scope="module")
def metadata_catalog() -> MetadataCatalog:
    reg = SchemaRegistry(config_dir=ENTITY_CONFIG_DIR)
    reg.load_all()

    dataset_catalog = DatasetCatalog(schema_registry=reg)
    dataset_catalog.register(
        entity_name="market_quote_daily",
        batch_id="batch_001",
        normalize_version="v1",
        release_version="market_quote_daily-r202604230930-01",
    )

    return MetadataCatalog(
        dataset_catalog=dataset_catalog,
        field_dictionary_path=FIELD_DICT_PATH,
        quality_config_dir=QUALITY_CONFIG_DIR,
    )


@pytest.mark.unit
@pytest.mark.contract
class TestMetadataCatalog:
    def test_list_datasets(self, metadata_catalog: MetadataCatalog):
        assert "market_quote_daily" in metadata_catalog.list_datasets()

    def test_get_dataset_versions(self, metadata_catalog: MetadataCatalog):
        versions = metadata_catalog.get_dataset_versions("market_quote_daily")
        assert versions
        assert versions[-1]["release_version"].startswith("market_quote_daily-r")

    def test_get_field_definition(self, metadata_catalog: MetadataCatalog):
        field = metadata_catalog.get_field_definition("security_id")
        assert field is not None
        assert field["type"] == "string"

    def test_list_quality_rules(self, metadata_catalog: MetadataCatalog):
        rules = metadata_catalog.list_quality_rules("market_quote_daily")
        assert rules

    def test_get_error_semantic_layered(self, metadata_catalog: MetadataCatalog):
        semantic = metadata_catalog.get_error_semantic(ErrorCode.NO_DATA)
        assert semantic.layer == "quality"
        assert semantic.http_status == 422
