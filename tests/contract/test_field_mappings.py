"""Contract tests for source->standard field mappings (T10-002)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from akshare_data.governance.field_naming import FieldNamingStandard
from akshare_data.governance.schema_registry import SchemaRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTITY_CONFIG_DIR = PROJECT_ROOT / "config" / "standards" / "entities"
FIELD_DICT_PATH = PROJECT_ROOT / "config" / "standards" / "field_dictionary.yaml"
MAPPINGS_ROOT = PROJECT_ROOT / "config" / "mappings" / "sources"
VERSIONS_PATH = PROJECT_ROOT / "config" / "standards" / "normalize_versions.yaml"


@pytest.fixture(scope="module")
def schema_registry() -> SchemaRegistry:
    reg = SchemaRegistry(config_dir=ENTITY_CONFIG_DIR)
    reg.load_all()
    return reg




@pytest.fixture(scope="module")
def naming_standard() -> FieldNamingStandard:
    return FieldNamingStandard()
@pytest.fixture(scope="module")
def canonical_fields() -> set[str]:
    with open(FIELD_DICT_PATH, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    fields = set((data.get("fields") or {}).keys())
    system_fields = set((data.get("system_fields") or {}).keys())
    return fields | system_fields


@pytest.mark.unit
@pytest.mark.contract
class TestFieldMappingsContract:
    """Mappings must align with standard schema and field dictionary."""

    def test_mapping_files_exist(self):
        mapping_files = sorted(MAPPINGS_ROOT.glob("*/*.yaml"))
        assert mapping_files, f"No mapping files found under {MAPPINGS_ROOT}"

    def test_mapping_minimum_structure(self):
        for path in sorted(MAPPINGS_ROOT.glob("*/*.yaml")):
            with open(path, encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}
            assert cfg.get("dataset"), f"Missing dataset in {path}"
            assert cfg.get("source"), f"Missing source in {path}"
            assert cfg.get("mapping_version"), f"Missing mapping_version in {path}"
            assert isinstance(cfg.get("fields"), dict) and cfg["fields"], (
                f"Missing or empty fields in {path}"
            )

    def test_standard_fields_exist_in_field_dictionary(
        self,
        canonical_fields: set[str],
    ):
        for path in sorted(MAPPINGS_ROOT.glob("*/*.yaml")):
            with open(path, encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}

            for source_field, spec in (cfg.get("fields") or {}).items():
                if isinstance(spec, str):
                    standard_field = spec
                else:
                    standard_field = (spec or {}).get("standard_field")
                    status = (spec or {}).get("status", "active")
                    if status != "active":
                        continue

                assert standard_field in canonical_fields, (
                    f"{path}: '{source_field}' -> '{standard_field}' not in canonical field dictionary"
                )

    def test_standard_fields_exist_in_entity_schema(self, schema_registry: SchemaRegistry):
        for path in sorted(MAPPINGS_ROOT.glob("*/*.yaml")):
            with open(path, encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}

            dataset = cfg.get("dataset")
            schema = schema_registry.get(dataset)
            allowed = set(schema.fields.keys()) | set(schema.system_fields.keys())

            for source_field, spec in (cfg.get("fields") or {}).items():
                if isinstance(spec, str):
                    standard_field = spec
                else:
                    standard_field = (spec or {}).get("standard_field")
                    status = (spec or {}).get("status", "active")
                    if status != "active":
                        continue

                assert standard_field in allowed, (
                    f"{path}: mapped field '{standard_field}' not declared in entity '{dataset}'"
                )

    def test_mapped_standard_fields_follow_naming_standard(
        self,
        naming_standard: FieldNamingStandard,
    ):
        for path in sorted(MAPPINGS_ROOT.glob("*/*.yaml")):
            with open(path, encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}

            for spec in (cfg.get("fields") or {}).values():
                if isinstance(spec, str):
                    standard_field = spec
                else:
                    standard_field = (spec or {}).get("standard_field")
                    status = (spec or {}).get("status", "active")
                    if status != "active":
                        continue

                result = naming_standard.validate(standard_field)
                assert result.valid, (
                    f"{path}: canonical field '{standard_field}' violates naming standard: {result.reasons}"
                )

    def test_normalize_version_registry_covers_mapped_sources(self):
        with open(VERSIONS_PATH, encoding="utf-8") as fh:
            versions = yaml.safe_load(fh) or {}

        datasets_cfg = versions.get("datasets") or {}

        for path in sorted(MAPPINGS_ROOT.glob("*/*.yaml")):
            with open(path, encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}

            dataset = cfg.get("dataset")
            source = cfg.get("source")
            assert dataset in datasets_cfg, (
                f"{path}: dataset '{dataset}' missing in normalize_versions registry"
            )
            source_cfg = (datasets_cfg.get(dataset) or {}).get("sources") or {}
            assert source in source_cfg, (
                f"{path}: source '{source}' missing in normalize_versions for dataset '{dataset}'"
            )
            assert source_cfg[source].get("current_version"), (
                f"{path}: current_version missing for {dataset}/{source}"
            )
import yaml


def _mapping_files() -> list[Path]:
    base = Path("config/mappings/sources")
    return sorted(base.glob("*/*.yaml"))


def test_mapping_files_exist() -> None:
    files = _mapping_files()
    assert files, "expected mapping config files under config/mappings/sources"


def test_mapping_has_dataset_source_and_fields() -> None:
    for file in _mapping_files():
        payload = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
        assert payload.get("dataset"), f"{file} should include dataset"
        assert payload.get("source"), f"{file} should include source"

        fields = payload.get("fields", {})
        sub_sources = payload.get("sub_sources", {})
        assert fields or sub_sources, f"{file} should define fields or sub_sources"

        for src_field, spec in fields.items():
            assert src_field
            assert isinstance(spec, dict)
            assert spec.get("status") in {"active", "deprecated", "pending"}
            if spec.get("status") == "active":
                assert spec.get("standard_field"), (
                    f"{file} active field '{src_field}' missing standard_field"
                )
