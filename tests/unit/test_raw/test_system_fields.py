"""Tests for system_fields module."""

from akshare_data.raw.system_fields import (
    SYSTEM_FIELD_NAMES,
    SYSTEM_FIELD_TYPES,
    SYSTEM_FIELDS,
    SystemField,
    is_system_field,
    get_system_field_names,
    get_system_field_types,
)


class TestSystemFieldNames:
    def test_count(self):
        assert len(SYSTEM_FIELD_NAMES) == 10

    def test_required_fields_present(self):
        expected = {
            "batch_id",
            "source_name",
            "interface_name",
            "request_params_json",
            "request_time",
            "ingest_time",
            "extract_date",
            "extract_version",
            "source_schema_fingerprint",
            "raw_record_hash",
        }
        assert set(SYSTEM_FIELD_NAMES) == expected

    def test_no_duplicates(self):
        assert len(SYSTEM_FIELD_NAMES) == len(set(SYSTEM_FIELD_NAMES))


class TestSystemFieldTypes:
    def test_all_fields_have_types(self):
        for field_name in SYSTEM_FIELD_NAMES:
            assert field_name in SYSTEM_FIELD_TYPES

    def test_type_values(self):
        assert SYSTEM_FIELD_TYPES["batch_id"] == "string"
        assert SYSTEM_FIELD_TYPES["request_time"] == "timestamp"
        assert SYSTEM_FIELD_TYPES["ingest_time"] == "timestamp"
        assert SYSTEM_FIELD_TYPES["extract_date"] == "date"
        assert SYSTEM_FIELD_TYPES["raw_record_hash"] == "string"


class TestSystemFieldsList:
    def test_count(self):
        assert len(SYSTEM_FIELDS) == 10

    def test_all_required(self):
        for sf in SYSTEM_FIELDS:
            assert sf.required is True

    def test_is_dataclass(self):
        for sf in SYSTEM_FIELDS:
            assert isinstance(sf, SystemField)


class TestHelperFunctions:
    def test_is_system_field_true(self):
        assert is_system_field("batch_id") is True
        assert is_system_field("raw_record_hash") is True

    def test_is_system_field_false(self):
        assert is_system_field("close") is False
        assert is_system_field("volume") is False
        assert is_system_field("unknown_field") is False

    def test_get_system_field_names(self):
        names = get_system_field_names()
        assert isinstance(names, list)
        assert len(names) == 10
        assert "batch_id" in names

    def test_get_system_field_types(self):
        types = get_system_field_types()
        assert isinstance(types, dict)
        assert len(types) == 10
        assert types["batch_id"] == "string"
