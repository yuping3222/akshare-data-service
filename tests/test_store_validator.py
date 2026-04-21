"""tests/test_store_validator.py

Comprehensive tests for validator.py
"""

import pandas as pd
import pytest
import pyarrow as pa

from akshare_data.store.validator import (
    SchemaValidationError,
    SchemaValidator,
    PYARROW_TYPE_MAP,
    infer_schema,
    normalize_date_columns,
    deduplicate_by_key,
)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
            "str_col": ["a", "b", "c"],
            "bool_col": [True, False, True],
            "date_col": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        }
    )


class TestSchemaValidationError:
    """Tests for SchemaValidationError exception."""

    def test_error_creation(self):
        """Test SchemaValidationError can be created."""
        errors = ["Missing column: 'id'", "Incompatible type"]
        error = SchemaValidationError("test_table", errors)
        assert error.table == "test_table"
        assert error.errors == errors

    def test_error_string_representation(self):
        """Test SchemaValidationError string representation."""
        errors = ["Missing column: 'id'"]
        error = SchemaValidationError("test_table", errors)
        assert "test_table" in str(error)
        assert "Missing column" in str(error)

    def test_error_multiple_errors(self):
        """Test SchemaValidationError with multiple errors."""
        errors = ["Error 1", "Error 2", "Error 3"]
        error = SchemaValidationError("table", errors)
        assert len(error.errors) == 3


class TestPYARROW_TYPE_MAP:
    """Tests for PYARROW_TYPE_MAP constant."""

    def test_type_map_contains_basic_types(self):
        """Test that type map contains all basic types."""
        assert "string" in PYARROW_TYPE_MAP
        assert "int64" in PYARROW_TYPE_MAP
        assert "float64" in PYARROW_TYPE_MAP
        assert "bool" in PYARROW_TYPE_MAP

    def test_type_map_values_are_pyarrow_types(self):
        """Test that type map values are valid PyArrow types."""
        for name, pa_type in PYARROW_TYPE_MAP.items():
            assert isinstance(pa_type, pa.DataType)


class TestSchemaValidatorInit:
    """Tests for SchemaValidator initialization."""

    def test_init(self):
        """Test SchemaValidator initialization."""
        schema = {"col1": "string", "col2": "int64"}
        validator = SchemaValidator("test_table", schema)
        assert validator.table == "test_table"
        assert validator.schema == schema


class TestSchemaValidatorValidate:
    """Tests for SchemaValidator.validate method."""

    def test_validate_missing_column(self):
        """Test validate detects missing columns."""
        schema = {"col1": "string", "col2": "int64"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": ["a", "b"]})
        errors = validator.validate(df)
        assert any("col2" in e and "Missing" in e for e in errors)

    def test_validate_no_errors(self):
        """Test validate returns no errors for valid data."""
        schema = {"col1": "string"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": ["a", "b", "c"]})
        errors = validator.validate(df)
        assert errors == []

    def test_validate_incompatible_type(self):
        """Test validate detects incompatible types."""
        schema = {"col1": "int64"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": pd.to_datetime(["2024-01-01", "2024-01-02"])})
        errors = validator.validate(df)
        assert len(errors) > 0


class TestSchemaValidatorValidateAndCast:
    """Tests for SchemaValidator.validate_and_cast method."""

    def test_validate_and_cast_raises_on_validation_error(self):
        """Test validate_and_cast raises SchemaValidationError on invalid data."""
        schema = {"col1": "string", "col2": "int64"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": ["a", "b"]})
        with pytest.raises(SchemaValidationError):
            validator.validate_and_cast(df)

    def test_validate_and_cast_string_to_int(self):
        """Test validate_and_cast converts string to int."""
        schema = {"col1": "int64"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": ["1", "2", "3"]})
        result = validator.validate_and_cast(df)
        assert result["col1"].dtype in (pd.Int64Dtype(), "int64")

    def test_validate_and_cast_no_changes_when_same_type(self):
        """Test validate_and_cast makes no changes when type is same."""
        schema = {"col1": "string"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": ["a", "b", "c"]})
        result = validator.validate_and_cast(df)
        pd.testing.assert_frame_equal(result, df)

    def test_validate_and_cast_primary_key_null_check(self):
        """Test validate_and_cast checks for nulls in primary key."""
        schema = {"id": "int64", "name": "string"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"id": [1, None, 3], "name": ["a", "b", "c"]})
        with pytest.raises(SchemaValidationError) as exc_info:
            validator.validate_and_cast(df, primary_key=["id"])
        assert any("null" in e.lower() for e in exc_info.value.errors)

    def test_validate_and_cast_float_to_float64(self):
        """Test validate_and_cast converts float to float64."""
        schema = {"col1": "float64"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": [1.1, 2.2, 3.3]})
        result = validator.validate_and_cast(df)
        assert result["col1"].dtype == "float64"

    def test_validate_and_cast_float32(self):
        """Test validate_and_cast handles float32."""
        schema = {"col1": "float32"}
        validator = SchemaValidator("test_table", schema)
        df = pd.DataFrame({"col1": [1.1, 2.2, 3.3]})
        result = validator.validate_and_cast(df)
        assert result["col1"].dtype == "float32"


class TestSchemaValidatorCastColumn:
    """Tests for SchemaValidator._cast_column method."""

    def test_cast_column_string(self):
        """Test _cast_column handles string type."""
        validator = SchemaValidator("test", {"col": "string"})
        series = pd.Series([1, 2, 3])
        result = validator._cast_column(series, "string")
        assert result.dtype == object

    def test_cast_column_int64(self):
        """Test _cast_column handles int64 type."""
        validator = SchemaValidator("test", {"col": "int64"})
        series = pd.Series(["1", "2", "3"])
        result = validator._cast_column(series, "int64")
        assert result.dtype == pd.Int64Dtype()

    def test_cast_column_int32(self):
        """Test _cast_column handles int32 type."""
        validator = SchemaValidator("test", {"col": "int32"})
        series = pd.Series(["1", "2", "3"])
        result = validator._cast_column(series, "int32")
        assert result.dtype == pd.Int32Dtype()

    def test_cast_column_float64(self):
        """Test _cast_column handles float64 type."""
        validator = SchemaValidator("test", {"col": "float64"})
        series = pd.Series([1, 2, 3])
        result = validator._cast_column(series, "float64")
        assert result.dtype == "float64"

    def test_cast_column_float32(self):
        """Test _cast_column handles float32 type."""
        validator = SchemaValidator("test", {"col": "float32"})
        series = pd.Series([1.0, 2.0, 3.0])
        result = validator._cast_column(series, "float32")
        assert result.dtype == "float32"

    def test_cast_column_date(self):
        """Test _cast_column handles date type."""
        validator = SchemaValidator("test", {"col": "date"})
        series = pd.Series(["2024-01-01", "2024-01-02"])
        result = validator._cast_column(series, "date")
        assert result.dtype == object

    def test_cast_column_datetime(self):
        """Test _cast_column handles datetime type."""
        validator = SchemaValidator("test", {"col": "datetime"})
        series = pd.Series(["2024-01-01", "2024-01-02"])
        result = validator._cast_column(series, "datetime")
        assert "datetime" in str(result.dtype)

    def test_cast_column_bool(self):
        """Test _cast_column handles bool type."""
        validator = SchemaValidator("test", {"col": "bool"})
        series = pd.Series([1, 0, 1])
        result = validator._cast_column(series, "bool")
        assert result.dtype == bool

    def test_cast_column_unknown_type(self):
        """Test _cast_column returns original for unknown type."""
        validator = SchemaValidator("test", {"col": "unknown"})
        series = pd.Series([1, 2, 3])
        result = validator._cast_column(series, "unknown")
        pd.testing.assert_series_equal(result, series)


class TestSchemaValidatorIsCompatible:
    """Tests for SchemaValidator.is_compatible static method."""

    def test_is_compatible_same_type(self):
        """Test is_compatible returns True for same types."""
        assert SchemaValidator.is_compatible("string", "string") is True
        assert SchemaValidator.is_compatible("int64", "int64") is True

    def test_is_compatible_numeric_to_numeric(self):
        """Test is_compatible allows numeric to numeric."""
        assert SchemaValidator.is_compatible("int64", "float64") is True
        assert SchemaValidator.is_compatible("int32", "int64") is True
        assert SchemaValidator.is_compatible("float32", "float64") is True

    def test_is_compatible_datetime64_to_date(self):
        """Test is_compatible allows datetime64 to date types."""
        assert SchemaValidator.is_compatible("datetime64[ns]", "date") is True
        assert SchemaValidator.is_compatible("datetime64[ns]", "timestamp") is True

    def test_is_compatible_object_to_date(self):
        """Test is_compatible allows object to date types."""
        assert SchemaValidator.is_compatible("object", "date") is True
        assert SchemaValidator.is_compatible("object", "datetime") is True

    def test_is_compatible_object_to_numeric(self):
        """Test is_compatible allows object to numeric types."""
        assert SchemaValidator.is_compatible("object", "int64") is True
        assert SchemaValidator.is_compatible("object", "float64") is True

    def test_is_compatible_numeric_to_string(self):
        """Test is_compatible allows numeric to string."""
        assert SchemaValidator.is_compatible("int64", "string") is True
        assert SchemaValidator.is_compatible("float64", "string") is True

    def test_is_compatible_object_to_string(self):
        """Test is_compatible allows object to string."""
        assert SchemaValidator.is_compatible("object", "string") is True

    def test_is_compatible_bool_conversions(self):
        """Test is_compatible allows bool to string/int."""
        assert SchemaValidator.is_compatible("bool", "string") is True
        assert SchemaValidator.is_compatible("bool", "int64") is True
        assert SchemaValidator.is_compatible("bool", "int32") is True

    def test_is_compatible_incompatible_types(self):
        """Test is_compatible handles various type combinations."""
        assert SchemaValidator.is_compatible("category", "int64") is False
        assert SchemaValidator.is_compatible("timedelta", "float64") is False


class TestInferSchema:
    """Tests for infer_schema function."""

    def test_infer_schema_float(self):
        """Test infer_schema detects float columns."""
        df = pd.DataFrame({"col": [1.1, 2.2, 3.3]})
        schema = infer_schema(df)
        assert schema["col"] == "float64"

    def test_infer_schema_int(self):
        """Test infer_schema detects int columns."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        schema = infer_schema(df)
        assert schema["col"] == "int64"

    def test_infer_schema_bool(self):
        """Test infer_schema detects bool columns."""
        df = pd.DataFrame({"col": [True, False, True]})
        schema = infer_schema(df)
        assert schema["col"] == "bool"

    def test_infer_schema_object(self):
        """Test infer_schema detects object columns."""
        df = pd.DataFrame({"col": ["a", "b", "c"]})
        schema = infer_schema(df)
        assert schema["col"] == "string"

    def test_infer_schema_datetime(self):
        """Test infer_schema detects datetime columns."""
        df = pd.DataFrame({"col": pd.to_datetime(["2024-01-01", "2024-01-02"])})
        schema = infer_schema(df)
        assert schema["col"] == "datetime"

    def test_infer_schema_unknown(self):
        """Test infer_schema defaults to string for unknown types."""
        df = pd.DataFrame({"col": pd.to_timedelta(["1 days", "2 days"])})
        schema = infer_schema(df)
        assert schema["col"] == "string"


class TestNormalizeDateColumns:
    """Tests for normalize_date_columns function."""

    def test_normalize_date_columns_specific_columns(self):
        """Test normalize_date_columns with specific columns."""
        df = pd.DataFrame({
            "date1": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "date2": pd.to_datetime(["2024-01-03", "2024-01-04"]),
            "other": [1, 2],
        })
        result = normalize_date_columns(df, columns=["date1"])
        assert result["date1"].dtype == object
        assert result["other"].dtype == "int64"

    def test_normalize_date_columns_all_columns(self):
        """Test normalize_date_columns normalizes all datetime columns."""
        df = pd.DataFrame({
            "date1": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "date2": pd.to_datetime(["2024-01-03", "2024-01-04"]),
        })
        result = normalize_date_columns(df)
        assert result["date1"].dtype == object
        assert result["date2"].dtype == object

    def test_normalize_date_columns_none_columns(self):
        """Test normalize_date_columns with None uses all columns."""
        df = pd.DataFrame({
            "date1": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        })
        result = normalize_date_columns(df, columns=None)
        assert result["date1"].dtype == object

    def test_normalize_date_columns_missing_column(self):
        """Test normalize_date_columns handles missing columns gracefully."""
        df = pd.DataFrame({"col1": pd.to_datetime(["2024-01-01", "2024-01-02"])})
        result = normalize_date_columns(df, columns=["nonexistent"])
        pd.testing.assert_frame_equal(result, df)


class TestDeduplicateByKey:
    """Tests for deduplicate_by_key function."""

    def test_deduplicate_by_key_removes_duplicates(self):
        """Test deduplicate_by_key removes duplicates."""
        df = pd.DataFrame({
            "id": [1, 2, 2, 3],
            "value": ["a", "b", "c", "d"],
        })
        result = deduplicate_by_key(df, ["id"])
        assert len(result) == 3

    def test_deduplicate_by_key_keeps_last(self):
        """Test deduplicate_by_key keeps last occurrence."""
        df = pd.DataFrame({
            "id": [1, 2, 2, 2],
            "value": ["a", "b", "c", "d"],
        })
        result = deduplicate_by_key(df, ["id"])
        assert result[result["id"] == 2]["value"].iloc[-1] == "d"

    def test_deduplicate_by_key_no_duplicates(self):
        """Test deduplicate_by_key with no duplicates."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        result = deduplicate_by_key(df, ["id"])
        assert len(result) == 3

    def test_deduplicate_by_key_multiple_keys(self):
        """Test deduplicate_by_key with multiple keys."""
        df = pd.DataFrame({
            "id1": [1, 1, 2, 2],
            "id2": [1, 1, 2, 2],
            "value": ["a", "b", "c", "d"],
        })
        result = deduplicate_by_key(df, ["id1", "id2"])
        assert len(result) == 2
