"""Tests for schema_fingerprint module."""

import pandas as pd

from akshare_data.raw.schema_fingerprint import (
    compute_schema_fingerprint,
    compute_column_fingerprint,
    schemas_match,
    describe_schema,
)


class TestComputeSchemaFingerprint:
    def test_deterministic(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        fp1 = compute_schema_fingerprint(df)
        fp2 = compute_schema_fingerprint(df)
        assert fp1 == fp2

    def test_format(self):
        df = pd.DataFrame({"a": [1]})
        fp = compute_schema_fingerprint(df)
        assert fp.startswith("sha256:")

    def test_different_schemas_different_fingerprints(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"a": [1], "b": [2]})
        fp1 = compute_schema_fingerprint(df1)
        fp2 = compute_schema_fingerprint(df2)
        assert fp1 != fp2

    def test_column_order_independent(self):
        df1 = pd.DataFrame({"a": [1], "b": [2.0]})
        df2 = pd.DataFrame({"b": [2.0], "a": [1]})
        fp1 = compute_schema_fingerprint(df1)
        fp2 = compute_schema_fingerprint(df2)
        assert fp1 == fp2

    def test_exclude_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2.0], "c": ["x"]})
        fp_all = compute_schema_fingerprint(df)
        fp_excluded = compute_schema_fingerprint(df, exclude_columns=["c"])
        assert fp_all != fp_excluded

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        fp = compute_schema_fingerprint(df)
        assert fp.startswith("sha256:")


class TestComputeColumnFingerprint:
    def test_returns_dict(self):
        df = pd.DataFrame({"a": [1], "b": [2.0]})
        result = compute_column_fingerprint(df)
        assert isinstance(result, dict)
        assert "a" in result
        assert "b" in result

    def test_format(self):
        df = pd.DataFrame({"a": [1]})
        result = compute_column_fingerprint(df)
        assert result["a"].startswith("sha256:")

    def test_exclude_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2.0]})
        result = compute_column_fingerprint(df, exclude_columns=["b"])
        assert "a" in result
        assert "b" not in result


class TestSchemasMatch:
    def test_same_fingerprints(self):
        assert schemas_match("sha256:abc", "sha256:abc") is True

    def test_different_fingerprints(self):
        assert schemas_match("sha256:abc", "sha256:def") is False


class TestDescribeSchema:
    def test_returns_list_of_dicts(self):
        df = pd.DataFrame({"a": [1], "b": [2.0]})
        schema = describe_schema(df)
        assert isinstance(schema, list)
        assert len(schema) == 2
        assert schema[0]["name"] in ("a", "b")
        assert "dtype" in schema[0]

    def test_exclude_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2.0], "c": ["x"]})
        schema = describe_schema(df, exclude_columns=["c"])
        names = [s["name"] for s in schema]
        assert "c" not in names
        assert len(names) == 2

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        schema = describe_schema(df)
        assert schema == []
