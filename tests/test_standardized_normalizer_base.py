"""Tests for akshare_data.standardized.normalizer.base module."""

import pandas as pd

from akshare_data.standardized.normalizer.base import (
    NormalizerBase,
    load_field_mapping,
    load_entity_schema,
)


class ConcreteNormalizer(NormalizerBase):
    """Minimal concrete implementation for testing the base class."""

    dataset_name = "test_dataset"
    normalize_version = "v1"
    schema_version = "v1"
    _required_standard_fields = {"security_id", "trade_date", "close_price"}

    def _field_mapping(self, source_name: str) -> dict:
        return {"dt": "trade_date", "px": "close_price", "sid": "security_id"}


class TestNormalizerBase:
    """Test the generic normalization pipeline."""

    def setup_method(self):
        self.normalizer = ConcreteNormalizer()

    def test_normalize_empty_dataframe(self):
        """Should return empty DataFrame for empty input."""
        result = self.normalizer.normalize(
            pd.DataFrame(),
            batch_id="b1",
            source_name="test",
            interface_name="test_if",
        )
        assert result.empty

    def test_normalize_none_dataframe(self):
        """Should return empty DataFrame for None input."""
        result = self.normalizer.normalize(
            None,
            batch_id="b1",
            source_name="test",
            interface_name="test_if",
        )
        assert result.empty

    def test_normalize_applies_field_mapping(self):
        """Should rename columns according to _field_mapping."""
        df = pd.DataFrame(
            {
                "dt": ["2024-01-02"],
                "px": [100.0],
                "sid": ["600519"],
            }
        )
        result = self.normalizer.normalize(
            df,
            batch_id="b1",
            source_name="test",
            interface_name="test_if",
        )
        assert "trade_date" in result.columns
        assert "close_price" in result.columns
        assert "security_id" in result.columns
        assert "dt" not in result.columns

    def test_normalize_injects_system_fields(self):
        """Should add all mandatory system columns."""
        df = pd.DataFrame(
            {
                "dt": ["2024-01-02"],
                "px": [100.0],
                "sid": ["600519"],
            }
        )
        result = self.normalizer.normalize(
            df,
            batch_id="batch_001",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
        )
        assert result["batch_id"].iloc[0] == "batch_001"
        assert result["source_name"].iloc[0] == "akshare"
        assert result["interface_name"].iloc[0] == "stock_zh_a_hist"
        assert result["normalize_version"].iloc[0] == "v1"
        assert result["schema_version"].iloc[0] == "v1"
        assert "ingest_time" in result.columns

    def test_normalize_selects_only_required_columns(self):
        """Output should contain only required fields + system fields."""
        df = pd.DataFrame(
            {
                "dt": ["2024-01-02"],
                "px": [100.0],
                "sid": ["600519"],
                "extra_col": ["should_be_dropped"],
            }
        )
        result = self.normalizer.normalize(
            df,
            batch_id="b1",
            source_name="test",
            interface_name="test_if",
        )
        expected_cols = {
            "security_id",
            "trade_date",
            "close_price",
            "batch_id",
            "source_name",
            "interface_name",
            "ingest_time",
            "normalize_version",
            "schema_version",
        }
        assert set(result.columns) == expected_cols

    def test_normalize_preserves_ingest_time(self):
        """Should use provided ingest_time instead of now()."""
        from datetime import datetime, timezone

        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        df = pd.DataFrame(
            {
                "dt": ["2024-01-02"],
                "px": [100.0],
                "sid": ["600519"],
            }
        )
        result = self.normalizer.normalize(
            df,
            batch_id="b1",
            source_name="test",
            interface_name="test_if",
            ingest_time=fixed_time,
        )
        assert result["ingest_time"].iloc[0] == fixed_time


class TestConfigLoader:
    """Test the config loader placeholder functions."""

    def test_load_field_mapping_returns_empty_for_missing(self):
        """Should return empty dict when config file doesn't exist."""
        result = load_field_mapping("nonexistent_dataset", "nonexistent_source")
        assert result == {}

    def test_load_entity_schema_returns_empty_for_missing(self):
        """Should return empty dict when schema file doesn't exist."""
        result = load_entity_schema("nonexistent_dataset")
        assert result == {}
