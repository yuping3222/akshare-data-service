"""Tests for financial_indicator and macro_indicator normalizers.

Covers:
- FinancialIndicatorNormalizer: AkShare EM source, Lixinger source
- MacroIndicatorNormalizer: CPI, GDP, PMI sources
- System field injection
- Date conversion
- Numeric coercion
- Primary key handling
- Ratio field _pct suffix
"""

import json
from pathlib import Path

import pytest

import pandas as pd

from akshare_data.standardized.normalizer.financial_indicator import (
    FinancialIndicatorNormalizer,
)
from akshare_data.standardized.normalizer.macro_indicator import MacroIndicatorNormalizer

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "standardized_samples"


def _load_json(name: str) -> list[dict]:
    """Load a JSON fixture file."""
    with open(name, "r", encoding="utf-8") as f:
        return json.load(f)


# ── FinancialIndicatorNormalizer Tests ─────────────────────────────


class TestFinancialIndicatorNormalizer:
    """Test FinancialIndicatorNormalizer."""

    @pytest.fixture
    def normalizer(self):
        return FinancialIndicatorNormalizer()

    @pytest.fixture
    def akshare_em_df(self):
        """Load AkShare EM raw sample."""
        data = _load_json(
            FIXTURES_DIR / "financial_indicator" / "akshare_em_raw.json"
        )
        return pd.DataFrame(data)

    @pytest.fixture
    def lixinger_df(self):
        """Load Lixinger raw sample."""
        data = _load_json(
            FIXTURES_DIR / "financial_indicator" / "lixinger_raw.json"
        )
        return pd.DataFrame(data)

    def test_akshare_em_produces_standard_fields(self, normalizer, akshare_em_df):
        """Should produce standard entity fields from AkShare EM raw data."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
            interface_name="finance_indicator",
            batch_id="test_001",
        )

        assert "security_id" in result.columns
        assert "report_date" in result.columns
        assert "report_type" in result.columns
        assert "roe_pct" in result.columns
        assert "net_margin_pct" in result.columns
        assert "gross_margin_pct" in result.columns
        assert "debt_ratio_pct" in result.columns
        assert "revenue" in result.columns
        assert "net_profit" in result.columns
        assert "basic_eps" in result.columns

    def test_lixinger_produces_standard_fields(self, normalizer, lixinger_df):
        """Should produce standard entity fields from Lixinger raw data."""
        result = normalizer.normalize(
            lixinger_df,
            source="lixinger",
            interface_name="finance_indicator",
            batch_id="test_002",
        )

        assert "security_id" in result.columns
        assert "report_date" in result.columns
        assert "report_type" in result.columns
        assert "pe_ratio_ttm" in result.columns
        assert "pb_ratio" in result.columns
        assert "ps_ratio_ttm" in result.columns
        assert "roe_pct" in result.columns
        assert "roa_pct" in result.columns
        assert "total_assets" in result.columns
        assert "total_equity" in result.columns

    def test_date_conversion(self, normalizer, akshare_em_df):
        """Should convert report_date to datetime."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
            interface_name="finance_indicator",
        )

        assert pd.api.types.is_datetime64_any_dtype(result["report_date"])

    def test_numeric_coercion(self, normalizer, lixinger_df):
        """Should coerce numeric fields."""
        result = normalizer.normalize(
            lixinger_df,
            source="lixinger",
            interface_name="finance_indicator",
        )

        assert pd.api.types.is_numeric_dtype(result["pe_ratio_ttm"])
        assert pd.api.types.is_numeric_dtype(result["roe_pct"])
        assert pd.api.types.is_numeric_dtype(result["revenue"])

    def test_system_fields_injected(self, normalizer, akshare_em_df):
        """Should inject system tracking fields."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
            interface_name="finance_indicator",
            batch_id="test_batch",
        )

        assert "batch_id" in result.columns
        assert "source_name" in result.columns
        assert "interface_name" in result.columns
        assert "ingest_time" in result.columns
        assert "normalize_version" in result.columns
        assert "schema_version" in result.columns

    def test_default_report_type(self, normalizer, akshare_em_df):
        """Should set default report_type when not provided."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
        )

        assert "report_type" in result.columns
        assert result["report_type"].iloc[0] == "Q"

    def test_custom_report_type(self, normalizer, akshare_em_df):
        """Should accept custom report_type via extra_fields."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
            extra_fields={"report_type": "A"},
        )

        assert result["report_type"].iloc[0] == "A"

    def test_empty_dataframe(self, normalizer):
        """Should return empty DataFrame for empty input."""
        result = normalizer.normalize(pd.DataFrame(), source="akshare_em")
        assert result.empty

    def test_none_dataframe(self, normalizer):
        """Should return empty DataFrame for None input."""
        result = normalizer.normalize(None, source="akshare_em")
        assert result.empty

    def test_primary_keys_order(self, normalizer, akshare_em_df):
        """Primary keys should appear first in output columns."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
        )

        cols = result.columns.tolist()
        pk_indices = [cols.index(pk) for pk in normalizer.PRIMARY_KEYS if pk in cols]
        assert pk_indices == sorted(pk_indices)

    def test_ratio_fields_have_pct_suffix(self, normalizer, lixinger_df):
        """Ratio fields should have _pct suffix per entity spec."""
        result = normalizer.normalize(
            lixinger_df,
            source="lixinger",
        )

        ratio_fields = [
            "roe_pct",
            "roa_pct",
            "debt_ratio_pct",
            "gross_margin_pct",
            "net_margin_pct",
        ]
        for field in ratio_fields:
            if field in result.columns:
                assert field.endswith("_pct"), f"{field} should end with _pct"

    def test_no_legacy_date_or_symbol_as_primary(self, normalizer, akshare_em_df):
        """Should not use 'date' or 'symbol' as primary field names."""
        result = normalizer.normalize(
            akshare_em_df,
            source="akshare_em",
        )

        pk_cols = list(normalizer.PRIMARY_KEYS)
        assert "date" not in pk_cols
        assert "symbol" not in pk_cols
        assert "report_date" in pk_cols
        assert "security_id" in pk_cols
        assert len(result) > 0


# ── MacroIndicatorNormalizer Tests ─────────────────────────────────


class TestMacroIndicatorNormalizer:
    """Test MacroIndicatorNormalizer."""

    @pytest.fixture
    def normalizer(self):
        return MacroIndicatorNormalizer()

    @pytest.fixture
    def cpi_df(self):
        """Load CPI raw sample."""
        data = _load_json(FIXTURES_DIR / "macro_indicator" / "macro_cpi_raw.json")
        return pd.DataFrame(data)

    @pytest.fixture
    def gdp_df(self):
        """Load GDP raw sample."""
        data = _load_json(FIXTURES_DIR / "macro_indicator" / "macro_gdp_raw.json")
        return pd.DataFrame(data)

    @pytest.fixture
    def pmi_df(self):
        """Load PMI raw sample."""
        data = _load_json(FIXTURES_DIR / "macro_indicator" / "macro_pmi_raw.json")
        return pd.DataFrame(data)

    def test_cpi_produces_standard_fields(self, normalizer, cpi_df):
        """Should produce standard entity fields from CPI raw data."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
            batch_id="test_cpi",
        )

        assert "indicator_code" in result.columns
        assert "observation_date" in result.columns
        assert "value" in result.columns
        assert "indicator_name" in result.columns
        assert "frequency" in result.columns

    def test_gdp_produces_standard_fields(self, normalizer, gdp_df):
        """Should produce standard entity fields from GDP raw data."""
        result = normalizer.normalize(
            gdp_df,
            source="akshare_em",
            interface_name="macro_gdp",
            batch_id="test_gdp",
        )

        assert "indicator_code" in result.columns
        assert "observation_date" in result.columns
        assert "value" in result.columns
        assert result["indicator_code"].iloc[0] == "china_gdp"

    def test_pmi_produces_standard_fields(self, normalizer, pmi_df):
        """Should produce standard entity fields from PMI raw data."""
        result = normalizer.normalize(
            pmi_df,
            source="akshare_em",
            interface_name="macro_pmi",
            batch_id="test_pmi",
        )

        assert "indicator_code" in result.columns
        assert "observation_date" in result.columns
        assert "value" in result.columns
        assert result["indicator_code"].iloc[0] == "china_pmi"

    def test_date_conversion(self, normalizer, cpi_df):
        """Should convert observation_date to datetime."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        assert pd.api.types.is_datetime64_any_dtype(result["observation_date"])

    def test_value_column_mapped(self, normalizer, cpi_df):
        """Should map indicator-specific value column to 'value'."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        assert "value" in result.columns
        assert pd.api.types.is_numeric_dtype(result["value"])

    def test_system_fields_injected(self, normalizer, cpi_df):
        """Should inject system tracking fields."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
            batch_id="test_batch",
        )

        assert "batch_id" in result.columns
        assert "source_name" in result.columns
        assert "interface_name" in result.columns
        assert "ingest_time" in result.columns

    def test_indicator_code_inferred_from_interface(self, normalizer, cpi_df):
        """Should infer indicator_code from interface_name."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        assert result["indicator_code"].iloc[0] == "china_cpi"

    def test_indicator_name_resolved(self, normalizer, cpi_df):
        """Should resolve indicator_name from indicator_code."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        assert result["indicator_name"].iloc[0] == "中国居民消费价格指数"

    def test_frequency_resolved(self, normalizer, gdp_df):
        """Should resolve frequency from indicator_code."""
        result = normalizer.normalize(
            gdp_df,
            source="akshare_em",
            interface_name="macro_gdp",
        )

        assert result["frequency"].iloc[0] == "Q"

    def test_default_region_and_source_org(self, normalizer, cpi_df):
        """Should set default region and source_org."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        assert "region" in result.columns
        assert result["region"].iloc[0] == "CN"
        assert "source_org" in result.columns

    def test_explicit_indicator_code(self, normalizer, pmi_df):
        """Should accept explicit indicator_code override."""
        result = normalizer.normalize(
            pmi_df,
            source="akshare_em",
            interface_name="macro_pmi",
            indicator_code="custom_pmi",
            indicator_name="自定义PMI",
            frequency="M",
        )

        assert result["indicator_code"].iloc[0] == "custom_pmi"
        assert result["indicator_name"].iloc[0] == "自定义PMI"
        assert result["frequency"].iloc[0] == "M"

    def test_empty_dataframe(self, normalizer):
        """Should return empty DataFrame for empty input."""
        result = normalizer.normalize(pd.DataFrame(), source="akshare_em")
        assert result.empty

    def test_none_dataframe(self, normalizer):
        """Should return empty DataFrame for None input."""
        result = normalizer.normalize(None, source="akshare_em")
        assert result.empty

    def test_primary_keys_order(self, normalizer, cpi_df):
        """Primary keys should appear first in output columns."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        cols = result.columns.tolist()
        pk_indices = [cols.index(pk) for pk in normalizer.PRIMARY_KEYS if pk in cols]
        assert pk_indices == sorted(pk_indices)

    def test_no_legacy_date_or_symbol_as_primary(self, normalizer, cpi_df):
        """Should not use 'date' or 'symbol' as primary field names."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        pk_cols = list(normalizer.PRIMARY_KEYS)
        assert "date" not in pk_cols
        assert "symbol" not in pk_cols
        assert "observation_date" in pk_cols
        assert "indicator_code" in pk_cols
        assert len(result) > 0

    def test_unit_field_populated(self, normalizer, cpi_df):
        """Should populate unit field based on indicator_code."""
        result = normalizer.normalize(
            cpi_df,
            source="akshare_em",
            interface_name="macro_cpi",
        )

        assert "unit" in result.columns
        assert result["unit"].iloc[0] == "index"

    def test_gdp_unit(self, normalizer, gdp_df):
        """GDP should have CNY_100M unit."""
        result = normalizer.normalize(
            gdp_df,
            source="akshare_em",
            interface_name="macro_gdp",
        )

        assert result["unit"].iloc[0] == "CNY_100M"
