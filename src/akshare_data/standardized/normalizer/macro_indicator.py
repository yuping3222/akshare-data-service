"""Normalizer for macro_indicator standard entity.

Standard entity definition (from 30-standard-entities.md):
- Primary keys: indicator_code, observation_date
- Date fields: observation_date, publish_date
- Ratio fields use _pct suffix (value_yoy_pct, value_mom_pct)

Raw source mapping (from config/interfaces/macro.yaml):
- macro_cpi: 月份 -> observation_date, CPI值 -> value
- macro_gdp: 季度 -> observation_date, GDP绝对额 -> value
- macro_pmi: 月份 -> observation_date, PMI -> value
- macro_lpr: 日期 -> observation_date, 1年期/5年期 -> value
- macro_ppi: 月份 -> observation_date, PPI -> value
- macro_m2: 月份 -> observation_date, M2 -> value

The normalizer supports two modes:
1. Single-indicator mode: raw data contains one indicator, caller provides
   indicator_code/indicator_name/frequency metadata.
2. Multi-indicator mode: raw data already contains indicator_code column.
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from akshare_data.standardized.normalizer.base import BaseNormalizer


class MacroIndicatorNormalizer(BaseNormalizer):
    """Normalizes raw macro indicator data to standard entity schema."""

    ENTITY_NAME = "macro_indicator"
    PRIMARY_KEYS = ["indicator_code", "observation_date"]
    DATE_FIELDS = ["observation_date", "publish_date"]

    FLOAT_FIELDS = {
        "value",
        "value_yoy_pct",
        "value_mom_pct",
    }

    STR_FIELDS = {
        "indicator_code",
        "indicator_name",
        "frequency",
        "region",
        "unit",
        "source_org",
    }

    # ── 常见日期列名映射 ──────────────────────────────────────────

    _DATE_FIELD_MAP: Dict[str, str] = {
        "月份": "observation_date",
        "季度": "observation_date",
        "日期": "observation_date",
        "时间": "observation_date",
        "统计期": "observation_date",
        "date": "observation_date",
        "month": "observation_date",
        "quarter": "observation_date",
        "year": "observation_date",
        "年度": "observation_date",
        "报告期": "observation_date",
    }

    # ── 宏观指标代码映射 (指标名/列名 -> indicator_code) ───────────

    _INDICATOR_CODE_MAP: Dict[str, str] = {
        # CPI
        "CPI": "china_cpi",
        "cpi": "china_cpi",
        "全国居民消费价格指数(CPI)上年同月=100": "china_cpi",
        "CPI指数": "china_cpi",
        "居民消费价格指数": "china_cpi",
        # GDP
        "GDP": "china_gdp",
        "gdp": "china_gdp",
        "国内生产总值绝对额(亿元)": "china_gdp",
        "国内生产总值": "china_gdp",
        "GDP绝对额": "china_gdp",
        # PMI
        "PMI": "china_pmi",
        "pmi": "china_pmi",
        "制造业PMI": "china_pmi",
        "采购经理指数": "china_pmi",
        # PPI
        "PPI": "china_ppi",
        "ppi": "china_ppi",
        "工业生产者出厂价格指数": "china_ppi",
        # LPR
        "LPR": "china_lpr",
        "lpr": "china_lpr",
        "贷款市场报价利率": "china_lpr",
        "1年期LPR": "china_lpr_1y",
        "5年期LPR": "china_lpr_5y",
        # M2
        "M2": "china_m2",
        "m2": "china_m2",
        "M2供应量(亿元)": "china_m2",
        "货币供应量M2": "china_m2",
        # 社会融资
        "social_financing": "china_social_financing",
        "社会融资规模": "china_social_financing",
        "社会融资规模(亿元)": "china_social_financing",
        # Shibor
        "Shibor": "china_shibor",
        "shibor": "china_shibor",
        "上海银行间同业拆放利率": "china_shibor",
        # 汇率
        "exchange_rate": "exchange_rate",
        "汇率": "exchange_rate",
    }

    # ── 指标名称映射 ──────────────────────────────────────────────

    _INDICATOR_NAME_MAP: Dict[str, str] = {
        "china_cpi": "中国居民消费价格指数",
        "china_gdp": "中国国内生产总值",
        "china_pmi": "中国制造业采购经理指数",
        "china_ppi": "中国工业生产者出厂价格指数",
        "china_lpr": "中国贷款市场报价利率",
        "china_lpr_1y": "中国1年期LPR",
        "china_lpr_5y": "中国5年期LPR",
        "china_m2": "中国M2货币供应量",
        "china_social_financing": "中国社会融资规模",
        "china_shibor": "上海银行间同业拆放利率",
        "exchange_rate": "汇率",
    }

    # ── 指标频率映射 ──────────────────────────────────────────────

    _FREQUENCY_MAP: Dict[str, str] = {
        "china_cpi": "M",
        "china_gdp": "Q",
        "china_pmi": "M",
        "china_ppi": "M",
        "china_lpr": "M",
        "china_lpr_1y": "M",
        "china_lpr_5y": "M",
        "china_m2": "M",
        "china_social_financing": "M",
        "china_shibor": "D",
        "exchange_rate": "D",
    }

    # ── 指标单位映射 ──────────────────────────────────────────────

    _UNIT_MAP: Dict[str, str] = {
        "china_cpi": "index",
        "china_gdp": "CNY_100M",
        "china_pmi": "index",
        "china_ppi": "index",
        "china_lpr": "pct",
        "china_lpr_1y": "pct",
        "china_lpr_5y": "pct",
        "china_m2": "CNY_100M",
        "china_social_financing": "CNY_100M",
        "china_shibor": "pct",
        "exchange_rate": "ratio",
    }

    def __init__(
        self,
        normalize_version: str = "v1",
        schema_version: str = "v1",
        mapping_loader=None,
        default_region: str = "CN",
        default_source_org: str = "国家统计局",
    ):
        super().__init__(
            normalize_version=normalize_version,
            schema_version=schema_version,
            mapping_loader=mapping_loader,
        )
        self.default_region = default_region
        self.default_source_org = default_source_org

    def normalize(
        self,
        df: pd.DataFrame,
        source: str = "",
        interface_name: str = "",
        batch_id: str = "",
        extra_fields: Optional[Dict] = None,
        indicator_code: Optional[str] = None,
        indicator_name: Optional[str] = None,
        frequency: Optional[str] = None,
    ) -> pd.DataFrame:
        """Normalize raw macro indicator DataFrame.

        Args:
            df: Raw input DataFrame.
            source: Source identifier.
            interface_name: Source interface name.
            batch_id: Batch identifier.
            extra_fields: Additional fields to include.
            indicator_code: Override indicator code (single-indicator mode).
            indicator_name: Override indicator name (single-indicator mode).
            frequency: Override frequency (single-indicator mode).

        Returns:
            Normalized DataFrame with standard macro indicator columns.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        result = df.copy()

        # Step 1: Rename date columns
        result = result.rename(columns=self._DATE_FIELD_MAP)

        # Step 2: Resolve indicator_code if not already present
        if "indicator_code" not in result.columns:
            if indicator_code:
                result["indicator_code"] = indicator_code
            elif interface_name:
                # Try to derive from interface name (e.g., "macro_cpi" -> "china_cpi")
                inferred = self._infer_indicator_code(interface_name, result)
                if inferred:
                    result["indicator_code"] = inferred

        # Step 3: Resolve indicator_name
        if "indicator_name" not in result.columns:
            if indicator_name:
                result["indicator_name"] = indicator_name
            elif "indicator_code" in result.columns:
                result["indicator_name"] = result["indicator_code"].map(
                    self._INDICATOR_NAME_MAP
                ).fillna(result["indicator_code"])

        # Step 4: Resolve frequency
        if "frequency" not in result.columns:
            if frequency:
                result["frequency"] = frequency
            elif "indicator_code" in result.columns:
                result["frequency"] = result["indicator_code"].map(
                    self._FREQUENCY_MAP
                ).fillna("M")

        # Step 5: Set defaults for region, unit, source_org
        if extra_fields is None:
            extra_fields = {}
        extra_fields.setdefault("region", self.default_region)
        extra_fields.setdefault("source_org", self.default_source_org)

        # Step 6: Map value column if not already named "value"
        result = self._map_value_column(result, interface_name)

        # Step 7: Apply parent normalization (field map, dates, types, system fields)
        parent_extra = {
            k: v for k, v in extra_fields.items()
            if k not in ("region", "source_org") or k not in result.columns
        }

        normalized = super().normalize(
            df=result,
            source=source,
            interface_name=interface_name,
            batch_id=batch_id,
            extra_fields=parent_extra if parent_extra else None,
        )

        # Ensure region and source_org are set
        if "region" not in normalized.columns:
            normalized["region"] = self.default_region
        if "source_org" not in normalized.columns:
            normalized["source_org"] = self.default_source_org
        if "unit" not in normalized.columns and "indicator_code" in normalized.columns:
            normalized["unit"] = normalized["indicator_code"].map(
                self._UNIT_MAP
            ).fillna("")

        return normalized

    def build_field_map(self, source: str) -> Dict[str, str]:
        """Build field mapping for the given source.

        For macro data, the primary mapping is done in normalize() since
        value columns vary by indicator. This method returns additional
        mappings for YoY/MoM fields.
        """
        return {
            "同比": "value_yoy_pct",
            "环比": "value_mom_pct",
            "yoy": "value_yoy_pct",
            "mom": "value_mom_pct",
            "同比增长": "value_yoy_pct",
            "环比增长": "value_mom_pct",
            "yoy_pct": "value_yoy_pct",
            "mom_pct": "value_mom_pct",
            "publish_date": "publish_date",
            "发布日期": "publish_date",
            "发布时间": "publish_date",
        }

    def _business_fields(self) -> list[str]:
        """Return all business field names."""
        return [
            "indicator_name",
            "frequency",
            "region",
            "observation_date",
            "publish_date",
            "value",
            "value_yoy_pct",
            "value_mom_pct",
            "unit",
            "source_org",
        ]

    def _infer_indicator_code(
        self, interface_name: str, df: pd.DataFrame
    ) -> Optional[str]:
        """Infer indicator code from interface name or DataFrame columns."""
        # Try interface name first
        if interface_name in self._INDICATOR_CODE_MAP:
            return self._INDICATOR_CODE_MAP[interface_name]

        # Try to match interface name pattern (e.g., "macro_cpi" -> "china_cpi")
        if interface_name.startswith("macro_"):
            suffix = interface_name[len("macro_"):]
            candidate = f"china_{suffix}"
            if candidate in self._INDICATOR_NAME_MAP:
                return candidate

        # Try to match column names
        for col in df.columns:
            if col in self._INDICATOR_CODE_MAP:
                return self._INDICATOR_CODE_MAP[col]

        return None

    def _map_value_column(
        self, df: pd.DataFrame, interface_name: str
    ) -> pd.DataFrame:
        """Map the value column to standard 'value' name.

        Handles indicator-specific column names that contain the actual value.
        """
        if "value" in df.columns:
            return df

        # Common value column patterns
        value_candidates = [
            "cpi",
            "gdp",
            "pmi",
            "ppi",
            "lpr",
            "m2",
            "social_financing",
            "shibor",
            "exchange_rate",
            "数值",
            "值",
            "数据",
            "index",
            "rate",
            "amount",
        ]

        # Interface-specific mappings
        interface_value_map = {
            "macro_cpi": ["cpi", "CPI指数", "全国居民消费价格指数(CPI)上年同月=100"],
            "macro_gdp": ["gdp", "国内生产总值绝对额(亿元)", "GDP"],
            "macro_pmi": ["pmi", "制造业PMI"],
            "macro_ppi": ["ppi", "PPI指数"],
            "macro_lpr": ["1年期LPR", "1年", "lpr"],
            "macro_m2": ["m2", "M2供应量(亿元)"],
            "macro_social_financing": ["social_financing", "社会融资规模(亿元)"],
        }

        # Check interface-specific candidates first
        if interface_name in interface_value_map:
            for candidate in interface_value_map[interface_name]:
                if candidate in df.columns:
                    df["value"] = df[candidate]
                    return df

        # Fall back to generic candidates
        for candidate in value_candidates:
            if candidate in df.columns:
                df["value"] = df[candidate]
                return df

        # If only one numeric column exists (besides date), use it
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        date_cols = [c for c in df.columns if "date" in c.lower() or "日期" in c or "月份" in c or "季度" in c]
        value_candidates = [c for c in numeric_cols if c not in date_cols]
        if len(value_candidates) == 1:
            df["value"] = df[value_candidates[0]]

        return df
