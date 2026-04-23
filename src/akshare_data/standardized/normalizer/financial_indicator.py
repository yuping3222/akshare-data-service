"""Normalizer for financial_indicator standard entity.

Standard entity definition (from 30-standard-entities.md):
- Primary keys: security_id, report_date, report_type
- Date fields: report_date, publish_date
- Ratio fields use _pct suffix
- Currency in CNY by default

Raw source mapping (from config/interfaces/equity.yaml finance_indicator):
- 报告日期 -> report_date
- 基本每股收益 -> basic_eps
- 加权净资产收益率 -> roe_pct
- 销售净利率 -> net_margin_pct
- 销售毛利率 -> gross_margin_pct
- 资产负债率 -> debt_ratio_pct
- 营业总收入 -> revenue
- 净利润 -> net_profit

Additional fields from other sources (lixinger, etc.):
- pe, pe_ttm -> pe_ratio_ttm
- pb -> pb_ratio
- ps, ps_ttm -> ps_ratio_ttm
- roa -> roa_pct
- 总资产 -> total_assets
- 净资产/所有者权益 -> total_equity
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from akshare_data.standardized.normalizer.base import BaseNormalizer


class FinancialIndicatorNormalizer(BaseNormalizer):
    """Normalizes raw financial indicator data to standard entity schema."""

    ENTITY_NAME = "financial_indicator"
    PRIMARY_KEYS = ["security_id", "report_date", "report_type"]
    DATE_FIELDS = ["report_date", "publish_date"]

    FLOAT_FIELDS = {
        "pe_ratio_ttm",
        "pb_ratio",
        "ps_ratio_ttm",
        "roe_pct",
        "roa_pct",
        "net_profit",
        "revenue",
        "total_assets",
        "total_equity",
        "debt_ratio_pct",
        "gross_margin_pct",
        "net_margin_pct",
        "basic_eps",
    }

    STR_FIELDS = {"security_id", "report_type", "currency"}

    # ── AkShare 东财财务指标字段映射 ──────────────────────────────

    _AKSHARE_EM_FIELD_MAP: Dict[str, str] = {
        "报告日期": "report_date",
        "基本每股收益": "basic_eps",
        "加权净资产收益率": "roe_pct",
        "销售净利率": "net_margin_pct",
        "销售毛利率": "gross_margin_pct",
        "资产负债率": "debt_ratio_pct",
        "营业总收入": "revenue",
        "净利润": "net_profit",
        # 常见变体
        "报告期": "report_date",
        "EPS": "basic_eps",
        "每股收益": "basic_eps",
        "ROE": "roe_pct",
        "净资产收益率": "roe_pct",
        "净利率": "net_margin_pct",
        "毛利率": "gross_margin_pct",
        "负债率": "debt_ratio_pct",
        "营业收入": "revenue",
        "归属净利润": "net_profit",
        "归母净利润": "net_profit",
    }

    # ── Lixinger / 通用财务指标字段映射 ────────────────────────────

    _LIXINGER_FIELD_MAP: Dict[str, str] = {
        "pe": "pe_ratio_ttm",
        "pe_ttm": "pe_ratio_ttm",
        "市盈率": "pe_ratio_ttm",
        "市盈率TTM": "pe_ratio_ttm",
        "pb": "pb_ratio",
        "市净率": "pb_ratio",
        "ps": "ps_ratio_ttm",
        "ps_ttm": "ps_ratio_ttm",
        "市销率": "ps_ratio_ttm",
        "市销率TTM": "ps_ratio_ttm",
        "roe": "roe_pct",
        "ROE": "roe_pct",
        "净资产收益率": "roe_pct",
        "roa": "roa_pct",
        "ROA": "roa_pct",
        "总资产收益率": "roa_pct",
        "net_profit": "net_profit",
        "净利润": "net_profit",
        "revenue": "revenue",
        "营业收入": "revenue",
        "total_assets": "total_assets",
        "总资产": "total_assets",
        "total_equity": "total_equity",
        "净资产": "total_equity",
        "所有者权益": "total_equity",
        "debt_ratio": "debt_ratio_pct",
        "资产负债率": "debt_ratio_pct",
        "gross_margin": "gross_margin_pct",
        "毛利率": "gross_margin_pct",
        "net_margin": "net_margin_pct",
        "净利率": "net_margin_pct",
    }

    # ── 通用别名映射 (其他来源可能使用的字段名) ────────────────────

    _GENERIC_FIELD_MAP: Dict[str, str] = {
        "symbol": "security_id",
        "code": "security_id",
        "stock_code": "security_id",
        "ts_code": "security_id",
        "公告日期": "publish_date",
        "发布日期": "publish_date",
        "publish_date": "publish_date",
        "currency": "currency",
        "币种": "currency",
    }

    def __init__(
        self,
        normalize_version: str = "v1",
        schema_version: str = "v1",
        mapping_loader=None,
        default_report_type: str = "Q",
    ):
        super().__init__(
            normalize_version=normalize_version,
            schema_version=schema_version,
            mapping_loader=mapping_loader,
        )
        self.default_report_type = default_report_type

    def normalize(
        self,
        df: pd.DataFrame,
        source: str = "",
        interface_name: str = "",
        batch_id: str = "",
        extra_fields: Optional[Dict] = None,
    ) -> pd.DataFrame:
        """Normalize raw financial indicator DataFrame.

        Args:
            df: Raw input DataFrame.
            source: Source identifier.
            interface_name: Source interface name.
            batch_id: Batch identifier.
            extra_fields: Additional fields to include.

        Returns:
            Normalized DataFrame with standard financial indicator columns.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        if extra_fields is None:
            extra_fields = {}

        # Set default report_type if not present
        if "report_type" not in df.columns and "report_type" not in extra_fields:
            extra_fields["report_type"] = self.default_report_type

        return super().normalize(
            df=df,
            source=source,
            interface_name=interface_name,
            batch_id=batch_id,
            extra_fields=extra_fields,
        )

    def build_field_map(self, source: str) -> Dict[str, str]:
        """Build field mapping for the given source.

        Combines generic mapping with source-specific mapping.
        """
        field_map = dict(self._GENERIC_FIELD_MAP)

        source_lower = source.lower()
        if "em" in source_lower or "akshare" in source_lower:
            field_map.update(self._AKSHARE_EM_FIELD_MAP)
        elif "lixinger" in source_lower:
            field_map.update(self._LIXINGER_FIELD_MAP)
        else:
            # Default: merge both for maximum coverage
            field_map.update(self._AKSHARE_EM_FIELD_MAP)
            field_map.update(self._LIXINGER_FIELD_MAP)

        return field_map

    def _business_fields(self) -> list[str]:
        """Return all business field names."""
        return [
            "publish_date",
            "currency",
            "pe_ratio_ttm",
            "pb_ratio",
            "ps_ratio_ttm",
            "roe_pct",
            "roa_pct",
            "net_profit",
            "revenue",
            "total_assets",
            "total_equity",
            "debt_ratio_pct",
            "gross_margin_pct",
            "net_margin_pct",
            "basic_eps",
        ]
