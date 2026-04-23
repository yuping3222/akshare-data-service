"""完整性检查器"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd


logger = logging.getLogger("akshare_data")


class CompletenessChecker:
    """缓存数据完整性检查器"""

    def check(
        self,
        df: pd.DataFrame,
        expected_dates: Optional[List[str]] = None,
        required_fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """检查数据完整性"""
        if df is None or df.empty:
            return {
                "has_data": False,
                "total_records": 0,
                "completeness_ratio": 0.0,
                "is_complete": False,
                "missing_dates": [],
                "missing_fields": required_fields or [],
            }

        date_col = self._find_date_column(df)
        missing_dates = []
        if expected_dates and date_col:
            actual_dates = set(df[date_col].astype(str).tolist())
            missing_dates = [d for d in expected_dates if d not in actual_dates]

        missing_fields = []
        if required_fields:
            missing_fields = [f for f in required_fields if f not in df.columns]

        total_expected = len(expected_dates) if expected_dates else len(df)
        completeness = (
            (total_expected - len(missing_dates)) / total_expected
            if total_expected > 0
            else 0
        )

        return {
            "has_data": True,
            "total_records": len(df),
            "missing_dates_count": len(missing_dates),
            "missing_dates": missing_dates[:100],
            "missing_fields": missing_fields,
            "completeness_ratio": round(completeness, 4),
            "is_complete": len(missing_dates) == 0 and len(missing_fields) == 0,
        }

    def _find_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """查找日期列"""
        for col in ("date", "datetime", "trade_date", "日期", "时间"):
            if col in df.columns:
                return col
        return None
