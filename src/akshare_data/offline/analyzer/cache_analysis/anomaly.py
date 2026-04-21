"""异常值检测器"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger("akshare_data")


class AnomalyDetector:
    """数据异常值检测器"""

    def detect(
        self,
        df: pd.DataFrame,
        price_change_threshold: float = 20.0,
        volume_change_threshold: float = 10.0,
    ) -> Dict[str, Any]:
        """检测数据异常"""
        if df is None or df.empty:
            return {"total_rows": 0, "anomaly_count": 0, "anomalies": []}

        anomalies = []
        anomalies.extend(self._check_price(df, price_change_threshold))
        anomalies.extend(self._check_high_low(df))
        anomalies.extend(self._check_volume(df, volume_change_threshold))

        return {
            "total_rows": len(df),
            "anomaly_count": len(anomalies),
            "anomalies": anomalies[:50],
            "price_anomalies": [a for a in anomalies if a["type"] == "price"],
            "volume_anomalies": [a for a in anomalies if a["type"] == "volume"],
            "high_low_anomalies": [a for a in anomalies if a["type"] == "high_low"],
        }

    def _check_price(self, df: pd.DataFrame, threshold: float) -> List[Dict]:
        """检查价格异常"""
        anomalies = []
        if "pct_change" not in df.columns and "涨跌幅" not in df.columns:
            return anomalies

        col = "pct_change" if "pct_change" in df.columns else "涨跌幅"
        for idx, row in df.iterrows():
            try:
                pct = float(row[col])
                if abs(pct) > threshold:
                    anomalies.append({
                        "type": "price",
                        "index": idx,
                        "value": pct,
                        "threshold": threshold,
                    })
            except (ValueError, TypeError):
                continue
        return anomalies

    def _check_high_low(self, df: pd.DataFrame) -> List[Dict]:
        """检查高低价格异常"""
        anomalies = []
        if "high" not in df.columns or "low" not in df.columns:
            return anomalies

        for idx, row in df.iterrows():
            try:
                if float(row["high"]) < float(row["low"]):
                    anomalies.append({
                        "type": "high_low",
                        "index": idx,
                        "high": row["high"],
                        "low": row["low"],
                    })
            except (ValueError, TypeError):
                continue
        return anomalies

    def _check_volume(self, df: pd.DataFrame, threshold: float) -> List[Dict]:
        """检查成交量异常"""
        anomalies = []
        if "volume" not in df.columns:
            return anomalies

        try:
            volumes = pd.to_numeric(df["volume"], errors="coerce").dropna()
            if len(volumes) < 2:
                return anomalies

            mean = volumes.mean()
            std = volumes.std()
            if std == 0:
                return anomalies

            for idx, row in df.iterrows():
                try:
                    vol = float(row["volume"])
                    z_score = abs(vol - mean) / std
                    if z_score >= threshold:
                        anomalies.append({
                            "type": "volume",
                            "index": idx,
                            "value": vol,
                            "z_score": round(z_score, 2),
                            "threshold": threshold,
                        })
                except (ValueError, TypeError):
                    continue
        except Exception:
            pass

        return anomalies
