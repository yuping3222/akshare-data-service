"""数据质量检查模块

Legacy quality checker for cache tables.  Scoring is now delegated to
the rule-based ``RuleBasedScorer`` in ``akshare_data.quality.scoring``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from akshare_data.store.manager import get_cache_manager


class DataQualityChecker:
    """数据质量检查器"""

    _TABLE_REQUIRED_FIELDS = {
        "stock_daily": ["date", "symbol", "open", "high", "low", "close"],
        "index_daily": ["date", "symbol", "open", "high", "low", "close"],
        "etf_daily": ["date", "symbol", "open", "high", "low", "close"],
    }

    def __init__(self, cache_manager=None):
        self._cache_manager = cache_manager

    @property
    def cache_manager(self):
        if self._cache_manager is None:
            self._cache_manager = get_cache_manager()
        return self._cache_manager

    def _find_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """查找日期列"""
        for col in ("date", "datetime", "trade_date", "日期", "时间"):
            if col in df.columns:
                return col
        return None

    def _get_required_fields(self, table: str) -> List[str]:
        """获取表所需的必需字段"""
        return self._TABLE_REQUIRED_FIELDS.get(table, [])

    def check_completeness(
        self,
        table: str,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        expected_trading_days: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """检查数据完整性"""
        try:
            where = {}
            if symbol:
                where["symbol"] = symbol

            df = self.cache_manager.read(table, where=where if where else None)

            if df is None or df.empty:
                return {
                    "has_data": False,
                    "total_records": 0,
                    "missing_dates_count": 0,
                    "completeness_ratio": 0.0,
                    "is_complete": False,
                    "missing_dates": [],
                }

            date_col = self._find_date_column(df)
            missing_dates = []
            missing_dates_count = 0

            if expected_trading_days and date_col:
                actual_dates = set(
                    pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d").tolist()
                )
                expected_set = set(expected_trading_days)
                missing_dates = [d for d in expected_set if d not in actual_dates]
                missing_dates_count = len(missing_dates)

            required_fields = self._get_required_fields(table)
            missing_fields = [f for f in required_fields if f not in df.columns]

            total_records = len(df)
            completeness_ratio = 1.0
            if expected_trading_days:
                completeness_ratio = (
                    1.0 - (missing_dates_count / len(expected_trading_days))
                    if expected_trading_days
                    else 1.0
                )

            result = {
                "has_data": True,
                "total_records": total_records,
                "missing_dates_count": missing_dates_count,
                "completeness_ratio": round(completeness_ratio, 4),
                "is_complete": missing_dates_count == 0 and not missing_fields,
                "missing_dates": missing_dates[:100] if missing_dates else [],
            }

            if missing_fields:
                result["missing_fields"] = missing_fields

            return result

        except Exception as e:
            return {"error": str(e), "has_data": False}

    def check_anomalies(
        self,
        df: pd.DataFrame,
        price_change_threshold: float = 20.0,
        volume_change_threshold: float = 10.0,
    ) -> Dict[str, Any]:
        """检测数据异常"""
        if df is None or df.empty:
            return {
                "total_rows": 0,
                "anomaly_count": 0,
                "anomalies": [],
                "price_anomalies": [],
                "volume_anomalies": [],
                "high_low_anomalies": [],
            }

        anomalies = []
        date_col = self._find_date_column(df)

        pct_col = (
            "pct_chg"
            if "pct_chg" in df.columns
            else "change"
            if "change" in df.columns
            else None
        )
        if pct_col:
            for idx, row in df.iterrows():
                try:
                    pct = float(row[pct_col])
                    if abs(pct) > price_change_threshold:
                        anomaly = {
                            "type": "price",
                            "date": row[date_col] if date_col else idx,
                            "value": pct,
                            "threshold": price_change_threshold,
                        }
                        anomalies.append(anomaly)
                except (ValueError, TypeError):
                    continue

        if "high" in df.columns and "low" in df.columns:
            for idx, row in df.iterrows():
                try:
                    if float(row["high"]) < float(row["low"]):
                        anomaly = {
                            "type": "high_low",
                            "date": row[date_col] if date_col else idx,
                            "high": row["high"],
                            "low": row["low"],
                        }
                        anomalies.append(anomaly)
                except (ValueError, TypeError):
                    continue

        if "volume" in df.columns:
            try:
                volumes = pd.to_numeric(df["volume"], errors="coerce").dropna()
                if len(volumes) >= 2:
                    mean = volumes.mean()
                    std = volumes.std()
                    if std > 0:
                        for idx, row in df.iterrows():
                            try:
                                vol = float(row["volume"])
                                z_score = abs(vol - mean) / std
                                if z_score >= volume_change_threshold:
                                    anomaly = {
                                        "type": "volume",
                                        "date": row[date_col] if date_col else idx,
                                        "value": vol,
                                        "z_score": round(z_score, 2),
                                        "threshold": volume_change_threshold,
                                    }
                                    anomalies.append(anomaly)
                            except (ValueError, TypeError):
                                continue
            except Exception:
                pass

        return {
            "total_rows": len(df),
            "anomaly_count": len(anomalies),
            "anomalies": anomalies[:50],
            "price_anomalies": [a for a in anomalies if a["type"] == "price"],
            "volume_anomalies": [a for a in anomalies if a["type"] == "volume"],
            "high_low_anomalies": [a for a in anomalies if a["type"] == "high_low"],
        }

    def check_consistency(
        self,
        table1: str,
        table2: str,
        symbol: str,
    ) -> Dict[str, Any]:
        """检查两个表的一致性"""
        try:
            df1 = self.cache_manager.read(table1, where={"symbol": symbol})
            df2 = self.cache_manager.read(table2, where={"symbol": symbol})

            if df1 is None and df2 is None:
                return {
                    "consistent": True,
                    "record_count_1": 0,
                    "record_count_2": 0,
                    "common_dates": 0,
                }

            if df1 is None:
                df1 = pd.DataFrame()
            if df2 is None:
                df2 = pd.DataFrame()

            date_col1 = self._find_date_column(df1)
            date_col2 = self._find_date_column(df2)

            count1 = len(df1)
            count2 = len(df2)

            common_dates = []
            only_in_table1 = []
            only_in_table2 = []

            if date_col1 and date_col2:
                dates1 = set(pd.to_datetime(df1[date_col1]).dt.strftime("%Y-%m-%d"))
                dates2 = set(pd.to_datetime(df2[date_col2]).dt.strftime("%Y-%m-%d"))
                common_dates = list(dates1 & dates2)
                only_in_table1 = list(dates1 - dates2)
                only_in_table2 = list(dates2 - dates1)

            consistent = len(only_in_table1) == 0 and len(only_in_table2) == 0

            return {
                "consistent": consistent,
                "record_count_1": count1,
                "record_count_2": count2,
                "common_dates": len(common_dates),
                "only_in_table1": only_in_table1,
                "only_in_table2": only_in_table2,
            }

        except Exception as e:
            return {"error": str(e)}

    def generate_report(
        self,
        table: str,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """生成综合质量报告"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        completeness = self.check_completeness(
            table, symbol=symbol, start_date=start_date, end_date=end_date
        )

        try:
            where = {}
            if symbol:
                where["symbol"] = symbol
            df = self.cache_manager.read(table, where=where if where else None)

            if df is not None and not df.empty:
                anomalies = self.check_anomalies(df)
            else:
                anomalies = {
                    "anomaly_count": 0,
                    "anomalies": [],
                }
        except Exception as e:
            anomalies = {"error": str(e), "anomaly_count": 0, "anomalies": []}

        issues_count = 0
        critical_issues = []

        if not completeness.get("is_complete", True):
            issues_count += 1
            critical_issues.append("Data completeness issues detected")

        if completeness.get("missing_dates_count", 0) > 0:
            issues_count += 1
            critical_issues.append(
                f"Missing dates: {completeness['missing_dates_count']}"
            )

        if "missing_fields" in completeness:
            issues_count += 1
            critical_issues.append(
                f"Missing fields: {', '.join(completeness['missing_fields'])}"
            )

        if anomalies.get("anomaly_count", 0) > 0:
            issues_count += 1
            critical_issues.append(f"Anomalies detected: {anomalies['anomaly_count']}")

        if issues_count == 0:
            critical_issues.append("No critical issues found")

        overall_score = self._compute_rule_based_score(completeness, anomalies)

        return {
            "timestamp": timestamp,
            "table": table,
            "symbol": symbol or "N/A",
            "checks": {
                "completeness": completeness,
                "anomalies": anomalies,
            },
            "summary": {
                "overall_score": overall_score,
                "issues_count": issues_count,
                "critical_issues": "; ".join(critical_issues)
                if critical_issues
                else "No critical issues found",
            },
        }

    def _compute_rule_based_score(
        self,
        completeness: Dict[str, Any],
        anomalies: Dict[str, Any],
    ) -> float:
        """Compute quality score via RuleBasedScorer instead of hardcoded values.

        The score is derived from rule outcomes only:
        - completeness pass/fail from ``is_complete``
        - required-field completeness from missing field count
        - anomaly check pass/fail from anomaly count
        """
        from akshare_data.quality.engine import GateAction, RuleResult, RuleStatus, Severity
        from akshare_data.quality.scoring import RuleBasedScorer

        results: List[RuleResult] = []

        is_complete = bool(completeness.get("is_complete", False))
        missing_dates = int(completeness.get("missing_dates_count", 0))
        missing_fields = list(completeness.get("missing_fields", []))

        results.append(RuleResult(
            rule_id="legacy_completeness_check",
            status=RuleStatus.PASSED if is_complete else RuleStatus.FAILED,
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            message="Completeness check",
            failed_count=missing_dates,
            total_count=max(1, missing_dates + int(completeness.get("total_records", 0))),
        ))

        results.append(RuleResult(
            rule_id="legacy_required_fields_check",
            status=RuleStatus.PASSED if not missing_fields else RuleStatus.FAILED,
            severity=Severity.ERROR,
            gate_action=GateAction.BLOCK,
            message=(
                f"Missing fields: {missing_fields}" if missing_fields else "All required fields present"
            ),
            failed_count=len(missing_fields),
            total_count=max(1, len(missing_fields) + 1),
        ))

        anomaly_count = int(anomalies.get("anomaly_count", 0))
        total_rows = int(anomalies.get("total_rows", 0))
        results.append(RuleResult(
            rule_id="legacy_anomaly_check",
            status=RuleStatus.PASSED if anomaly_count == 0 else RuleStatus.FAILED,
            severity=Severity.WARNING,
            gate_action=GateAction.ALERT,
            message=f"{anomaly_count} anomalies detected",
            failed_count=anomaly_count,
            total_count=max(1, total_rows),
        ))

        scorer = RuleBasedScorer()
        return scorer.compute_score(results)


class QualityChecker:
    """数据质量检查工具类（静态方法）"""

    @staticmethod
    def check_daily_completeness(
        df: pd.DataFrame, expected_days: List[str]
    ) -> Dict[str, Any]:
        """检查日线数据完整性

        Args:
            df: 待检查的 DataFrame
            expected_days: 期望的交易日期列表

        Returns:
            Dict: 包含 missing_count, missing_days, completeness
        """
        if df is None or df.empty:
            return {
                "missing_count": len(expected_days),
                "missing_days": expected_days,
                "completeness": 0.0,
            }

        date_col = None
        for col in ("date", "datetime", "trade_date"):
            if col in df.columns:
                date_col = col
                break

        if date_col is None:
            return {
                "missing_count": len(expected_days),
                "missing_days": expected_days,
                "completeness": 0.0,
            }

        actual_dates = set(
            pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d").tolist()
        )
        expected_set = set(expected_days)
        missing_days = [d for d in expected_set if d not in actual_dates]

        completeness = (
            (len(expected_set) - len(missing_days)) / len(expected_set)
            if expected_set
            else 1.0
        )

        return {
            "missing_count": len(missing_days),
            "missing_days": missing_days,
            "completeness": completeness,
        }

    @staticmethod
    def detect_anomalies(df: pd.DataFrame) -> List[str]:
        """检测数据异常

        Args:
            df: 待检查的 DataFrame

        Returns:
            List[str]: 异常描述列表
        """
        if df is None or df.empty:
            return []

        anomalies = []

        pct_col = None
        for col in ("pct_chg", "change"):
            if col in df.columns:
                pct_col = col
                break

        if pct_col:
            for idx, row in df.iterrows():
                try:
                    pct = float(row[pct_col])
                    if abs(pct) > 20.0:
                        date_str = ""
                        date_col = None
                        for col in ("date", "datetime"):
                            if col in df.columns:
                                date_col = col
                                break
                        if date_col:
                            date_str = f" on {row[date_col]}"
                        anomalies.append(f"Abnormal price change{date_str}: {pct:.2f}%")
                except (ValueError, TypeError):
                    continue

        if "high" in df.columns and "low" in df.columns:
            for idx, row in df.iterrows():
                try:
                    if float(row["high"]) < float(row["low"]):
                        date_str = ""
                        date_col = None
                        for col in ("date", "datetime"):
                            if col in df.columns:
                                date_col = col
                                break
                        if date_col:
                            date_str = f" on {row[date_col]}"
                        anomalies.append(
                            f"High < Low{date_str}: high={row['high']}, low={row['low']}"
                        )
                except (ValueError, TypeError):
                    continue

        return anomalies
