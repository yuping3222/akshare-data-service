"""质量报告生成器"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


from akshare_data.offline.core.paths import paths
from akshare_data.offline.report.renderer import ReportRenderer

logger = logging.getLogger("akshare_data")


class QualityReportGenerator:
    """质量报告生成器"""

    def __init__(self):
        self._renderer = ReportRenderer()
        self._output_dir = paths.quality_reports_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        quality_results: Dict[str, Any],
        output_file: Optional[Path] = None,
    ) -> str:
        """生成质量报告"""
        sections = {
            "Data Quality Report": {
                "Report Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Table": quality_results.get("table", "unknown"),
                "Symbol": quality_results.get("symbol", "N/A"),
            },
        }

        checks = quality_results.get("checks", {})
        if "completeness" in checks:
            sections["Completeness Check"] = checks["completeness"]

        if "anomalies" in checks:
            sections["Anomaly Detection"] = {
                "Total Anomalies": checks["anomalies"].get("anomaly_count", 0),
                "Details": checks["anomalies"].get("anomalies", [])[:20],
            }

        summary = quality_results.get("summary", {})
        if summary:
            sections["Summary"] = summary

        content = self._renderer.render_markdown(sections)

        if output_file is None:
            output_file = (
                self._output_dir
                / f"quality_report_{datetime.now().strftime('%Y%m%d')}.md"
            )

        self._renderer.save(content, output_file)
        logger.info(f"Quality report saved to {output_file}")
        return content
