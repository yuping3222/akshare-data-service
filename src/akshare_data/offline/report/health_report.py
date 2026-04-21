"""健康报告生成器"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from akshare_data.offline.core.paths import paths
from akshare_data.offline.report.renderer import ReportRenderer

logger = logging.getLogger("akshare_data")


class HealthReportGenerator:
    """健康报告生成器"""

    def __init__(self):
        self._renderer = ReportRenderer()
        self._output_dir = paths.health_reports_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        results: Dict[str, Any],
        total_elapsed: float = 0.0,
        output_file: Optional[Path] = None,
    ) -> str:
        """生成健康报告"""
        if not results:
            return ""

        if isinstance(results, dict):
            first_val = next(iter(results.values()), None)
            if isinstance(first_val, dict):
                df = pd.DataFrame.from_dict(results, orient="index")
            else:
                df = pd.DataFrame([results])
        elif isinstance(results, list):
            df = pd.DataFrame(results)
        else:
            return ""

        if df.empty:
            return ""

        total = len(df)
        success = len(df[df["status"].str.contains("Success", na=False)]) if "status" in df.columns else 0
        rate = (success / total * 100) if total > 0 else 0

        sections = {
            "AkShare Health Audit Report": {
                "Report Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Total APIs": total,
                "Available APIs": success,
                "Health Rate": f"{rate:.1f}%",
                "Total Elapsed": f"{total_elapsed:.2f}s",
            },
        }

        if "exec_time" in df.columns:
            slowest = df.sort_values("exec_time", ascending=False).head(20)
            sections["Top 20 Slowest APIs"] = slowest[["func_name", "exec_time", "status"]]

        content = self._renderer.render_markdown(sections)

        if output_file is None:
            output_file = self._output_dir / f"health_report_{datetime.now().strftime('%Y%m%d')}.md"

        self._renderer.save(content, output_file)
        logger.info(f"Health report saved to {output_file}")
        return content
