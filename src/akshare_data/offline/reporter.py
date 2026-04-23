"""报告生成器模块"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from akshare_data.offline.core.paths import paths


class Reporter:
    """报告生成器"""

    REPORT_DIR = paths.reports_dir
    HEALTH_REPORT_FILE = paths.reports_dir / "health_report.md"
    QUALITY_REPORT_FILE = paths.reports_dir / "quality_report.md"
    VOLUME_REPORT_FILE = paths.reports_dir / "volume_report.md"

    def __init__(self):
        os.makedirs(self.REPORT_DIR, exist_ok=True)

    def to_md(self, df: pd.DataFrame) -> str:
        """DataFrame 转 Markdown 表格"""
        if df is None or df.empty:
            return ""

        cols = df.columns.tolist()
        h = "|" + "|".join(cols) + "|"
        s = "|" + "|".join(["---"] * len(cols)) + "|"
        body = []
        for _, r in df.iterrows():
            row_vals = []
            for c, v in r.items():
                if c == "last_check":
                    try:
                        row_vals.append(
                            datetime.fromtimestamp(v).strftime("%m-%d %H:%M")
                        )
                    except Exception:
                        row_vals.append(str(v))
                elif "Time" in c or c == "exec_time":
                    try:
                        row_vals.append(f"{float(v):.2f}s")
                    except (ValueError, TypeError):
                        row_vals.append(str(v))
                else:
                    row_vals.append(str(v).replace("\n", " "))
            body.append("|" + "|".join(row_vals) + "|")
        return "\n".join([h, s] + body)

    def generate_health_report(self, results: Any) -> str:
        """生成健康检查报告"""
        if not results or (isinstance(results, dict) and not results):
            return ""

        if isinstance(results, pd.DataFrame):
            if results.empty:
                return ""
            records = results.to_dict("records")
        elif isinstance(results, list):
            records = results
        else:
            return ""

        if not records:
            return ""

        total = len(records)
        available = sum(1 for r in records if r.get("status") == "Success")

        lines = []
        lines.append("# Akshare Health Audit Report")
        lines.append("")
        lines.append(f"**Total APIs:** {total}")
        lines.append(
            f"**Available APIs:** {available} ({available / total * 100:.2f}% if total > 0 else 0%)"
        )
        lines.append("")

        slow_apis = sorted(
            [r for r in records if r.get("exec_time")],
            key=lambda x: x.get("exec_time", 0),
            reverse=True,
        )[:20]

        if slow_apis:
            lines.append("## Top 20 Slowest APIs")
            lines.append("")
            for r in slow_apis:
                func_name = r.get("func_name", "unknown")
                exec_time = r.get("exec_time", 0)
                domain = r.get("domain_group", "unknown")
                status = r.get("status", "unknown")
                lines.append(
                    f"- **{func_name}** ({domain}) - {exec_time:.2f}s - {status}"
                )
            lines.append("")

        return "\n".join(lines)

    def generate_quality_report(self, df: Optional[pd.DataFrame]) -> str:
        """生成质量报告"""
        if df is None or df.empty:
            return ""

        lines = []
        lines.append("# Akshare Quality Report")
        lines.append("")

        total = len(df)
        lines.append(f"**Total Interfaces:** {total}")
        lines.append("")

        if "interface_name" in df.columns and "分类" in df.columns:
            data_interfaces = df[~df["分类"].isin(["exception", "tool", "unknown"])]
            lines.append(f"**Data Interfaces:** {len(data_interfaces)}")
            lines.append("")

            category_map = {
                "stock": "Stock",
                "fund": "Fund",
                "bond": "Bond",
                "futures": "Futures",
                "index": "Index",
                "forex": "Forex",
                "crypto": "Crypto",
            }


            categories = df["分类"].value_counts()
            if not categories.empty:
                lines.append("### Categories")
                lines.append("")
                for cat, count in categories.items():
                    cat_name = category_map.get(
                        cat, cat.title() if isinstance(cat, str) else str(cat)
                    )
                    lines.append(f"- **{cat_name}:** {count}")
                lines.append("")

            lines.append("### Cache Strategy Recommendations")
            lines.append("")
            lines.append("- **Realtime**: Update every 5 minutes")
            lines.append("- **Daily**: Update once per trading day")
            lines.append("- **Weekly**: Update every Monday")
            lines.append("- **Monthly**: Update on the 1st of each month")

        return "\n".join(lines)

    def generate_volume_report(self, df: Optional[pd.DataFrame]) -> str:
        """生成数据量报告"""
        if df is None or df.empty:
            return ""

        lines = []
        lines.append("# Akshare Data Volume Report")
        lines.append("")

        total_rows = df["数据行数"].sum() if "数据行数" in df.columns else 0
        total_memory = df["内存占用_KB"].sum() if "内存占用_KB" in df.columns else 0

        lines.append(f"**Total Interfaces:** {len(df)}")
        lines.append(f"**Total Rows:** {total_rows:,}")
        lines.append(f"**Total Memory:** {total_memory:.1f} KB")
        if total_memory >= 1024:
            lines.append(f" ({total_memory / 1024:.2f} MB)")
        if total_memory >= 1024 * 1024:
            lines.append(f" ({total_memory / (1024 * 1024):.2f} GB)")
        lines.append("")

        if "分类" in df.columns:
            category_stats = (
                df.groupby("分类")
                .agg(
                    {
                        "数据行数": "sum",
                        "内存占用_KB": "sum",
                    }
                )
                .sort_values("内存占用_KB", ascending=False)
            )

            if not category_stats.empty:
                lines.append("### By Category")
                lines.append("")
                for cat, row in category_stats.iterrows():
                    lines.append(f"### {cat.upper()}")
                    lines.append(f"- Rows: {row['数据行数']:,}")
                    lines.append(f"- Memory: {row['内存占用_KB']:.1f} KB")
                    lines.append("")

        if "内存占用_KB" in df.columns:
            top20 = df.nlargest(20, "内存占用_KB")
            if not top20.empty:
                lines.append("### Top 20 Largest Data Interfaces")
                lines.append("")
                for _, row in top20.iterrows():
                    name = row.get("接口名称", "unknown")
                    mem = row.get("内存占用_KB", 0)
                    rows = row.get("数据行数", 0)
                    lines.append(f"- **{name}**: {mem:.1f} KB ({rows:,} rows)")
                lines.append("")

            large = df[df["内存占用_KB"] > 1000]
            medium = df[(df["内存占用_KB"] >= 100) & (df["内存占用_KB"] <= 1000)]
            small = df[df["内存占用_KB"] < 100]

            lines.append("### Cache Strategy by Size")
            lines.append("")
            if not large.empty:
                lines.append(f"- **Large (>1000 KB)**: {len(large)} interfaces")
            if not medium.empty:
                lines.append(f"- **Medium (100-1000 KB)**: {len(medium)} interfaces")
            if not small.empty:
                lines.append(f"- **Small (<100 KB)**: {len(small)} interfaces")

        return "\n".join(lines)

    @staticmethod
    def save_json(data: Dict[str, Any], output_path: str) -> None:
        """保存 JSON 文件"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    @staticmethod
    def generate_summary(probe_results: Dict[str, Dict[str, str]]) -> str:
        """生成探测结果摘要"""
        if not probe_results:
            return "Health Audit: 0/0 APIs passed."

        total = len(probe_results)
        success = sum(1 for r in probe_results.values() if r.get("status") == "SUCCESS")

        return f"Health Audit: {success}/{total} APIs passed."

    def integrate_with_summary(
        self,
        total_apis: int,
        available_apis: int,
        success_rate: float,
        avg_response_time: float,
    ) -> None:
        """集成摘要到最终报告"""
        summary_path = self.REPORT_DIR / "final_summary.txt"

        existing_content = ""
        if summary_path.exists():
            try:
                with open(summary_path, "r", encoding="utf-8") as f:
                    existing_content = f.read()
            except Exception:
                existing_content = ""

        new_content_lines = []
        new_content_lines.append("Interface Health Audit:")
        new_content_lines.append(f"  Audited APIs: {total_apis}")
        new_content_lines.append(
            f"  Available APIs: {available_apis} ({success_rate:.1f}%)"
        )
        new_content_lines.append("")

        if existing_content:
            lines = existing_content.split("\n")
            in_health_section = False
            for line in lines:
                if "Interface Health Audit:" in line:
                    in_health_section = True
                    continue
                if in_health_section and line.startswith("Overall Statistics:"):
                    in_health_section = False
                if not in_health_section:
                    new_content_lines.append(line)
        else:
            new_content_lines.append("Overall Statistics:")
            new_content_lines.append(
                f"  Average Response Time: {avg_response_time:.2f}s"
            )

        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(new_content_lines))
