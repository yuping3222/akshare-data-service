"""报告渲染器 - Markdown/HTML/JSON 渲染"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd


class ReportRenderer:
    """报告渲染器"""

    def to_md(self, df: pd.DataFrame) -> str:
        """DataFrame 转 Markdown 表格"""
        if df.empty:
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
                    row_vals.append(f"{v:.2f}s")
                else:
                    row_vals.append(str(v).replace("\n", " "))
            body.append("|" + "|".join(row_vals) + "|")
        return "\n".join([h, s] + body)

    def render_markdown(self, sections: Dict[str, Any]) -> str:
        """渲染 Markdown 报告"""
        lines = []
        for title, content in sections.items():
            lines.append(f"# {title}")
            lines.append("")
            if isinstance(content, str):
                lines.append(content)
            elif isinstance(content, pd.DataFrame):
                lines.append(self.to_md(content))
            elif isinstance(content, dict):
                for k, v in content.items():
                    lines.append(f"- **{k}**: {v}")
            elif isinstance(content, list):
                for item in content:
                    lines.append(f"- {item}")
            lines.append("")
        return "\n".join(lines)

    def render_json(self, data: Dict[str, Any]) -> str:
        """渲染 JSON 报告"""
        return json.dumps(data, indent=2, ensure_ascii=False)

    def save(self, content: str, output_path: Path):
        """保存报告"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
