"""报告生成模块"""

from akshare_data.offline.report.renderer import ReportRenderer
from akshare_data.offline.report.health_report import HealthReportGenerator
from akshare_data.offline.report.quality_report import QualityReportGenerator

__all__ = ["ReportRenderer", "HealthReportGenerator", "QualityReportGenerator"]
