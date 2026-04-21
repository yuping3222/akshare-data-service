"""访问日志分析子模块"""

# AccessLogger 已移至 offline/access_logger.py，此处保留向后兼容
from akshare_data.offline.access_logger import AccessLogger
from akshare_data.offline.analyzer.access_log.stats import CallStatsAnalyzer

__all__ = ["AccessLogger", "CallStatsAnalyzer"]
