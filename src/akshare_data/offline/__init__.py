"""离线工具层 - 重构版

模块划分:
- core: 基础设施（路径、配置加载、错误、重试）
- scanner: AkShare 接口扫描
- registry: 注册表管理
- prober: 接口健康探测
- downloader: 批量数据下载
- analyzer: 数据分析（日志、缓存、字段）
- scheduler: 定时任务调度
- source_manager: 数据源健康管理
- report: 报告生成
- cli: 命令行入口
"""

import importlib

_LAZY_IMPORTS = {
    "Paths": ".core.paths",
    "ConfigLoader": ".core.config_loader",
    "OfflineError": ".core.errors",
    "ConfigError": ".core.errors",
    "DownloadError": ".core.errors",
    "ProbeError": ".core.errors",
    "AnalysisError": ".core.errors",
    "retry": ".core.retry",
    "RetryConfig": ".core.retry",
    "AkShareScanner": ".scanner.akshare_scanner",
    "DomainExtractor": ".scanner.domain_extractor",
    "CategoryInferrer": ".scanner.category_inferrer",
    "ParamInferrer": ".scanner.param_inferrer",
    "RegistryBuilder": ".registry.builder",
    "RegistryMerger": ".registry.merger",
    "RegistryExporter": ".registry.exporter",
    "RegistryValidator": ".registry.validator",
    "APIProber": ".prober.prober",
    "CheckpointManager": ".prober.checkpoint",
    "SampleManager": ".prober.samples",
    "BatchDownloader": ".downloader.downloader",
    "DownloadTask": ".downloader.task_builder",
    "DomainRateLimiter": ".downloader.rate_limiter",
    "AccessLogger": ".analyzer.access_log.logger",
    "CallStatsAnalyzer": ".analyzer.access_log.stats",
    "CompletenessChecker": ".analyzer.cache_analysis.completeness",
    "DataQualityChecker": ".analyzer.cache_analysis.completeness",
    "AnomalyDetector": ".analyzer.cache_analysis.anomaly",
    "FieldMapper": ".analyzer.interface_analysis.field_mapper",
    "Scheduler": ".scheduler.scheduler",
    "HealthTracker": ".source_manager.health_tracker",
    "FailoverManager": ".source_manager.failover",
    "ReportRenderer": ".report.renderer",
    "HealthReportGenerator": ".report.health_report",
    "QualityReportGenerator": ".report.quality_report",
    "main": ".cli.main",
}

__all__ = [
    # Core
    "Paths",
    "ConfigLoader",
    "OfflineError",
    "ConfigError",
    "DownloadError",
    "ProbeError",
    "AnalysisError",
    "retry",
    "RetryConfig",
    # Scanner
    "AkShareScanner",
    "DomainExtractor",
    "CategoryInferrer",
    "ParamInferrer",
    # Registry
    "RegistryBuilder",
    "RegistryMerger",
    "RegistryExporter",
    "RegistryValidator",
    # Prober
    "APIProber",
    "CheckpointManager",
    "SampleManager",
    # Downloader
    "BatchDownloader",
    "DownloadTask",
    "DomainRateLimiter",
    # Analyzer
    "AccessLogger",
    "CallStatsAnalyzer",
    "CompletenessChecker",
    "AnomalyDetector",
    "FieldMapper",
    # Scheduler
    "Scheduler",
    # Source Manager
    "HealthTracker",
    "FailoverManager",
    # Report
    "ReportRenderer",
    "HealthReportGenerator",
    "QualityReportGenerator",
    # CLI
    "main",
]


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module = importlib.import_module(_LAZY_IMPORTS[name], __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
