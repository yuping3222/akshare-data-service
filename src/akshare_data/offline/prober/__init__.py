"""接口探测模块"""

from akshare_data.offline.core.paths import paths
from akshare_data.offline.prober.prober import (
    APIProber,
    MAX_WORKERS,
    DOMAIN_CONCURRENCY_DEFAULT,
    DELAY_BETWEEN_CALLS,
    TIMEOUT_LIMIT,
)
from akshare_data.offline.prober.checkpoint import CheckpointManager
from akshare_data.offline.prober.samples import SampleManager
from akshare_data.offline.prober.task_builder import ValidationResult
from akshare_data.offline.prober.executor import SYMBOL_FALLBACKS
from akshare_data.offline.scanner.param_inferrer import SIZE_LIMIT_PARAMS

BASE_DIR = paths.project_root
TEST_DATA_DIR = paths.prober_samples_dir
REPORT_FILE = paths.health_reports_dir / "health_report.md"
CHECKPOINT_FILE = paths.prober_state_file
CONFIG_FILE = paths.prober_config_file

__all__ = [
    "APIProber",
    "CheckpointManager",
    "SampleManager",
    "ValidationResult",
    "MAX_WORKERS",
    "DOMAIN_CONCURRENCY_DEFAULT",
    "DELAY_BETWEEN_CALLS",
    "TIMEOUT_LIMIT",
    "SYMBOL_FALLBACKS",
    "SIZE_LIMIT_PARAMS",
    "BASE_DIR",
    "TEST_DATA_DIR",
    "REPORT_FILE",
    "CHECKPOINT_FILE",
    "CONFIG_FILE",
]
