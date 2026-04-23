"""Pluggable quality check implementations."""

from akshare_data.quality.checks.completeness import (
    ContinuityCheck,
    PartitionCoverageCheck,
    PrimaryKeyCoverageCheck,
)
from akshare_data.quality.checks.consistency import (
    CrossLayerConsistencyCheck,
    CrossSourceDiffCheck,
    CrossTableConsistencyCheck,
)
from akshare_data.quality.checks.anomaly import (
    NumericRangeAnomalyCheck,
    PriceAnomalyCheck,
    VolatilityAnomalyCheck,
)

__all__ = [
    "ContinuityCheck",
    "PartitionCoverageCheck",
    "PrimaryKeyCoverageCheck",
    "CrossLayerConsistencyCheck",
    "CrossSourceDiffCheck",
    "CrossTableConsistencyCheck",
    "NumericRangeAnomalyCheck",
    "PriceAnomalyCheck",
    "VolatilityAnomalyCheck",
]
