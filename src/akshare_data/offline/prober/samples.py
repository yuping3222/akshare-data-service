"""探测样本数据管理"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from akshare_data.offline.core.paths import paths

logger = logging.getLogger("akshare_data")


class SampleManager:
    """探测样本数据管理器"""

    def __init__(self, samples_dir: Optional[Path] = None):
        self._samples_dir = samples_dir or paths.prober_samples_dir
        self._samples_dir.mkdir(parents=True, exist_ok=True)

    def save_sample(self, func_name: str, data: pd.DataFrame):
        """保存探测样本"""
        if data is None or data.empty:
            return

        sample_file = self._samples_dir / f"{func_name}.csv"
        try:
            data.head(100).to_csv(sample_file, index=False)
            logger.debug(f"Saved sample for {func_name}")
        except Exception as e:
            logger.warning(f"Failed to save sample for {func_name}: {e}")

    def load_sample(self, func_name: str) -> Optional[pd.DataFrame]:
        """加载探测样本"""
        sample_file = self._samples_dir / f"{func_name}.csv"
        if not sample_file.exists():
            return None

        try:
            return pd.read_csv(sample_file)
        except Exception as e:
            logger.warning(f"Failed to load sample for {func_name}: {e}")
            return None
