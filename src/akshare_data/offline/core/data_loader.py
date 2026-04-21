import logging

import pandas as pd

logger = logging.getLogger("akshare_data")


def load_table(table_name: str, layer: str | None = None) -> pd.DataFrame:
    """Load table data from cache, trying all storage layers if no layer specified."""
    from akshare_data.store.manager import get_cache_manager

    manager = get_cache_manager()

    if layer is not None:
        df = manager.read(table_name, storage_layer=layer)
        return df if df is not None and not df.empty else pd.DataFrame()

    for candidate in ("daily", "meta", "intraday", "realtime"):
        df = manager.read(table_name, storage_layer=candidate)
        if df is not None and not df.empty:
            return df

    logger.warning(f"No cached data found for table: {table_name}, using empty dataset")
    return pd.DataFrame()


def get_cache_manager_instance():
    """Return the global CacheManager singleton."""
    from akshare_data.store.manager import CacheManager

    return CacheManager()
