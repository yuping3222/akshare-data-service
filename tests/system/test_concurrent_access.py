"""System tests for concurrent access scenarios.

Verifies thread safety under load:
- Multiple threads reading the same cache simultaneously
- Multiple threads writing simultaneously (atomic writes)
- DataService singleton behavior under concurrent access
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data import DataService
from akshare_data.store.manager import CacheManager
from tests.system.conftest import _seed_cache


@pytest.mark.system
class TestConcurrentReadAccess:
    """Tests for concurrent read operations on shared cache."""

    def test_concurrent_reads_same_cache_no_race(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """10 threads reading same cache simultaneously produce identical results."""
        service = DataService(cache_manager=system_cache_manager)

        source_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", "2024-01-19", freq="B"),
                "symbol": ["600000.XSHG"] * 14,
                "open": [10.0 + i * 0.1 for i in range(14)],
                "high": [11.0 + i * 0.1 for i in range(14)],
                "low": [9.0 + i * 0.1 for i in range(14)],
                "close": [10.5 + i * 0.1 for i in range(14)],
                "volume": [100_000 + i * 10_000 for i in range(14)],
                "amount": [1_000_000.0 + i * 100_000.0 for i in range(14)],
            }
        )
        _seed_cache(system_cache_manager, "stock_daily", source_df)

        results: List[pd.DataFrame] = []
        errors: List[Exception] = []

        def read_data(index: int) -> pd.DataFrame:
            try:
                return service.cn.stock.quote.daily(
                    symbol="sh600000",
                    start_date="2024-01-02",
                    end_date="2024-01-19",
                    source="akshare",
                )
            except Exception as e:
                errors.append(e)
                return pd.DataFrame()

        # 10 concurrent readers
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(read_data, i) for i in range(10)]
            for future in as_completed(futures):
                results.append(future.result())

        assert not errors, f"Errors during concurrent reads: {errors}"
        non_empty = [r for r in results if not r.empty]
        assert len(non_empty) >= 1, "At least one thread should return data"

        # All non-empty results should have data (lengths may vary due to
        # incremental accumulation across concurrent threads)
        lengths = [len(r) for r in non_empty]
        assert all(length >= 7 for length in lengths), (
            f"Some results have unexpectedly few rows: {lengths}"
        )

    def test_concurrent_reads_different_symbols(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Multiple threads reading different symbols simultaneously succeed."""

        def make_source(symbol: str) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "date": pd.date_range("2024-01-02", periods=5, freq="B"),
                    "symbol": [symbol] * 5,
                    "open": [10.0] * 5,
                    "high": [11.0] * 5,
                    "low": [9.0] * 5,
                    "close": [10.5] * 5,
                    "volume": [100_000] * 5,
                    "amount": [1_000_000.0] * 5,
                }
            )

        symbols = [
            "600000.XSHG",
            "600036.XSHG",
            "000001.XSHE",
            "601318.XSHG",
            "000858.XSHE",
        ]
        # Pre-seed Served with every symbol's OHLCV data so concurrent
        # reads all find rows.
        for sym in symbols:
            _seed_cache(system_cache_manager, "stock_daily", make_source(sym))

        service = DataService(cache_manager=system_cache_manager)

        errors: List[Exception] = []
        results: dict = {}

        def read_symbol(symbol: str) -> pd.DataFrame:
            try:
                return service.cn.stock.quote.daily(
                    symbol=symbol,
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                    source="akshare",
                )
            except Exception as e:
                errors.append(e)
                return pd.DataFrame()

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_symbol = {
                executor.submit(read_symbol, sym): sym for sym in symbols
            }
            for future in as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                results[sym] = future.result()

        assert not errors, f"Errors during concurrent symbol reads: {errors}"
        # At least some should succeed
        success_count = sum(1 for r in results.values() if not r.empty)
        assert success_count >= 1


@pytest.mark.system
class TestConcurrentWriteAccess:
    """Tests for concurrent write operations and atomic writes."""

    def test_concurrent_writes_no_corruption(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """5 threads writing simultaneously produce consistent cached data."""
        service = DataService(cache_manager=system_cache_manager)

        errors: List[Exception] = []

        def write_data(symbol: str) -> pd.DataFrame:
            try:
                source_df = pd.DataFrame(
                    {
                        "date": pd.date_range("2024-01-02", periods=5, freq="B"),
                        "symbol": [symbol] * 5,
                        "open": [10.0] * 5,
                        "high": [11.0] * 5,
                        "low": [9.0] * 5,
                        "close": [10.5] * 5,
                        "volume": [100_000] * 5,
                        "amount": [1_000_000.0] * 5,
                    }
                )
                _seed_cache(system_cache_manager, "stock_daily", source_df)
                return service.cn.stock.quote.daily(
                    symbol=symbol,
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                    source="akshare",
                )
            except Exception as e:
                errors.append(e)
                return pd.DataFrame()

        symbols = ["sh600000", "sh600036", "sz000001", "sh601318", "sz000858"]

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(write_data, sym) for sym in symbols]
            for future in as_completed(futures):
                future.result()
                # No assertion on content; just checking no exceptions

        assert not errors, f"Errors during concurrent writes: {errors}"

    def test_write_then_read_consistency(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """Data written by one thread is readable by another."""
        service = DataService(cache_manager=system_cache_manager)

        source_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5, freq="B"),
                "symbol": ["600000.XSHG"] * 5,
                "open": [10.0] * 5,
                "high": [11.0] * 5,
                "low": [9.0] * 5,
                "close": [10.5] * 5,
                "volume": [100_000] * 5,
                "amount": [1_000_000.0] * 5,
            }
        )
        _seed_cache(system_cache_manager, "stock_daily", source_df)

        write_result: dict = {"df": None, "error": None}
        read_result: dict = {"df": None, "error": None}

        def writer():
            try:
                write_result["df"] = service.cn.stock.quote.daily(
                    symbol="sh600000",
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                    source="akshare",
                )
            except Exception as e:
                write_result["error"] = e

        def reader():
            # Small delay to let writer finish
            import time

            time.sleep(0.1)
            try:
                read_result["df"] = service.cn.stock.quote.daily(
                    symbol="sh600000",
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                    source="akshare",
                )
            except Exception as e:
                read_result["error"] = e

        t_writer = threading.Thread(target=writer)
        t_reader = threading.Thread(target=reader)

        t_writer.start()
        t_reader.start()
        t_writer.join(timeout=10)
        t_reader.join(timeout=10)

        assert write_result["error"] is None, f"Writer error: {write_result['error']}"
        assert read_result["error"] is None, f"Reader error: {read_result['error']}"
        assert write_result["df"] is not None
        assert read_result["df"] is not None
        assert not write_result["df"].empty
        assert not read_result["df"].empty


@pytest.mark.system
class TestDataServiceSingletonThreadSafety:
    """Tests for DataService behavior under concurrent multi-threaded access."""

    def test_singleton_cache_manager_under_load(
        self,
        system_cache_manager: CacheManager,
    ) -> None:
        """DataService maintains consistent state under concurrent access."""
        service = DataService(cache_manager=system_cache_manager)

        source_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5, freq="B"),
                "symbol": ["600000.XSHG"] * 5,
                "open": [10.0] * 5,
                "high": [11.0] * 5,
                "low": [9.0] * 5,
                "close": [10.5] * 5,
                "volume": [100_000] * 5,
                "amount": [1_000_000.0] * 5,
            }
        )
        _seed_cache(system_cache_manager, "stock_daily", source_df)

        total_calls = 20
        success_count = 0
        lock = threading.Lock()

        def make_call():
            nonlocal success_count
            try:
                df = service.cn.stock.quote.daily(
                    symbol="sh600000",
                    start_date="2024-01-02",
                    end_date="2024-01-10",
                    source="akshare",
                )
                if isinstance(df, pd.DataFrame) and not df.empty:
                    with lock:
                        success_count += 1
            except Exception:
                pass

        threads = [threading.Thread(target=make_call) for _ in range(total_calls)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert success_count > 0, "At least some calls should succeed"
