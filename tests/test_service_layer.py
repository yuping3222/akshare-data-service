"""Tests for the Service layer: ServedReader, VersionSelector, MissingDataPolicy."""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data.service.reader import ServedReader
from akshare_data.service.version_selector import VersionSelector, VersionInfo
from akshare_data.service.missing_data_policy import (
    MissingAction,
    MissingDataPolicy,
    MissingDataReport,
)
from akshare_data.ingestion.backfill_request import get_backfill_registry
from akshare_data.store.manager import CacheManager, reset_cache_manager

pytestmark = pytest.mark.unit


class TestServiceServedReaderBasic:
    def test_read_empty_table(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))
        reader = ServedReader(cache_manager=cache)

        result = reader.read("nonexistent_table")
        assert result.empty

    def test_read_with_columns(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
                "open": [10.0] * 5,
                "close": [10.5] * 5,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        result = reader.read("test_table", columns=["date", "close"])
        assert list(result.columns) == ["date", "close"]

    def test_read_with_where_condition(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10),
                "symbol": ["sh600000"] * 10,
                "value": [i for i in range(10)],
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        result = reader.read("test_table", where={"value": [5, 9]})
        assert len(result) == 5
        assert result["value"].min() >= 5

    def test_read_with_order_by(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
                "value": [5, 3, 1, 4, 2],
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        result = reader.read("test_table", order_by=["value"])
        assert list(result["value"]) == [1, 2, 3, 4, 5]

    def test_read_with_limit(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=100),
                "symbol": ["sh600000"] * 100,
                "value": [i for i in range(100)],
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        result = reader.read("test_table", limit=10)
        assert len(result) == 10


class TestServiceServedReaderPartition:
    def test_read_with_partition_by(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df1 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
                "value": [1, 2, 3, 4, 5],
            }
        )
        df2 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-09", periods=5),
                "symbol": ["sh600001"] * 5,
                "value": [6, 7, 8, 9, 10],
            }
        )

        cache.write(
            "partitioned_table",
            df1,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600000",
        )
        cache.write(
            "partitioned_table",
            df2,
            storage_layer="daily",
            partition_by="symbol",
            partition_value="sh600001",
        )

        reader = ServedReader(cache_manager=cache)
        result = reader.read(
            "partitioned_table", partition_by="symbol", partition_value="sh600000"
        )
        assert len(result) == 5
        assert all(result["symbol"] == "sh600000")

    def test_read_partition_validation_fallback(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        result = reader.read(
            "test_table", partition_by="wrong_column", partition_value="value"
        )
        assert not result.empty


class TestServiceServedReaderExists:
    def test_exists_true(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        assert reader.exists("test_table")

    def test_exists_false(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))
        reader = ServedReader(cache_manager=cache)
        assert not reader.exists("nonexistent_table")

    def test_exists_with_where(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
                "value": [1, 2, 3, 4, 5],
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        assert reader.exists("test_table")
        assert not reader.exists("nonexistent_table")


class TestServiceServedReaderHasDateRange:
    def test_has_date_range_full_coverage(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10),
                "symbol": ["sh600000"] * 10,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        assert reader.has_date_range("test_table", "2024-01-02", "2024-01-11")

    def test_has_date_range_partial(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-05", periods=5),
                "symbol": ["sh600000"] * 5,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        assert not reader.has_date_range("test_table", "2024-01-02", "2024-01-11")

    def test_has_date_range_no_data(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))
        reader = ServedReader(cache_manager=cache)
        assert not reader.has_date_range("nonexistent", "2024-01-01", "2024-01-10")

    def test_has_date_range_with_custom_date_col(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "trade_date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        assert reader.has_date_range(
            "test_table", "2024-01-02", "2024-01-06", date_col="trade_date"
        )


class TestServiceServedReaderMetadata:
    def test_get_table_info(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=5),
                "symbol": ["sh600000"] * 5,
            }
        )
        cache.write("test_table", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        info = reader.get_table_info("test_table")
        assert info["name"] == "test_table"
        assert "file_count" in info

    def test_get_table_info_nonexistent(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))
        reader = ServedReader(cache_manager=cache)
        info = reader.get_table_info("nonexistent_table")
        assert info["name"] == "nonexistent_table"
        assert info["file_count"] == 0

    def test_list_tables(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame({"date": [pd.Timestamp("2024-01-02")]})
        cache.write("table_a", df, storage_layer="daily")
        cache.write("table_b", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        tables = reader.list_tables()
        assert "table_a" in tables
        assert "table_b" in tables

    def test_list_tables_empty(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))
        reader = ServedReader(cache_manager=cache)
        tables = reader.list_tables()
        assert tables == []


class TestVersionSelectorComplete:
    def test_resolve_none_returns_latest(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        assert selector.resolve(None) == "v1"

    def test_resolve_latest_keyword_returns_latest(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="active"))
        assert selector.resolve("latest") == "v2"

    def test_resolve_specific_active_version(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="active"))
        assert selector.resolve("v1") == "v1"

    def test_resolve_inactive_version_fallback(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="rolled_back"))
        assert selector.resolve("v2") == "v1"

    def test_resolve_nonexistent_fallback(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        assert selector.resolve("v99") == "v1"

    def test_resolve_empty_registry(self):
        selector = VersionSelector()
        assert selector.resolve() == "latest"

    def test_get_version_info_registered(self):
        selector = VersionSelector()
        selector.register_version(
            "v1", VersionInfo(version="v1", status="active", publish_time="2024-01-01")
        )
        info = selector.get_version_info("v1")
        assert info.version == "v1"
        assert info.status == "active"
        assert info.publish_time == "2024-01-01"

    def test_get_version_info_unregistered(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        info = selector.get_version_info()
        assert info.version == "v1"
        assert info.status == "active"

    def test_get_version_info_empty_registry(self):
        selector = VersionSelector()
        info = selector.get_version_info()
        assert info.status == "unknown"

    def test_register_version_overwrites(self):
        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v1", VersionInfo(version="v1", status="inactive"))
        resolved = selector.resolve("v1")
        assert resolved != "v1"

    def test_custom_default_version(self):
        selector = VersionSelector(default_version="stable")
        assert selector._default_version == "stable"


class TestMissingDataPolicy:
    def test_default_action_return_empty(self):
        policy = MissingDataPolicy()
        assert policy.resolve_action("any_table") == MissingAction.RETURN_EMPTY

    def test_table_specific_action(self):
        policy = MissingDataPolicy(
            default_action=MissingAction.RAISE_ERROR,
            table_actions={"important_table": MissingAction.REQUEST_BACKFILL},
        )
        assert (
            policy.resolve_action("important_table") == MissingAction.REQUEST_BACKFILL
        )
        assert policy.resolve_action("other_table") == MissingAction.RAISE_ERROR

    def test_set_table_action(self):
        policy = MissingDataPolicy()
        policy.set_table_action("special_table", MissingAction.RETURN_STALE)
        assert policy.resolve_action("special_table") == MissingAction.RETURN_STALE

    def test_handle_missing_return_empty(self):
        policy = MissingDataPolicy(default_action=MissingAction.RETURN_EMPTY)
        report = policy.handle_missing("test_table", {"date": "2024-01-01"})
        assert report.action == MissingAction.RETURN_EMPTY
        assert report.table == "test_table"
        assert "suggested_action" in report.to_dict()

    def test_handle_missing_return_stale(self):
        policy = MissingDataPolicy(default_action=MissingAction.RETURN_STALE)
        stale_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")]})
        report = policy.handle_missing("test_table", {}, stale_data=stale_df)
        assert report.action == MissingAction.RETURN_STALE
        assert report.metadata.get("has_stale") is True

    def test_handle_missing_return_stale_no_stale(self):
        policy = MissingDataPolicy(default_action=MissingAction.RETURN_STALE)
        report = policy.handle_missing("test_table", {}, stale_data=None)
        assert report.action == MissingAction.RETURN_STALE
        assert report.metadata.get("has_stale") is False

    def test_handle_missing_request_backfill(self):
        policy = MissingDataPolicy(
            default_action=MissingAction.REQUEST_BACKFILL,
        )
        report = policy.handle_missing("test_table", {"date": "2024-01-01"})
        assert report.action == MissingAction.REQUEST_BACKFILL
        assert report.backfill_request_id is not None

    def test_handle_missing_raise_error(self):
        policy = MissingDataPolicy(default_action=MissingAction.RAISE_ERROR)
        report = policy.handle_missing("test_table", {})
        assert report.action == MissingAction.RAISE_ERROR
        assert "Ensure data has been ingested" in report.suggested_action

    def test_handle_missing_unknown_action(self):
        policy = MissingDataPolicy()
        report = policy.handle_missing("test_table", {})
        assert report.action == MissingAction.RETURN_EMPTY


class TestMissingDataReport:
    def test_to_dict(self):
        report = MissingDataReport(
            table="test_table",
            query_params={"date": "2024-01-01"},
            action=MissingAction.RETURN_EMPTY,
            message="No data",
            suggested_action="Run downloader",
            backfill_request_id=None,
            metadata={"key": "value"},
        )
        d = report.to_dict()
        assert d["table"] == "test_table"
        assert d["action"] == "return_empty"
        assert d["query_params"]["date"] == "2024-01-01"

    def test_default_metadata(self):
        report = MissingDataReport(
            table="test_table",
            query_params={},
            action=MissingAction.RETURN_EMPTY,
            message="No data",
        )
        assert report.metadata == {}


class TestBackfillRequestRegistry:
    """Tests for BackfillRegistry singleton (migrated from service layer to ingestion layer).

    BackfillRequestRegistry was removed from missing_data_policy.py in favour of
    the full-featured ingestion.backfill_request.BackfillRegistry.
    """

    def test_get_backfill_registry_returns_singleton(self):
        r1 = get_backfill_registry()
        r2 = get_backfill_registry()
        assert r1 is r2

    def test_missing_data_policy_backfill_submits_to_real_registry(self):
        policy = MissingDataPolicy(default_action=MissingAction.REQUEST_BACKFILL)
        report = policy.handle_missing("test_table_registry", {"date": "2024-01-01"})
        assert report.action == MissingAction.REQUEST_BACKFILL
        assert report.backfill_request_id is not None


class TestMissingActionEnum:
    def test_all_actions_exist(self):
        assert MissingAction.RETURN_EMPTY.value == "return_empty"
        assert MissingAction.RETURN_STALE.value == "return_stale"
        assert MissingAction.REQUEST_BACKFILL.value == "request_backfill"
        assert MissingAction.RAISE_ERROR.value == "raise_error"

    def test_action_from_string(self):
        assert MissingAction("return_empty") == MissingAction.RETURN_EMPTY
        assert MissingAction("request_backfill") == MissingAction.REQUEST_BACKFILL


class TestServiceReaderConcurrency:
    def test_concurrent_reads(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=100),
                "symbol": ["sh600000"] * 100,
                "value": [i for i in range(100)],
            }
        )
        cache.write("concurrent_test", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        errors = []
        results = []
        lock = threading.Lock()

        def read_worker():
            try:
                result = reader.read("concurrent_test", limit=10)
                with lock:
                    results.append(len(result))
            except Exception as e:
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=read_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(r == 10 for r in results)

    def test_concurrent_exists_checks(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame({"date": [pd.Timestamp("2024-01-02")]})
        cache.write("exists_test", df, storage_layer="daily")

        reader = ServedReader(cache_manager=cache)
        errors = []
        results = []
        lock = threading.Lock()

        def exists_worker():
            try:
                exists = reader.exists("exists_test")
                with lock:
                    results.append(exists)
            except Exception as e:
                with lock:
                    errors.append(str(e))

        threads = [threading.Thread(target=exists_worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(r is True for r in results)


class TestServiceReaderErrorHandling:
    def test_read_handles_cache_error(self, tmp_path: Path):
        mock_cache = MagicMock()
        mock_cache.read.side_effect = Exception("Cache error")
        reader = ServedReader(cache_manager=mock_cache)
        result = reader.read("test_table")
        assert result.empty

    def test_exists_handles_cache_error(self, tmp_path: Path):
        mock_cache = MagicMock()
        mock_cache.exists.side_effect = Exception("Cache error")
        reader = ServedReader(cache_manager=mock_cache)
        assert not reader.exists("test_table")

    def test_has_date_range_handles_cache_error(self, tmp_path: Path):
        mock_cache = MagicMock()
        mock_cache.has_range.side_effect = Exception("Cache error")
        reader = ServedReader(cache_manager=mock_cache)
        assert not reader.has_date_range("test_table", "2024-01-01", "2024-01-10")

    def test_get_table_info_handles_cache_error(self, tmp_path: Path):
        mock_cache = MagicMock()
        mock_cache.table_info.side_effect = Exception("Cache error")
        reader = ServedReader(cache_manager=mock_cache)
        info = reader.get_table_info("test_table")
        assert "error" in info

    def test_list_tables_handles_cache_error(self, tmp_path: Path):
        mock_cache = MagicMock()
        mock_cache.list_tables.side_effect = Exception("Cache error")
        reader = ServedReader(cache_manager=mock_cache)
        tables = reader.list_tables()
        assert tables == []


class TestVersionSelectorWithMockCache:
    def test_version_selector_integration(self, tmp_path: Path):
        reset_cache_manager()
        CacheManager.reset_instance()
        cache = CacheManager(base_dir=str(tmp_path))

        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-02", periods=10),
                "symbol": ["sh600000"] * 10,
                "value": [i for i in range(10)],
            }
        )
        cache.write("versioned_table", df, storage_layer="daily")

        selector = VersionSelector()
        selector.register_version("v1", VersionInfo(version="v1", status="active"))
        selector.register_version("v2", VersionInfo(version="v2", status="active"))

        reader = ServedReader(cache_manager=cache)
        result = reader.read("versioned_table")
        assert len(result) == 10


class TestMissingDataPolicyIntegration:
    def test_policy_with_backfill_registry_integration(self, tmp_path: Path):
        policy = MissingDataPolicy(default_action=MissingAction.REQUEST_BACKFILL)

        reports = []
        for i in range(5):
            report = policy.handle_missing(f"table_integ_{i}", {"date": f"2024-01-0{i}"})
            reports.append(report)

        for report in reports:
            assert report.action == MissingAction.REQUEST_BACKFILL
            assert report.backfill_request_id is not None
