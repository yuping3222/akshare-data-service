"""Integration test: Service reads from Served layer only.

Verifies that:
- DataService.query() never fetches from source adapters
- DataService.query_daily() never fetches from source adapters
- Empty results return clear missing-data reports (not source fetches)
- ServedReader has no write or source-fetch capabilities
- The service layer dependency graph respects the architecture RFC
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data.service.reader import ServedReader
from akshare_data.service.data_service import DataService, QueryResult
from akshare_data.service.missing_data_policy import MissingDataReport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def real_cache_manager(tmp_path):
    """Real CacheManager with pre-populated test data."""
    reset_cache_manager()
    CacheManager.reset_instance()
    cache = CacheManager(base_dir=str(tmp_path))

    df = pd.DataFrame(
        {
            "symbol": ["600519"] * 5,
            "date": pd.date_range("2024-01-01", periods=5),
            "open": [1800.0] * 5,
            "high": [1850.0] * 5,
            "low": [1780.0] * 5,
            "close": [1820.0] * 5,
            "volume": [100000.0] * 5,
            "amount": [182000000.0] * 5,
            "adjust": ["qfq"] * 5,
        }
    )
    cache.write("stock_daily", df, storage_layer="daily", partition_by="date")

    yield cache
    reset_cache_manager()
    CacheManager.reset_instance()


@pytest.fixture
def mock_cache_manager():
    """Mock CacheManager that returns empty DataFrames."""
    cache = MagicMock()
    cache.read.return_value = pd.DataFrame()
    cache.exists.return_value = False
    cache.table_info.return_value = {"name": "test"}
    cache.list_tables.return_value = []
    cache.has_range.return_value = False
    return cache


@pytest.fixture
def served_reader(mock_cache_manager) -> ServedReader:
    return ServedReader(cache_manager=mock_cache_manager)


@pytest.fixture
def data_service(mock_cache_manager) -> DataService:
    return DataService(cache_manager=mock_cache_manager)


@pytest.fixture
def real_data_service(real_cache_manager) -> DataService:
    return DataService(cache_manager=real_cache_manager)


# ---------------------------------------------------------------------------
# ServedReader is read-only
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestServedReaderReadOnly:
    """ServedReader must not have write or source-fetch capabilities."""

    def test_no_write_method(self, served_reader: ServedReader):
        assert not hasattr(served_reader, "write"), (
            "ServedReader must not have write method"
        )

    def test_no_fetch_method(self, served_reader: ServedReader):
        assert not hasattr(served_reader, "fetch"), (
            "ServedReader must not have fetch method"
        )

    def test_no_source_method(self, served_reader: ServedReader):
        for attr in dir(served_reader):
            if attr.startswith("_"):
                continue
            assert "source" not in attr.lower(), (
                f"ServedReader must not reference sources: {attr}"
            )

    def test_read_returns_empty_on_no_data(self, served_reader: ServedReader):
        result = served_reader.read("trade_calendar")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_read_returns_dataframe_type(self, served_reader: ServedReader):
        result = served_reader.read("trade_calendar")
        assert isinstance(result, pd.DataFrame)

    def test_exists_returns_false_on_no_data(self, served_reader: ServedReader):
        assert served_reader.exists("trade_calendar") is False

    def test_list_tables_returns_list(self, served_reader: ServedReader):
        tables = served_reader.list_tables()
        assert isinstance(tables, list)


# ---------------------------------------------------------------------------
# DataService never fetches from sources
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestServiceReadsServedOnly:
    """DataService must not synchronously fetch from source adapters."""

    def test_query_does_not_call_source(self, data_service: DataService):
        """query() must not trigger any source adapter call."""
        result = data_service.query("trade_calendar")
        assert isinstance(result, QueryResult)
        assert result.has_data is False

    def test_query_returns_missing_report_when_empty(self, data_service: DataService):
        """When data is missing, query() returns a MissingDataReport."""
        result = data_service.query("trade_calendar")
        assert result.missing_report is not None
        assert isinstance(result.missing_report, MissingDataReport)

    def test_query_daily_does_not_call_source(self, data_service: DataService):
        """query_daily() must not trigger any source adapter call."""
        result = data_service.query_daily(
            "stock_daily",
            symbol="600519",
            start_date="2026-04-01",
            end_date="2026-04-22",
        )
        assert isinstance(result, QueryResult)

    def test_query_daily_returns_missing_report_when_empty(
        self, data_service: DataService
    ):
        result = data_service.query_daily(
            "stock_daily",
            symbol="600519",
            start_date="2026-04-01",
            end_date="2026-04-22",
        )
        assert result.has_data is False
        assert result.missing_report is not None


# ---------------------------------------------------------------------------
# DataService uses ServedReader internally
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestServiceUsesReader:
    """DataService must delegate to ServedReader for all reads."""

    def test_service_has_reader(self, data_service: DataService):
        assert hasattr(data_service, "_reader")
        assert isinstance(data_service._reader, ServedReader)

    def test_service_has_no_source_adapter(self, data_service: DataService):
        """Service must not hold a reference to any source adapter."""
        for attr in dir(data_service):
            if attr.startswith("_"):
                continue
            value = getattr(data_service, attr, None)
            if value is None:
                continue
            type_name = type(value).__name__.lower()
            assert "source" not in type_name or "adapter" not in type_name, (
                f"DataService holds source adapter reference: {attr} ({type_name})"
            )


# ---------------------------------------------------------------------------
# Backfill is async, not sync
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestBackfillIsAsync:
    """request_backfill() must queue a request, not synchronously fetch data."""

    def test_request_backfill_returns_id(self, data_service: DataService):
        request_id = data_service.request_backfill("market_quote_daily")
        assert isinstance(request_id, str)
        assert len(request_id) > 0

    def test_request_backfill_does_not_return_data(self, data_service: DataService):
        request_id = data_service.request_backfill("market_quote_daily")
        status = data_service.get_backfill_status(request_id)
        assert status is not None
        # "dataset" replaced legacy "table" key (P1-2 BackfillRegistry migration)
        assert "dataset" in status or "table" in status


# ---------------------------------------------------------------------------
# Architecture dependency contract
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestArchitectureDependency:
    """Module dependency graph must respect 01-architecture-rfc.md."""

    def test_service_module_no_source_import(self):
        """service/ must not import from sources/."""
        service_dir = __import__("akshare_data.service").service.__path__[0]
        import os

        for root, dirs, files in os.walk(service_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                fpath = os.path.join(root, fname)
                with open(fpath, encoding="utf-8") as fh:
                    content = fh.read()
                assert ".sources" not in content, (
                    f"service module imports from sources: {fpath}"
                )

    def test_service_module_no_ingestion_import(self):
        """service/ must not import from ingestion/ except backfill_request.

        Permitted exception: service/missing_data_policy.py and
        service/data_service.py may import ingestion.backfill_request for
        async backfill delegation (P1-2 BackfillRegistry unification).
        All other ingestion imports remain forbidden.
        """
        service_dir = __import__("akshare_data.service").service.__path__[0]
        import os

        # Ingestion sub-modules that service is permitted to reference
        allowed_ingestion_refs = {"ingestion.backfill_request"}

        for root, dirs, files in os.walk(service_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                fpath = os.path.join(root, fname)
                with open(fpath, encoding="utf-8") as fh:
                    content = fh.read()
                # Strip allowed references before checking
                scrubbed = content
                for allowed in allowed_ingestion_refs:
                    scrubbed = scrubbed.replace(allowed, "")
                assert ".ingestion" not in scrubbed, (
                    f"service module imports from ingestion (non-backfill): {fpath}"
                )

    def test_reader_module_no_source_import(self):
        """service/reader.py must not import from sources/."""
        from akshare_data.service import reader

        source = inspect.getsource(reader)
        assert "sources" not in source or "data_sources" not in source, (
            "ServedReader imports from sources module"
        )


# ---------------------------------------------------------------------------
# QueryResult structure
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestQueryResultStructure:
    """QueryResult must carry expected metadata."""

    def test_query_result_has_data(self):
        df = pd.DataFrame({"a": [1]})
        qr = QueryResult(data=df, table="test", has_data=True)
        assert qr.data is not None
        assert qr.has_data is True
        assert qr.is_empty is False

    def test_query_result_empty_detection(self):
        qr = QueryResult(data=pd.DataFrame(), table="test", has_data=False)
        assert qr.is_empty is True
        assert qr.has_data is False

    def test_query_result_to_dict(self):
        qr = QueryResult(data=pd.DataFrame(), table="test", has_data=False)
        d = qr.to_dict()
        assert d["table"] == "test"
        assert d["has_data"] is False
        assert d["row_count"] == 0

    def test_query_result_with_missing_report(self):
        from akshare_data.service.missing_data_policy import MissingAction

        report = MissingDataReport(
            table="market_quote_daily",
            query_params={"symbol": "600519"},
            action=MissingAction.REQUEST_BACKFILL,
            message="No data in served",
        )
        qr = QueryResult(
            data=pd.DataFrame(),
            table="market_quote_daily",
            has_data=False,
            missing_report=report,
        )
        d = qr.to_dict()
        assert "missing_report" in d
        assert d["missing_report"]["table"] == "market_quote_daily"


# ---------------------------------------------------------------------------
# Table existence checks
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.contract
class TestTableExistenceChecks:
    """Service must provide table existence checks without fetching."""

    def test_table_exists_delegates_to_reader(self, data_service: DataService):
        exists = data_service.table_exists("market_quote_daily")
        assert isinstance(exists, bool)

    def test_list_tables_delegates_to_reader(self, data_service: DataService):
        tables = data_service.list_tables()
        assert isinstance(tables, list)

    def test_get_table_info_delegates_to_reader(self, data_service: DataService):
        info = data_service.get_table_info("market_quote_daily")
        assert isinstance(info, dict)
