"""Ingestion layer tests for executor, models, and adapters."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
import pandas as pd

from akshare_data.ingestion.executor.base import (
    BaseTaskExecutor,
    ExecutorContext,
    ExecutorStats,
    ExecutionContext,
    TaskExecutionResult,
    ExecutionResult,
)
from akshare_data.ingestion.models.task import ExtractTask
from akshare_data.ingestion.models.batch import BatchContext, BatchStatus
from akshare_data.ingestion.task_state import TaskStatus
from akshare_data.offline.downloader.executor import TaskExecutor
from akshare_data.offline.downloader.task_builder import DownloadTask


@pytest.fixture
def sample_extract_task() -> ExtractTask:
    """Create a sample ExtractTask for testing."""
    return ExtractTask.new(
        batch_id="batch_001",
        dataset="market_quote_daily",
        domain="cn",
        source_name="akshare",
        interface_name="stock_zh_a_hist",
        params={"symbol": "000001", "period": "daily"},
        extract_date=date(2024, 1, 15),
        task_window=(date(2024, 1, 1), date(2024, 1, 31)),
        max_retries=3,
        priority=0,
    )


@pytest.fixture
def sample_batch_context(sample_extract_task) -> BatchContext:
    """Create a sample BatchContext for testing."""
    tasks = [
        sample_extract_task,
        ExtractTask.new(
            batch_id="batch_001",
            dataset="market_quote_daily",
            domain="cn",
            source_name="akshare",
            interface_name="stock_zh_a_hist",
            params={"symbol": "000002"},
            extract_date=date(2024, 1, 15),
        ),
    ]
    return BatchContext.new(tasks=tasks, domain="cn")


@pytest.mark.unit
class TestExtractTask:
    """Test ExtractTask model."""

    def test_extract_task_creation(self, sample_extract_task):
        """Test ExtractTask is created with correct attributes."""
        assert sample_extract_task.batch_id == "batch_001"
        assert sample_extract_task.dataset == "market_quote_daily"
        assert sample_extract_task.domain == "cn"
        assert sample_extract_task.source_name == "akshare"
        assert sample_extract_task.interface_name == "stock_zh_a_hist"
        assert sample_extract_task.status == TaskStatus.PENDING
        assert sample_extract_task.max_retries == 3

    def test_extract_task_idempotency_key(self, sample_extract_task):
        """Test ExtractTask generates idempotency key."""
        assert sample_extract_task.idempotency_key != ""
        assert len(sample_extract_task.idempotency_key) > 0

    def test_extract_task_state_transitions(self, sample_extract_task):
        """Test ExtractTask state transitions."""
        task = sample_extract_task
        assert task.status == TaskStatus.PENDING

        running = task.mark_running()
        assert running.status == TaskStatus.RUNNING

        succeeded = running.mark_succeeded()
        assert succeeded.status == TaskStatus.SUCCEEDED

        failed = running.mark_failed()
        assert failed.status == TaskStatus.FAILED

        partial = running.mark_partial()
        assert partial.status == TaskStatus.PARTIAL

    def test_extract_task_retry_logic(self, sample_extract_task):
        """Test ExtractTask retry logic."""
        task = sample_extract_task.mark_failed()
        assert task.can_retry() is True
        assert task.retry_count == 0

        retrying = task.mark_retrying()
        assert retrying.status == TaskStatus.RETRYING
        assert retrying.retry_count == 1

        final_failed = retrying.mark_failed()
        for _ in range(3):
            final_failed = final_failed.mark_retrying().mark_failed()

        assert final_failed.can_retry() is False

    def test_extract_task_is_terminal(self, sample_extract_task):
        """Test ExtractTask terminal state detection."""
        task = sample_extract_task
        assert task.is_terminal() is False

        succeeded = task.mark_succeeded()
        assert succeeded.is_terminal() is True

        failed = task.mark_failed()
        assert failed.is_terminal() is True

        running = task.mark_running()
        assert running.is_terminal() is False

    def test_extract_task_to_dict(self, sample_extract_task):
        """Test ExtractTask serialization."""
        d = sample_extract_task.to_dict()
        assert d["task_id"] == sample_extract_task.task_id
        assert d["batch_id"] == "batch_001"
        assert d["dataset"] == "market_quote_daily"
        assert d["status"] == "pending"
        assert "extract_date" in d
        assert "task_window" in d

    def test_extract_task_from_dict(self, sample_extract_task):
        """Test ExtractTask deserialization."""
        d = sample_extract_task.to_dict()
        restored = ExtractTask.from_dict(d)
        assert restored.task_id == sample_extract_task.task_id
        assert restored.batch_id == sample_extract_task.batch_id
        assert restored.dataset == sample_extract_task.dataset
        assert restored.status == sample_extract_task.status

    def test_extract_task_request_params_alias(self):
        """Test ExtractTask accepts request_params alias."""
        task = ExtractTask(
            task_id="test_id",
            batch_id="batch_001",
            dataset="test_dataset",
            domain="cn",
            source_name="akshare",
            interface_name="test_func",
            request_params={"symbol": "000001"},
            extract_date=date(2024, 1, 15),
        )
        assert task.params == {"symbol": "000001"}

    def test_extract_task_with_status_updates_timestamp(self, sample_extract_task):
        """Test with_status updates updated_at."""
        old_time = sample_extract_task.updated_at
        new_task = sample_extract_task.with_status(TaskStatus.RUNNING)
        assert new_task.updated_at > old_time


@pytest.mark.unit
class TestBatchContext:
    """Test BatchContext model."""

    def test_batch_context_creation(self, sample_batch_context):
        """Test BatchContext is created correctly."""
        assert sample_batch_context.batch_id != ""
        assert sample_batch_context.domain == "cn"
        assert len(sample_batch_context.tasks) == 2
        assert sample_batch_context.status == BatchStatus.PENDING

    def test_batch_context_aggregate_status(self, sample_batch_context):
        """Test BatchContext aggregate status computation."""
        batch = sample_batch_context
        assert batch.aggregate_status() == BatchStatus.PENDING

        running_tasks = [t.mark_running() for t in batch.tasks]
        running_batch = BatchContext(
            batch_id=batch.batch_id,
            tasks=running_tasks,
            domain=batch.domain,
        )
        assert running_batch.aggregate_status() == BatchStatus.RUNNING

        succeeded_tasks = [t.mark_succeeded() for t in running_tasks]
        succeeded_batch = BatchContext(
            batch_id=batch.batch_id,
            tasks=succeeded_tasks,
            domain=batch.domain,
        )
        assert succeeded_batch.aggregate_status() == BatchStatus.SUCCEEDED

        mixed_tasks = [batch.tasks[0].mark_succeeded(), batch.tasks[1].mark_failed()]
        mixed_batch = BatchContext(
            batch_id=batch.batch_id,
            tasks=mixed_tasks,
            domain=batch.domain,
        )
        assert mixed_batch.aggregate_status() == BatchStatus.PARTIAL

    def test_batch_context_state_transitions(self, sample_batch_context):
        """Test BatchContext state transitions."""
        batch = sample_batch_context
        assert batch.status == BatchStatus.PENDING

        running = batch.mark_running()
        assert running.status == BatchStatus.RUNNING
        assert running.started_at is not None

        succeeded = running.mark_succeeded()
        assert succeeded.status == BatchStatus.SUCCEEDED
        assert succeeded.finished_at is not None

        failed = running.mark_failed()
        assert failed.status == BatchStatus.FAILED

        partial = running.mark_partial()
        assert partial.status == BatchStatus.PARTIAL

        cancelled = running.mark_cancelled()
        assert cancelled.status == BatchStatus.CANCELLED

    def test_batch_context_task_access(self, sample_batch_context):
        """Test BatchContext task access methods."""
        batch = sample_batch_context
        task_id = batch.tasks[0].task_id
        found = batch.get_task_by_id(task_id)
        assert found is not None
        assert found.task_id == task_id

        not_found = batch.get_task_by_id("nonexistent")
        assert not_found is None

        pending = batch.pending_tasks()
        assert len(pending) == 2

    def test_batch_context_retryable_tasks(self, sample_batch_context):
        """Test BatchContext retryable tasks filtering."""
        batch = sample_batch_context
        assert len(batch.retryable_tasks()) == 0

        failed_tasks = [t.mark_failed() for t in batch.tasks]
        failed_batch = BatchContext(
            batch_id=batch.batch_id,
            tasks=failed_tasks,
            domain=batch.domain,
        )
        retryable = failed_batch.retryable_tasks()
        assert len(retryable) == 2

    def test_batch_context_to_dict(self, sample_batch_context):
        """Test BatchContext serialization."""
        d = sample_batch_context.to_dict()
        assert d["batch_id"] == sample_batch_context.batch_id
        assert d["status"] == "pending"
        assert d["domain"] == "cn"
        assert d["task_count"] == 2

    def test_batch_context_from_dict(self, sample_batch_context):
        """Test BatchContext deserialization."""
        d = sample_batch_context.to_dict()
        restored = BatchContext.from_dict(d)
        assert restored.batch_id == sample_batch_context.batch_id
        assert restored.status == sample_batch_context.status


@pytest.mark.unit
class TestExecutorBase:
    """Test executor base classes."""

    def test_executor_context_creation(self):
        """Test ExecutorContext creation."""
        ctx = ExecutorContext(
            batch_id="batch_001",
            run_id="run_001",
            trigger="scheduler",
            metadata={"key": "value"},
        )
        assert ctx.batch_id == "batch_001"
        assert ctx.run_id == "run_001"
        assert ctx.trigger == "scheduler"
        assert ctx.metadata["key"] == "value"

    def test_execution_context_creation(self):
        """Test ExecutionContext creation."""
        ctx = ExecutionContext(
            request_id="req_001",
            batch_id="batch_001",
            source="akshare",
            dataset="market_quote_daily",
        )
        assert ctx.request_id == "req_001"
        assert ctx.batch_id == "batch_001"
        assert ctx.source == "akshare"
        assert ctx.dataset == "market_quote_daily"

    def test_executor_stats_creation(self):
        """Test ExecutorStats creation."""
        stats = ExecutorStats(
            attempt=2,
            latency_ms=150.0,
            input_count=1,
            output_count=100,
        )
        assert stats.attempt == 2
        assert stats.latency_ms == 150.0
        assert stats.input_count == 1
        assert stats.output_count == 100

    def test_task_execution_result_creation(self):
        """Test TaskExecutionResult creation."""
        start = datetime.now(timezone.utc)
        end = datetime.now(timezone.utc)
        result = TaskExecutionResult(
            success=True,
            task_name="test_task",
            rows=100,
            payload={"data": "value"},
            error="",
            started_at=start,
            finished_at=end,
            metadata={"extra": "info"},
        )
        assert result.success is True
        assert result.task_name == "test_task"
        assert result.rows == 100
        assert result.duration_ms >= 0

    def test_task_execution_result_to_dict(self):
        """Test TaskExecutionResult serialization."""
        result = TaskExecutionResult(
            success=True,
            task_name="test_task",
            rows=50,
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["task"] == "test_task"
        assert d["rows"] == 50
        assert "duration_ms" in d

    def test_execution_result_create_success(self):
        """Test ExecutionResult.create_success factory."""
        payload = pd.DataFrame({"col": [1, 2, 3]})
        result = ExecutionResult.create_success(
            payload=payload,
            task_name="test_task",
            rows=3,
        )
        assert result.ok is True
        assert result.success is True
        assert result.rows == 3
        assert result.error == ""

    def test_execution_result_create_failure(self):
        """Test ExecutionResult.create_failure factory."""
        result = ExecutionResult.create_failure(
            error_code="E001",
            error_message="Data fetch failed",
            task_name="test_task",
        )
        assert result.ok is False
        assert result.success is False
        assert result.error_code == "E001"
        assert result.error == "Data fetch failed"

    def test_execution_result_to_dict(self):
        """Test ExecutionResult serialization."""
        result = ExecutionResult.create_success(
            task_name="test_task",
            rows=10,
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["task"] == "test_task"
        assert d["rows"] == 10

    def test_execution_result_duration_ms(self):
        """Test ExecutionResult duration calculation."""
        start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 10, 0, 5, tzinfo=timezone.utc)
        result = ExecutionResult(
            ok=True,
            started_at=start,
            finished_at=end,
        )
        assert result.duration_ms == 5000

    def test_base_executor_result_helper(self):
        """Test BaseTaskExecutor.result helper method."""

        class DummyExecutor(BaseTaskExecutor[str, int]):
            def run(self, task, *, context=None):
                return self.result(
                    success=True,
                    task_name=task,
                    rows=10,
                    payload=42,
                )

        executor = DummyExecutor()
        result = executor.run("test_task")
        assert result.success is True
        assert result.rows == 10

    def test_offline_executor_success(self, monkeypatch):
        """Test TaskExecutor run success."""

        class FakeRateLimiter:
            def wait(self, key):
                return None

        class FakeCacheManager:
            def write(self, **kwargs):
                self.last = kwargs

        task = DownloadTask(
            interface="stock_zh_a_hist",
            func="stock_zh_a_hist",
            table="stock_daily",
            kwargs={"symbol": "000001"},
        )

        cache_mgr = FakeCacheManager()
        executor = TaskExecutor(rate_limiter=FakeRateLimiter(), cache_manager=cache_mgr)

        monkeypatch.setattr(
            executor,
            "_call_akshare",
            lambda *a, **kw: pd.DataFrame([{"开盘": 10.0, "收盘": 10.5}]),
        )

        result = executor.run(task, context=ExecutorContext(batch_id="b1", run_id="r1"))
        assert result.success is True
        assert result.rows == 1
        assert result.metadata.get("batch_id") == "b1"

    def test_offline_executor_empty_data(self, monkeypatch):
        """Test TaskExecutor handles empty data."""

        class FakeRateLimiter:
            def wait(self, key):
                return None

        task = DownloadTask(
            interface="stock_zh_a_hist",
            func="stock_zh_a_hist",
            table="stock_daily",
            kwargs={"symbol": "000001"},
        )
        executor = TaskExecutor(rate_limiter=FakeRateLimiter())

        monkeypatch.setattr(executor, "_call_akshare", lambda *a, **kw: pd.DataFrame())

        result = executor.run(task)
        assert result.success is False
        assert result.error == "Empty data"

    def test_offline_executor_legacy_execute(self, monkeypatch):
        """Test TaskExecutor legacy execute method."""

        class FakeRateLimiter:
            def wait(self, key):
                return None

        task = DownloadTask(
            interface="stock_zh_a_hist",
            func="stock_zh_a_hist",
            table="stock_daily",
            kwargs={"symbol": "000001"},
        )
        executor = TaskExecutor(rate_limiter=FakeRateLimiter())

        monkeypatch.setattr(
            executor,
            "_call_akshare",
            lambda *a, **kw: pd.DataFrame([{"开盘": 10.0}]),
        )

        legacy_result = executor.execute(task)
        assert legacy_result["success"] is True
        assert legacy_result["task"] == "stock_zh_a_hist"

    def test_offline_executor_writes_schema_columns_only(self, monkeypatch):
        """有 schema 的表落盘时应只保留 schema 字段。"""

        class FakeRateLimiter:
            def wait(self, key):
                return None

        class FakeCacheManager:
            def __init__(self):
                self.last = None

            def write(self, **kwargs):
                self.last = kwargs

        task = DownloadTask(
            interface="equity_daily",
            func="equity_daily",
            table="stock_daily",
            kwargs={"symbol": "000001", "adjust": "qfq"},
            use_multi_source=True,
        )
        cache_mgr = FakeCacheManager()
        executor = TaskExecutor(rate_limiter=FakeRateLimiter(), cache_manager=cache_mgr)

        monkeypatch.setattr(
            executor,
            "_call_akshare",
            lambda *a, **kw: pd.DataFrame(
                [
                    {
                        "日期": "2025-01-02",
                        "开盘": 10.0,
                        "最高": 10.2,
                        "最低": 9.8,
                        "收盘": 10.1,
                        "成交量": 1000.0,
                        "成交额": 10000.0,
                        "流通股": 5000.0,
                    }
                ]
            ),
        )

        result = executor.run(task)
        assert result.success is True
        written = cache_mgr.last["data"]
        assert set(written.columns) == {
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjust",
        }


@pytest.mark.unit
class TestMockAdapter:
    """Test MockAdapter functionality."""

    def test_mock_adapter_get_daily_data(self):
        """Test MockAdapter.get_daily_data."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_daily_data("sh600000", "2024-01-01", "2024-01-10")
        assert not df.empty
        assert "date" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns
        assert "symbol" in df.columns
        assert df["symbol"].iloc[0] == "sh600000"

    def test_mock_adapter_get_index_stocks(self):
        """Test MockAdapter.get_index_stocks."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        stocks = adapter.get_index_stocks("000300")
        assert len(stocks) > 0
        assert all(s.isdigit() for s in stocks[:5])

    def test_mock_adapter_get_index_components(self):
        """Test MockAdapter.get_index_components."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_index_components("000300", include_weights=True)
        assert not df.empty
        assert "index_code" in df.columns
        assert "code" in df.columns
        assert "weight" in df.columns

    def test_mock_adapter_get_trading_days(self):
        """Test MockAdapter.get_trading_days."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        days = adapter.get_trading_days("2024-01-01", "2024-01-10")
        assert len(days) > 0
        assert all("-" in d for d in days)

    def test_mock_adapter_get_securities_list(self):
        """Test MockAdapter.get_securities_list."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_securities_list("stock")
        assert not df.empty
        assert "code" in df.columns
        assert "display_name" in df.columns

    def test_mock_adapter_get_security_info(self):
        """Test MockAdapter.get_security_info."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        info = adapter.get_security_info("sh600000")
        assert info["code"] == "sh600000"
        assert "display_name" in info
        assert "type" in info

    def test_mock_adapter_price_constraints(self):
        """Test MockAdapter generates valid price data."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_daily_data("sh600000", "2024-01-01", "2024-01-31")
        assert all(df["high"] >= df["low"])
        assert all(df["high"] >= df["open"])
        assert all(df["high"] >= df["close"])
        assert all(df["low"] <= df["open"])
        assert all(df["low"] <= df["close"])

    def test_mock_adapter_get_minute_data(self):
        """Test MockAdapter.get_minute_data."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_minute_data("sh600000", freq="1min")
        assert not df.empty
        assert "date" in df.columns or "datetime" in df.columns

    def test_mock_adapter_get_money_flow(self):
        """Test MockAdapter.get_money_flow."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_money_flow("sh600000")
        assert not df.empty
        assert "main_net" in df.columns

    def test_mock_adapter_get_finance_indicator(self):
        """Test MockAdapter.get_finance_indicator."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_finance_indicator("sh600000")
        assert not df.empty
        assert "pe_ttm" in df.columns or "symbol" in df.columns

    def test_mock_adapter_get_etf_daily(self):
        """Test MockAdapter.get_etf_daily."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_etf_daily("510300", "2024-01-01", "2024-01-10")
        assert not df.empty

    def test_mock_adapter_name_attribute(self):
        """Test MockAdapter has correct name attribute."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        assert adapter.name == "mock"
        assert adapter.source_type == "mock"

    def test_mock_adapter_empty_range(self):
        """Test MockAdapter handles empty date range."""
        from akshare_data.ingestion.adapters.mock import MockAdapter

        adapter = MockAdapter()
        df = adapter.get_daily_data("sh600000", "2024-01-01", "2024-01-01")
        assert df.empty or len(df) == 1


@pytest.mark.unit
class TestIngestionIntegration:
    """Test ingestion layer integration scenarios."""

    def test_batch_with_multiple_tasks(self):
        """Test BatchContext with multiple tasks."""
        tasks = [
            ExtractTask.new(
                batch_id="batch_002",
                dataset="market_quote_daily",
                domain="cn",
                source_name="akshare",
                interface_name="stock_zh_a_hist",
                params={"symbol": f"{i:06d}"},
                extract_date=date(2024, 1, 15),
            )
            for i in range(5)
        ]
        batch = BatchContext.new(tasks=tasks, domain="cn")
        assert len(batch.tasks) == 5
        assert all(t.batch_id == tasks[0].batch_id for t in batch.tasks)

    def test_task_execution_with_context(self):
        """Test task execution with ExecutionContext."""

        class SimpleExecutor(BaseTaskExecutor[str, pd.DataFrame]):
            def run(self, task, *, context=None):
                df = pd.DataFrame({"col": [1, 2, 3]})
                return self.result(
                    success=True,
                    task_name=task,
                    rows=len(df),
                    payload=df,
                    metadata={"trigger": context.trigger if context else "manual"},
                )

        executor = SimpleExecutor()
        ctx = ExecutorContext(batch_id="b1", run_id="r1", trigger="scheduler")
        result = executor.run("test_task", context=ctx)
        assert result.success is True
        assert result.metadata["trigger"] == "scheduler"

    def test_batch_serialization_roundtrip(self, sample_batch_context):
        """Test batch serialization preserves all data."""
        original = sample_batch_context
        d = original.to_dict()
        restored = BatchContext.from_dict(d)
        assert restored.batch_id == original.batch_id
        assert restored.status == original.status
        assert restored.domain == original.domain

    def test_task_serialization_with_all_fields(self, sample_extract_task):
        """Test task serialization preserves all fields."""
        original = sample_extract_task
        d = original.to_dict()
        restored = ExtractTask.from_dict(d)
        assert restored.task_id == original.task_id
        assert restored.params == original.params
        assert restored.task_window == original.task_window
        assert restored.priority == original.priority
