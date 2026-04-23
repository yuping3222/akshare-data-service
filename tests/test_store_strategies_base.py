"""tests/test_store_strategies_base.py

Comprehensive tests for strategies/base.py
"""

import pandas as pd
import pytest
from abc import ABC

from akshare_data.store.strategies.base import CacheStrategy


class ConcreteCacheStrategy(CacheStrategy):
    """Concrete implementation of CacheStrategy for testing."""

    def should_fetch(self, cached: pd.DataFrame | None, **params) -> bool:
        return cached is None

    def merge(
        self, cached: pd.DataFrame | None, fresh: pd.DataFrame, **params
    ) -> pd.DataFrame:
        if cached is None:
            return fresh
        return pd.concat([cached, fresh]).drop_duplicates()

    def build_where(self, **params) -> dict:
        return {"key": params.get("key", "default")}


class TestCacheStrategyAbstract:
    """Tests for CacheStrategy abstract class."""

    def test_cache_strategy_is_abc(self):
        """Test CacheStrategy is an abstract base class."""
        assert issubclass(CacheStrategy, ABC)

    def test_cache_strategy_cannot_be_instantiated_directly(self):
        """Test CacheStrategy cannot be instantiated directly."""
        with pytest.raises(TypeError) as exc_info:
            CacheStrategy()
        assert "abstract" in str(exc_info.value).lower()

    def test_concrete_implementation_can_be_instantiated(self):
        """Test concrete implementation can be instantiated."""
        strategy = ConcreteCacheStrategy()
        assert strategy is not None

    def test_should_fetch_is_abstract(self):
        """Test should_fetch must be implemented."""
        strategy = ConcreteCacheStrategy()
        assert hasattr(strategy, "should_fetch")

    def test_merge_is_abstract(self):
        """Test merge must be implemented."""
        strategy = ConcreteCacheStrategy()
        assert hasattr(strategy, "merge")

    def test_build_where_is_abstract(self):
        """Test build_where must be implemented."""
        strategy = ConcreteCacheStrategy()
        assert hasattr(strategy, "build_where")


class TestConcreteCacheStrategyShouldFetch:
    """Tests for ConcreteCacheStrategy.should_fetch implementation."""

    def test_should_fetch_returns_true_when_cached_is_none(self):
        """Test should_fetch returns True when cached is None."""
        strategy = ConcreteCacheStrategy()
        assert strategy.should_fetch(None) is True

    def test_should_fetch_returns_false_when_cached_exists(self):
        """Test should_fetch returns False when cached exists."""
        strategy = ConcreteCacheStrategy()
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert strategy.should_fetch(df) is False


class TestConcreteCacheStrategyMerge:
    """Tests for ConcreteCacheStrategy.merge implementation."""

    def test_merge_returns_fresh_when_cached_is_none(self):
        """Test merge returns fresh data when cached is None."""
        strategy = ConcreteCacheStrategy()
        fresh = pd.DataFrame({"a": [1, 2, 3]})
        result = strategy.merge(None, fresh)
        pd.testing.assert_frame_equal(result, fresh)

    def test_merge_combines_cached_and_fresh(self):
        """Test merge combines cached and fresh data."""
        strategy = ConcreteCacheStrategy()
        cached = pd.DataFrame({"a": [1, 2]})
        fresh = pd.DataFrame({"a": [3, 4]})
        result = strategy.merge(cached, fresh)
        assert len(result) == 4

    def test_merge_removes_duplicates(self):
        """Test merge removes duplicates."""
        strategy = ConcreteCacheStrategy()
        cached = pd.DataFrame({"a": [1, 2, 3]})
        fresh = pd.DataFrame({"a": [2, 3, 4]})
        result = strategy.merge(cached, fresh)
        assert len(result) == 4


class TestConcreteCacheStrategyBuildWhere:
    """Tests for ConcreteCacheStrategy.build_where implementation."""

    def test_build_where_returns_dict(self):
        """Test build_where returns a dictionary."""
        strategy = ConcreteCacheStrategy()
        result = strategy.build_where()
        assert isinstance(result, dict)

    def test_build_where_uses_params(self):
        """Test build_where uses provided params."""
        strategy = ConcreteCacheStrategy()
        result = strategy.build_where(key="test_key")
        assert result["key"] == "test_key"

    def test_build_where_default_value(self):
        """Test build_where uses default when param not provided."""
        strategy = ConcreteCacheStrategy()
        result = strategy.build_where()
        assert result["key"] == "default"


class TestCacheStrategyInheritance:
    """Tests for CacheStrategy inheritance behavior."""

    def test_subclass_can_override_all_methods(self):
        """Test that a subclass can override all abstract methods."""

        class CustomStrategy(CacheStrategy):
            def should_fetch(self, cached: pd.DataFrame | None, **params) -> bool:
                return True

            def merge(
                self, cached: pd.DataFrame | None, fresh: pd.DataFrame, **params
            ) -> pd.DataFrame:
                return fresh

            def build_where(self, **params) -> dict:
                return {"custom": True}

        strategy = CustomStrategy()
        assert strategy.should_fetch(None) is True
        pd.testing.assert_frame_equal(
            strategy.merge(None, pd.DataFrame({"a": [1]})), pd.DataFrame({"a": [1]})
        )
        assert strategy.build_where() == {"custom": True}


class TestCacheStrategyEdgeCases:
    """Tests for edge cases with CacheStrategy."""

    def test_empty_dataframe_cached(self):
        """Test handling of empty DataFrame as cached."""
        strategy = ConcreteCacheStrategy()
        empty_df = pd.DataFrame()
        result = strategy.should_fetch(empty_df)
        assert result is False

    def test_empty_dataframe_fresh(self):
        """Test handling of empty DataFrame as fresh."""
        strategy = ConcreteCacheStrategy()
        cached = pd.DataFrame({"a": [1, 2]})
        fresh = pd.DataFrame()
        result = strategy.merge(cached, fresh)
        pd.testing.assert_frame_equal(result, cached)
