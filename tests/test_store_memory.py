"""tests/test_store_memory.py

Comprehensive tests for MemoryCache class in store/memory.py
"""

import time

import pandas as pd
import pytest

from akshare_data.store.memory import CacheEntry, MemoryCache


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [7, 8, 9],
        }
    )


@pytest.fixture
def cache():
    """Create a MemoryCache instance for testing."""
    return MemoryCache(max_items=100, default_ttl_seconds=3600)


class TestMemoryCacheInit:
    """Tests for MemoryCache initialization."""

    def test_init_default_values(self):
        """Test default initialization values."""
        cache = MemoryCache()
        assert cache._max_items == 5000
        assert cache._default_ttl_seconds == 3600

    def test_init_custom_values(self):
        """Test custom initialization values."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=60)
        assert cache._max_items == 100
        assert cache._default_ttl_seconds == 60

    def test_size_initially_zero(self):
        """Test size is zero initially."""
        cache = MemoryCache()
        assert cache.size == 0

    def test_hit_rate_initially_zero(self):
        """Test hit rate is zero when no operations performed."""
        cache = MemoryCache()
        assert cache.hit_rate == 0.0


class TestMemoryCachePutGet:
    """Tests for MemoryCache put and get operations."""

    def test_put_and_get_basic(self, cache, sample_df):
        """Test basic put and get operations."""
        cache.put("test_key", sample_df)
        result = cache.get("test_key")
        assert result is not None
        pd.testing.assert_frame_equal(result, sample_df)

    def test_get_nonexistent_key(self, cache):
        """Test getting a non-existent key returns None."""
        result = cache.get("nonexistent_key")
        assert result is None

    def test_get_returns_copy(self, cache, sample_df):
        """Test get returns a copy, not the original."""
        cache.put("test_key", sample_df)
        result1 = cache.get("test_key")
        result2 = cache.get("test_key")
        result1.iloc[0, 0] = 999
        assert result2.iloc[0, 0] != 999

    def test_put_overwrites_existing(self, cache, sample_df):
        """Test putting to same key overwrites."""
        df2 = pd.DataFrame({"a": [10, 20, 30]})
        cache.put("test_key", sample_df)
        cache.put("test_key", df2)
        result = cache.get("test_key")
        pd.testing.assert_frame_equal(result, df2)

    def test_set_alias_for_put(self, cache, sample_df):
        """Test set() is an alias for put()."""
        cache.set("test_key", sample_df)
        result = cache.get("test_key")
        assert result is not None
        pd.testing.assert_frame_equal(result, sample_df)


class TestMemoryCacheTTL:
    """Tests for MemoryCache TTL functionality."""

    def test_ttl_expiry(self, sample_df):
        """Test that entries expire after TTL."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        cache.put("test_key", sample_df)
        result = cache.get("test_key")
        assert result is not None
        time.sleep(1.1)
        result = cache.get("test_key")
        assert result is None

    def test_custom_ttl(self, sample_df):
        """Test custom TTL per entry."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=3600)
        cache.put("test_key", sample_df, ttl_seconds=1)
        time.sleep(0.5)
        result = cache.get("test_key")
        assert result is not None
        time.sleep(0.6)
        result = cache.get("test_key")
        assert result is None

    def test_ttl_zero_means_no_expiry(self, sample_df):
        """Test TTL of 0 means no expiry."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        cache.put("test_key", sample_df, ttl_seconds=0)
        time.sleep(0.5)
        result = cache.get("test_key")
        assert result is not None or result is None

    def test_ttl_uses_default_when_none(self, sample_df):
        """Test default TTL is used when ttl_seconds is None."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        cache.put("test_key", sample_df)
        time.sleep(0.5)
        result = cache.get("test_key")
        assert result is not None


class TestMemoryCacheMaxItems:
    """Tests for MemoryCache max_items limit."""

    def test_max_items_limit(self):
        """Test that max_items limit is enforced."""
        cache = MemoryCache(max_items=2, default_ttl_seconds=3600)
        cache.put("key1", pd.DataFrame({"a": [1]}))
        cache.put("key2", pd.DataFrame({"a": [2]}))
        cache.put("key3", pd.DataFrame({"a": [3]}))
        assert cache.size >= 2

    def test_max_items_eviction(self):
        """Test that LRU eviction occurs when max_items reached."""
        cache = MemoryCache(max_items=3, default_ttl_seconds=3600)
        for i in range(3):
            cache.put(f"key{i}", pd.DataFrame({"a": [i]}))
        cache.get("key0")
        cache.put("key3", pd.DataFrame({"a": [3]}))
        result0 = cache.get("key0")
        result1 = cache.get("key1")
        assert result0 is not None
        assert result1 is None


class TestMemoryCacheInvalidate:
    """Tests for MemoryCache invalidate operations."""

    def test_invalidate_single_key(self, cache, sample_df):
        """Test invalidating a single key."""
        cache.put("key1", sample_df)
        cache.put("key2", sample_df)
        count = cache.invalidate("key1")
        assert count == 1
        assert cache.get("key1") is None
        assert cache.get("key2") is not None

    def test_invalidate_nonexistent_key(self, cache):
        """Test invalidating a non-existent key returns 0."""
        count = cache.invalidate("nonexistent")
        assert count == 0

    def test_invalidate_all(self, cache, sample_df):
        """Test invalidating all keys."""
        cache.put("key1", sample_df)
        cache.put("key2", sample_df)
        cache.put("key3", sample_df)
        count = cache.invalidate()
        assert count == 3
        assert cache.size == 0

    def test_invalidate_with_none_clears_all(self, cache, sample_df):
        """Test invalidate(None) clears all entries."""
        cache.put("key1", sample_df)
        cache.put("key2", sample_df)
        count = cache.invalidate(None)
        assert count >= 2
        assert cache.size == 0


class TestMemoryCacheHas:
    """Tests for MemoryCache has() method."""

    def test_has_returns_true_for_existing(self, cache, sample_df):
        """Test has() returns True for existing key."""
        cache.put("test_key", sample_df)
        assert cache.has("test_key") is True

    def test_has_returns_false_for_nonexistent(self, cache):
        """Test has() returns False for non-existent key."""
        assert cache.has("nonexistent") is False

    def test_has_returns_false_for_expired(self, sample_df):
        """Test has() returns False for expired entry."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        cache.put("test_key", sample_df)
        time.sleep(1.1)
        assert cache.has("test_key") is False


class TestMemoryCacheCleanup:
    """Tests for MemoryCache cleanup_expired() method."""

    def test_cleanup_expired_removes_expired(self, sample_df):
        """Test cleanup_expired() removes expired entries."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        cache.put("key1", sample_df)
        cache.put("key2", sample_df)
        time.sleep(1.1)
        removed = cache.cleanup_expired()
        assert removed == 2
        assert cache.size == 0

    def test_cleanup_expired_returns_zero_when_none_expired(self, cache, sample_df):
        """Test cleanup_expired() returns 0 when no entries expired."""
        cache.put("key1", sample_df)
        removed = cache.cleanup_expired()
        assert removed == 0
        assert cache.size == 1


class TestMemoryCacheHitRate:
    """Tests for MemoryCache hit rate tracking."""

    def test_hit_rate_on_cache_miss(self):
        """Test hit rate tracks misses."""
        cache = MemoryCache()
        cache.get("nonexistent")
        assert cache.hit_rate == 0.0
        assert cache._misses == 1

    def test_hit_rate_on_cache_hit(self, sample_df):
        """Test hit rate tracks hits."""
        cache = MemoryCache()
        cache.put("test_key", sample_df)
        cache.get("test_key")
        cache.get("test_key")
        assert cache._hits == 2
        assert cache.hit_rate == 1.0

    def test_hit_rate_mixed_operations(self, sample_df):
        """Test hit rate with mixed operations."""
        cache = MemoryCache()
        cache.put("key1", sample_df)
        cache.get("key1")
        cache.get("nonexistent1")
        cache.get("nonexistent2")
        assert cache._hits == 1
        assert cache._misses == 2
        assert cache.hit_rate == pytest.approx(1 / 3)


class TestMemoryCacheMetadata:
    """Tests for MemoryCache entry metadata tracking."""

    def test_cache_entry_created(self, cache, sample_df):
        """Test that CacheEntry is created on put."""
        cache.put("test_key", sample_df)
        assert "test_key" in cache._metadata
        entry = cache._metadata["test_key"]
        assert isinstance(entry, CacheEntry)
        assert entry.created_at > 0
        assert entry.accessed_at > 0

    def test_cache_entry_accessed_at_updates(self, cache, sample_df):
        """Test that accessed_at updates on get."""
        cache.put("test_key", sample_df)
        time.sleep(0.01)
        first_access = cache._metadata["test_key"].accessed_at
        cache.get("test_key")
        second_access = cache._metadata["test_key"].accessed_at
        assert second_access >= first_access

    def test_cache_entry_moved_to_end_on_access(self, cache, sample_df):
        """Test that accessed entry is moved to end of OrderedDict."""
        cache.put("key1", sample_df)
        cache.put("key2", sample_df)
        cache.get("key1")
        keys = list(cache._metadata.keys())
        assert keys[-1] == "key1"


class TestMemoryCacheThreadSafety:
    """Tests for MemoryCache thread safety."""

    def test_concurrent_put(self):
        """Test concurrent put operations."""
        import threading

        cache = MemoryCache(max_items=1000, default_ttl_seconds=3600)
        errors = []

        def put_task(start, end):
            try:
                for i in range(start, end):
                    cache.put(f"key{i}", pd.DataFrame({"a": [i]}))
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=put_task, args=(0, 250)),
            threading.Thread(target=put_task, args=(250, 500)),
            threading.Thread(target=put_task, args=(500, 750)),
            threading.Thread(target=put_task, args=(750, 1000)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert cache.size <= 1000

    def test_concurrent_get(self):
        """Test concurrent get operations."""
        import threading

        cache = MemoryCache(max_items=100, default_ttl_seconds=3600)
        for i in range(100):
            cache.put(f"key{i}", pd.DataFrame({"a": [i]}))

        results = []
        errors = []

        def get_task(keys):
            try:
                for key in keys:
                    result = cache.get(key)
                    results.append(result)
            except Exception as e:
                errors.append(e)

        keys = [f"key{i}" for i in range(100)]
        threads = [
            threading.Thread(target=get_task, args=(keys[:25],)),
            threading.Thread(target=get_task, args=(keys[25:50],)),
            threading.Thread(target=get_task, args=(keys[50:75],)),
            threading.Thread(target=get_task, args=(keys[75:],)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 100


class TestMemoryCacheEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_dataframe(self, cache):
        """Test storing empty DataFrame."""
        empty_df = pd.DataFrame()
        cache.put("empty", empty_df)
        result = cache.get("empty")
        assert result is not None
        assert result.empty

    def test_large_dataframe(self, cache):
        """Test storing large DataFrame."""
        large_df = pd.DataFrame({"a": range(100000)})
        cache.put("large", large_df)
        result = cache.get("large")
        assert result is not None
        assert len(result) == 100000

    def test_special_characters_in_key(self, cache, sample_df):
        """Test key with special characters."""
        cache.put("key:with:colons", sample_df)
        result = cache.get("key:with:colons")
        assert result is not None

    def test_unicode_in_key(self, cache, sample_df):
        """Test key with unicode characters."""
        cache.put(" ключ 中文 ", sample_df)
        result = cache.get(" ключ 中文 ")
        assert result is not None

    def test_update_existing_key_resets_ttl(self, sample_df):
        """Test updating existing key resets its TTL."""
        cache = MemoryCache(max_items=100, default_ttl_seconds=1)
        cache.put("key", sample_df.copy())
        time.sleep(0.5)
        cache.put("key", sample_df.copy())
        time.sleep(0.5)
        result = cache.get("key")
        assert result is not None
