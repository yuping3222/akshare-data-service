import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

from cachetools import TTLCache

import pandas as pd


@dataclass
class CacheEntry:
    data: pd.DataFrame
    expire_at: float | None
    accessed_at: float
    created_at: float


class MemoryCache:
    def __init__(self, max_items: int = 5000, default_ttl_seconds: int = 3600):
        self._max_items = max_items
        self._default_ttl_seconds = default_ttl_seconds
        self._ttl_cache = TTLCache(maxsize=max_items, ttl=default_ttl_seconds)
        self._metadata: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> pd.DataFrame | None:
        with self._lock:
            if key not in self._ttl_cache:
                self._misses += 1
                return None

            data = self._ttl_cache[key]
            if key in self._metadata:
                entry = self._metadata[key]
                if entry.expire_at is not None and time.time() > entry.expire_at:
                    if key in self._ttl_cache:
                        del self._ttl_cache[key]
                    if key in self._metadata:
                        del self._metadata[key]
                    self._misses += 1
                    return None
                self._metadata.move_to_end(key)
                entry.accessed_at = time.time()
            self._hits += 1
            return data.copy() if data is not None else None

    def put(
        self, key: str, value: pd.DataFrame, ttl_seconds: int | None = None
    ) -> None:
        with self._lock:
            if ttl_seconds is None:
                ttl_seconds = self._default_ttl_seconds

            now = time.time()
            expire_at = None if ttl_seconds == 0 else now + ttl_seconds

            self._ttl_cache[key] = value.copy()
            if key in self._metadata:
                self._metadata.move_to_end(key)
                self._metadata[key] = CacheEntry(
                    data=value.copy(),
                    expire_at=expire_at,
                    accessed_at=now,
                    created_at=self._metadata[key].created_at,
                )
            else:
                self._metadata[key] = CacheEntry(
                    data=value.copy(),
                    expire_at=expire_at,
                    accessed_at=now,
                    created_at=now,
                )

    def set(
        self, key: str, value: pd.DataFrame, ttl_seconds: int | None = None
    ) -> None:
        self.put(key, value, ttl_seconds)

    def invalidate(self, key: str | None = None) -> int:
        with self._lock:
            if key is None:
                count = len(self._ttl_cache)
                self._ttl_cache.clear()
                self._metadata.clear()
                return count

            if key in self._ttl_cache:
                del self._ttl_cache[key]
                if key in self._metadata:
                    del self._metadata[key]
                return 1
            return 0

    def has(self, key: str) -> bool:
        with self._lock:
            if key not in self._ttl_cache:
                return False

            if key in self._metadata:
                entry = self._metadata[key]
                if entry.expire_at is not None and time.time() > entry.expire_at:
                    if key in self._ttl_cache:
                        del self._ttl_cache[key]
                    if key in self._metadata:
                        del self._metadata[key]
                    return False
            return True

    def cleanup_expired(self) -> int:
        with self._lock:
            now = time.time()
            expired_keys = [
                k
                for k, v in self._metadata.items()
                if v.expire_at is not None and now > v.expire_at
            ]
            for key in expired_keys:
                if key in self._ttl_cache:
                    del self._ttl_cache[key]
                if key in self._metadata:
                    del self._metadata[key]
            return len(expired_keys)

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._metadata)

    @property
    def hit_rate(self) -> float:
        with self._lock:
            total = self._hits + self._misses
            if total == 0:
                return 0.0
            return self._hits / total


__all__ = ["MemoryCache"]
