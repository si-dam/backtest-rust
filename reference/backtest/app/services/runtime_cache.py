from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class TTLCache(Generic[K, V]):
    """Small in-process TTL cache for hot interactive paths."""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = max(0.0, float(ttl_seconds))
        self._max_entries = max(1, int(max_entries))
        self._values: OrderedDict[K, tuple[float, V]] = OrderedDict()
        self._lock = threading.RLock()

    def _evict_expired_unlocked(self, now: float) -> None:
        expired = [key for key, (expires_at, _) in self._values.items() if expires_at <= now]
        for key in expired:
            self._values.pop(key, None)

    def _evict_overflow_unlocked(self) -> None:
        while len(self._values) > self._max_entries:
            self._values.popitem(last=False)

    def get(self, key: K) -> V | None:
        now = time.monotonic()
        with self._lock:
            self._evict_expired_unlocked(now)
            row = self._values.get(key)
            if row is None:
                return None
            expires_at, value = row
            if expires_at <= now:
                self._values.pop(key, None)
                return None
            self._values.move_to_end(key)
            return value

    def set(self, key: K, value: V) -> None:
        expires_at = time.monotonic() + self._ttl_seconds
        with self._lock:
            self._values[key] = (expires_at, value)
            self._values.move_to_end(key)
            self._evict_overflow_unlocked()

    def clear(self) -> None:
        with self._lock:
            self._values.clear()

    def invalidate(self, predicate) -> int:
        """Invalidate entries for which predicate(key) is truthy."""
        removed = 0
        with self._lock:
            for key in list(self._values.keys()):
                if predicate(key):
                    self._values.pop(key, None)
                    removed += 1
        return removed
