from datetime import timedelta
from typing import Any, Optional

from cacheme.models import CachedData, CacheKey
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage
from cacheme.tinylfu import tinylfu


class TLFUStorage(BaseStorage):
    def __init__(self, size: int):
        self.cache = tinylfu.Cache(size)

    async def connect(self):
        return

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        return self.cache.get(key)

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        evicated = self.cache.set(key, value, ttl)
        if evicated and key.metrics is not None:
            key.metrics.eviction_count += 1
        return

    async def remove(self, key: CacheKey):
        self.cache.remove(key)
