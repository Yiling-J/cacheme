from cacheme.v2.models import CacheKey, CachedData
from cacheme.v2.serializer import Serializer
from cacheme.v2.storages.base import BaseStorage
from cacheme.v2.tinylfu import tinylfu
from typing import Optional, Any
from datetime import timedelta


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
