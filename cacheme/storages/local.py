from datetime import timedelta
from typing import Any, Optional, Sequence, Tuple, List
from cacheme.interfaces import Cachable, CachedData

from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage
from cacheme.tinylfu import tinylfu


class TLFUStorage(BaseStorage):
    def __init__(self, size: int, **options):
        self.cache = tinylfu.Cache(size)

    async def connect(self):
        return

    async def get(
        self, node: Cachable, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        return self._sync_get(node, serializer)

    def _sync_get(
        self, node: Cachable, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        e = self.cache.get(node.full_key())
        if e is None:
            return None
        return CachedData(node=node, data=e.item.value, updated_at=e.item.updated_at)

    async def set(
        self,
        node: Cachable,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        evicated = self.cache.set(node.full_key(), value, ttl)
        metrics = node.get_metrics()
        if evicated and metrics is not None:
            metrics.eviction_count += 1
        return

    async def remove(self, node: Cachable):
        self.cache.remove(node.full_key())

    async def get_all(
        self,
        nodes: Sequence[Cachable],
        serializer: Optional[Serializer],
        fields: List[str] = [],
    ) -> Sequence[Tuple[Cachable, CachedData]]:
        data = []
        for node in nodes:
            v = self._sync_get(node, serializer)
            if v is not None:
                data.append((node, v))
        return data

    async def set_all(
        self,
        data: Sequence[Tuple[Cachable, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        for node, value in data:
            await self.set(node, value, ttl, serializer)
