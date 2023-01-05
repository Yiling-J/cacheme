from datetime import timedelta
from typing import Any, Optional
from cacheme.interfaces import BaseNode

from cacheme.models import CachedData
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage
from cacheme.tinylfu import tinylfu


class TLFUStorage(BaseStorage):
    def __init__(self, size: int, **options):
        self.cache = tinylfu.Cache(size)

    async def connect(self):
        return

    async def get(
        self, node: BaseNode, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        return self.cache.get(node._full_key)

    async def set(
        self,
        node: BaseNode,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        evicated = self.cache.set(node._full_key, value, ttl)
        if evicated and node.Meta.metrics is not None:
            node.Meta.metrics.eviction_count += 1
        return

    async def remove(self, node: BaseNode):
        self.cache.remove(node._full_key)
