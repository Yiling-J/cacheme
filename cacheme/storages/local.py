from datetime import timedelta
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import urlparse

from theine import Cache

from cacheme.interfaces import Cachable
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage
from cacheme.models import sentinel


class LocalStorage(BaseStorage):
    def __init__(self, size: int, address: str, **options):
        policy_name = urlparse(address).netloc
        self.cache: Cache = Cache(policy_name, size)

    async def connect(self):
        return

    async def get(self, node: Cachable, serializer: Optional[Serializer]) -> Any:
        return self.cache.get(node.full_key(), sentinel)

    async def set(
        self,
        node: Cachable,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        self.cache.set(node.full_key(), value, ttl)

    async def remove(self, node: Cachable):
        self.cache.delete(node.full_key())

    async def get_all(
        self,
        nodes: Sequence[Cachable],
        serializer: Optional[Serializer],
    ) -> Sequence[Tuple[Cachable, Any]]:
        if len(nodes) == 0:
            return []
        results = []
        for node in nodes:
            v = self.cache.get(node.full_key(), sentinel)
            if v != sentinel:
                results.append((node, v))
        return results

    async def set_all(
        self,
        data: Sequence[Tuple[Cachable, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        for node, value in data:
            self.cache.set(node.full_key(), value, ttl)
