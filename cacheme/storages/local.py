from datetime import timedelta
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import urlparse

from theine import Cache

from cacheme.interfaces import Node
from cacheme.models import sentinel
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(self, size: int, address: str, **options):
        policy_name = urlparse(address).netloc
        self.cache: Cache = Cache(policy_name, size)

    async def connect(self):
        return

    async def get(self, node: Node, serializer: Optional[Serializer]) -> Any:
        return self.cache.get(node.full_key(), sentinel)

    def get_sync(self, node: Node, serializer: Optional[Serializer]) -> Any:
        return self.cache.get(node.full_key(), sentinel)

    async def set(
        self,
        node: Node,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        self.cache.set(node.full_key(), value, ttl)

    async def remove(self, node: Node):
        self.cache.delete(node.full_key())

    async def get_all(
        self,
        nodes: Sequence[Node],
        serializer: Optional[Serializer],
    ) -> Sequence[Tuple[Node, Any]]:
        if len(nodes) == 0:
            return []
        results = []
        for node in nodes:
            v = self.cache.get(node.full_key(), sentinel)
            if v != sentinel:
                results.append((node, v))
        return results

    def get_all_sync(
        self,
        nodes: Sequence[Node],
        serializer: Optional[Serializer],
    ) -> Sequence[Tuple[Node, Any]]:
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
        data: Sequence[Tuple[Node, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        for node, value in data:
            self.cache.set(node.full_key(), value, ttl)
