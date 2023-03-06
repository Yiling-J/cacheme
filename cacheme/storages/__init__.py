import importlib
from datetime import timedelta
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import urlparse

from cacheme.interfaces import Node
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage


class Storage:
    SUPPORTED_STORAGES = {
        "local": "cacheme.storages.local:LocalStorage",
        "redis": "cacheme.storages.redis:RedisStorage",
        "sqlite": "cacheme.storages.sqlite:SQLiteStorage",
        "mongodb": "cacheme.storages.mongo:MongoStorage",
        "postgresql": "cacheme.storages.postgres:PostgresStorage",
        "mysql": "cacheme.storages.mysql:MySQLStorage",
        "sqlite": "cacheme.storages.sqlite:SQLiteStorage",
    }

    def __init__(self, url: str, **options: Any):
        u = urlparse(url)
        self._scheme = u.scheme
        self._is_local = True if self._scheme == "local" else False

        name = self.SUPPORTED_STORAGES.get(u.scheme)
        if name is None:
            raise Exception(f"storage:{u.scheme} not found")
        storage_cls = self.__import(name)
        assert issubclass(storage_cls, BaseStorage)
        self._storage = storage_cls(address=url, **options)

    def scheme(self) -> str:
        return self._scheme

    def is_local(self) -> bool:
        return self._is_local

    def __import(self, name: str) -> Any:
        mod_name, attr_name = name.rsplit(":", 1)
        module = importlib.import_module(mod_name)
        return getattr(module, attr_name)

    async def connect(self):
        await self._storage.connect()

    async def get(self, node: Node, serializer: Optional[Serializer]) -> Any:
        return await self._storage.get(node, serializer)

    async def get_all(
        self, nodes: Sequence[Node], serializer: Optional[Serializer]
    ) -> Sequence[Tuple[Node, Any]]:
        return await self._storage.get_all(nodes, serializer)

    async def set(
        self,
        node: Node,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        return await self._storage.set(node, value, ttl, serializer)

    async def remove(self, node: Node):
        return await self._storage.remove(node)

    async def set_all(
        self,
        data: Sequence[Tuple[Node, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        return await self._storage.set_all(data, ttl, serializer)

    async def close(self):
        return await self._storage.close()

    # local storage only
    def get_sync(self, node: Node, serializer: Optional[Serializer]) -> Any:
        return self._storage.get_sync(node, serializer)

    # local storage only
    def get_all_sync(
        self, nodes: Sequence[Node], serializer: Optional[Serializer]
    ) -> Sequence[Tuple[Node, Any]]:
        return self._storage.get_all_sync(nodes, serializer)
