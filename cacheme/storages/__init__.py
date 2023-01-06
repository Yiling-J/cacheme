import importlib
from typing import Any, Optional, List, Sequence, Tuple
from urllib.parse import urlparse
from cacheme.interfaces import BaseNode
from cacheme.storages.base import BaseStorage
from cacheme.models import CachedData
from cacheme.serializer import Serializer
from datetime import timedelta, datetime


class Storage:
    SUPPORTED_STORAGES = {
        "tlfu": "cacheme.storages.local:TLFUStorage",
        "redis": "cacheme.storages.redis:RedisStorage",
        "sqlite": "cacheme.storages.sqlite:SQLiteStorage",
        "mongodb": "cacheme.storages.mongo:MongoStorage",
        "postgresql": "cacheme.storages.postgres:PostgresStorage",
        "mysql": "cacheme.storages.mysql:MySQLStorage",
        "sqlite": "cacheme.storages.sqlite:SQLiteStorage",
    }

    def __init__(self, url: str, **options: Any):
        u = urlparse(url)
        name = self.SUPPORTED_STORAGES.get(u.scheme)
        if name is None:
            raise
        storage_cls = self.__import(name)
        assert issubclass(storage_cls, BaseStorage)
        self._storage = storage_cls(address=url, **options)

    def __import(self, name: str) -> Any:
        mod_name, attr_name = name.rsplit(":", 1)
        module = importlib.import_module(mod_name)
        return getattr(module, attr_name)

    async def connect(self):
        await self._storage.connect()

    async def get(
        self, node: BaseNode, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        return await self._storage.get(node, serializer)

    async def get_all(
        self, nodes: Sequence[BaseNode], serializer: Optional[Serializer]
    ) -> Sequence[Tuple[BaseNode, CachedData]]:
        return await self._storage.get_all(nodes, serializer)

    async def set(
        self,
        node: BaseNode,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        return await self._storage.set(node, value, ttl, serializer)

    async def remove(self, node: BaseNode):
        return await self._storage.remove(node)

    async def validate_tags(self, updated_at: datetime, tags: List[str]) -> bool:
        return await self._storage.validate_tags(updated_at, tags)

    async def set_all(
        self,
        data: Sequence[Tuple[BaseNode, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        return await self._storage.set_all(data, ttl, serializer)


tag_storage: Optional[Storage] = None


def get_tag_storage() -> Storage:
    global tag_storage
    if tag_storage is None:
        raise Exception()
    return tag_storage


def set_tag_storage(storage: Storage):
    global tag_storage
    tag_storage = storage
