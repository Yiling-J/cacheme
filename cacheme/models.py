from __future__ import annotations

import asyncio
from datetime import timedelta
from time import time_ns
from typing import (
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from typing_extensions import Any

from cacheme.data import get_storage_by_name
from cacheme.interfaces import DoorKeeper, Metrics, Serializer, Storage
from cacheme.interfaces import Node as NodeP

_nodes: List[Type[Node]] = []
_prefix: str = "cacheme"

sentinel = object()
C = TypeVar("C")


def get_nodes():
    return _nodes


def _add_node(node: Type[Node]):
    _nodes.append(node)


def set_prefix(prefix: str):
    global _prefix
    _prefix = prefix


class Cache:
    __slots__ = ["_storage", "_storage_name", "ttl", "_is_local"]

    def __init__(self, storage: str, ttl: Optional[timedelta]):
        self._storage: Optional[Storage] = None
        self._storage_name: str = storage
        self.ttl: Optional[timedelta] = ttl
        self._is_local: Optional[bool] = None

    @property
    def is_local(self):
        if self._is_local is None:
            self._is_local = self.storage.is_local()
        return self._is_local

    @property
    def storage(self) -> Storage:
        if self._storage is None:
            self._storage = get_storage_by_name(self._storage_name)
        return cast(Storage, self._storage)


class MetaNode(type):
    def __new__(cls, name, bases, dct):
        new = super().__new__(cls, name, bases, dct)
        if len(new.Meta.caches) > 0:
            _nodes.append(cast(Type[Node], cls))
            new.Meta.metrics = Metrics()
        return new

    class Meta:
        metrics: ClassVar[Metrics]
        storage: ClassVar[str] = ""
        caches: List = []


class Node(Generic[C], metaclass=MetaNode):
    _full_key = None

    def key(self) -> str:
        raise NotImplementedError()

    def full_key(self) -> str:
        if self._full_key is None:
            self._full_key = f"{_prefix}:{self.key()}:{self.Meta.version}"
        return self._full_key

    async def load(self) -> C:
        raise NotImplementedError()

    @classmethod
    async def load_all(cls, nodes: Sequence[NodeP]) -> Sequence[Tuple[NodeP, Any]]:
        data = []
        for node in nodes:
            v = await node.load()
            data.append((node, v))
        return data

    def get_version(self) -> str:
        return self.Meta.version

    def get_caches(self) -> List[Cache]:
        return self.Meta.caches

    def get_seriaizer(self) -> Optional[Serializer]:
        return self.Meta.serializer

    def get_doorkeeper(self) -> Optional[DoorKeeper]:
        return self.Meta.doorkeeper

    @classmethod
    def get_metrics(cls) -> Metrics:
        return cls.Meta.metrics

    class Meta:
        version: ClassVar[str] = ""
        caches: List[Cache] = []
        serializer: ClassVar[Optional[Serializer]] = None
        doorkeeper: ClassVar[Optional[DoorKeeper]] = None
        metrics: ClassVar[Metrics]


class DynamicNode(Node):
    key_str: str

    def __init__(self, key: str):
        super().__init__()
        self.key_str = key

    def key(self) -> str:
        return self.key_str


# https://github.com/python/cpython/issues/90780
# use event to protect from thundering herd
class CachedAwaitable:
    def __init__(self, awaitable, metrics: Metrics):
        self.awaitable = awaitable
        self.event: Optional[asyncio.Event] = None
        self.result = sentinel
        self.metrics = metrics

    def __await__(self):
        if self.result is not sentinel:
            self.metrics._hit_count += 1
            return self.result

        if self.event is None:
            self.metrics._miss_count += 1
            self.event = asyncio.Event()
            now = time_ns()
            try:
                result = yield from self.awaitable.__await__()
            except Exception as e:
                self.metrics._load_failure_count += 1
                self.metrics._total_load_time += time_ns() - now
                raise (e)
            self.metrics._load_success_count += 1
            self.metrics._total_load_time += time_ns() - now
            self.result = result
            self.event.set()
            self.event = None
            return result
        else:
            self.metrics._hit_count += 1
            yield from self.event.wait().__await__()
        return self.result


class Fetcher:
    def __init__(self):
        self.data: Dict[str, Any] = {}
