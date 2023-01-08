from __future__ import annotations

import datetime
from typing import List, Optional, Sequence, ClassVar

from typing_extensions import Any

from cacheme.interfaces import Cachable, Metrics, Serializer, Storage
from cacheme.filter import BloomFilter
from cacheme.utils import cached_property, hash_string
from cacheme.data import get_storage_by_name


class Item:
    key: str
    value: Any
    list_id: Optional[int]
    expire: Optional[datetime.datetime] = None
    updated_at: datetime.datetime

    def __init__(
        self,
        key: str,
        value: Any,
        ttl: Optional[datetime.timedelta],
        list_id: int | None = None,
    ):
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)
        if ttl is not None:
            self.expire = datetime.datetime.now(datetime.timezone.utc) + ttl
        self.key = key
        self.value = value
        self.list_id = list_id

    @cached_property
    def keyh(self) -> int:
        return hash_string(self.key)


class Element:
    prev: Optional[Element]
    next: Optional[Element]
    list: Any
    item: Item

    def __init__(self, item: Item):
        self.item = item

    @property
    def keyh(self) -> int:
        return self.item.keyh


_nodes = []


class MetaNode(type):
    def __new__(cls, name, bases, dct):
        new = super().__new__(cls, name, bases, dct)
        internal = getattr(new.Meta, "internal", False)
        if internal == False:
            _nodes.append(cls)
        return new

    class Meta:
        ...


class Node(metaclass=MetaNode):
    def key(self) -> str:
        raise NotImplementedError()

    def full_key(self) -> str:
        return f"cacheme:{self.key()}:{self.Meta.version}"

    def key_hash(self) -> int:
        return self._keyh

    @cached_property
    def _keyh(self) -> int:
        return hash_string(self.full_key())

    def tags(self) -> List[str]:
        raise NotImplementedError()

    async def load(self):
        raise NotImplementedError()

    @classmethod
    async def load_all(cls, nodes: Sequence[Cachable]) -> Any:
        data = []
        for node in nodes:
            v = await node.load()
            data.append((node, v))
        return data

    def get_version(self) -> str:
        return self.Meta.version

    def get_stroage(self) -> Storage:
        return get_storage_by_name(self.Meta.storage)

    def get_ttl(self) -> Optional[datetime.timedelta]:
        return self.Meta.ttl

    def get_local_cache(self) -> Optional[Storage]:
        if self.Meta.local_cache is None:
            return None
        return get_storage_by_name(self.Meta.local_cache)

    def get_seriaizer(self) -> Optional[Serializer]:
        return self.Meta.serializer

    def get_doorkeeper(self) -> Optional[BloomFilter]:
        return self.Meta.doorkeeper

    def get_metrics(self) -> Metrics:
        return self.Meta.metrics

    class Meta:
        version: ClassVar[str] = ""
        storage: ClassVar[str] = ""
        ttl: ClassVar[Optional[datetime.timedelta]] = None
        local_cache: ClassVar[Optional[str]] = None
        serializer: ClassVar[Optional[Serializer]] = None
        doorkeeper: ClassVar[Optional[BloomFilter]] = None
        metrics: ClassVar[Metrics] = Metrics()


class TagNode(Node):
    def __init__(self, tag: str):
        self.tag = tag

    @property
    def _full_key(self) -> str:
        return f"cacheme:tags:{self.tag}"
