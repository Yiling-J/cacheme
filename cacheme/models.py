from __future__ import annotations
from typing import Optional, List, cast
from typing_extensions import Any, NamedTuple
from cacheme.utils import hash_string, cached_property
from dataclasses import dataclass
from cacheme.interfaces import Metrics, MetaBase
import datetime


@dataclass
class CacheKey:
    node: str
    prefix: str
    key: str
    version: str
    tags: List[str]
    metrics: Optional[Metrics] = None

    @property
    def full_key(self) -> str:
        return f"{self.prefix}:{self.key}:{self.version}"

    @cached_property
    def hash(self) -> int:
        return hash_string(self.full_key)


class CachedData(NamedTuple):
    data: Any
    updated_at: datetime.datetime
    expire: Optional[datetime.datetime] = None


class Item:
    key: CacheKey
    value: Any
    list_id: Optional[int]
    expire: Optional[datetime.datetime] = None
    updated_at: datetime.datetime

    def __init__(
        self,
        key: CacheKey,
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


class Element:
    prev: Optional[Element]
    next: Optional[Element]
    list: Any
    item: Item

    def __init__(self, item: Item):
        self.item = item

    @property
    def keyh(self) -> int:
        return cast(int, self.item.key.hash)


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
    class Meta(MetaBase.Meta):
        metrics = Metrics()
