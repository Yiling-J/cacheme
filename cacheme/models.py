from __future__ import annotations

import datetime
from typing import List, Optional

from typing_extensions import Any, NamedTuple

from cacheme.interfaces import MetaBase, Metrics
from cacheme.utils import cached_property, hash_string


class CachedData(NamedTuple):
    data: Any
    updated_at: datetime.datetime
    expire: Optional[datetime.datetime] = None


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

    @property
    def _full_key(self) -> str:
        return f"cacheme:{self.key()}:{self.Meta.version}"

    @cached_property
    def _keyh(self) -> int:
        return hash_string(self._full_key)

    def tags(self) -> List[str]:
        raise NotImplementedError()

    class Meta(MetaBase.Meta):
        metrics = Metrics()
        ttl = None
        local_cache = None
        doorkeeper = None
        version = ""


class TagNode(Node):
    def __init__(self, tag: str):
        self.tag = tag

    @property
    def _full_key(self) -> str:
        return f"cacheme:tags:{self.tag}"
