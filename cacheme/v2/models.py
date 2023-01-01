from __future__ import annotations
from typing import Optional, List, cast
from typing_extensions import Any, NamedTuple
from cacheme.v2.utils import hash_string, cached_property
from dataclasses import dataclass
import structlog
import datetime


logger = structlog.getLogger(__name__)


@dataclass
class CacheKey:
    node: str
    prefix: str
    key: str
    version: str
    tags: List[str]

    @property
    def full_key(self) -> str:
        return f"{self.prefix}:{self.key}:{self.version}"

    @cached_property
    def hash(self) -> int:
        return hash_string(self.full_key)

    def log(self, msg: str):
        logger.debug(msg, key=self.full_key, node=self.node)


class CachedData(NamedTuple):
    data: Any
    updated_at: datetime.datetime


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
        if ttl != None:
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
        internal = getattr(new, "internal", False)
        if internal == False:
            _nodes.append(cls)
        return new

    class Meta:
        ...


class Node(metaclass=MetaNode):
    internal = True

    class Meta:
        ttl = None
        local_cache = None
        doorkeeper = None
        record_stats = False
