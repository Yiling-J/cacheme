from __future__ import annotations
from typing import Callable, Optional, Generic
from typing_extensions import TypeVar, Any, ParamSpec
from asyncio import Task, create_task
from functools import cached_property
from cacheme.v2.utils import hash_string
from dataclasses import dataclass
from cacheme.v2.interfaces import CacheNode, MemoNode
import structlog
import datetime


logger = structlog.getLogger(__name__)

C_co = TypeVar("C_co")

T = TypeVar("T", bound=MemoNode)
P = ParamSpec("P")


def get_many(nodes: list[CacheNode[C_co]]) -> list[C_co]:
    results: list[C_co] = []
    for node in nodes:
        results.append(node.load())
    return results


class Wrapper(Generic[P, T]):
    def __init__(self, fn: Callable[P, Any], node: type[T]):
        self.func = fn
        self.tasks: dict[str, Task] = {}

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        node = self.key_func(*args, **kwargs)
        key = node.key()
        tmp = create_task(self.func(*args, **kwargs))
        tmp.add_done_callback(lambda task: self.tasks.pop(key))
        task = self.tasks.setdefault(key, tmp)
        await task
        return task.result()

    def to_node(self, fn: Callable[P, T]) -> Wrapper:
        self.key_func = fn
        return self


class Memoize(Generic[T]):
    def __init__(self, node: type[T]):
        version = node.Meta.version
        self.node = node

    def __call__(self, fn: Callable[P, Any]) -> Wrapper[P, T]:
        self.func = fn
        return Wrapper(fn, self.node)


@dataclass
class CacheKey:
    node: str
    prefix: str
    key: str
    version: str
    tags: list[str]

    @property
    def full_key(self) -> str:
        return f"{self.prefix}:{self.key}:{self.version}"

    @cached_property
    def hash(self) -> int:
        return hash_string(self.full_key)

    def log(self, msg: str):
        logger.debug(msg, key=self.full_key, node=self.node)


class Item:
    key: CacheKey
    value: Any
    list_id: int | None
    expire: datetime.datetime

    def __init__(
        self,
        key: CacheKey,
        value: Any,
        ttl: datetime.timedelta,
        list_id: int | None = None,
    ):
        self.expire = datetime.datetime.now() + ttl
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
        return self.item.key.hash


_nodes = []


class MetaNode(type):
    def __new__(cls, name, bases, dct):
        new = super().__new__(cls, name, bases, dct)
        internal = getattr(new.Meta, "internal", False)
        if internal == False:
            print("new", new, new.__name__)
            _nodes.append(cls)
        return new

    class Meta:
        ...


class Node(metaclass=MetaNode):
    class Meta:
        internal = True
