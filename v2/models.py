from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, TypeVar, ParamSpec, Any, Generic, Protocol, cast, Optional
from asyncio import Event
from storage import Storage, CacheKey, log, get_tag_storage, set_tag_storage
from serializer import Serializer
from datetime import datetime, timedelta


from localcache import LocalCache


C_co = TypeVar("C_co", covariant=True)
S = TypeVar("S", bound=Serializer)

_storages: dict[str, Storage] = {}


async def init_storages(storages: dict[str, Storage]):
    global _storages
    _storages = storages
    for v in storages.values():
        await v.connect()


async def init_tag_storage(storage: Storage):
    await storage.connect()
    set_tag_storage(storage)


class MemoNode(Protocol):
    def key(self) -> str:
        ...

    def tags(self) -> list[str]:
        ...

    class Meta(Protocol[S]):
        version: str
        storage: str
        ttl: timedelta
        local_cache: LocalCache
        serializer: S


class CacheNode(Protocol[C_co]):
    def key(self) -> str:
        ...

    def fetch(self) -> C_co:
        ...

    def tags(self) -> list[str]:
        ...

    class Meta(Protocol[S]):
        version: str
        storage: str
        ttl: timedelta
        local_cache: LocalCache
        serializer: S


T = TypeVar("T", bound=MemoNode)
P = ParamSpec("P")


async def get(node: CacheNode[C_co]) -> C_co:
    storage = _storages[node.Meta.storage]
    cache_key = CacheKey(
        node=node.__class__.__name__,
        prefix="cacheme",
        key=node.key(),
        version=node.Meta.version,
        tags=node.tags(),
    )
    if node.Meta.local_cache.enable:
        result = node.Meta.local_cache.get(cache_key)
        if result != None:
            log("local cache hit", cache_key)
            return result
    raw = await storage.get(cache_key)
    if raw == None:
        log("cache miss", cache_key)
        result = node.fetch()
        b = node.Meta.serializer.dumps(result)
        await storage.set(cache_key, b, node.Meta.ttl)
    else:
        log("cache hit", cache_key)
        result = node.Meta.serializer.loads(raw)
    if node.Meta.local_cache.enable:
        node.Meta.local_cache.set(cache_key, result)
        log("local cache set", cache_key)
    return cast(C_co, result)


def get_many(nodes: list[CacheNode[C_co]]) -> list[C_co]:
    results: list[C_co] = []
    for node in nodes:
        results.append(node.fetch())
    return results


async def invalid_tag(tag: str):
    storage = get_tag_storage()
    await storage.invalid_tag(tag)


class Locker:
    event: Event
    value: Any

    def __init__(self):
        self.event = Event()
        self.value = None


class Wrapper(Generic[P, T]):
    def __init__(self, fn: Callable[P, Any], node: type[T]):
        self.func = fn
        self.lockers: dict[str, Locker] = {}

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        node = self.key_func(*args, **kwargs)
        key = node.key()
        locker = self.lockers.get(key, None)
        if locker != None:
            await locker.event.wait()
            return locker.value
        else:
            locker = Locker()
            self.lockers[key] = locker
            result = await self.func(*args, **kwargs)
            locker.value = result
            self.lockers.pop(key)
            locker.event.set()
            return result

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
