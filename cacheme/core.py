import types
from time import time_ns
from datetime import timezone, datetime
from cacheme.serializer import MsgPackSerializer
from cacheme.storages.interfaces import Storage
from cacheme.models import CacheKey, CachedData
from typing import (
    cast,
    Callable,
    Generic,
    Dict,
    Any,
    overload,
    Type,
    TypeVar,
    Awaitable,
)
from typing_extensions import ParamSpec, Self
from cacheme.interfaces import CacheNode, MemoNode
from cacheme.storages.base import get_tag_storage, set_tag_storage
from asyncio import Lock


C_co = TypeVar("C_co", covariant=True)
P = ParamSpec("P")
R = TypeVar("R")


_storages: Dict[str, Storage] = {}


class Locker:
    lock: Lock
    value: Any

    def __init__(self):
        self.lock = Lock()
        self.value = None


_lockers: Dict[str, Locker] = {}


# local storage(if enable) -> storage -> cache miss, load from source
async def get(node: CacheNode[C_co]) -> C_co:
    storage = _storages[node.Meta.storage]
    metrics = node.Meta.metrics
    cache_key = CacheKey(
        node=node.__class__.__name__,
        prefix="cacheme",
        key=node.key(),
        version=node.Meta.version,
        tags=node.tags(),
        metrics=metrics,
    )
    result = None
    if node.Meta.local_cache is not None:
        local_storage = _storages[node.Meta.local_cache]
        result = await local_storage.get(cache_key, None)
    if result is None:
        result = await storage.get(cache_key, node.Meta.serializer)
    # get result from cache, check tags
    if result is not None and len(node.tags()) > 0:
        tag_storage = get_tag_storage()
        valid = await tag_storage.validate_tags(
            result.updated_at,
            [
                CacheKey(
                    node="__TAG__",
                    prefix="cacheme",
                    key=tag,
                    version="",
                    tags=[],
                )
                for tag in node.tags()
            ],
        )
        if not valid:
            await storage.remove(cache_key)
            result = None
    if result is None:
        metrics.miss_count += 1
        locker = _lockers.setdefault(cache_key.full_key, Locker())
        async with locker.lock:
            if locker.value is None:
                now = time_ns()
                try:
                    loaded = await node.load()
                except Exception as e:
                    metrics.load_failure_count += 1
                    metrics.total_load_time += time_ns() - now
                    raise (e)
                locker.value = loaded
                metrics.load_success_count += 1
                metrics.total_load_time += time_ns() - now
                result = CachedData(data=loaded, updated_at=datetime.now(timezone.utc))
                if node.Meta.doorkeeper is not None:
                    exist = node.Meta.doorkeeper.set(cache_key.hash)
                    if not exist:
                        return cast(C_co, result)
                await storage.set(
                    cache_key, loaded, node.Meta.ttl, node.Meta.serializer
                )
                if node.Meta.local_cache is not None:
                    local_storage = _storages[node.Meta.local_cache]
                    await local_storage.set(cache_key, loaded, node.Meta.ttl, None)
                _lockers.pop(cache_key.full_key, None)
            else:
                result = CachedData(
                    data=locker.value, updated_at=datetime.now(timezone.utc)
                )
    else:
        metrics.hit_count += 1

    return cast(C_co, result.data)


async def init_storages(storages: Dict[str, Storage]):
    global _storages
    _storages = storages
    for v in storages.values():
        await v.connect()


async def init_tag_storage(storage: Storage):
    await storage.connect()
    set_tag_storage(storage)


async def invalid_tag(tag: str):
    storage = get_tag_storage()
    cache_key = CacheKey(
        node="__TAG__",
        prefix="cacheme",
        key=tag,
        version="",
        tags=[],
    )
    await storage.set(cache_key, None, ttl=None, serializer=MsgPackSerializer())


class Wrapper(Generic[P, R]):
    def __init__(self, fn: Callable[P, Awaitable[R]], node: Type[MemoNode]):
        self.func = fn

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        node = self.key_func(*args, **kwargs)
        node = cast(CacheNode, node)

        # inline load function
        async def load() -> Any:
            return await self.func(*args, **kwargs)

        node.load = load  # type: ignore
        return await get(node)

    def to_node(self, fn: Callable[P, MemoNode]) -> Self:  # type: ignore
        self.key_func = fn
        return self

    @overload
    def __get__(self, instance, owner) -> Callable[..., R]:
        ...

    @overload
    def __get__(self, instance, owner) -> Self:  # type: ignore
        ...

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return cast(Callable[..., R], types.MethodType(self, instance))


class Memoize:
    def __init__(self, node: Type[MemoNode]):
        version = node.Meta.version
        self.node = node

    def __call__(self, fn: Callable[P, Awaitable[R]]) -> Wrapper[P, R]:
        return Wrapper(fn, self.node)
