import types
from datetime import timezone, datetime
from cacheme.v2.serializer import MsgPackSerializer
from cacheme.v2.storage import Storage
from cacheme.v2.models import CacheKey, CachedData
from typing import (
    cast,
    Callable,
    Generic,
    Dict,
    Any,
    overload,
    Type,
)
from typing_extensions import TypeVar, ParamSpec, Self
from cacheme.v2.interfaces import CacheNode, MemoNode
from cacheme.v2.storage import get_tag_storage, set_tag_storage
from asyncio import Lock


C_co = TypeVar("C_co", covariant=True)
T = TypeVar("T", bound=MemoNode)
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
    cache_key = CacheKey(
        node=node.__class__.__name__,
        prefix="cacheme",
        key=node.key(),
        version=node.Meta.version,
        tags=node.tags(),
    )
    result = None
    if node.Meta.local_cache != None:
        local_storage = _storages[node.Meta.local_cache]
        result = await local_storage.get(cache_key, None)
    if result == None:
        result = await storage.get(cache_key, node.Meta.serializer)
    # get result from cache, check tags
    if result != None and len(node.tags()) > 0:
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
    if result == None:
        locker = _lockers.setdefault(cache_key.full_key, Locker())
        async with locker.lock:
            if locker.value == None:
                loaded = await node.load()
                locker.value = loaded
                result = CachedData(data=loaded, updated_at=datetime.now(timezone.utc))
                if node.Meta.doorkeeper != None:
                    exist = node.Meta.doorkeeper.set(cache_key.hash)
                    if not exist:
                        return cast(C_co, result)
                await storage.set(
                    cache_key, loaded, node.Meta.ttl, node.Meta.serializer
                )
                if node.Meta.local_cache != None:
                    local_storage = _storages[node.Meta.local_cache]
                    await local_storage.set(cache_key, loaded, node.Meta.ttl, None)
                _lockers.pop(cache_key.full_key, None)
            else:
                result = CachedData(
                    data=locker.value, updated_at=datetime.now(timezone.utc)
                )

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


class Wrapper(Generic[P, T, R]):
    def __init__(self, fn: Callable[P, R], node: Type[T]):
        self.func = fn

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        node = self.key_func(*args, **kwargs)
        node = cast(CacheNode[Any], node)

        # inline load function
        async def load() -> Any:
            return await self.func(*args, **kwargs)

        node.load = load
        return await get(node)

    def to_node(self, fn: Callable[P, T]) -> Self:
        self.key_func = fn
        return self

    @overload
    def __get__(self, instance, owner) -> Callable[..., R]:
        ...

    @overload
    def __get__(self, instance, owner) -> Self:
        ...

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return cast(Callable[..., R], types.MethodType(self, instance))


class Memoize(Generic[T]):
    def __init__(self, node: type[T]):
        version = node.Meta.version
        self.node = node

    def __call__(self, fn: Callable[P, R]) -> Wrapper[P, T, R]:
        return Wrapper(fn, self.node)
