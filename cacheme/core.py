import types
from asyncio import Lock
from datetime import datetime, timezone
from time import time_ns
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    OrderedDict,
    Sequence,
    Set,
    Type,
    TypeVar,
    cast,
    overload,
)


from typing_extensions import ParamSpec, Self

from cacheme.interfaces import BaseNode, CacheNode, MemoNode
from cacheme.models import CachedData, TagNode
from cacheme.serializer import MsgPackSerializer
from cacheme.storages import get_tag_storage, set_tag_storage
from cacheme.storages import Storage


C = TypeVar("C")
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
    result = None
    if node.Meta.local_cache is not None:
        local_storage = _storages[node.Meta.local_cache]
        result = await local_storage.get(node, None)
    if result is None:
        result = await storage.get(node, node.Meta.serializer)
    # get result from cache, check tags
    if result is not None and len(node.tags()) > 0:
        tag_storage = get_tag_storage()
        valid = await tag_storage.validate_tags(
            result.updated_at,
            node.tags(),
        )
        if not valid:
            await storage.remove(node)
            result = None
    if result is None:
        metrics.miss_count += 1
        locker = _lockers.setdefault(node._full_key, Locker())
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
                    exist = node.Meta.doorkeeper.set(node._keyh)
                    if not exist:
                        return cast(C_co, result)
                await storage.set(node, loaded, node.Meta.ttl, node.Meta.serializer)
                if node.Meta.local_cache is not None:
                    local_storage = _storages[node.Meta.local_cache]
                    await local_storage.set(node, loaded, node.Meta.ttl, None)
                _lockers.pop(node._full_key, None)
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
    await storage.set(TagNode(tag), None, ttl=None, serializer=MsgPackSerializer())


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


class NodeSet:
    def __init__(self, nodes: Sequence[BaseNode]):
        self.hashmap: Dict[int, BaseNode] = {}
        for node in nodes:
            self.hashmap[node._keyh] = node

    def remove(self, node: BaseNode):
        self.hashmap.pop(node._keyh, None)

    @property
    def list(self) -> Sequence[BaseNode]:
        return tuple(self.hashmap.values())

    def __len__(self):
        return len(self.hashmap)


async def get_all(nodes: Sequence[CacheNode[C]]) -> Sequence[C]:
    if len(nodes) == 0:
        return tuple()
    node_cls = nodes[0].__class__
    s: OrderedDict[int, Optional[C]] = OrderedDict()
    for node in nodes:
        if node.__class__ != node_cls:
            raise Exception(
                f"node class mismatch: expect [{node_cls}], get [{node.__class__}]"
            )
        s[node._keyh] = None
    pending_nodes = NodeSet(nodes)
    storage = _storages[node_cls.Meta.storage]
    metrics = node_cls.Meta.metrics
    if node_cls.Meta.local_cache is not None:
        local_storage = _storages[node_cls.Meta.local_cache]
        cached = await local_storage.get_all(nodes, node_cls.Meta.serializer)
        for k, v in cached:
            pending_nodes.remove(k)
            s[k._keyh] = cast(C, v.data)
    cached = await storage.get_all(pending_nodes.list, node_cls.Meta.serializer)
    for k, v in cached:
        pending_nodes.remove(k)
        s[k._keyh] = cast(C, v.data)
    metrics.miss_count += len(pending_nodes)
    now = time_ns()
    try:
        ns = cast(Sequence[CacheNode], pending_nodes.list)
        loaded = await node_cls.load_all(ns)
    except Exception as e:
        metrics.load_failure_count += len(pending_nodes)
        metrics.total_load_time += time_ns() - now
        raise (e)
    metrics.load_success_count += len(pending_nodes)
    metrics.total_load_time += time_ns() - now
    if node_cls.Meta.local_cache is not None:
        local_storage = _storages[node_cls.Meta.local_cache]
        await local_storage.set_all(loaded, node_cls.Meta.ttl, node_cls.Meta.serializer)
    await storage.set_all(loaded, node_cls.Meta.ttl, node_cls.Meta.serializer)
    for node, value in loaded:
        s[node._keyh] = cast(C, value)
    return cast(Sequence[C], tuple(s.values()))
