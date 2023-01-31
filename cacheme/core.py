import types
from asyncio import Event
from collections import OrderedDict
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
    Sequence,
    Type,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import ParamSpec, Self

from cacheme.interfaces import (
    Cachable,
    CachedData,
    Memoizable,
    Metrics,
    Serializer,
    DoorKeeper,
)
from cacheme.models import Cache, get_nodes, DynamicNode, Node, _add_node

C = TypeVar("C")
CB = TypeVar("CB", bound=Cachable)
C_co = TypeVar("C_co", covariant=True)
P = ParamSpec("P")
R = TypeVar("R")


class Locker:
    lock: Event
    value: Any

    def __init__(self):
        self.lock = Event()
        self.value = None


_lockers: Dict[str, Locker] = {}


@overload
async def get(node: Cachable[C_co]) -> C_co:
    ...


@overload
async def get(node: CB, load_fn: Callable[[CB], Awaitable[R]]) -> R:
    ...


async def get(node: Cachable, load_fn=None):
    metrics = node.get_metrics()
    result = None
    caches = node.get_caches()
    locker = _lockers.get(node.full_key(), None)
    if locker is not None:
        await locker.lock.wait()
        result = locker.value
        metrics._hit_count += 1
    else:
        locker = Locker()
        _lockers[node.full_key()] = locker
        serializer = node.get_seriaizer()
        miss: List[Cache] = []
        for cache in caches:
            storage = cache.get_storage()
            result = await storage.get(node, serializer)
            if result is not None:
                break
            miss.append(cache)
        if result is None:
            metrics._miss_count += 1
            now = time_ns()
            try:
                if load_fn is not None:
                    loaded = await load_fn(node)
                else:
                    loaded = await node.load()
            except Exception as e:
                metrics._load_failure_count += 1
                metrics._total_load_time += time_ns() - now
                raise (e)
            metrics._load_success_count += 1
            metrics._total_load_time += time_ns() - now
            result = CachedData(
                data=loaded, node=node, updated_at=datetime.now(timezone.utc)
            )
            doorkeeper = node.get_doorkeeper()
            if doorkeeper is not None:
                exist = doorkeeper.contains(node.full_key())
                if not exist:
                    doorkeeper.put(node.full_key())
                    return result.data
        else:
            metrics._hit_count += 1
        locker.value = result
        locker.lock.set()
        _lockers.pop(node.full_key(), None)
        if len(miss) > 0:
            for cache in miss:
                await cache.get_storage().set(
                    node, result.data, cache.get_ttl(), serializer
                )
    return result.data


class Wrapper(Generic[P, R]):
    def __init__(self, fn: Callable[P, Awaitable[R]], node: Type[Memoizable]):
        self.func = fn

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        node = self.key_func(*args, **kwargs)
        node = cast(Cachable, node)

        # inline load function
        async def load() -> Any:
            return await self.func(*args, **kwargs)

        node.load = load  # type: ignore
        return await get(node)

    def to_node(self, fn: Callable[P, Memoizable]) -> Self:  # type: ignore
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
    def __init__(self, node: Type[Memoizable]):
        self.node = node

    def __call__(self, fn: Callable[P, Awaitable[R]]) -> Wrapper[P, R]:
        return Wrapper(fn, self.node)


class NodeSet:
    def __init__(self, nodes: Sequence[Cachable]):
        self.hashmap: Dict[str, Cachable] = {}
        for node in nodes:
            self.hashmap[node.full_key()] = node

    def remove(self, node: Cachable):
        self.hashmap.pop(node.full_key(), None)

    def list(self) -> Sequence[Cachable]:
        return tuple(self.hashmap.values())

    def __len__(self):
        return len(self.hashmap)


async def get_all(nodes: Sequence[Cachable[C]]) -> Sequence[C]:
    if len(nodes) == 0:
        return tuple()
    node_cls = nodes[0].__class__
    waiting = []
    missing = []
    s: OrderedDict[str, Optional[C]] = OrderedDict()
    for node in nodes:
        if node.__class__ != node_cls:
            raise Exception(
                f"node class mismatch: expect [{node_cls}], get [{node.__class__}]"
            )
        s[node.full_key()] = None
        locker = _lockers.get(node.full_key(), None)
        if locker is not None:
            waiting.append((node, locker))
        else:
            locker = Locker()
            _lockers[node.full_key()] = locker
            missing.append(node)
    result = None
    metrics = nodes[0].get_metrics()
    pending_nodes = NodeSet(missing)
    serializer = nodes[0].get_seriaizer()
    caches = nodes[0].get_caches()
    missing_nodes = {}
    for cache in caches:
        cached = await cache.get_storage().get_all(pending_nodes.list(), serializer)
        for k, v in cached:
            pending_nodes.remove(k)
            locker = _lockers.pop(k.full_key(), None)
            if locker is not None:
                locker.value = v.data
                locker.lock.set()
            s[k.full_key()] = cast(C, v.data)
        missing_nodes[cache] = pending_nodes.list()
        metrics._hit_count += len(cached)
    metrics._miss_count += len(pending_nodes)
    if len(pending_nodes) > 0:
        now = time_ns()
        try:
            ns = cast(Sequence[Cachable], pending_nodes.list())
            loaded = await node_cls.load_all(ns)
        except Exception as e:
            metrics._load_failure_count += len(pending_nodes)
            metrics._total_load_time += time_ns() - now
            raise (e)
        metrics._load_success_count += len(pending_nodes)
        metrics._total_load_time += time_ns() - now
        for k, v in loaded:
            locker = _lockers.pop(k.full_key(), None)
            if locker is not None:
                locker.value = v
                locker.lock.set()
            s[k.full_key()] = cast(C, v)
        for cache in caches:
            data = [(node, s[node.full_key()]) for node in missing_nodes[cache]]
            if len(data) > 0:
                await cache.get_storage().set_all(data, cache.get_ttl(), serializer)
    for n in waiting:
        node, locker = n
        await locker.lock.wait()
        s[node.full_key()] = cast(C, locker.value)
    metrics._hit_count += len(waiting)
    result = cast(Sequence[C], tuple(s.values()))
    return result


def nodes() -> List[Type[Cachable]]:
    return get_nodes()


def stats(node: Type[Cachable]) -> Metrics:
    return node.get_metrics()


async def invalidate(node: Cachable):
    caches = node.get_caches()
    for cache in caches:
        await cache.get_storage().remove(node)


async def refresh(node: Cachable[C_co]) -> C_co:
    await invalidate(node)
    return await get(node)


_dynamic_nodes: Dict[str, Type[Node]] = {}


def build_node(
    name: str,
    version: str,
    caches: List[Cache],
    serializer: Optional[Serializer] = None,
    doorkeeper: Optional[DoorKeeper] = None,
) -> Type[Node]:
    if name in _dynamic_nodes:
        return _dynamic_nodes[name]
    new: Type[DynamicNode] = type(name, (DynamicNode,), {})
    new.Meta.version = version
    new.Meta.caches = caches
    new.Meta.serializer = serializer
    new.Meta.doorkeeper = doorkeeper
    new.Meta.metrics = Metrics()
    _dynamic_nodes[name] = new
    _add_node(new)
    return new
