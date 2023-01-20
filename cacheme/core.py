import types
from asyncio import Event
from datetime import datetime, timezone
from time import time_ns
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Optional,
    OrderedDict,
    Sequence,
    Type,
    TypeVar,
    cast,
    overload,
    List,
)

from typing_extensions import ParamSpec, Self

from cacheme.interfaces import Cachable, CachedData, Memoizable, Metrics
from cacheme.models import get_nodes

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


# local storage(if enable) -> storage -> cache miss, load from source
@overload
async def get(node: Cachable[C_co]) -> C_co:
    ...


@overload
async def get(node: CB, load_fn: Callable[[CB], Awaitable[R]]) -> R:
    ...


async def get(node: Cachable, load_fn=None):
    storage = node.get_stroage()
    metrics = node.get_metrics()
    result = None
    local_storage = node.get_local_cache()
    locker = _lockers.get(node.full_key(), None)
    if locker is not None:
        await locker.lock.wait()
        result = locker.value
        metrics._hit_count += 1
    else:
        locker = Locker()
        _lockers[node.full_key()] = locker
        if local_storage is not None:
            result = await local_storage.get(node, None)
        if result is None:
            result = await storage.get(node, node.get_seriaizer())
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
            await storage.set(node, loaded, node.get_ttl(), node.get_seriaizer())
            if local_storage is not None:
                await local_storage.set(node, loaded, node.get_ttl(), None)
        else:
            metrics._hit_count += 1
        locker.value = result
        locker.lock.set()
        _lockers.pop(node.full_key(), None)
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
        self.hashmap: Dict[int, Cachable] = {}
        for node in nodes:
            self.hashmap[node.key_hash()] = node

    def remove(self, node: Cachable):
        self.hashmap.pop(node.key_hash(), None)

    @property
    def list(self) -> Sequence[Cachable]:
        return tuple(self.hashmap.values())

    def __len__(self):
        return len(self.hashmap)


async def get_all(nodes: Sequence[Cachable[C]]) -> Sequence[C]:
    if len(nodes) == 0:
        return tuple()
    node_cls = nodes[0].__class__
    keys = []
    s: OrderedDict[int, Optional[C]] = OrderedDict()
    for node in nodes:
        if node.__class__ != node_cls:
            raise Exception(
                f"node class mismatch: expect [{node_cls}], get [{node.__class__}]"
            )
        s[node.key_hash()] = None
        keys.append(node.full_key())
    keys.sort()
    result = None
    storage = nodes[0].get_stroage()
    metrics = nodes[0].get_metrics()
    lock_key = "/".join(keys)
    locker = _lockers.get(lock_key, None)
    if locker is not None:
        await locker.lock.wait()
        result = locker.value
        metrics._hit_count += 1
    else:
        locker = Locker()
        _lockers[lock_key] = locker
        pending_nodes = NodeSet(nodes)
        local_storage = nodes[0].get_local_cache()
        serializer = nodes[0].get_seriaizer()
        ttl = nodes[0].get_ttl()
        if local_storage is not None:
            cached = await local_storage.get_all(nodes, serializer)
            for k, v in cached:
                pending_nodes.remove(k)
                s[k.key_hash()] = cast(C, v.data)
        cached = await storage.get_all(pending_nodes.list, serializer)
        for k, v in cached:
            pending_nodes.remove(k)
            s[k.key_hash()] = cast(C, v.data)
        metrics._miss_count += len(pending_nodes)
        now = time_ns()
        try:
            ns = cast(Sequence[Cachable], pending_nodes.list)
            loaded = await node_cls.load_all(ns)
        except Exception as e:
            metrics._load_failure_count += len(pending_nodes)
            metrics._total_load_time += time_ns() - now
            raise (e)
        metrics._load_success_count += len(pending_nodes)
        metrics._total_load_time += time_ns() - now
        if local_storage is not None:
            await local_storage.set_all(loaded, ttl, serializer)
        await storage.set_all(loaded, ttl, serializer)
        for node, value in loaded:
            s[node.key_hash()] = cast(C, value)
        result = cast(Sequence[C], tuple(s.values()))
        locker.value = result
        locker.lock.set()
        _lockers.pop(lock_key, None)
    return result


async def nodes() -> List[Type[Cachable]]:
    return get_nodes()


def stats(node: Type[Cachable]) -> Metrics:
    return node.get_metrics()
