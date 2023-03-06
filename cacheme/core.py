from asyncio import Event, Future
from collections import OrderedDict
from functools import update_wrapper
from time import time_ns
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
    overload,
)

from typing_extensions import ParamSpec, Protocol

from cacheme.interfaces import DoorKeeper, Metrics, Serializer, Node
from cacheme.models import (
    Cache,
    CachedAwaitable,
    DynamicNode,
    Fetcher,
    _add_node,
    get_nodes,
    sentinel,
)


P = ParamSpec("P")
R = TypeVar("R", covariant=True)
N = TypeVar("N", bound=Node)


class Locker:
    lock: Event
    value: Any

    def __init__(self):
        self.lock = Event()
        self.value = None


# temp storage for futures which are loading from source now,
# will removed automatically when loading done
_awaits: Dict[str, CachedAwaitable] = {}


def _awaits_len():
    return len(_awaits)


@overload
async def get(node: Node[R]) -> R:
    ...


@overload
async def get(node: N, load_fn: Callable[[N], Awaitable[R]]) -> R:
    ...


async def get(node: Node, load_fn=None):
    """
    Get data from node. Will call load function if cahce miss.

    :param node: node instance to get data.
    :param load_fn: override load function, which will be called instead of node load function if set.
    """
    metrics = node.Meta.metrics
    result = sentinel
    caches = node.Meta.caches
    local_caches: List[Cache] = []
    remote_caches: List[Cache] = []
    miss: List[Cache] = []

    for cache in caches:
        if cache.is_local:
            local_caches.append(cache)
        else:
            remote_caches.append(cache)

    # try get cached data from local storages first
    for cache in local_caches:
        result = cache.storage.get_sync(node, None)
        if result is not sentinel:
            metrics._hit_count += 1
            # return fast if hit on first local cache
            if not miss:
                return result
            break
        miss.append(cache)

    # can't find cached result in any local storage, try load from remote storage
    # remote storages are slow and asynchronous, use tmp cached awaitables to avoid thundering herd
    if result is sentinel:
        key = node.full_key()
        awaitable = _awaits.get(key, None)
        if awaitable is None:
            awaitable = CachedAwaitable(
                _load_from_caches(node, remote_caches, miss, load_fn), metrics
            )
            _awaits[node.full_key()] = awaitable
        # wait
        result = await awaitable

    # fill missing caches
    for cache in miss:
        await cache.storage.set(node, result, cache.ttl, node.Meta.serializer)
    # remove from tmp cache after fill
    _awaits.pop(node.full_key(), None)

    return result


# try load data from remote storages, load from source if not found
async def _load_from_caches(
    node: Node, caches: List[Cache], miss: List[Cache], load_fn=None
):
    serializer = node.get_seriaizer()
    result = sentinel
    for cache in caches:
        result = await cache.storage.get(node, serializer)
        if result is not sentinel:
            break
        miss.append(cache)
    # load from source
    if result is sentinel:
        result = await node.load() if load_fn is None else await load_fn(node)

    return result


async def get_all(nodes: Sequence[Node[R]]) -> List[R]:
    """
    Get data from multiple nodes. Will call load function if cahce miss.

    :param nodes: sequence of nodes, must be same type.
    """
    if len(nodes) == 0:
        return []
    node_cls = nodes[0].__class__
    metrics = nodes[0].get_metrics()
    pending: Dict[str, Node] = {}
    missing: Dict[Cache, Iterable[Node]] = {}
    caches = nodes[0].get_caches()
    results: OrderedDict[str, Any] = OrderedDict()
    # initialize reuslts dict and pending list
    for node in nodes:
        if node.__class__ != node_cls:
            raise Exception(
                f"node class mismatch: expect [{node_cls}], get [{node.__class__}]"
            )
        pending[node.full_key()] = node
        results[node.full_key()] = sentinel

    # split local/remote cache
    local_caches: List[Cache] = []
    remote_caches: List[Cache] = []
    for cache in caches:
        if cache.is_local:
            local_caches.append(cache)
        else:
            remote_caches.append(cache)

    # load from local caches first
    for cache in local_caches:
        result = cache.storage.get_all_sync(tuple(pending.values()), None)
        for k, v in result:
            pending.pop(k.full_key(), None)
            results[k.full_key()] = v
        missing[cache] = tuple(pending.values())

    # load from remote cache
    fetch: Dict[str, Node] = {}  # missing nodes, need to load from source
    if len(pending) > 0:
        wait: List[
            Tuple[str, CachedAwaitable]
        ] = []  # nodes already loading by others, only need to wait here
        for node in pending.values():
            awaitable = _awaits.get(node.full_key(), None)
            if awaitable is None:
                fetch[node.full_key()] = node
            else:
                wait.append((node.full_key(), awaitable))

        # update metrics
        metrics._miss_count += len(fetch)
        metrics._hit_count += len(nodes) - len(fetch)

        if len(fetch) > 0:
            fetcher = Fetcher()
            aws: List[Tuple[str, CachedAwaitable]] = []
            for key, node in fetch.items():
                awaitable = CachedAwaitable(Future(), metrics)
                # set event directly
                awaitable.event = Event()
                _awaits[key] = awaitable
                aws.append((key, awaitable))
            fetcher.data = await _get_multi(
                nodes[0], remote_caches, fetch, missing, metrics
            )
            # load done, set all events and results
            for aw in aws:
                cast(Event, aw[1].event).set()
                aw[1].result = fetcher.data[aw[0]]
            for ks, vs in fetcher.data.items():
                results[ks] = vs
        for w in wait:
            results[w[0]] = await w[1]

    # fill missing caches
    for cache, missing_nodes in missing.items():
        data = [(node, results[node.full_key()]) for node in missing_nodes]
        if len(data) > 0:
            await cache.storage.set_all(data, cache.ttl, node_cls.Meta.serializer)

    # remove tmp_cache
    for key in fetch:
        _awaits.pop(key)

    # finally
    return list(results.values())


async def _get_multi(
    node: Node,
    caches: List[Cache],
    nodes: Dict[str, Node],
    missing: Dict[Cache, Iterable],
    metrics: Metrics,
) -> Dict[str, Any]:
    serializer = node.get_seriaizer()
    results: Dict[str, Any] = {}
    for cache in caches:
        cached = await cache.storage.get_all(list(nodes.values()), serializer)
        for k, v in cached:
            nodes.pop(k.full_key(), None)
            results[k.full_key()] = v
        missing[cache] = tuple(nodes.values())

    # load from source
    if len(nodes) > 0:
        now = time_ns()
        try:
            loaded = await node.load_all(tuple(nodes.values()))
            for k, v in loaded:
                results[k.full_key()] = v
        except Exception as e:
            metrics._load_failure_count += len(nodes)
            metrics._total_load_time += time_ns() - now
            raise (e)
        metrics._load_success_count += len(nodes)
        metrics._total_load_time += time_ns() - now
    return results


class Cached(Protocol[P, R]):
    def to_node(self, fn: Callable[P, Node]):
        ...

    def __call__(self, *args, **kwargs) -> R:
        ...


def Wrapper(
    fn: Callable[P, R],
) -> Cached[P, R]:
    _func = fn
    _node_func = None

    def to_node(fn: Callable[P, Node]):
        nonlocal _node_func
        _node_func = fn

    async def fetch(*args: P.args, **kwargs: P.kwargs) -> R:
        node = _node_func(*args, **kwargs)  # type: ignore
        node = cast(Node, node)

        # inline load function
        async def load() -> Any:
            return await _func(*args, **kwargs)  # type: ignore

        node.load = load  # type: ignore
        return await get(node)

    fetch.to_node = to_node  # type: ignore
    return fetch  # type: ignore


class Memoize:
    def __init__(self, node: Type[Node]):
        self.node = node

    def __call__(self, fn: Callable[P, R]) -> Cached[P, R]:
        wrapper = Wrapper(fn)
        return update_wrapper(wrapper, fn)


def nodes() -> List[Type[Node]]:
    return get_nodes()


def stats(node: Type[Node]) -> Metrics:
    return node.get_metrics()


async def invalidate(node: Node):
    caches = node.get_caches()
    for cache in caches:
        await cache.storage.remove(node)


async def refresh(node: Node[R]) -> R:
    await invalidate(node)
    return await get(node)


_dynamic_nodes: Dict[str, Type[DynamicNode]] = {}


def build_node(
    name: str,
    version: str,
    caches: List[Cache],
    serializer: Optional[Serializer] = None,
    doorkeeper: Optional[DoorKeeper] = None,
) -> Type[DynamicNode]:
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
