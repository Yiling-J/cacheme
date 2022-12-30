from datetime import timedelta, timezone, datetime
from cacheme.v2.serializer import MsgPackSerializer
from cacheme.v2.storage import Storage
from cacheme.v2.models import CacheKey, CachedData
from typing import TypeVar, cast
from cacheme.v2.interfaces import CacheNode
from cacheme.v2.storage import get_tag_storage, set_tag_storage


C_co = TypeVar("C_co", covariant=True)


_storages: dict[str, Storage] = {}


# local storage(if enable) -> storage -> call load function
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
    created = False
    if node.Meta.local_cache != None:
        local_storage = _storages[node.Meta.local_cache]
        result = await local_storage.get(cache_key, None)
    if result == None:
        result = await storage.get(cache_key, node.Meta.serializer)
    if result == None:
        loaded = node.load()
        result = CachedData(data=result, updated_at=datetime.now(timezone.utc))
        created = True
        if node.Meta.doorkeeper != None:
            exist = node.Meta.doorkeeper.set(cache_key.hash)
            if not exist:
                return cast(C_co, result)
        await storage.set(cache_key, loaded, node.Meta.ttl, node.Meta.serializer)
        if node.Meta.local_cache != None:
            local_storage = _storages[node.Meta.local_cache]
            await local_storage.set(cache_key, loaded, node.Meta.ttl, None)
    if created == False:
        print("check tags")
    return cast(C_co, result)


async def init_storages(storages: dict[str, Storage]):
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
    await storage.set(
        cache_key, None, ttl=timedelta(days=1000), serializer=MsgPackSerializer()
    )
