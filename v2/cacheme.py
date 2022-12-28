from storage import Storage
from models import CacheKey
from typing import TypeVar, cast
from interfaces import CacheNode
from storage import get_tag_storage, set_tag_storage

C_co = TypeVar("C_co", covariant=True)


_storages: dict[str, Storage] = {}


async def get(node: CacheNode[C_co]) -> C_co:
    storage = _storages[node.Meta.storage]
    cache_key = CacheKey(
        node=node.__class__.__name__,
        prefix="cacheme",
        key=node.key(),
        version=node.Meta.version,
        tags=node.tags(),
    )
    if node.Meta.local_cache != None:
        local_storage = _storages[node.Meta.local_cache]
        result = await local_storage.get(cache_key, None)
        if result != None:
            cache_key.log("local cache hit")
            return result
    result = await storage.get(cache_key, node.Meta.serializer)
    if result == None:
        cache_key.log("cache miss")
        result = node.fetch()
        if node.Meta.doorkeeper != None:
            exist = node.Meta.doorkeeper.set(cache_key.hash)
            if not exist:
                return cast(C_co, result)
        await storage.set(cache_key, result, node.Meta.ttl, node.Meta.serializer)
    else:
        cache_key.log("cache hit")
    if node.Meta.local_cache != None:
        local_storage = _storages[node.Meta.local_cache]
        await local_storage.set(cache_key, result, node.Meta.ttl, None)
        cache_key.log("local cache set")
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
    await storage.invalid_tag(tag)
