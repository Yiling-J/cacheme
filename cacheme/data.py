from typing import Optional, Dict
from cacheme.interfaces import Storage

_tag_storage: Optional[Storage] = None


def get_tag_storage() -> Storage:
    global _tag_storage
    if _tag_storage is None:
        raise Exception()
    return _tag_storage


def set_tag_storage(storage: Storage):
    global _tag_storage
    _tag_storage = storage


_storages: Dict[str, Storage] = {}


async def init_storages(storages: Dict[str, Storage]):
    global _storages
    _storages = storages
    for v in storages.values():
        await v.connect()


async def init_tag_storage(storage: Storage):
    await storage.connect()
    set_tag_storage(storage)


def get_storage_by_name(name: str) -> Storage:
    global _storages
    return _storages[name]
