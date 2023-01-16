from typing import Dict

from cacheme.interfaces import Storage

TAG_STORAGE_KEY = "__tag__"
_storages: Dict[str, Storage] = {}


def get_tag_storage() -> Storage:
    global _storages
    return _storages[TAG_STORAGE_KEY]


def set_tag_storage(storage: Storage):
    global _storages
    _storages[TAG_STORAGE_KEY] = storage


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
