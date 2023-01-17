from typing import Dict

from cacheme.interfaces import Storage

_storages: Dict[str, Storage] = {}


async def init_storages(storages: Dict[str, Storage]):
    global _storages
    _storages = storages
    for v in storages.values():
        await v.connect()


def get_storage_by_name(name: str) -> Storage:
    global _storages
    return _storages[name]


def set_storage_by_name(name: str, storage: Storage):
    _storages[name] = storage
