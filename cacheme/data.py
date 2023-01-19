from typing import Dict

from cacheme.interfaces import Storage

_storages: Dict[str, Storage] = {}


async def register_storage(name: str, storage: Storage):
    _storages[name] = storage
    await storage.connect()


def get_storage_by_name(name: str) -> Storage:
    global _storages
    return _storages[name]


def list_storages() -> Dict[str, Storage]:
    return _storages
