from datetime import datetime, timedelta, timezone
from typing import List, Optional, cast

from typing_extensions import Any

from cacheme.models import CachedData, CacheKey
from cacheme.serializer import Serializer
from cacheme.storages.interfaces import Storage


class BaseStorage:
    def __init__(self, address: str, *args, **kwargs):
        self.address = address

    async def connect(self):
        raise NotImplementedError()

    async def get_by_key(self, key: str) -> Any:
        raise NotImplementedError()

    async def remove_by_key(self, key: str):
        raise NotImplementedError()

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        raise NotImplementedError()

    def serialize(self, raw: Any, serializer: Optional[Serializer]) -> CachedData:
        data = raw["value"]
        if serializer is not None:
            data = serializer.loads(cast(bytes, raw["value"]))
        return CachedData(
            data=data,
            updated_at=raw["updated_at"],
            expire=raw["expire"],
        )

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        result = await self.get_by_key(key.full_key)
        if result is None:
            return None
        data = self.serialize(result, serializer)
        if data.expire is not None and data.expire.replace(
            tzinfo=timezone.utc
        ) <= datetime.now(timezone.utc):
            return None
        return data

    def deserialize(self, raw: Any, serializer: Optional[Serializer]) -> Any:
        if serializer is not None:
            return serializer.dumps(raw)

        return raw

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        v = self.deserialize(value, serializer)
        await self.set_by_key(key.full_key, v, ttl)

    async def remove(self, key: CacheKey):
        await self.remove_by_key(key.full_key)

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        raise NotImplementedError()


tag_storage: Optional[Storage] = None


def get_tag_storage() -> Storage:
    global tag_storage
    if tag_storage is None:
        raise Exception()
    return tag_storage


def set_tag_storage(storage: Storage):
    global tag_storage
    tag_storage = storage
