import redis.asyncio as redis

from cacheme.v2.storages.base import BaseStorage
from cacheme.v2.models import CachedData
from cacheme.v2.serializer import Serializer
from typing import Optional, Any, cast
from datetime import timedelta, timezone, datetime
from redis.asyncio.connection import BlockingConnectionPool


class RedisStorage(BaseStorage):
    def __init__(self, address: str, pool_size: int = 50):
        super().__init__(address=address)
        self.pool_size = pool_size

    async def connect(self):
        self.client = await redis.from_url(self.address)
        self.client.connection_pool = BlockingConnectionPool.from_url(
            self.address, max_connections=self.pool_size
        )

    async def get_by_key(self, key: str) -> Any:
        return await self.client.get(key)

    def serialize(self, raw: Any, serializer: Serializer) -> Optional[CachedData]:
        data = serializer.loads(cast(bytes, raw))
        return CachedData(
            data=data["value"], updated_at=data["updated_at"], expire=None
        )

    def deserialize(self, raw: Any, serializer: Optional[Serializer]) -> Any:
        value = {"value": raw, "updated_at": datetime.now(timezone.utc)}
        return super().deserialize(value, serializer)

    async def remove_key(self, key: str):
        await self.client.delete(key)

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        if ttl != None:
            await self.client.setex(key, int(ttl.total_seconds()), value)
        else:
            await self.client.set(key, value)
