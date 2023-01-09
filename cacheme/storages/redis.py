from datetime import datetime, timedelta, timezone
from typing import Any, Optional, cast, List, Dict

import redis.asyncio as redis
from cacheme.interfaces import CachedData
import redis.asyncio.cluster as redis_cluster
from redis.asyncio.connection import BlockingConnectionPool

from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage


class RedisStorage(BaseStorage):
    def __init__(
        self, address: str, pool_size: int = 100, cluster: bool = False, **options
    ):
        super().__init__(address=address)
        self.pool_size = pool_size
        self.cluster = cluster
        self.options = options

    async def connect(self):
        if self.cluster:
            self.client = redis_cluster.RedisCluster.from_url(
                self.address,
                max_connections=10 * self.pool_size,
                **self.options,
            )
        else:
            self.client = await redis.from_url(self.address, **self.options)
            self.client.connection_pool = BlockingConnectionPool.from_url(
                self.address, max_connections=self.pool_size, timeout=None
            )

    async def get_by_key(self, key: str) -> Any:
        return await self.client.get(key)

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        values = await self.client.mget(keys)
        return {keys[i]: v for i, v in enumerate(values) if v is not None}

    def serialize(self, raw: Any, serializer: Optional[Serializer]) -> CachedData:
        if serializer is None:
            raise Exception("serializer is None")
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
        if ttl is not None:
            await self.client.setex(key, int(ttl.total_seconds()), value)
        else:
            await self.client.set(key, value)
