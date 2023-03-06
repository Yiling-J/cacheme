from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union, cast

import redis.asyncio as redis
import redis.asyncio.cluster as redis_cluster
from redis.asyncio.connection import BlockingConnectionPool

from cacheme.interfaces import CachedData
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage


class RedisStorage(BaseStorage):
    client: Union[redis.Redis, redis_cluster.RedisCluster]

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
            cast(
                redis.Redis, self.client
            ).connection_pool = BlockingConnectionPool.from_url(
                self.address, max_connections=self.pool_size, timeout=None
            )

    async def get_by_key(self, key: str) -> Any:
        return await self.client.get(key)  # type: ignore

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        values = await self.client.mget(keys)  # type: ignore
        return {keys[i]: v for i, v in enumerate(values) if v is not None}

    def serialize(self, raw: Any, serializer: Optional[Serializer]) -> CachedData:
        if serializer is None:
            raise Exception("serializer is None")
        data = serializer.loads(cast(bytes, raw))
        return CachedData(data=data["value"], expire=None)

    def deserialize(self, raw: Any, serializer: Optional[Serializer]) -> Any:
        value = {"value": raw, "updated_at": datetime.now(timezone.utc)}
        return super().deserialize(value, serializer)

    async def remove_by_key(self, key: str):
        await self.client.delete(key)  # type: ignore

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        if ttl is not None:
            await self.client.setex(key, int(ttl.total_seconds()), value)  # type: ignore
        else:
            await self.client.set(key, value)  # type: ignore

    async def set_by_keys(self, data: Dict[str, Any], ttl: Optional[timedelta]):
        async with self.client.pipeline() as pipe:
            if ttl is not None:
                seconds = int(ttl.total_seconds())
                for k, v in data.items():
                    pipe.setex(k, seconds, v)  # type: ignore
            else:
                for k, v in data.items():
                    pipe.set(k, v)  # type: ignore
            await pipe.execute()  # type: ignore
