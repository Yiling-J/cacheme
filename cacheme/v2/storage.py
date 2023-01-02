import redis.asyncio as redis
import motor.motor_asyncio as mongo
from typing import Optional, cast, List
from typing_extensions import Any, Protocol
from databases import Database
from sqlalchemy import MetaData, Table, Column, Integer, String, LargeBinary, DateTime
from datetime import timedelta, datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import now

from cacheme.v2.serializer import PickleSerializer, Serializer
from cacheme.v2.tinylfu import tinylfu
from cacheme.v2.models import CacheKey, CachedData


@compiles(now, "sqlite")
def sl_now(element, compiler, **kw):
    return "strftime('%Y-%m-%d %H:%M:%f', 'now')"


class Storage(Protocol):
    async def connect(self):
        ...

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        ...

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        ...

    async def remove(self, key: CacheKey):
        ...

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        ...


tag_storage: Optional[Storage] = None


def get_tag_storage() -> Storage:
    global tag_storage
    if tag_storage is None:
        raise Exception()
    return tag_storage


def set_tag_storage(storage: Storage):
    global tag_storage
    tag_storage = storage


class SQLStorage:
    def __init__(self, address: str, migrate: bool = False):
        database = Database(address)
        self.database = database
        self.address = address
        self.migrate = migrate

    async def connect(self):
        await self.database.connect()
        self.table = await self.get_cache_table()

    async def get_cache_table(self) -> Table:
        meta = MetaData()
        tb = Table(
            "cacheme_data",
            meta,
            Column("id", Integer, primary_key=True),
            Column("key", String(512), unique=True),
            Column("value", LargeBinary),
            Column("expire", DateTime(timezone=True), index=True),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=now(),
                server_onupdate=now(),
            ),
        )

        if self.migrate:
            engine = create_async_engine(self.address, echo=True)
            async with engine.begin() as conn:
                await conn.run_sync(meta.create_all)
        return tb

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        if serializer is None:
            serializer = PickleSerializer()
        query = self.table.select().where(self.table.c.key == key.full_key)
        result = await self.database.fetch_one(query)
        if result is None:
            return None
        if result["expire"] is not None and result["expire"].replace(
            tzinfo=timezone.utc
        ) <= datetime.now(timezone.utc):
            return None
        return CachedData(
            data=serializer.loads(cast(bytes, result["value"])),
            updated_at=result["updated_at"],
        )

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        if serializer is None:
            serializer = PickleSerializer()
        v = serializer.dumps(value)
        query = self.table.select(self.table.c.key == key.full_key).with_for_update()
        async with self.database.transaction():
            record = await self.database.fetch_one(query)
            expire = None
            if ttl != None:
                expire = datetime.now(timezone.utc) + ttl
            if record is None:
                await self.database.execute(
                    self.table.insert().values(key=key.full_key, value=v, expire=expire)
                )
            else:
                await self.database.execute(
                    self.table.update(self.table.c.key == key.full_key).values(
                        value=v, expire=expire
                    )
                )

    async def remove(self, key: CacheKey):
        await self.database.execute(
            self.table.delete().where(self.table.c.key == key.full_key)
        )

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        ...


class TLFUStorage:
    def __init__(self, size: int):
        self.cache = tinylfu.Cache(size)

    async def connect(self):
        return

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        return self.cache.get(key)

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        evicated = self.cache.set(key, value, ttl)
        if evicated and key.metrics is not None:
            key.metrics.eviction_count += 1
        return

    async def remove(self, key: CacheKey):
        self.cache.remove(key)
        ...

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        ...


class RedisStorage:
    def __init__(self, address: str):
        self.address = address

    async def connect(self):
        self.client = await redis.from_url(self.address)

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        if serializer is None:
            serializer = PickleSerializer()
        result = await self.client.get(key.full_key)
        if result is None:
            return None
        data = serializer.loads(cast(bytes, result))
        return CachedData(data=data["value"], updated_at=data["updated_at"])

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: timedelta,
        serializer: Optional[Serializer],
    ):
        if serializer is None:
            serializer = PickleSerializer()
        v = serializer.dumps({"value": value, "updated_at": datetime.now(timezone.utc)})
        await self.client.setex(key.full_key, int(ttl.total_seconds()), v)

    async def remove(self, key: CacheKey):
        await self.client.delete(key.full_key)

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        ...


class MongoStorage:
    def __init__(self, address: str, migrate: bool = False):
        self.address = address
        self.migrate = migrate

    async def connect(self):
        client = mongo.AsyncIOMotorClient(self.address)
        self.table = client.cacheme.data
        if self.migrate:
            await self.table.create_index("key", unique=True)
            await self.table.create_index("expire")

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        if serializer is None:
            serializer = PickleSerializer()
        result = await self.table.find_one({"key": key.full_key})
        if result is None:
            return None
        if result["expire"].replace(tzinfo=timezone.utc) <= datetime.now(timezone.utc):
            return None
        data = serializer.loads(cast(bytes, result["value"]))
        return CachedData(data=data, updated_at=result["updated_at"])

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: timedelta,
        serializer: Optional[Serializer],
    ):
        if serializer is None:
            serializer = PickleSerializer()
        v = serializer.dumps(value)
        expire = datetime.now(timezone.utc) + ttl
        await self.table.update_one(
            {"key": key.full_key},
            {
                "$set": {
                    "value": v,
                    "updated_at": datetime.now(timezone.utc),
                    "expire": expire,
                }
            },
            True,
        )

    async def remove(self, key: CacheKey):
        await self.table.delete_one({"key": key.full_key})

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        ...
