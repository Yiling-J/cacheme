from typing import Any, Optional, Protocol, cast
from databases import Database
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    LargeBinary,
    DateTime,
)
from datetime import timedelta, datetime
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import now

from cacheme.v2.serializer import PickleSerializer, Serializer
from cacheme.v2.tinylfu import tinylfu
from cacheme.v2.models import CacheKey


@compiles(now, "sqlite")
def sl_now(element, compiler, **kw):
    return "strftime('%Y-%m-%d %H:%M:%f', 'now')"


async def create_cache_table(address: str, table: str) -> Table:
    meta = MetaData()
    tb = Table(
        table,
        meta,
        Column("id", Integer, primary_key=True),
        Column("key", String, unique=True),
        Column("value", LargeBinary),
        Column("expire", DateTime, index=True),
        Column(
            "updated_at",
            DateTime,
            server_default=now(),
            server_onupdate=now(),
        ),
    )
    engine = create_async_engine(address, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)
    return tb


class Storage(Protocol):
    async def connect(self):
        ...

    async def get(self, key: CacheKey, serializer: Optional[Serializer]) -> Any | None:
        ...

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: timedelta,
        serializer: Optional[Serializer],
    ):
        ...

    async def validate_key_with_tags(
        self, updated_at: datetime, tags: list[str]
    ) -> bool:
        ...

    async def invalid_tag(self, tag: str):
        ...


tag_storage: Storage | None = None


def get_tag_storage() -> Storage:
    global tag_storage
    if tag_storage == None:
        raise Exception()
    return tag_storage


def set_tag_storage(storage: Storage):
    global tag_storage
    tag_storage = storage


class SQLStorage:
    def __init__(self, address: str):
        database = Database(address)
        self.database = database
        self.address = address

    async def connect(self):
        await self.database.connect()
        self.table = await create_cache_table(self.address, "cacheme_data")

    async def get(self, key: CacheKey, serializer: Optional[Serializer]) -> Any | None:
        if serializer == None:
            serializer = PickleSerializer()
        query = self.table.select().where(self.table.c.key == key.full_key)
        result = await self.database.fetch_one(query)
        if result == None:
            key.log("cache miss")
            return None
        if result["expire"] <= datetime.utcnow():
            key.log("cache expired")
            return None
        if len(key.tags) > 0:
            if tag_storage == None:
                raise Exception("")
            valid = await tag_storage.validate_key_with_tags(
                cast(datetime, result["updated_at"]), key.tags
            )
            if not valid:
                key.log("cache tag expired")
                return None
        return serializer.loads(cast(bytes, result["value"]))

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: timedelta,
        serializer: Optional[Serializer],
    ):
        if serializer == None:
            serializer = PickleSerializer()
        v = serializer.dumps(value)
        query = self.table.select(self.table.c.key == key.full_key).with_for_update()
        async with self.database.transaction():
            record = await self.database.fetch_one(query)
            expire = datetime.utcnow() + ttl
            if record == None:
                key.log("cache set")
                await self.database.execute(
                    self.table.insert().values(key=key.full_key, value=v, expire=expire)
                )
            else:
                key.log("cache update")
                await self.database.execute(
                    self.table.update(self.table.c.key == key.full_key).values(
                        value=v, expire=expire
                    )
                )

    async def invalid_tag(self, tag: str):
        full_tag = f"cacheme:internal:{tag}"
        query = (
            self.table.select(self.table.c.key == full_tag)
            .values("id")
            .with_for_update()
        )
        async with self.database.transaction():
            record = await self.database.fetch_one(query)
            if record == None:
                await self.database.execute(self.table.insert().values(key=full_tag))
            else:
                await self.database.execute(
                    self.table.update(self.table.c.key == full_tag).values(key=full_tag)
                )

    async def validate_key_with_tags(
        self, updated_at: datetime, tags: list[str]
    ) -> bool:
        full_tags = [f"cacheme:internal:{tag}" for tag in tags]
        query = (
            self.table.select()
            .where(self.table.c.key.in_(full_tags))
            .values("updated_at")
        )
        records = await self.database.fetch_all(query)
        for tag in records:
            if tag["updated_at"] >= updated_at:
                return False
        return True


class TLFUStorage:
    def __init__(self, size: int):
        self.cache = tinylfu.Cache(size)

    async def connect(self):
        return

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[Any]:
        return self.cache.get(key)

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: timedelta,
        serializer: Optional[Serializer],
    ):
        self.cache.set(key, value, ttl)
        return

    async def validate_key_with_tags(
        self, updated_at: datetime, tags: list[str]
    ) -> bool:
        raise NotImplementedError()

    async def invalid_tag(self, tag: str):
        raise NotImplementedError()
