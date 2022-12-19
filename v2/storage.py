from typing import Protocol, cast
from databases import Database
import structlog
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
from dataclasses import dataclass

logger = structlog.get_logger()


async def create_cache_table(address: str, table: str) -> Table:
    meta = MetaData()
    tb = Table(
        table,
        meta,
        Column("id", Integer, primary_key=True),
        Column("key", String, unique=True),
        Column("value", LargeBinary),
        Column("expire", DateTime, index=True),
    )
    # engine = create_async_engine(address, echo=True)
    # async with engine.begin() as conn:
    #     await conn.run_sync(meta.create_all)
    return tb


@dataclass
class CacheKey:
    node: str
    prefix: str
    key: str
    version: str

    @property
    def full_key(self) -> str:
        return f"{self.prefix}:{self.key}:{self.version}"


class Storage(Protocol):
    def __init__(self, address: str, table: str):
        ...

    async def connect(self):
        ...

    async def get(self, key: CacheKey) -> bytes | None:
        ...

    async def set(self, key: CacheKey, value: bytes, ttl: timedelta):
        ...


class SQLStorage:
    def __init__(self, address: str, table: str):
        database = Database(address)
        self.database = database
        self.a = address
        self.b = table

    async def connect(self):
        await self.database.connect()
        self.table = await create_cache_table(self.a, self.b)

    async def get(self, key: CacheKey) -> bytes | None:
        query = self.table.select().where(self.table.c.key == key.full_key)
        result = await self.database.fetch_one(query)
        if result == None:
            logger.info("cache miss", key=key.full_key, node=key.node)
            return None
        if result["expire"] <= datetime.utcnow():
            logger.info("cache expired", key=key.full_key, node=key.node)
            return None
        logger.info("cache hit", key=key.full_key, node=key.node)
        return cast(bytes, result["value"])

    async def set(self, key: CacheKey, value: bytes, ttl: timedelta):
        query = self.table.select(self.table.c.key == key.full_key).with_for_update()
        async with self.database.transaction():
            record = await self.database.fetch_one(query)
            expire = datetime.utcnow() + ttl
            if record == None:
                logger.info("cache set", key=key.full_key, node=key.node)
                await self.database.execute(
                    self.table.insert().values(
                        key=key.full_key, value=value, expire=expire
                    )
                )
            else:
                logger.info("cache update", key=key.full_key, node=key.node)
                await self.database.execute(
                    self.table.update(self.table.c.key == key.full_key).values(
                        value=value, expire=expire
                    )
                )
