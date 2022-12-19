from typing import Protocol, Any, cast
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
from sqlalchemy.ext.asyncio import create_async_engine
from datetime import timedelta, datetime

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


class Storage(Protocol):
    def __init__(self, address: str, table: str):
        ...

    async def connect(self):
        ...

    async def get(self, key: str) -> bytes | None:
        ...

    async def set(self, key: str, value: bytes, ttl: timedelta):
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

    async def get(self, key: str) -> bytes | None:
        query = self.table.select().where(self.table.c.key == key)
        result = await self.database.fetch_one(query)
        if result == None:
            logger.info("cache miss", key=key)
            return None
        if result["expire"] <= datetime.utcnow():
            logger.info("cache expired", key=key)
            return None
        logger.info("cache hit", key=key)
        return cast(bytes, result["value"])

    async def set(self, key: str, value: bytes, ttl: timedelta):
        query = self.table.select(self.table.c.key == key).with_for_update()
        async with self.database.transaction():
            record = await self.database.fetch_one(query)
            expire = datetime.utcnow() + ttl
            if record == None:
                logger.info("cache set", key=key)
                await self.database.execute(
                    self.table.insert().values(key=key, value=value, expire=expire)
                )
            else:
                logger.info("cache update", key=key)
                await self.database.execute(
                    self.table.update(self.table.c.key == key).values(
                        value=value, expire=expire
                    )
                )
