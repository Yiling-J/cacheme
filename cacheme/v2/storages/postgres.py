from cacheme.v2.storages.sqldb import SQLStorage
from asyncpg.connection import asyncpg
from datetime import datetime, timezone, timedelta
from typing import Optional, Any


class PostgresStorage(SQLStorage):
    def __init__(self, address: str, initialize: bool = False, pool_size: int = 50):
        super().__init__(address, initialize=initialize)
        self.pool = asyncpg.create_pool(dsn=address, max_size=pool_size)

    async def _connect(self):
        await self.pool

    async def execute_ddl(self, ddl):
        async with self.pool.acquire() as conn:
            await conn.execute(ddl)

    async def get_by_key(self, key: str) -> Any:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("select * from cacheme_data where key=$1", key)

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl != None:
            expire = datetime.now(timezone.utc) + ttl
        async with self.pool.acquire() as conn:
            await conn.execute(
                "insert into cacheme_data(key, value, expire) values($1,$2,$3) on conflict(key) do update set value=EXCLUDED.value, expire=EXCLUDED.expire",
                key,
                value,
                expire,
            )
