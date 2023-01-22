from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiomysql

from cacheme.storages.sqldb import SQLStorage


class MySQLStorage(SQLStorage):
    def __init__(self, address: str, table: str, pool_size: int = 50):
        super().__init__(address, table=table)
        self.pool_size = pool_size
        self.table = table

    async def _connect(self):
        url = urlparse(self.address)
        db = url.path[1:]
        self.pool = await aiomysql.create_pool(
            host=url.hostname,
            port=url.port or 3306,
            user=url.username,
            password=url.password,
            db=db,
            autocommit=True,
            maxsize=self.pool_size,
        )

    async def execute_ddl(self, ddl):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(ddl)

    async def get_by_key(self, key: str) -> Any:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    f"select * from {self.table} where `key`=%s",
                    (key,),
                )
                return await cur.fetchone()

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"insert into {self.table}(`key`, value, expire) values(%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value), expire=VALUES(expire)",
                    (
                        key,
                        value,
                        expire,
                    ),
                )

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                sql = "SELECT * FROM {} WHERE `key` in ({})".format(
                    self.table, ", ".join("%s" for _ in keys)
                )
                await cur.execute(sql, keys)
                result = await cur.fetchall()
        return {i["key"]: i for i in result}

    async def set_by_keys(self, data: Dict[str, Any], ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    f"insert into {self.table}(`key`, value, expire) values(%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value), expire=VALUES(expire)",
                    [
                        (
                            key,
                            value,
                            expire,
                        )
                        for key, value in data.items()
                    ],
                )

    async def remove_by_key(self, key: str):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    f"delete from {self.table} where `key`=%s",
                    (key,),
                )
