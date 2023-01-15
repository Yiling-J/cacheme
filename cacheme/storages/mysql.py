from datetime import datetime, timedelta, timezone
from typing import Any, Optional, List, Dict

import aiomysql
from sqlalchemy.engine import make_url

from cacheme.storages.sqldb import SQLStorage


class MySQLStorage(SQLStorage):
    def __init__(self, address: str, initialize: bool = False, pool_size: int = 50):
        super().__init__(address, initialize=initialize)
        self.pool_size = pool_size

    async def _connect(self):
        dsn = make_url(self.address)
        self.pool = await aiomysql.create_pool(
            host=dsn.host,
            port=dsn.port or 3306,
            user=dsn.username,
            password=dsn.password,
            db=dsn.database,
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
                await cur.execute("select * from cacheme_data where `key`=%s", (key,))
                return await cur.fetchone()

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "insert into cacheme_data(`key`, value, expire) values(%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value), expire=VALUES(expire)",
                    (
                        key,
                        value,
                        expire,
                    ),
                )

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                sql = "SELECT * FROM cacheme_data WHERE `key` in ({0})".format(
                    ", ".join("%s" for _ in keys)
                )
                await cur.execute(sql, keys)
                result = await cur.fetchall()
        return {i["key"]: i for i in result}
