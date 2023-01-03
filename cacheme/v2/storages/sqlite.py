import aiosqlite
from cacheme.v2.storages.sqldb import SQLStorage
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, cast
from sqlalchemy.engine import make_url
from cacheme.v2.models import CachedData
from cacheme.v2.serializer import Serializer


class SQLiteStorage(SQLStorage):
    def __init__(self, address: str, initialize: bool = False):
        super().__init__(address, initialize=initialize)
        dsn = make_url(self.address)
        self.db = dsn.database or ""

    async def _connect(self):
        pass

    def serialize(self, raw: Any, serializer: Optional[Serializer]) -> CachedData:
        data = raw["value"]
        if serializer is not None:
            data = serializer.loads(cast(bytes, raw["value"]))
        updated_at = datetime.fromisoformat(raw["updated_at"])
        expire = None
        if raw["expire"] != None:
            expire = datetime.fromisoformat(raw["expire"])
        return CachedData(
            data=data,
            updated_at=updated_at,
            expire=expire,
        )

    async def execute_ddl(self, ddl):
        async with aiosqlite.connect(self.db, isolation_level=None) as conn:
            await conn.execute(ddl)

    async def get_by_key(self, key: str) -> Any:
        async with aiosqlite.connect(self.db, isolation_level=None) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "select * from cacheme_data where key=?", (key,)
            ) as cursor:
                return await cursor.fetchone()

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl != None:
            expire = datetime.now(timezone.utc) + ttl
        async with aiosqlite.connect(self.db, isolation_level=None) as conn:
            await conn.execute(
                "insert into cacheme_data(key, value, expire) values(?,?,?) on conflict(key) do update set value=EXCLUDED.value, expire=EXCLUDED.expire",
                (
                    key,
                    value,
                    expire,
                ),
            )
