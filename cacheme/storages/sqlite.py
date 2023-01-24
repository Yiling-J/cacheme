import asyncio
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, cast
from urllib.parse import urlparse

from cacheme.interfaces import Cachable, CachedData
from cacheme.serializer import Serializer
from cacheme.storages.sqldb import SQLStorage


class SQLiteStorage(SQLStorage):
    def __init__(self, address: str, table: str, pool_size: int = 10):
        super().__init__(address, table=table)
        url = urlparse(self.address)
        db = url.path[1:]
        self.db = db
        self.sem = asyncio.BoundedSemaphore(pool_size)
        self.pool: List[sqlite3.Connection] = []
        self.table = table

    async def _connect(self):
        conn = sqlite3.connect(
            self.db, isolation_level=None, timeout=30, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        cur = conn.execute("pragma journal_mode=wal")
        cur.close()
        self.writer = conn

    async def execute_ddl(self, ddl):
        with sqlite3.connect(self.db, isolation_level=None) as conn:
            conn.execute(ddl)

    def serialize(
        self, node: Cachable, raw: Any, serializer: Optional[Serializer]
    ) -> CachedData:
        data = raw["value"]
        if serializer is not None:
            data = serializer.loads(cast(bytes, raw["value"]))
        updated_at = datetime.fromisoformat(raw["updated_at"])
        expire = None
        if raw["expire"] != None:
            expire = datetime.fromisoformat(raw["expire"]).replace(tzinfo=timezone.utc)
        return CachedData(
            node=node,
            data=data,
            updated_at=updated_at.replace(tzinfo=timezone.utc),
            expire=expire,
        )

    def get_connection(self) -> sqlite3.Connection:
        if len(self.pool) > 0:
            return self.pool.pop(0)
        conn = sqlite3.connect(
            self.db, isolation_level=None, timeout=30, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        return conn

    def sync_get_by_key(
        self,
        key: str,
    ) -> Any:
        conn = self.get_connection()
        cur = conn.execute(
            f"select * from {self.table} where key=?",
            (key,),
        )
        data = cur.fetchone()
        cur.close()
        self.pool.append(conn)
        return data

    def sync_remove_by_key(self, key: str):
        cur = self.writer.execute(
            f"delete from {self.table} where key=?",
            (key,),
        )
        cur.close()

    def sync_get_by_keys(
        self,
        keys: List[str],
    ) -> Dict[str, Any]:
        conn = self.get_connection()
        sql = (
            f"SELECT * FROM {self.table} WHERE key in ({', '.join('?' for _ in keys)})"
        )
        cur = conn.execute(sql, keys)
        data = cur.fetchall()
        cur.close()
        self.pool.append(conn)
        return {i["key"]: i for i in data}

    def sync_set_data(
        self,
        key: str,
        value: Any,
        expire: Optional[datetime],
    ):
        cur = self.writer.execute(
            f"insert into {self.table}(key, value, expire) values(?,?,?) on conflict(key) do update set value=EXCLUDED.value, expire=EXCLUDED.expire",
            (
                key,
                value,
                expire,
            ),
        )
        cur.close()

    def sync_set_data_batch(
        self,
        data: Dict[str, Any],
        expire: Optional[datetime],
    ):
        cur = self.writer.executemany(
            f"insert into {self.table}(key, value, expire) values(?,?,?) on conflict(key) do update set value=EXCLUDED.value, expire=EXCLUDED.expire",
            [
                (
                    key,
                    value,
                    expire,
                )
                for key, value in data.items()
            ],
        )
        cur.close()

    async def get_by_key(self, key: str) -> Any:
        await self.sem.acquire()
        if sys.version_info >= (3, 9):
            data = await asyncio.to_thread(self.sync_get_by_key, key)
        else:
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, self.sync_get_by_key, key)
        self.sem.release()
        return data

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        self.sync_set_data(key, value, expire)

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        await self.sem.acquire()
        if sys.version_info >= (3, 9):
            data = await asyncio.to_thread(self.sync_get_by_keys, keys)
        else:
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, self.sync_get_by_keys, keys)
        self.sem.release()
        return data

    async def set_by_keys(self, data: Dict[str, Any], ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        self.sync_set_data_batch(data, expire)

    async def remove_by_key(self, key: str):
        self.sync_remove_by_key(key)
