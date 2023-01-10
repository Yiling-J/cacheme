import asyncio
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple, cast, Dict

from sqlalchemy.engine import make_url
from cacheme.interfaces import Cachable, CachedData

from cacheme.serializer import Serializer
from cacheme.storages.sqldb import SQLStorage


class SQLiteStorage(SQLStorage):
    def __init__(self, address: str, initialize: bool = False, pool_size: int = 10):
        super().__init__(address, initialize=initialize)
        dsn = make_url(self.address)
        self.db = dsn.database or ""
        self.sem = asyncio.BoundedSemaphore(pool_size)
        self.pool: List[sqlite3.Connection] = []

    async def _connect(self):
        pass

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

    async def execute_ddl(self, ddl):
        with sqlite3.connect(self.db, isolation_level=None) as conn:
            conn.execute(ddl)

    def sync_get_by_key(
        self, key: str, conn: Optional[sqlite3.Connection]
    ) -> Tuple[sqlite3.Connection, Any]:
        cur = None
        if conn is None:
            conn = sqlite3.connect(
                self.db, isolation_level=None, timeout=30, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("pragma journal_mode=wal")
        if cur is None:
            cur = conn.cursor()
        cur.execute("select * from cacheme_data where key=?", (key,))
        data = cur.fetchone()
        cur.close()
        return conn, data

    def sync_get_by_keys(
        self,
        keys: List[str],
        conn: Optional[sqlite3.Connection],
        fields: List[str] = [],
    ) -> Tuple[sqlite3.Connection, Dict[str, Any]]:
        cur = None
        tmp = "*"
        if len(fields) > 0:
            tmp = f"({','.join(fields)})"
        if conn is None:
            conn = sqlite3.connect(
                self.db, isolation_level=None, timeout=30, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("pragma journal_mode=wal")
        if cur is None:
            cur = conn.cursor()
        sql = "SELECT {0} FROM cacheme_data WHERE key in ({1})".format(
            tmp, ", ".join("?" for _ in keys)
        )
        cur.execute(sql, keys)
        data = cur.fetchall()
        cur.close()
        return conn, {i["key"]: i for i in data}

    def sync_set_data(
        self,
        key: str,
        value: Any,
        expire: Optional[datetime],
        conn: Optional[sqlite3.Connection],
    ) -> sqlite3.Connection:
        cur = None
        if conn is None:
            conn = sqlite3.connect(
                self.db, isolation_level=None, timeout=30, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("pragma journal_mode=wal")
        if cur is None:
            cur = conn.cursor()
        cur.execute(
            "insert into cacheme_data(key, value, expire) values(?,?,?) on conflict(key) do update set value=EXCLUDED.value, expire=EXCLUDED.expire",
            (
                key,
                value,
                expire,
            ),
        )
        cur.close()
        return conn

    async def get_by_key(self, key: str) -> Any:
        await self.sem.acquire()
        if len(self.pool) > 0:
            conn = self.pool.pop(0)
        else:
            conn = None
        if sys.version_info >= (3, 9):
            conn, data = await asyncio.to_thread(self.sync_get_by_key, key, conn)
        else:
            loop = asyncio.get_running_loop()
            conn, data = await loop.run_in_executor(
                None, self.sync_get_by_key, key, conn
            )
        self.pool.append(cast(sqlite3.Connection, conn))
        self.sem.release()
        return data

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        await self.sem.acquire()
        if len(self.pool) > 0:
            conn = self.pool.pop(0)
        else:
            conn = None
        if sys.version_info >= (3, 9):
            conn = await asyncio.to_thread(self.sync_set_data, key, value, expire, conn)
        else:
            loop = asyncio.get_running_loop()
            conn = await loop.run_in_executor(
                None, self.sync_set_data, key, value, expire, conn
            )
        self.pool.append(cast(sqlite3.Connection, conn))
        self.sem.release()

    async def get_by_keys(
        self, keys: List[str], fields: List[str] = []
    ) -> Dict[str, Any]:
        await self.sem.acquire()
        if len(self.pool) > 0:
            conn = self.pool.pop(0)
        else:
            conn = None
        if sys.version_info >= (3, 9):
            conn, data = await asyncio.to_thread(self.sync_get_by_keys, keys, conn)
        else:
            loop = asyncio.get_running_loop()
            conn, data = await loop.run_in_executor(
                None, self.sync_get_by_keys, keys, conn, fields
            )
        self.pool.append(cast(sqlite3.Connection, conn))
        self.sem.release()
        return data
