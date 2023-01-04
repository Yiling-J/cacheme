import os
import random
from asyncio import sleep
from datetime import timedelta

import pytest

from cacheme.models import CacheKey
from cacheme.serializer import PickleSerializer
from cacheme.storages.local import TLFUStorage
from cacheme.storages.mongo import MongoStorage
from cacheme.storages.mysql import MySQLStorage
from cacheme.storages.postgres import PostgresStorage
from cacheme.storages.redis import RedisStorage
from cacheme.storages.sqlite import SQLiteStorage


@pytest.mark.parametrize(
    "storage",
    [
        {"s": TLFUStorage(200), "local": True},
        {
            "s": SQLiteStorage(
                f"sqlite:///test{random.randint(0, 50000)}",
                initialize=True,
            ),
            "local": True,
        },
        {
            "s": MySQLStorage(
                "mysql://username:password@localhost:3306/test",
                initialize=True,
            ),
            "local": False,
        },
        {
            "s": PostgresStorage(
                f"postgresql://username:password@127.0.0.1:5432/test",
                initialize=True,
            ),
            "local": False,
        },
        {
            "s": RedisStorage(
                "redis://localhost:6379",
            ),
            "local": False,
        },
        {
            "s": MongoStorage(
                "mongodb://test:password@localhost:27017", initialize=True
            ),
            "local": False,
        },
    ],
)
@pytest.mark.asyncio
async def test_storages(storage):
    if storage["local"] is False and os.environ.get("CI") != "TRUE":
        return
    s = storage["s"]
    filename = ""
    if isinstance(s, SQLiteStorage):
        filename = s.address.split("///")[-1]
    await s.connect()
    key = CacheKey(
        node="foo",
        prefix="test",
        key="foo",
        version="v1",
        tags=[],
    )
    await s.set(
        key=key,
        value={"foo": "bar"},
        ttl=timedelta(days=10),
        serializer=PickleSerializer(),
    )
    result = await s.get(key, serializer=PickleSerializer())
    assert result is not None
    assert result.data == {"foo": "bar"}

    # expire test
    key = CacheKey(
        node="foo",
        prefix="test",
        key="foo_expire",
        version="v1",
        tags=[],
    )
    await s.set(
        key=key,
        value={"foo": "bar"},
        ttl=timedelta(seconds=1),
        serializer=PickleSerializer(),
    )
    await sleep(2)
    result = await s.get(key, serializer=PickleSerializer())
    assert result is None
    if filename != "":
        os.remove(filename)
        os.remove(f"{filename}-shm")
        os.remove(f"{filename}-wal")
