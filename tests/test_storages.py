from asyncio import sleep
import os
import pytest
import random
from cacheme.v2.storage import *


@pytest.mark.parametrize(
    "storage",
    [
        {"s": TLFUStorage(200), "local": True},
        {
            "s": SQLStorage(
                f"sqlite+aiosqlite:///test{random.randint(0, 50000)}",
                migrate=True,
            ),
            "local": True,
        },
        {
            "s": RedisStorage(
                "redis://localhost:6379",
            ),
            "local": False,
        },
        {
            "s": MongoStorage("mongodb://test:password@localhost:27017", migrate=True),
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
    if hasattr(s, "database") and "sqlite" in s.database.url._url:
        filename = s.database._backend._pool._url.database
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
    assert result == {"foo": "bar"}

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
    assert result == None
    if filename != "":
        os.remove(filename)
