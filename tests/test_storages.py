import os
import random
from asyncio import sleep
from dataclasses import dataclass
from datetime import timedelta

import pytest
from cacheme.core import invalidate, refresh

from cacheme.models import Node
from cacheme.serializer import PickleSerializer
from cacheme.storages.local import LocalStorage
from cacheme.storages.mongo import MongoStorage
from cacheme.storages.mysql import MySQLStorage
from cacheme.storages.postgres import PostgresStorage
from cacheme.storages.redis import RedisStorage
from cacheme.storages.sqlite import SQLiteStorage
from tests.utils import setup_storage


@dataclass
class FooNode(Node):
    id: str

    def key(self) -> str:
        return f"{self.id}"

    class Meta(Node.Meta):
        version = "v1"
        storage = "local"


@pytest.mark.parametrize(
    "storage",
    [
        {"s": LocalStorage(200, "local://tlfu"), "local": True},
        {
            "s": SQLiteStorage(
                f"sqlite:///test{random.randint(0, 50000)}",
                table="data",
            ),
            "local": True,
        },
        {
            "s": MySQLStorage(
                "mysql://username:password@localhost:3306/test",
                table="data",
            ),
            "local": False,
        },
        {
            "s": PostgresStorage(
                f"postgresql://username:password@127.0.0.1:5432/test",
                table="data",
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
                "mongodb://test:password@localhost:27017",
                database="test",
                collection="data",
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
    await setup_storage(s)
    node = FooNode(id="foo")
    await s.set(
        node=node,
        value={"foo": "bar"},
        ttl=timedelta(days=10),
        serializer=PickleSerializer(),
    )
    result = await s.get(node, serializer=PickleSerializer())
    assert result is not None
    assert result.data == {"foo": "bar"}

    # expire test
    node = FooNode(id="foo_expire")
    await s.set(
        node=node,
        value={"foo": "bar"},
        ttl=timedelta(seconds=1),
        serializer=PickleSerializer(),
    )
    await sleep(2)
    result = await s.get(node, serializer=PickleSerializer())
    assert result is None

    # get/set all
    nodes = []
    result = await s.get_all(nodes, PickleSerializer())
    assert result == []
    data = []
    for i in [3, 1, 2]:
        node = FooNode(id=f"foo-{i}")
        nodes.append(node)
        data.append((node, f"bar-{i}"))
    nodes.append(FooNode(id=f"foo-foo"))
    await s.set_all(data, ttl=timedelta(seconds=1), serializer=PickleSerializer())
    result = await s.get_all(nodes, PickleSerializer())
    assert len(result) == 3
    assert {r[0].key() for r in result} == {"foo-3", "foo-1", "foo-2"}
    assert {r[1].data for r in result} == {
        "bar-3",
        "bar-1",
        "bar-2",
    }

    # invalidate
    node = FooNode(id="invalidate")
    await s.set(
        node=node,
        value={"foo": "bar"},
        ttl=timedelta(days=10),
        serializer=PickleSerializer(),
    )
    result = await s.get(node, serializer=PickleSerializer())
    assert result is not None
    await s.remove(node)
    result = await s.get(node, serializer=PickleSerializer())
    assert result is None

    if filename != "":
        os.remove(filename)
        os.remove(f"{filename}-shm")
        os.remove(f"{filename}-wal")
