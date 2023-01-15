from dataclasses import dataclass
import os
import random
from asyncio import sleep
from datetime import timedelta

import pytest

from cacheme.models import Node
from cacheme.serializer import PickleSerializer
from cacheme.storages.local import TLFUStorage
from cacheme.storages.mongo import MongoStorage
from cacheme.storages.mysql import MySQLStorage
from cacheme.storages.postgres import PostgresStorage
from cacheme.storages.redis import RedisStorage
from cacheme.storages.sqlite import SQLiteStorage


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

    nodes = []
    result = await s.get_all(nodes, PickleSerializer())
    assert result == []

    for i in [3, 1, 2]:
        node = FooNode(id=f"foo-{i}")
        await s.set(
            node=node,
            value=f"bar-{i}",
            ttl=timedelta(seconds=1),
            serializer=PickleSerializer(),
        )
        nodes.append(node)
    nodes.append(FooNode(id=f"foo-foo"))
    result = await s.get_all(nodes, PickleSerializer())
    assert len(result) == 3
    assert {r[0].key() for r in result} == {"foo-3", "foo-1", "foo-2"}
    assert {r[1].data for r in result} == {
        "bar-3",
        "bar-1",
        "bar-2",
    }

    if filename != "":
        os.remove(filename)
        os.remove(f"{filename}-shm")
        os.remove(f"{filename}-wal")
