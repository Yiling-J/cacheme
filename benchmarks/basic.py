import time
import asyncio
import random
import json
from dataclasses import dataclass
from typing import Dict, List

from benchmarks.zipf import Zipf
from cacheme.v2.models import Node, Metrics
from cacheme.v2.serializer import (
    MsgPackSerializer,
    PickleSerializer,
    JSONSerializer,
    CompressedPickleSerializer,
    CompressedJSONSerializer,
    CompressedMsgPackSerializer,
)
from cacheme.v2.core import get, init_storages
from cacheme.v2.storages.local import TLFUStorage
from cacheme.v2.storages.sqlite import SQLiteStorage
from cacheme.v2.storages.postgres import PostgresStorage
from cacheme.v2.storages.redis import RedisStorage
from cacheme.v2.storages.mysql import MySQLStorage
from cacheme.v2.storages.mongo import MongoStorage

payload = lambda uid: {
    "uid": uid,
    "name": "test123",
    "tags": ["a", "b", "c"],
    "rating": 3,
}


@dataclass
class FooNode(Node):
    uid: int

    def key(self) -> str:
        return f"uid:{self.uid}"

    async def load(self) -> Dict:
        return payload(self.uid)

    def tags(self) -> List[str]:
        return []

    class Meta(Node.Meta):
        version = "v1"
        storage = "test"
        serializer = MsgPackSerializer()


async def simple_get(i: int):
    result = await get(FooNode(uid=i))
    assert result["uid"] == i


async def setup_storage(requests: int, storage: str):
    storages = {
        "local": TLFUStorage(requests // 10),
        "sqlite": SQLiteStorage(
            f"sqlite:///test{random.randint(0, 50000)}",
            initialize=True,
        ),
        "mysql": MySQLStorage("mysql://username:password@localhost:3306/test"),
        "postgres": PostgresStorage(
            "postgresql://username:password@127.0.0.1:5432/test"
        ),
        "redis": RedisStorage("redis://localhost:6379"),
        "mongo": MongoStorage("mongodb://test:password@localhost:27017"),
    }
    await init_storages({"test": storages[storage]})


def update_node(serializer: str, compressed: bool, payload_size: str):
    global payload
    serializers = {
        "pickle": PickleSerializer(),
        "json": JSONSerializer(),
        "msgpack": MsgPackSerializer(),
    }
    compressed_serializer = {
        "pickle": CompressedPickleSerializer(),
        "json": CompressedJSONSerializer(),
        "msgpack": CompressedMsgPackSerializer(),
    }
    if compressed:
        s = compressed_serializer[serializer]
    else:
        s = serializers[serializer]
    FooNode.Meta.serializer = s
    FooNode.Meta.metrics = Metrics()
    if payload_size == "large":
        with open("benchmarks/large.json") as f:
            content = f.read()
        payload = lambda uid: {"uid": uid, "data": json.loads(content)}
    else:
        payload = lambda uid: {
            "uid": uid,
            "name": "test123",
            "tags": ["a", "b", "c"],
            "rating": 3,
        }


async def bench_zipf(
    requests: int,
    storage: str,
    serializer: str,
    compressed: bool,
    payload_size: str = "small",
):
    z = Zipf(1.0001, 10, requests)
    update_node(serializer, compressed, payload_size)
    await setup_storage(requests, storage)
    now = time.time_ns()
    await asyncio.gather(*[simple_get(z.get()) for i in range(requests)])
    result = {
        "storage": storage,
        "serializer": serializer,
        "compressed": compressed,
        "requests": requests,
        "payload_size": payload_size,
        "spent": (time.time_ns() - now) / 1e9,
    }
    if storage == "local":
        result["serializer"] = None
    print(result)
    print(FooNode.Meta.metrics.__dict__)
    print("-" * 50)


async def bench_all():
    await bench_zipf(10000, "local", "msgpack", False)
    await bench_zipf(10000, "redis", "msgpack", False)
    await bench_zipf(10000, "mongo", "msgpack", False)
    await bench_zipf(10000, "postgres", "msgpack", False)
    await bench_zipf(10000, "mysql", "msgpack", False)

    await bench_zipf(10000, "local", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "redis", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "mongo", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "postgres", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "mysql", "msgpack", False, payload_size="large")


asyncio.run(bench_all())
