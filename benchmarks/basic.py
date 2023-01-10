import asyncio
import json
import random
import time
from dataclasses import dataclass
from typing import Dict, List

from benchmarks.zipf import Zipf
from cacheme.core import get
from cacheme.data import _storages, init_tag_storage
from cacheme.data import init_storages
from cacheme.interfaces import Serializer
from cacheme.interfaces import Storage as StorageP
from cacheme.models import Metrics, Node
from cacheme.serializer import (
    CompressedJSONSerializer,
    CompressedMsgPackSerializer,
    CompressedPickleSerializer,
    JSONSerializer,
    MsgPackSerializer,
    PickleSerializer,
)
from cacheme.storages import Storage

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


async def setup_storage():
    storages: Dict[str, StorageP] = {
        "local": Storage(url="tlfu://", size=1000),
        "sqlite": Storage(
            f"sqlite:///test{random.randint(0, 50000)}",
            initialize=True,
            pool_size=10,
        ),
        "mysql": Storage("mysql://username:password@localhost:3306/test"),
        "postgres": Storage("postgresql://username:password@127.0.0.1:5432/test"),
        "redis": Storage("redis://localhost:6379"),
        "mongo": Storage("mongodb://test:password@localhost:27017"),
    }
    await init_storages(storages)
    await init_tag_storage(Storage("redis://localhost:6379"))


def update_node(serializer: str, storage: str, compressed: bool, payload_size: str):
    global payload
    serializers: Dict[str, Serializer] = {
        "pickle": PickleSerializer(),
        "json": JSONSerializer(),
        "msgpack": MsgPackSerializer(),
    }
    compressed_serializer: Dict[str, Serializer] = {
        "pickle": CompressedPickleSerializer(),
        "json": CompressedJSONSerializer(),
        "msgpack": CompressedMsgPackSerializer(),
    }
    if compressed:
        s = compressed_serializer[serializer]
    else:
        s = serializers[serializer]
    FooNode.Meta.serializer = s  # type: ignore
    FooNode.Meta.metrics = Metrics()
    FooNode.Meta.storage = storage
    if payload_size == "large":
        with open("benchmarks/large.json") as f:
            content = f.read()
            content_json = json.loads(content)
        payload = lambda uid: {"uid": uid, "data": content_json}
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
    z = Zipf(1.0001, 10, requests // 10)
    update_node(serializer, storage, compressed, payload_size)
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
    await setup_storage()
    print("========== READ+WRITE ==========")
    await bench_zipf(10000, "local", "msgpack", False)
    await bench_zipf(10000, "redis", "msgpack", False)
    await bench_zipf(10000, "mongo", "msgpack", False)
    await bench_zipf(10000, "postgres", "msgpack", False)
    await bench_zipf(10000, "mysql", "msgpack", False)
    # await bench_zipf(1000000, "sqlite", "msgpack", False)

    print("========== READ+TAG ==========")
    FooNode.tags = lambda self: [f"t:{self.uid}"]
    await bench_zipf(10000, "local", "msgpack", False)
    await bench_zipf(10000, "redis", "msgpack", False)
    await bench_zipf(10000, "mongo", "msgpack", False)
    await bench_zipf(10000, "postgres", "msgpack", False)
    await bench_zipf(10000, "mysql", "msgpack", False)
    FooNode.tags = lambda self: []

    print("========== READ+WRITE LARGE ==========")
    FooNode.Meta.version = "v2"
    _storages["local"] = Storage(url="tlfu://", size=1000)
    await bench_zipf(10000, "local", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "redis", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "mongo", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "postgres", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "mysql", "msgpack", False, payload_size="large")
    # await bench_zipf(10000, "sqlite", "msgpack", False, payload_size="large")

    # read only
    print("========== READ LARGE ==========")
    await bench_zipf(10000, "local", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "redis", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "mongo", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "postgres", "msgpack", False, payload_size="large")
    await bench_zipf(10000, "mysql", "msgpack", False, payload_size="large")
    # await bench_zipf(10000, "sqlite", "msgpack", False, payload_size="large")
    return


asyncio.run(bench_all())
