import asyncio
import json
import uuid
from dataclasses import dataclass
from random import sample
from time import time
from typing import Callable, ClassVar, Dict, List

import pytest

from benchmarks.zipf import Zipf
from cacheme import Cache, Node, Storage, get, get_all, register_storage
from cacheme.serializer import MsgPackSerializer
from tests.utils import setup_storage

REQUESTS = 1000


async def storage_init(storage):
    if not isinstance(storage, Storage):
        return
    await register_storage("test", storage)
    await setup_storage(storage._storage)


async def simple_get(Node: Callable, i: int):
    result = await get(Node(uid=i))
    assert result["uid"] == i


async def simple_get_all(Node: Callable, l: List[int]):
    result = await get_all([Node(uid=i) for i in l])
    assert [r["uid"] for r in result] == l


async def bench_run(queue):
    for f in queue:
        await f


@pytest.fixture(
    params=[
        "theine-tlfu",
        "redis",
        "mongo",
        "postgres",
        "mysql",
    ]
)
def storage_provider(request):
    @dataclass
    class FooNode(Node):
        uid: int
        payload_fn: ClassVar[Callable]
        uuid: ClassVar[int]

        def key(self) -> str:
            return f"uid:{self.uid}:{self.uuid}"

        async def load(self) -> Dict:
            return self.payload_fn(self.uid)

        class Meta(Node.Meta):
            version = "v1"
            caches = [Cache(storage="test", ttl=None)]
            serializer = MsgPackSerializer()

    storages = {
        "theine-tlfu": lambda table, size: Storage(url="local://tlfu", size=size),
        "mysql": lambda table, _: Storage(
            "mysql://username:password@localhost:3306/test", table=table
        ),
        "postgres": lambda table, _: Storage(
            "postgresql://username:password@127.0.0.1:5432/test", table=table
        ),
        "redis": lambda table, _: Storage("redis://localhost:6379"),
        "mongo": lambda table, _: Storage(
            "mongodb://test:password@localhost:27017",
            database="test",
            collection=table,
        ),
    }
    yield {
        "storage": storages[request.param],
        "name": request.param,
        "node_cls": FooNode,
    }


@pytest.fixture(params=["small", "medium", "large"])
def payload(request):
    with open(f"benchmarks/{request.param}.json") as f:
        content = f.read()
        content_json = json.loads(content)
    return {
        "fn": lambda _, uid: {"uid": uid, "data": content_json},
        "name": request.param,
    }


# each request contains 1 operation: a hit get
def test_read_only(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    Node = storage_provider["node_cls"]
    Node.payload_fn = payload["fn"]
    Node.uuid = _uuid
    storage = storage_provider["storage"](table, REQUESTS)
    loop.run_until_complete(storage_init(storage))
    queue = []
    for i in range(REQUESTS):
        queue.append(simple_get(Node, i))
    loop.run_until_complete(bench_run(queue))

    def setup():
        queue = []
        for i in range(REQUESTS):
            queue.append(simple_get(Node, i))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run(queue)),
        setup=setup,
        rounds=3,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request contains 3 operations: a miss get -> load from source -> set result to cache
def test_write_only(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    Node = storage_provider["node_cls"]
    Node.payload_fn = payload["fn"]
    Node.uuid = _uuid
    storage = storage_provider["storage"](table, REQUESTS)
    loop.run_until_complete(storage_init(storage))

    def setup():
        queue = []
        rand = int(time())
        for i in range(REQUESTS):
            queue.append(simple_get(Node, rand + i))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run(queue)),
        setup=setup,
        rounds=3,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request use a random zipf number: read >> write, size limit to REQUESTS//10
def test_zipf(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    Node = storage_provider["node_cls"]
    Node.payload_fn = payload["fn"]
    Node.uuid = _uuid
    storage = storage_provider["storage"](table, REQUESTS // 10)
    loop.run_until_complete(storage_init(storage))

    def setup():
        queue = []
        z = Zipf(1.0001, 10, REQUESTS)
        for _ in range(REQUESTS):
            queue.append(simple_get(Node, z.get()))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run(queue)),
        setup=setup,
        rounds=3,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request use 20 unique random numbers already in cache
# REQUESTS // 10 requests to make benchmark run fast
def test_read_only_batch(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    Node = storage_provider["node_cls"]
    Node.payload_fn = payload["fn"]
    Node.uuid = _uuid
    Node.sleep = True
    storage = storage_provider["storage"](table, REQUESTS // 10)
    loop.run_until_complete(storage_init(storage))
    queue = []
    for i in range(REQUESTS // 10):
        queue.append(simple_get(Node, i))
    loop.run_until_complete(bench_run(queue))

    def setup():

        queue = []
        for _ in range(REQUESTS // 10):
            queue.append(simple_get_all(Node, sample(range(REQUESTS // 10), 20)))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run(queue)),
        setup=setup,
        rounds=3,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()
