import asyncio
import json
import uuid
from dataclasses import dataclass
from typing import Callable, ClassVar, Dict, List

import pytest


from benchmarks.zipf import Zipf
from cacheme import Cache, Node, Storage, get, get_all, register_storage
from cacheme.serializer import MsgPackSerializer
from tests.utils import setup_storage
from random import sample
from time import time

REQUESTS = 10000


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


async def worker(queue):
    while True:
        try:
            task = queue.get_nowait()
        except:
            return
        await task
        queue.task_done()


async def bench_run(queue):
    for f in queue:
        await f


async def bench_run_concurrency(queue, workers):
    await asyncio.gather(*[worker(queue) for _ in range(workers)])


@pytest.fixture(params=[500, 2000, 5000, 10000])
def workers(request):
    return int(request.param)


@pytest.fixture(params=["local-tlfu", "sqlite", "redis", "mongo", "postgres", "mysql"])
def storage_provider(request):
    @dataclass
    class FooNode(Node):
        uid: int
        payload_fn: ClassVar[Callable]
        uuid: ClassVar[int]
        sleep = False

        def key(self) -> str:
            return f"uid:{self.uid}:{self.uuid}"

        async def load(self) -> Dict:
            if self.sleep:
                await asyncio.sleep(0.1)
            return self.payload_fn(self.uid)

        class Meta(Node.Meta):
            version = "v1"
            caches = [Cache(storage="test", ttl=None)]
            serializer = MsgPackSerializer()

    storages = {
        "local-lru": lambda table, size: Storage(url="local://lru", size=size),
        "local-tlfu": lambda table, size: Storage(url="local://tlfu", size=size),
        "sqlite": lambda table, _: Storage(
            f"sqlite:///{table}",
            table="test",
            pool_size=10,
        ),
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
def test_read_only_async(benchmark, storage_provider, payload):
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
        for i in range(REQUESTS):
            queue.append(simple_get(Node, i))
        # warm cache first because this is read only test
        loop.run_until_complete(bench_run(queue))
        queue.clear()
        for i in range(REQUESTS):
            queue.append(simple_get(Node, i))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run(queue)),
        setup=setup,
        rounds=1,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request contains 3 operations: a miss get -> load from source -> set result to cache
def test_write_only_async(benchmark, storage_provider, payload):
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
        rounds=1,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request use a random zipf number: read >> write, size limit to REQUESTS//10
def test_zipf_async(benchmark, storage_provider, payload):
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
        rounds=1,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request use a random zipf number: read >> write, cache capacity limit to REQUESTS//10
# the load function will sleep 100 ms
# requests are processed simultaneously using worker
def test_zipf_async_concurrency(benchmark, storage_provider, payload, workers):
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

    def setup():
        queue = asyncio.Queue()
        z = Zipf(1.0001, 10, REQUESTS)
        for _ in range(REQUESTS):
            queue.put_nowait(simple_get(Node, z.get()))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run_concurrency(queue, workers)),
        setup=setup,
        rounds=1,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request contains 1 get_all with 20 nodes
def test_read_only_batch_async(benchmark, storage_provider, payload):
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
        for i in range(REQUESTS):
            queue.append(simple_get(Node, i))
        # warm cache first because this is read only test
        loop.run_until_complete(bench_run(queue))
        queue.clear()
        for i in range(REQUESTS):
            queue.append(simple_get_all(Node, sample(range(REQUESTS), 20)))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run(queue)),
        setup=setup,
        rounds=1,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()


# each request use 20 unique random zipf number: read >> write, cache capacity limit to REQUESTS//10
# the load function will sleep 100 ms
# requests are processed simultaneously using worker
def test_zipf_async_batch_concurrency(benchmark, storage_provider, payload, workers):
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

    def setup():

        # get 20 unique zipf numbers
        def get20(z):
            l = set()
            while True:
                l.add(z.get())
                if len(l) == 20:
                    break
            return list(l)

        queue = asyncio.Queue()
        z = Zipf(1.0001, 10, REQUESTS)
        for _ in range(REQUESTS):
            queue.put_nowait(simple_get_all(Node, get20(z)))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_run_concurrency(queue, workers)),
        setup=setup,
        rounds=1,
    )
    loop.run_until_complete(storage.close())
    asyncio.events.set_event_loop(None)
    loop.close()
