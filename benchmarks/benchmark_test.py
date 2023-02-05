import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, ClassVar, Dict, List

import pytest

from benchmarks.zipf import Zipf
from cacheme import Cache, Node, Storage, get, get_all, register_storage
from cacheme.serializer import MsgPackSerializer
from tests.utils import setup_storage

REQUESTS = 10000
WORKERS = 20


async def storage_init(storage):
    await register_storage("test", storage)
    await setup_storage(storage._storage)


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


async def simple_get(i: int):
    result = await get(FooNode(uid=i))
    assert result["uid"] == i


async def simple_get_all(l: List[int]):
    result = await get_all([FooNode(uid=i) for i in l])
    assert [r["uid"] for r in result] == l


async def worker(queue):
    while True:
        try:
            task = queue.get_nowait()
        except:
            return
        await task
        queue.task_done()


async def bench_with_zipf(queue):
    for _ in range(WORKERS):
        asyncio.create_task(worker(queue))
    await queue.join()


@pytest.fixture(
    params=["local-lru", "local-tlfu", "sqlite", "redis", "mongo", "postgres", "mysql"]
)
def storage_provider(request):
    storages = {
        "local-lru": lambda table: Storage(url="local://lru", size=REQUESTS // 10),
        "local-tlfu": lambda table: Storage(url="local://tlfu", size=REQUESTS // 10),
        "sqlite": lambda table: Storage(
            f"sqlite:///{table}",
            table="test",
            pool_size=10,
        ),
        "mysql": lambda table: Storage(
            "mysql://username:password@localhost:3306/test", table=table
        ),
        "postgres": lambda table: Storage(
            "postgresql://username:password@127.0.0.1:5432/test", table=table
        ),
        "redis": lambda table: Storage("redis://localhost:6379"),
        "mongo": lambda table: Storage(
            "mongodb://test:password@localhost:27017",
            database="test",
            collection=table,
        ),
    }
    return {"storage": storages[request.param], "name": request.param}


@pytest.fixture(params=["small", "medium", "large"])
def payload(request):
    with open(f"benchmarks/{request.param}.json") as f:
        content = f.read()
        content_json = json.loads(content)
    return {
        "fn": lambda _, uid: {"uid": uid, "data": content_json},
        "name": request.param,
    }


def test_read_write_async(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider["storage"](table)
    FooNode.payload_fn = payload["fn"]
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        FooNode.uuid = uuid.uuid4().int
        queue = asyncio.Queue()
        for _ in range(REQUESTS):
            queue.put_nowait(simple_get(z.get()))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_with_zipf(queue)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()


def test_read_write_with_local_async(benchmark, storage_provider, payload):
    if storage_provider["name"] not in {"redis", "mongo", "postgres"}:
        pytest.skip("skip")
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    loop.run_until_complete(
        register_storage("local", Storage(url="local://tlfu", size=650))
    )
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider["storage"](table)
    FooNode.payload_fn = payload["fn"]
    FooNode.Meta.caches = [
        Cache(storage="local", ttl=timedelta(seconds=10)),
        Cache(storage="test", ttl=None),
    ]
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        FooNode.uuid = uuid.uuid4().int
        queue = asyncio.Queue()
        for _ in range(REQUESTS):
            queue.put_nowait(simple_get(z.get()))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_with_zipf(queue)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()
    FooNode.Meta.caches = [
        Cache(storage="test", ttl=None),
    ]


def test_read_only_async(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider["storage"](table)
    FooNode.payload_fn = payload["fn"]
    FooNode.uuid = _uuid
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)
    # fill data
    queue = asyncio.Queue()
    for _ in range(REQUESTS * 2):
        queue.put_nowait(simple_get(z.get()))
    loop.run_until_complete(bench_with_zipf(queue))

    def setup():
        queue = asyncio.Queue()
        for _ in range(REQUESTS):
            queue.put_nowait(simple_get(z.get()))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_with_zipf(queue)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()


def test_read_only_with_local_async(benchmark, storage_provider, payload):
    if storage_provider["name"] not in {"redis", "mongo", "postgres"}:
        pytest.skip("skip")
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    loop.run_until_complete(
        register_storage("local", Storage(url="local://tlfu", size=650))
    )
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider["storage"](table)
    FooNode.payload_fn = payload["fn"]
    FooNode.uuid = _uuid
    FooNode.Meta.caches = [
        Cache(storage="local", ttl=timedelta(seconds=120)),
        Cache(storage="test", ttl=None),
    ]
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)
    queue = asyncio.Queue()
    for _ in range(REQUESTS * 2):
        queue.put_nowait(simple_get(z.get()))
    loop.run_until_complete(bench_with_zipf(queue))

    def setup():
        queue = asyncio.Queue()
        for _ in range(REQUESTS):
            queue.put_nowait(simple_get(z.get()))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_with_zipf(queue)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()
    FooNode.Meta.caches = [
        Cache(storage="test", ttl=None),
    ]


def test_read_write_batch_async(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider["storage"](table)
    FooNode.payload_fn = payload["fn"]
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        FooNode.uuid = uuid.uuid4().int

        def get20(z):
            l = set()
            while True:
                l.add(z.get())
                if len(l) == 20:
                    break
            return list(l)

        queue = asyncio.Queue()
        for _ in range(REQUESTS // 10):
            queue.put_nowait(simple_get_all(get20(z)))
        return (queue,), {}

    benchmark.pedantic(
        lambda queue: loop.run_until_complete(bench_with_zipf(queue)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()
