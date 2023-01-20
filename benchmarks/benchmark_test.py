import pytest
import uuid
import asyncio
import json
from cacheme.core import get, get_all
from cacheme.serializer import MsgPackSerializer
from cacheme.storages import Storage
from benchmarks.zipf import Zipf
from cacheme.models import Node
from cacheme.data import register_storage
from dataclasses import dataclass
from tests.utils import setup_storage
from typing import Callable, ClassVar, Dict


REQUESTS = 10000


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
        storage = "test"
        serializer = MsgPackSerializer()


async def simple_get(i: int):
    result = await get(FooNode(uid=i))
    assert result["uid"] == i


async def simple_get_all(i: int):
    uids = [i for i in range(i, i + 10)]
    result = await get_all([FooNode(uid=i) for i in uids])
    assert [r["uid"] for r in result] == uids


async def bench_with_zipf(tasks):
    await asyncio.gather(*tasks)


@pytest.fixture(
    params=["local-lru", "local-tlfu", "redis", "mongo", "postgres", "mysql"]
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
    return storages[request.param]


@pytest.fixture(params=["small", "medium", "large"])
def payload(request):
    with open(f"benchmarks/{request.param}.json") as f:
        content = f.read()
        content_json = json.loads(content)
    return lambda _, uid: {"uid": uid, "data": content_json}


def test_read_write_async(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider(table)
    FooNode.payload_fn = payload
    FooNode.uuid = _uuid
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        return ([simple_get(z.get()) for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda tasks: loop.run_until_complete(bench_with_zipf(tasks)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()


def test_read_only_async(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider(table)
    FooNode.payload_fn = payload
    FooNode.uuid = _uuid
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)
    # fill data
    loop.run_until_complete(bench_with_zipf([simple_get(i) for i in range(REQUESTS)]))

    def setup():
        return ([simple_get(z.get()) for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda tasks: loop.run_until_complete(bench_with_zipf(tasks)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()


def test_read_write_batch_async(benchmark, storage_provider, payload):
    loop = asyncio.events.new_event_loop()
    asyncio.events.set_event_loop(loop)
    _uuid = uuid.uuid4().int
    table = f"test_{_uuid}"
    storage = storage_provider(table)
    FooNode.payload_fn = payload
    FooNode.uuid = _uuid
    loop.run_until_complete(storage_init(storage))
    z = Zipf(1.0001, 10, REQUESTS // 10)

    def setup():
        return ([simple_get_all(z.get()) for _ in range(REQUESTS)],), {}

    benchmark.pedantic(
        lambda tasks: loop.run_until_complete(bench_with_zipf(tasks)),
        setup=setup,
        rounds=3,
    )
    asyncio.events.set_event_loop(None)
    loop.close()
