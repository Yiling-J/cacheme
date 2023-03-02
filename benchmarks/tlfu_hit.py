from dataclasses import dataclass

from asyncpg.connection import asyncio

from benchmarks.zipf import Zipf
from cacheme.core import get, stats
from cacheme.data import register_storage
from cacheme.interfaces import Metrics
from cacheme.models import Cache, Node
from cacheme.storages import Storage


@dataclass
class FooNode(Node):
    uid: int

    def key(self) -> str:
        return f"uid:{self.uid}"

    async def load(self) -> str:
        return "test"

    class Meta(Node.Meta):
        version = "v1"
        caches = [Cache(storage="local", ttl=None)]


async def simple_get(i: int):
    result = await get(FooNode(uid=i))
    assert result == "test"


async def bench_size(size: int, policy: str):
    await register_storage("local", Storage(url=f"local://{policy}", size=size))
    z = Zipf(1.0001, 10, 100000)
    FooNode.Meta.metrics = Metrics()
    for i in range(10000):
        await asyncio.gather(*[simple_get(z.get()) for j in range(100)])
    s = stats(FooNode)
    result = {
        "policy": policy,
        "size": size,
        "hit_rate": s.hit_rate(),
    }
    return result


async def bench_hits():
    for size in range(500, 100000, 2000):
        r1 = await bench_size(size, "tlfu")
        r2 = await bench_size(size, "lru")
        print(r1)
        print(r2)


async def infinit_run():
    await register_storage("local", Storage(url=f"local://tlfu", size=10000))
    count = 0
    await get(FooNode(uid=1))
    while True:
        data = await get(FooNode(uid=1))
        assert data == "test"
        count += 1
        if count % 100000 == 0:
            print(f"finish {count // 100000}")


# asyncio.run(bench_hits())
asyncio.run(infinit_run())
