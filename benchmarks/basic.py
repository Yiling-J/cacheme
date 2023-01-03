import time
import asyncio
from dataclasses import dataclass
from typing import Dict, List

from benchmarks.zipf import Zipf
from cacheme.v2.models import Node
from cacheme.v2.serializer import MsgPackSerializer
from cacheme.v2.core import get, init_storages
from cacheme.v2.storages.local import TLFUStorage
from cacheme.v2.storages.postgres import PostgresStorage
from cacheme.v2.storages.redis import RedisStorage


@dataclass
class FooNode(Node):
    uid: int

    def key(self) -> str:
        return f"uid:{self.uid}"

    async def load(self) -> Dict:
        return {"uid": self.uid}

    def tags(self) -> List[str]:
        return []

    class Meta(Node.Meta):
        version = "v1"
        storage = "redis"
        serializer = MsgPackSerializer()


async def simple_get(i: int):
    result = await get(FooNode(uid=i))
    assert result["uid"] == i


async def bench_zipf(n):
    z = Zipf(1.0001, 10, n * 10)
    await init_storages(
        {
            "local": TLFUStorage(n),
            "sqlite": PostgresStorage(
                "postgresql://postgres:password@127.0.0.1:5432/postgres",
                initialize=False,
            ),
            "redis": RedisStorage("redis://127.0.0.1:6379", pool_size=100),
        }
    )
    now = time.time_ns()
    await asyncio.gather(*[simple_get(z.get()) for i in range(100 * n)])
    with open("result.txt", "w") as f:
        print("spent:", time.time_ns() - now)
        f.write(f"spent: {time.time_ns() - now}\n")
        f.write(str(FooNode.Meta.metrics.__dict__))


asyncio.run(bench_zipf(100))
