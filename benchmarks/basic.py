import time
import asyncio
from dataclasses import dataclass
from typing import Dict, List
from benchmarks.zipf import Zipf
from cacheme.v2.models import Node
from cacheme.v2.serializer import MsgPackSerializer
from cacheme.v2.core import get, init_storages, get_storage
from cacheme.v2.storage import TLFUStorage


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
        storage = "local"
        serializer = None


async def simple_get(i: int):
    result = await get(FooNode(uid=i))
    assert result["uid"] == i


async def bench_zipf(n):
    z = Zipf(1.0001, 10, n * 10)
    await init_storages({"local": TLFUStorage(n)})
    now = time.time_ns()
    await asyncio.gather(*[simple_get(z.get()) for i in range(n * 100)])
    with open("result.txt", "w") as f:
        f.write(f"spent: {time.time_ns() - now}\n")
        f.write(str(FooNode.Meta.metrics.__dict__))


asyncio.run(bench_zipf(10000))
