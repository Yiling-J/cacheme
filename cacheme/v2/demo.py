import asyncio
from dataclasses import dataclass
from cacheme.v2.models import *
from cacheme.v2.storage import SQLStorage, TLFUStorage
from cacheme.v2.serializer import PickleSerializer, MsgPackSerializer
from datetime import timedelta
from cacheme.v2.core import init_storages, init_tag_storage, get


@dataclass
class FooNode:
    user_id: str
    foo_id: str
    level: int

    def key(self) -> str:
        return f"{self.user_id}:{self.foo_id}:{self.level}"

    def fetch(self) -> dict[str, Any]:
        return {
            "a": self.user_id,
            "b": self.foo_id,
            "c": self.level,
        }

    def tags(self) -> list[str]:
        return [f"user:{self.user_id}", f"foo:{self.foo_id}"]

    class Meta:
        version = "v1"
        storage = "local"
        ttl = timedelta(seconds=20)
        local_cache = None
        serializer = MsgPackSerializer()
        doorkeeper = None


@dataclass
class BarNode:
    user_id: int

    def key(self) -> str:
        return f"{self.user_id}"

    def fetch(self) -> dict[int, Any]:
        return {}

    def tags(self) -> list[str]:
        return []

    class Meta:
        version = "v1"
        storage = "bar"
        ttl = timedelta(seconds=20)
        local_cache = None
        serializer = PickleSerializer()
        doorkeeper = None


@Memoize(FooNode)
async def test(a: int, b: str) -> str:
    print("run")
    await asyncio.sleep(1)
    return f"{a}/{b}/apple"


@test.to_node
def _(a: int, b: str) -> FooNode:
    return FooNode(user_id=str(a), foo_id=b, level=43)


@Memoize(BarNode)
async def test2(a: int, b: int) -> str:
    return f"apple-{a}-{b}"


@test2.to_node
def _(a: int, b: int) -> BarNode:
    return BarNode(user_id=a + b)


async def main():
    await init_storages(
        {
            "sqlite": SQLStorage("sqlite+aiosqlite:///example.db"),
            "local": TLFUStorage(50),
        }
    )
    await init_tag_storage(SQLStorage("sqlite+aiosqlite:///example.db"))
    # await test(a=1, b="2")
    # await test(a=3, b="4")
    for i in range(100):
        await get(FooNode(user_id="a", foo_id="b", level=i))
    for i in range(100):
        await get(FooNode(user_id="a", foo_id="b", level=i))


asyncio.run(main())
