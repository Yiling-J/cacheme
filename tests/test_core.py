import os
from asyncio import gather, sleep
from dataclasses import dataclass
from typing import List

import pytest

from cacheme.core import Memoize, get, get_all, stats
from cacheme.data import init_storages
from cacheme.models import Node
from cacheme.serializer import MsgPackSerializer
from cacheme.storages import Storage

fn1_counter = 0
fn2_counter = 0


@dataclass
class FooNode(Node):
    user_id: str
    foo_id: str
    level: int

    def key(self) -> str:
        return f"{self.user_id}:{self.foo_id}:{self.level}"

    async def load(self) -> str:
        global fn1_counter
        fn1_counter += 1
        return f"{self.user_id}-{self.foo_id}-{self.level}"

    class Meta(Node.Meta):
        version = "v1"
        storage = "local"
        serializer = MsgPackSerializer()


@Memoize(FooNode)
async def fn1(a: int, b: str) -> str:
    global fn1_counter
    fn1_counter += 1
    return f"{a}/{b}/apple"


@fn1.to_node
def _(a: int, b: str) -> FooNode:
    return FooNode(user_id=str(a), foo_id=b, level=40)


class Bar:
    @Memoize(FooNode)
    async def fn2(self, a: int, b: str, c: int) -> str:
        global fn2_counter
        fn2_counter += 1
        return f"{a}/{b}/{c}/orange"

    @fn2.to_node
    def _(self, a: int, b: str, c: int) -> FooNode:
        return FooNode(user_id=str(a), foo_id=b, level=30)


@pytest.mark.asyncio
async def test_memoize():
    await init_storages({"local": Storage(url="local://tlfu", size=50)})
    assert fn1_counter == 0
    result = await fn1(1, "2")
    assert result == "1/2/apple"
    assert fn1_counter == 1
    result = await fn1(1, "2")
    assert result == "1/2/apple"
    assert fn1_counter == 1

    b = Bar()
    assert fn2_counter == 0
    result = await b.fn2(1, "2", 3)
    assert result == "1/2/3/orange"
    assert fn1_counter == 1
    result = await b.fn2(1, "2", 3)
    assert result == "1/2/3/orange"
    assert fn1_counter == 1
    result = await b.fn2(1, "2", 5)
    assert result == "1/2/3/orange"
    assert fn1_counter == 1


@pytest.mark.asyncio
async def test_get():
    global fn1_counter
    await init_storages({"local": Storage(url="local://tlfu", size=50)})
    fn1_counter = 0
    result = await get(FooNode(user_id="a", foo_id="1", level=10))
    assert fn1_counter == 1
    assert result == "a-1-10"
    result = await get(FooNode(user_id="a", foo_id="1", level=10))
    assert fn1_counter == 1
    assert result == "a-1-10"


@pytest.mark.asyncio
async def test_get_override():
    await init_storages({"local": Storage(url="local://tlfu", size=50)})
    counter = 0

    async def override(node: FooNode) -> str:
        nonlocal counter
        counter += 1
        return f"{node.user_id}-{node.foo_id}-{node.level}-o"

    result = await get(FooNode(user_id="a", foo_id="1", level=10), override)
    assert counter == 1
    assert result == "a-1-10-o"
    result = await get(FooNode(user_id="a", foo_id="1", level=10), override)
    assert fn1_counter == 1
    assert result == "a-1-10-o"


@pytest.mark.asyncio
async def test_get_all():
    global fn1_counter
    await init_storages({"local": Storage(url="local://tlfu", size=50)})
    fn1_counter = 0
    nodes = [
        FooNode(user_id="c", foo_id="2", level=1),
        FooNode(user_id="a", foo_id="1", level=1),
        FooNode(user_id="b", foo_id="3", level=1),
    ]
    results = await get_all(nodes)
    assert fn1_counter == 3
    assert results == ("c-2-1", "a-1-1", "b-3-1")

    results = await get_all(nodes)
    assert fn1_counter == 3
    assert results == ("c-2-1", "a-1-1", "b-3-1")
    nodes = [
        FooNode(user_id="c", foo_id="2", level=1),
        FooNode(user_id="a", foo_id="1", level=1),
        FooNode(user_id="b", foo_id="4", level=1),
    ]
    results = await get_all(nodes)
    assert fn1_counter == 4
    assert results == ("c-2-1", "a-1-1", "b-4-1")


@dataclass
class FooNode2(Node):
    user_id: str
    foo_id: str
    level: int

    def key(self) -> str:
        return f"{self.user_id}:{self.foo_id}:{self.level}"

    class Meta(Node.Meta):
        version = "v1"
        storage = "local"
        serializer = MsgPackSerializer()


fn3_counter = 0


@Memoize(FooNode2)
async def fn3(a: int, b: str) -> str:
    global fn3_counter
    fn3_counter += 1
    await sleep(0.2)
    return f"{a}/{b}/apple"


@fn3.to_node
def _(a: int, b: str) -> FooNode2:
    return FooNode2(user_id=str(a), foo_id=b, level=40)


@pytest.mark.asyncio
async def test_memoize_cocurrency():
    await init_storages({"local": Storage(url="local://tlfu", size=50)})
    assert fn3_counter == 0
    results = await gather(*[fn3(a=1, b="2") for i in range(50)])
    assert len(results) == 50
    for r in results:
        assert r == "1/2/apple"
    assert fn3_counter == 1


@pytest.mark.asyncio
async def test_get_cocurrency():
    global fn1_counter
    fn1_counter = 0
    await init_storages({"local": Storage(url="local://tlfu", size=50)})
    results = await gather(
        *[get(FooNode(user_id="b", foo_id="a", level=10)) for i in range(50)]
    )
    assert len(results) == 50
    for r in results:
        assert r == "b-a-10"
    assert fn1_counter == 1


@dataclass
class StatsNode(Node):
    id: str

    def key(self) -> str:
        return f"{self.id}"

    async def load(self) -> str:
        return f"{self.id}"

    class Meta(Node.Meta):
        version = "v1"
        storage = "local"


@pytest.mark.asyncio
async def test_stats():
    await init_storages({"local": Storage(url="local://lru", size=100)})
    await get(StatsNode("a"))
    await get(StatsNode("b"))
    await get(StatsNode("c"))
    await get(StatsNode("a"))
    await get(StatsNode("d"))
    metrics = stats(StatsNode)
    assert metrics.request_count() == 5
    assert metrics.hit_count() == 1
    assert metrics.load_count() == 4
    assert metrics.hit_rate() == 1 / 5
    assert metrics.load_success_count() == 4
    assert metrics.miss_count() == 4
    assert metrics.miss_rate() == 4 / 5
