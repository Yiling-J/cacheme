import os
from asyncio import gather, sleep
from dataclasses import dataclass
from datetime import timedelta
from unittest.mock import Mock

import pytest

from cacheme.core import (Memoize, build_node, get, get_all, invalidate, nodes,
                          refresh, stats)
from cacheme.data import register_storage
from cacheme.models import Cache, DynamicNode, Node, sentinel, set_prefix
from cacheme.serializer import MsgPackSerializer
from cacheme.storages import Storage


def node_cls(mock: Mock):
    @dataclass
    class FooNode(Node):
        user_id: str
        foo_id: str
        level: int

        def key(self) -> str:
            return f"{self.user_id}:{self.foo_id}:{self.level}"

        async def load(self) -> str:
            mock()
            return f"{self.user_id}-{self.foo_id}-{self.level}"

        class Meta(Node.Meta):
            version = "v1"
            caches = [Cache(storage="local", ttl=None)]
            serializer = MsgPackSerializer()

    return FooNode


async def fn(a: int, b: str, m: Mock) -> str:
    m()
    return f"{a}/{b}/apple"


@pytest.mark.asyncio
async def test_memoize():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    test_fn = Memoize(Node)(fn)
    test_fn.to_node(lambda a, b, m: Node(user_id=str(a), foo_id=b, level=10))
    assert mock.call_count == 0
    result = await test_fn(1, "2", mock)
    assert result == "1/2/apple"
    assert mock.call_count == 1
    result = await test_fn(1, "2", mock)
    assert result == "1/2/apple"
    assert mock.call_count == 1

    class Bar:
        @Memoize(Node)
        async def fn(self, a: int, b: str, c: int, m: Mock) -> str:
            m()
            return f"{a}/{b}/{c}/orange"

        @fn.to_node
        def _(self, a: int, b: str, c: int, m: Mock) -> Node:
            return Node(user_id=str(a), foo_id=b, level=20)

    mock.reset_mock()
    b = Bar()
    assert mock.call_count == 0
    result = await b.fn(1, "2", 3, mock)
    assert result == "1/2/3/orange"
    assert mock.call_count == 1
    result = await b.fn(1, "2", 3, mock)
    assert result == "1/2/3/orange"
    assert mock.call_count == 1
    result = await b.fn(1, "2", 5, mock)
    assert result == "1/2/3/orange"
    assert mock.call_count == 1


@pytest.mark.asyncio
async def test_get():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    result = await get(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 1
    assert result == "a-1-10"
    result = await get(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 1
    assert result == "a-1-10"


@pytest.mark.asyncio
async def test_get_override():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    mock2 = Mock()

    async def override(node: Node) -> str:
        mock2()
        return f"{node.user_id}-{node.foo_id}-{node.level}-o"  # type: ignore

    result = await get(Node(user_id="a", foo_id="1", level=10), override)
    assert mock.call_count == 0
    assert mock2.call_count == 1
    assert result == "a-1-10-o"
    result = await get(Node(user_id="a", foo_id="1", level=10), override)
    assert mock.call_count == 0
    assert mock2.call_count == 1
    assert result == "a-1-10-o"


@pytest.mark.asyncio
async def test_get_all():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    nodes = [
        Node(user_id="c", foo_id="2", level=1),
        Node(user_id="a", foo_id="1", level=1),
        Node(user_id="b", foo_id="3", level=1),
    ]
    results = await get_all(nodes)
    assert mock.call_count == 3
    assert results == ("c-2-1", "a-1-1", "b-3-1")

    results = await get_all(nodes)
    assert mock.call_count == 3
    assert results == ("c-2-1", "a-1-1", "b-3-1")
    nodes = [
        Node(user_id="c", foo_id="2", level=1),
        Node(user_id="a", foo_id="1", level=1),
        Node(user_id="b", foo_id="4", level=1),
    ]
    results = await get_all(nodes)
    assert mock.call_count == 4
    assert results == ("c-2-1", "a-1-1", "b-4-1")


@pytest.mark.asyncio
async def test_memoize_cocurrency():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    test_fn = Memoize(Node)(fn)
    test_fn.to_node(lambda a, b, m: Node(user_id=str(a), foo_id=b, level=10))
    results = await gather(*[test_fn(a=1, b="2", m=mock) for _ in range(50)])
    assert len(results) == 50
    for r in results:
        assert r == "1/2/apple"
    assert mock.call_count == 1


@pytest.mark.asyncio
async def test_get_cocurrency():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    results = await gather(
        *[get(Node(user_id="b", foo_id="a", level=10)) for _ in range(50)]
    )
    assert len(results) == 50
    for r in results:
        assert r == "b-a-10"
    assert mock.call_count == 1


@dataclass
class StatsNode(Node):
    id: str

    def key(self) -> str:
        return f"{self.id}"

    async def load(self) -> str:
        return f"{self.id}"

    class Meta(Node.Meta):
        version = "v1"
        caches = [Cache(storage="local", ttl=None)]


@pytest.mark.asyncio
async def test_stats():
    await register_storage("local", Storage(url="local://lru", size=100))
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
    await get_all([StatsNode("a"), StatsNode("b"), StatsNode("f")])
    assert metrics.request_count() == 8
    assert metrics.hit_count() == 3
    assert metrics.load_count() == 5


@pytest.mark.asyncio
async def test_invalidate():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    await get(Node(user_id="a", foo_id="1", level=10))
    await get(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 1
    await invalidate(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 1
    await get(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 2


@pytest.mark.asyncio
async def test_refresh():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    mock = Mock()
    Node = node_cls(mock)
    await get(Node(user_id="a", foo_id="1", level=10))
    await get(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 1
    await refresh(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 2
    await get(Node(user_id="a", foo_id="1", level=10))
    assert mock.call_count == 2


def node_multi_cls(mock: Mock):
    @dataclass
    class FooNode(Node):
        id: str

        def key(self) -> str:
            return f"{self.id}"

        async def load(self) -> str:
            mock()
            return "test"

        class Meta(Node.Meta):
            version = "v1"
            caches = [
                Cache(storage="local1", ttl=timedelta(seconds=10)),
                Cache(storage="local2", ttl=None),
            ]
            serializer = MsgPackSerializer()

    return FooNode


@pytest.mark.asyncio
async def test_multiple_storage():
    storage1 = Storage(url="local://tlfu", size=50)
    storage2 = Storage(url="local://tlfu", size=50)
    await register_storage("local1", storage1)
    await register_storage("local2", storage2)
    mock = Mock()
    Node = node_multi_cls(mock)
    node = Node(id="1")
    result = await get(node)
    assert result == "test"
    assert mock.call_count == 1
    r1 = await storage1.get(node, None)
    assert r1 == "test"
    r2 = await storage2.get(node, None)
    assert r2 == "test"
    # invalidate node
    await invalidate(node)
    r1 = await storage1.get(node, None)
    assert r1 is sentinel
    r2 = await storage2.get(node, None)
    assert r2 is sentinel

    # test remove cache from local only
    result = await get(node)
    assert result == "test"
    assert mock.call_count == 2
    await storage1.remove(node)
    result = await get(node)
    assert result == "test"
    r1 = await storage1.get(node, None)
    assert r1 == "test"
    r2 = await storage2.get(node, None)
    assert r2 == "test"
    assert mock.call_count == 2


def test_nodes():
    test_nodes = nodes()
    assert len(test_nodes) > 0
    for n in test_nodes:
        assert type(n) != Node


def test_set_prefix():
    set_prefix("youcache")
    mock = Mock()
    Node = node_multi_cls(mock)
    node = Node(id="test")
    assert node.full_key() == "youcache:test:v1"


@pytest.mark.asyncio
async def test_build_node():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    Node = build_node("DynamicFooNode", "v1", [Cache(storage="local", ttl=None)])
    c = 0

    async def counter(node) -> int:
        nonlocal c
        c += 1
        return c

    for i in range(0, 10):
        result = await get(Node(key=f"foo:{i}"), load_fn=counter)
        assert result == i + 1
    assert c == 10
    for i in range(0, 10):
        result = await get(Node(key=f"foo:{i}"), load_fn=counter)
        assert result == i + 1
    assert c == 10

    # assert nodes/stats API
    assert Node in nodes()
    metrics = stats(Node)
    assert metrics.request_count() == 20
    assert metrics.hit_count() == 10

    # build with same name, should use existing one
    Node2 = build_node("DynamicFooNode", "v1", [Cache(storage="local", ttl=None)])
    assert Node == Node2


fn_dynamic_counter = 0


@Memoize(build_node("DynamicBarNode", "v1", [Cache(storage="local", ttl=None)]))
async def fn_dynamic(a: int) -> int:
    global fn_dynamic_counter
    fn_dynamic_counter += 1
    return a


@fn_dynamic.to_node
def _(a: int) -> DynamicNode:
    return DynamicNode(key=f"bar:{a}")


@pytest.mark.asyncio
async def test_build_node_decorator():
    await register_storage("local", Storage(url="local://tlfu", size=50))
    assert fn_dynamic_counter == 0
    result = await fn_dynamic(1)
    assert result == 1
    assert fn_dynamic_counter == 1
    result = await fn_dynamic(1)
    assert result == 1
    assert fn_dynamic_counter == 1
    result = await fn_dynamic(2)
    assert result == 2
    assert fn_dynamic_counter == 2
