# type: ignore
import asyncio
import time
from asyncio import sleep
from dataclasses import dataclass
from typing import Callable, Iterable, List, cast

import aiocache
import redis
from cashews import cache as wcache
from redis.asyncio import Redis
from redis.asyncio.connection import BlockingConnectionPool

from benchmarks.zipf import Zipf
from cacheme.core import get, get_all
from cacheme.data import list_storages, register_storage
from cacheme.models import Cache, Node
from cacheme.serializer import MsgPackSerializer
from cacheme.storages import Storage

wb = wcache.setup("redis://", max_connections=100, wait_for_connection_timeout=300)


REQUESTS = 200000

aiocache.caches.set_config(
    {
        "default": {
            "cache": "aiocache.SimpleMemoryCache",
            "serializer": {"class": "aiocache.serializers.StringSerializer"},
        },
        "redis_alt": {
            "cache": "aiocache.RedisCache",
            "endpoint": "127.0.0.1",
            "port": 6379,
            "timeout": 200,
            "plugins": [],
        },
    }
)


async def simple_get(Node: Callable, i: int):
    result = await get(Node(uid=i))
    assert result == i


async def simple_get_all(Node: Callable, l: List[int]):
    result = await get_all([Node(uid=i) for i in l])
    assert list(result) == l


async def simple_get_ii(Node: Callable, i: int):
    result = await Node(uid=i).load_ii()
    assert result == i


async def simple_get_iii(Node: Callable, i: int):
    result = await Node(uid=i).load_iii()
    assert result == i


async def simple_get_iv(Node: Callable, i: int):
    result = await Node(uid=i).load_iv()
    assert result == i


async def simple_get_v(Node: Callable, i: int):
    result = await Node(uid=i).load_v()
    assert result == i


def zipf_key_gen() -> Iterable:
    z = Zipf(1.001, 10, REQUESTS)
    for _ in range(REQUESTS):
        yield f"{z.get()}"


def ucb_key_gen() -> Iterable:
    with open(f"benchmarks/trace/ucb", "rb") as f:
        for line in f:
            vb = line.split(b" ")[-2]
            try:
                v = vb.decode()
            except:
                v = "failed"
            yield v


def ds1_key_gen() -> Iterable:
    with open(f"benchmarks/trace/ds1", "r") as f:
        for line in f:
            yield line.split(",")[0]


def s3_key_gen() -> Iterable:
    with open(f"benchmarks/trace/s3", "r") as f:
        for line in f:
            yield line.split(",")[0]


async def worker(queue):
    while True:
        try:
            task = queue.get_nowait()
        except:
            return
        await task
        queue.task_done()


async def run_concurrency(queue, workers):
    await asyncio.gather(*[worker(queue) for _ in range(workers)])


@dataclass
class FooNode(Node):
    uid: str
    load_count = 0

    def key(self) -> str:
        return f"uid:{self.uid}"

    async def load(self) -> int:
        self.__class__.load_count += 1
        await sleep(0.1)
        return self.uid

    @aiocache.cached(
        alias="redis_alt", key_builder=lambda *args, **kw: f"uid2:{args[1].uid}"
    )
    async def load_ii(self) -> int:
        self.__class__.load_count += 1
        await sleep(0.1)
        return self.uid

    @aiocache.cached_stampede(
        alias="redis_alt",
        key_builder=lambda *args, **kw: f"uid3:{args[1].uid}",
        lease=30,
    )
    async def load_iii(self) -> int:
        self.__class__.load_count += 1
        await sleep(0.1)
        return self.uid

    @wcache(key="uid4:{self.uid}", ttl=None)
    async def load_iv(self) -> int:
        self.__class__.load_count += 1
        await sleep(0.1)
        return self.uid

    @wcache(key="uid5:{self.uid}", ttl=500, lock=True)
    async def load_v(self) -> int:
        self.__class__.load_count += 1
        await sleep(0.1)
        return self.uid

    class Meta(Node.Meta):
        version = "v1"
        caches = [Cache(storage="redis", ttl=None)]
        serializer = MsgPackSerializer()


async def bench_cacheme_zipf(gen: Callable[..., Iterable], workers: int):
    # reset node cache
    FooNode.Meta.caches = [Cache(storage="redis", ttl=None)]
    redis_counter = 0
    await register_storage("redis", Storage(url="redis://localhost:6379"))
    client = cast(Redis, list_storages()["redis"]._storage.client)
    FooNode.load_count = 0

    def callback(response):
        nonlocal redis_counter
        redis_counter += 1
        return response

    client.set_response_callback("GET", callback)

    queue = asyncio.Queue()
    for uid in gen():
        queue.put_nowait(simple_get(FooNode, uid))
    now = time.time()
    await run_concurrency(queue, workers)
    print(
        f"cacheme redis count {redis_counter}, load count {FooNode.load_count}, spent {time.time() - now}s"
    )
    await client.close()


async def bench_cacheme_batch_zipf(workers: int):
    if workers > 10000:
        return
    # reset node cache
    FooNode.Meta.caches = [Cache(storage="redis", ttl=None)]
    redis_counter = 0
    await register_storage("redis", Storage(url="redis://localhost:6379"))
    client = cast(Redis, list_storages()["redis"]._storage.client)
    FooNode.load_count = 0

    def callback(response):
        nonlocal redis_counter
        redis_counter += 1
        return response

    client.set_response_callback("MGET", callback)
    z = Zipf(1.0001, 10, REQUESTS)

    def get20(z):
        l = set()
        while True:
            l.add(z.get())
            if len(l) == 20:
                break
        return list(l)

    queue = asyncio.Queue()
    for _ in range(REQUESTS // 20):
        queue.put_nowait(simple_get_all(FooNode, get20(z)))
    now = time.time()
    await run_concurrency(queue, workers)
    print(
        f"cacheme redis count {redis_counter}, load count {FooNode.load_count}, spent {time.time() - now}s"
    )
    await client.close()


async def bench_aiocache_zipf(gen: Callable[..., Iterable], workers: int):
    redis_counter = 0
    client = cast(Redis, FooNode.load_ii.cache.client)
    FooNode.load_count = 0

    def callback(response):
        nonlocal redis_counter
        redis_counter += 1
        return response

    client.set_response_callback("GET", callback)
    client.connection_pool = BlockingConnectionPool.from_url(
        "redis://localhost:6379", max_connections=100, timeout=None
    )

    queue = asyncio.Queue()
    for uid in gen():
        queue.put_nowait(simple_get_ii(FooNode, uid))
    now = time.time()
    await run_concurrency(queue, workers)
    print(
        f"aiocache redis count {redis_counter}, load count {FooNode.load_count}, spent {time.time() - now}s"
    )
    await client.close()


async def bench_aiocache_stampede_zipf(gen: Callable[..., Iterable], workers: int):
    redis_counter = 0
    client = cast(Redis, FooNode.load_iii.cache.client)
    FooNode.load_count = 0

    def callback(response):
        nonlocal redis_counter
        redis_counter += 1
        return response

    client.set_response_callback("GET", callback)
    client.connection_pool = BlockingConnectionPool.from_url(
        "redis://localhost:6379", max_connections=100, timeout=None
    )

    queue = asyncio.Queue()
    for uid in gen():
        queue.put_nowait(simple_get_iii(FooNode, uid))
    now = time.time()
    await run_concurrency(queue, workers)
    print(
        f"aiocache stampede redis count {redis_counter}, load count {FooNode.load_count}, spent {time.time() - now}s"
    )
    await client.close()


async def bench_cashews_zipf(gen: Callable[..., Iterable], workers: int):
    redis_counter = 0
    await wcache.get("foo")
    client = cast(Redis, wb._client)
    FooNode.load_count = 0

    def callback(response):
        nonlocal redis_counter
        redis_counter += 1
        return response

    client.set_response_callback("GET", callback)

    queue = asyncio.Queue()
    for uid in gen():
        queue.put_nowait(simple_get_iv(FooNode, uid))
    now = time.time()
    await run_concurrency(queue, workers)
    print(
        f"cashews redis count {redis_counter}, load count {FooNode.load_count}, spent {time.time() - now}s"
    )


async def bench_cashews_lock_zipf(gen: Callable[..., Iterable], workers: int):
    redis_counter = 0
    await wcache.get("foo")
    client = cast(Redis, wb._client)
    FooNode.load_count = 0

    def callback(response):
        nonlocal redis_counter
        redis_counter += 1
        return response

    client.set_response_callback("GET", callback)

    queue = asyncio.Queue()
    for uid in gen():
        queue.put_nowait(simple_get_v(FooNode, uid))
    now = time.time()
    await run_concurrency(queue, workers)
    print(
        f"cashews locked redis count {redis_counter}, load count {FooNode.load_count}, spent {time.time() - now}s"
    )


async def run():

    for w in [1000, 10000, 100000]:
        r = redis.Redis(host="localhost", port=6379)
        r.flushall()

        print(f"==== zipf benchmark: concurrency {w} ====")
        await bench_cacheme_zipf(zipf_key_gen, w)
        await bench_aiocache_zipf(zipf_key_gen, w)
        await bench_aiocache_stampede_zipf(zipf_key_gen, w)
        await bench_cashews_zipf(zipf_key_gen, w)
        await bench_cashews_lock_zipf(zipf_key_gen, w)

    for w in [1000, 10000, 100000]:
        r = redis.Redis(host="localhost", port=6379)
        r.flushall()

        print(f"==== zipf batch benchmark: concurrency {w} ====")
        await bench_cacheme_batch_zipf(w)


asyncio.run(run())
