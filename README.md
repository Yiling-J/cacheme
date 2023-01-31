# Cacheme

Asyncio cache framework with multiple cache storages. [中文文档](README_ZH.md)

- **Better cache management:** Cache configuration with node, you can apply different strategies on different nodes.
- **Multiple cache storages:** in-memory/redis/mongodb/postgres..., also support chain storages.
- **Multiple serializers:** Pickle/Json/Msgpack serializers.
- **Type annotated:** All cacheme API are type annotated with generics.
- **High hit ratio in-memory cache:** TinyLFU written in Rust with little memory overhead.
- **Thundering herd protection:** Simultaneously requests to same key are blocked by asyncio Event and only load from source once.
- **Cache stats API:** Stats of each node and colected automatically.

Related projects:
- Rust tiny-lfu/lru/bloomfilter used in cacheme: https://github.com/Yiling-J/cacheme-utils
- Benchmark(auto updated): https://github.com/Yiling-J/cacheme-benchmark

## Table of Contents

- [Installation](#installation)
- [Add Node](#add-node)
- [Register Storage](#register-storage)
- [Cacheme API](#cacheme-api)
- [Cache Node](#cache-node)
    + [Key](#key)
    + [Meta Class](#meta-class)
    + [Serializers](#serializers)
    + [DoorKeeper](#doorkeeper)
- [Cache Storage](#cache-storage)
    + [Local Storage](#local-storage)
    + [Redis Storage](#redis-storage)
    + [MongoDB Storage](#mongodb-storage)
    + [Sqlite Storage](#sqlite-storage)
    + [PostgreSQL Storage](#postgresql-storage)
    + [MySQL Storage](#mysql-storage)
- [Benchmarks](#benchmarks)

## Requirements
Python 3.7+

## Installation

```
pip install cacheme
```

Multiple storages are supported by drivers. You can install the required drivers with:
```
pip install cacheme[redis]
pip install cacheme[aiomysql]
pip install cacheme[motor]
pip install cacheme[asyncpg]
```

## Add Node
Node is the core part of cache. Each node has its own key function, load function and storage options. Stats of each node are collected independently. You can place all node definations into one package/module, so everyone knows exactly what is cached now and how they are cached. All cacheme API are based on node.

Each node contains:
- Key attritubes and `key` method,  which are used to generate cache key. Here the `UserInfoNode` is a dataclass, so `__init__` method is generated automatically.
- Async `load` method, which will be called to load data from data source on cache missing. This method can be omitted if you use `Memoize` decorator only.
- `Meta` class, node cache configurations. See [Cache Node](#cache-node)

```python
import cacheme
from dataclasses import dataclass
from cacheme.serializer import MsgPackSerializer

@dataclass
class UserInfoNode(cacheme.Node):
    user_id: int

    def key(self) -> str:
        return f"user:{self.user_id}:info"

    async def load(self) -> Dict:
        user = get_user_from_db(self.user_id)
        return serialize(user)

    class Meta(cacheme.Node.Meta):
        version = "v1"
        caches = [cacheme.Cache(storage="my-redis", ttl=None)]
        serializer = MsgPackSerializer()
```
This simple example use a cache storage called "my-redis", which will be registered next step. Also we use `MsgPackSerializer` here to dump and load data from redis. See [Cache Node] for more details.

## Register Storage

Register a redis storage called "my-redis", which you can use in node meta data. The `register_storage` is asynchronous and will try to establish connection to cache store.
See [Cache Storage] for more details.

```python
import cacheme

await cacheme.register_storage("my-redis", cacheme.Storage(url="redis://localhost:6379"))
```

## Cacheme API

`get`: get data from single node.
```python
user = await cacheme.get(UserInfoNode(user_id=1))
```

`get_all`: get data from multiple nodes, same node type.
```python
users = await cacheme.get_all([UserInfoNode(user_id=1), UserInfoNode(user_id=2)])
```

`invalidate`: invalidate a node, remove data from cache.
```python
await cacheme.invalidate(UserInfoNode(user_id=1))
```

`refresh`: reload node data using `load` method.
```python
await cacheme.refresh(UserInfoNode(user_id=1))
```

`Memoize`: memoize function with this decorator.

Decorate your function with `cacheme.Memoize` decorator and cache node. Cacheme will load data using the decorated function and ignore `load` method.
Because your function may contain variable number of args/kwargs, we need one more step to map between args/kwargs to node. The decorated map function should have same input signature as memoized function, and return a cache node.

```python
@cacheme.Memoize(UserInfoNode)
async def get_user_info(user_id: int) -> Dict:
    return {}

# function name is not important, so just use _ here
@get_user_info.to_node
def _(user_id: int) -> UserInfoNode:
    return UserInfoNode(user_id=user_id)
```

`nodes`: list all nodes.
```python
nodes = cacheme.nodes()
```

`stats`: get node stats.
```
metrics = cacheme.stats(UserInfoNode)

metrics.request_count() # total request count
metrics.hit_count() # total hit count
metrics.hit_rate() # hit_count/request_count
metrics.miss_count() # (request_count - hit_count)/request_count
metrics.miss_rate() # miss_count/request_count
metric.load_success_count() # total load success count
metrics.load_failure_count() # total load fail count
metrics.load_failure_rate() # load_failure_count/load_count
metrics.load_count() # total load count
metrics.total_load_time() # total load time in nanoseconds
metrics.average_load_time() # total_load_time/load_count
```

`set_prefix`: set prefix for all keys. Default prefix is `cacheme`. Change prefix will invalid all keys, because prefix is part of the key.
```python
cacheme.set_prefix("mycache")
```


## Cache Node

#### Key
Generated cache key will be: `{prefix}:{key()}:{Meta.version}`. So change `version` will invalid all keys automatically.

#### Meta Class
- `version[str]`: Version of node, will be used as suffix of cache key.
- `caches[List[Cache]]`: Caches for node. Each `Cache` has 2 attributes, `storage[str]` and `ttl[Optional[timedelta]]`. `storage` is the name you registered with `register_storage` and `ttl` is how long this cache will live. Cacheme will try to get data from each cache from left to right. In most cases, use single cache or [local, remote] combination.
- `serializer[Optional[Serializer]]`: Serializer used to dump/load data. If storage type is `local`, serializer is ignored. See [Serializers](#serializers).
- `doorkeeper[Optional[DoorKeeper]]`: See [DoorKeeper](#doorkeeper).

Multiple caches example. Local cache is not synchronized, so set a much shorter ttl compared to redis one. Then we don't need to worry too much about stale data.

```python
import cacheme
from dataclasses import dataclass
from datetime import timedelta
from cacheme.serializer import MsgPackSerializer

@dataclass
class UserInfoNode(cacheme.Node):
    user_id: int

    def key(self) -> str:
        return f"user:{self.user_id}:info"

    async def load(self) -> Dict:
        user = get_user_from_db(self.user_id)
        return serialize(user)

    class Meta(cacheme.Node.Meta):
        version = "v1"
        caches = [
            cacheme.Cache(storage="local", ttl=timedelta(seconds=30)),
            cacheme.Cache(storage="my-redis", ttl=timedelta(days=10))
        ]
        serializer = MsgPackSerializer()
```

Cacheme also support creating Node dynamically, you can use this together with `Memoize` decorator:

```python
@Memoize(cacheme.build_node("TestNodeDynamic", "v1", [Cache(storage="local", ttl=None)]))
async def fn(a: int) -> int:
    return 1


@fn.to_node
def _(a: int) -> cacheme.DynamicNode:
    return DynamicNode(key=f"bar:{a}")
```
Here we use `DynamicNode`, which only support one param: `key`

#### Serializers
Cacheme provides serveral builtin serializers, you can also write your own serializer.

- `PickleSerializer`: All python objects.
- `JSONSerializer`: Use `pydantic_encoder` and `json`, support python primitive types, dataclass, pydantic model. See [pydantic types](https://docs.pydantic.dev/usage/types/).
- `MsgPackSerializer`: Use `pydantic_encoder` and `msgpack`, support python primitive types, dataclass, pydantic model. See [pydantic types](https://docs.pydantic.dev/usage/types/).

serializer with compression, use zlib level-3

- `CompressedPickleSerializer`
- `CompressedJSONSerializer`
- `CompressedMsgPackSerializer`

#### DoorKeeper
Idea from [TinyLfu paper](https://arxiv.org/pdf/1512.00727.pdf).

*The Doorkeeper is a regular Bloom filter placed in front of the cahce. Upon
item arrival, we first check if the item is contained in the Doorkeeper. If it is not contained in the
Doorkeeper (as is expected with first timers and tail items), the item is inserted to the Doorkeeper and
otherwise, it is inserted to the cache.*

```python
from cacheme import BloomFilter

@dataclass
class UserInfoNode(cacheme.Node):

    class Meta(cacheme.Node.Meta):
        # size 100000, false positive probability 0.01
        doorkeeper = BloomFilter(100000, 0.01)
```
BloomFilter is cleared automatically when requests count == size.


## Cache Storage

#### Local Storage
Local storage uses dictionary to store data. A policy is used to evicate keys when cache is full.
```python
# lru policy
Storage(url="local://lru", size=10000)

# tinylfu policy
Storage(url="local://tlfu", size=10000)

```
Parameters:

- `url`: `local://{policy}`. 2 policies are currently supported:
  - `lru`
  - `tlfu`: TinyLfu policy, see https://arxiv.org/pdf/1512.00727.pdf

- `size`: size of the storage. Policy will be used to evicate key when cache is full.

#### Redis Storage
```python
Storage(url="redis://localhost:6379")

# cluster
Storage(url="redis://localhost:6379", cluster=True)
```
Parameters:

- `url`: redis connection url.
- `cluster`: bool, cluster or not, default False.
- `pool_size`: connection pool size, default 100.

#### MongoDB Storage
To use mongodb storage, create index first. See [mongo.js](cacheme/storages/scripts/mongo.js)
```python
Storage(url="mongodb://test:password@localhost:27017",database="test",collection="cache")
```
Parameters:

- `url`: mongodb connection url.
- `database`: mongodb database name.
- `collection`: mongodb collection name.
- `pool_size`: connection pool size, default 50.

#### Sqlite Storage
To use sqlite storage, create table and index first. See [sqlite.sql](cacheme/storages/scripts/sqlite.sql)
```python
Storage(url="sqlite:///test", table="cache")
```
Parameters:

- `url`: sqlite connection url.
- `table`: cache table name.
- `pool_size`: connection pool size, default 50.

#### PostgreSQL Storage
To use postgres storage, create table and index first. See [postgresql.sql](cacheme/storages/scripts/postgresql.sql)
```python
Storage(url="postgresql://username:password@127.0.0.1:5432/test", table="cache")
```
Parameters:

- `url`: postgres connection url.
- `table`: cache table name.
- `pool_size`: connection pool size, default 50.

#### MySQL Storage
To use mysql storage, create table and index first. See [mysql.sql](cacheme/storages/scripts/mysql.sql)
```python
Storage("mysql://username:password@localhost:3306/test", table="cache")
```
Parameters:

- `url`: mysql connection url.
- `table`: cache table name.
- `pool_size`: connection pool size, default 50.

## Benchmarks
- Local Storage Hit Ratios(hit_count/request_count)
  ![hit ratios](benchmarks/hit_ratio.png)
  [source code](benchmarks/tlfu_hit.py)

- Throughput Benchmark of different storages

  See [benchmark]( https://github.com/Yiling-J/cacheme-benchmark)
