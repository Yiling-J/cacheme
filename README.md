# Cacheme

Asyncio cache framework with multiple cache storages.

- **Cache configuration by node:** Cache configuration with node class, you can apply different cache strategies on different nodes.
- **Multiple cache storages support:** in-memory/redis/mongodb/postgres..., also support chain in-memory storage with other storages.
- **Type annotated:** All cacheme API are type annotated with generics.
- **High hit ratio in-memory cache:** TinyLFU written in Rust with little memory overhead.
- **Thundering herd protection:** Simultaneously requests to same key will blocked by asyncio Event and only load from source once.
- **All Protocols:** Node/Storage/Serializer are all protocols, you can customize easily.
- **Cache stats API:** Stats of each node and colected automatically.

Related projects:
- Rust tiny-lfu/lru/bloomfilter used in cacheme: https://github.com/Yiling-J/cacheme-utils
- Benchmark(auto updated): https://github.com/Yiling-J/cacheme-benchmark

## Installation

```
pip install cacheme
```

Multiple storages are supported by drivers. You can install the required drivers with:
```
pip install cacheme[redis]
pip install cacheme[mysql]
pip install cacheme[mongo]
pip install cacheme[postgresql]
```

## Add Node
Node is the core part of your cache, each node contains:

- Key attritubes and `key` method,  which generate the cache key. Here the `UserInfoNode` is a dataclass, so the `__init__` method are create automatically.
- Async `load` method, which will be called to load data from data source on cache missing. This method can be omitted if you use `Memoize` decorator only.
- `Meta` class, include storage related params: storage backend, serializer, ttl...

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
        storage = "my-redis"
        serializer = MsgPackSerializer()
```
This simple example use a storage called "my-redis", which will be registered next step. Also we use `MsgPackSerializer` here to dump and load data from redis. See [Cache Node] for more details.

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

## Cache Node

#### Meta Class:
- `version`
- `storage`
- `ttl`
- `local_ttl`
- `local_storage`
- `serializer`
- `doorkeeper`

## Cache Storage

#### Local Storage
Local storage uses dictionary to sotre data. A policy is used to evicate keys because size is limited.
```python
# lru policy
Storage(url="local://lru", size=10000)

# tinylfu policy
Storage(url="local://tlfu", size=10000)

```
- url: `local://{policy}`. 2 policies are currently supported:
  - `lru`
  - `tlfu`: TinyLfu policy, see https://arxiv.org/pdf/1512.00727.pdf

- size: size of the storage. Policy will be used to evicate key when cache is full.

#### Redis Storage
```python
Storage(url="redis://localhost:6379")

# cluster
Storage(url="redis://localhost:6379", cluster=True)
```
- url: redis connection url.
- cluster: bool, cluster or not, default False.
- pool_size: connection pool size, default 100.

#### MongoDB Storage
To use mongodb storage, create index first. See [mongo.js](cacheme/storages/scripts/mongo.js)
```python
Storage(url="mongodb://test:password@localhost:27017",database="test",collection="cache")
```
- url: mongodb connection url.
- database: mongodb database name.
- collection: mongodb collection name.

#### Sqlite Storage
To use sqlite storage, create table and index first. See [sqlite.sql](cacheme/storages/scripts/sqlite.sql)
```python
Storage(url="sqlite:///test", table="cache")
```
- url: sqlite connection url.
- table: cache table name.
- pool_size: connection pool size, default 50.

#### PostgreSQL Storage
To use postgres storage, create table and index first. See [postgresql.sql](cacheme/storages/scripts/postgresql.sql)
```python
Storage(url="postgresql://username:password@127.0.0.1:5432/test", table="cache")
```
- url: postgres connection url.
- table: cache table name.
- pool_size: connection pool size, default 50.

#### MySQL Storage
To use mysql storage, create table and index first. See [mysql.sql](cacheme/storages/scripts/mysql.sql)
```python
Storage("mysql://username:password@localhost:3306/test", table="cache")
```
- url: mysql connection url.
- table: cache table name.
- pool_size: connection pool size, default 50.

## Benchmarks
- Local Storage Hit Ratios
  ![hit ratios](/benchmarks/hit_ratio.png)
  [source code](/benchmarks/tlfu_hit.py)

- Throughput Benchmark of different storages

  See [benchmark]( https://github.com/Yiling-J/cacheme-benchmark)
