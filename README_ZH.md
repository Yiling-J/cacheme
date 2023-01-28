# Cacheme

多源结构化异步缓存框架。

- 通过Node结构化管理缓存，可以给不同的Node分配不同的缓存存储方式及缓存策略
- 多种序列化方式， 支持 pickle/json/msgpack及压缩存储
- API全部添加Type hint
- 基于TinyLFU的高效缓存管理策略，使用Rust编写
- 通过asyncio Event避免Thundering herd问题，提高命中率及减轻高并发下数据源/缓存源压力
- 基于Node的缓存统计API

相关项目：
- Rust编写的tiny-lfu/lru/bloomfilter：https://github.com/Yiling-J/cacheme-utils
- 相关benchmarks：https://github.com/Yiling-J/cacheme-benchmark

## 目录

- [安装](#安装)
- [定义Node](#定义Node)
- [注册Storage](#注册Storage)
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

## 基本要求
Python 3.7+

## 安装
```
pip install cacheme
```

不同存储源通过对应driver支持，可以根据情况选择安装
```
pip install cacheme[redis]
pip install cacheme[aiomysql]
pip install cacheme[motor]
pip install cacheme[asyncpg]
```

## 定义Node
Node是Cacheme的核心部分。Node定义包含了缓存的key定义，缓存源数据读取以及存储相关的各种配置。通过例子比较直接：
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
以上这个例子定义了UserInfoNode，用于缓存UserInfo数据。缓存的key通过`key`函数生成。通过dataclass装饰器自动生成init方法。这样在调用Cacheme API时只使用node，避免手工输入key string。load函数定义了当缓存miss时如何从数据源获取数据。而Meta class则定义了cache的version(会自动加入key中)，cache的存储方式，这里用了名叫my-redis的存储源以及存储/读取时用的serializer。

## 注册Storage
Cacheme的Node和Storage是分开的，Node表示业务信息，比如用户信息node。而storage则是cache的存储方式。一个Node可以支持串联多种存储方式，同样一个存储方式也可以用在多种node上。
```python
import cacheme

await cacheme.register_storage("my-redis", cacheme.Storage(url="redis://localhost:6379"))
```

## Cacheme API

`get`: 通过node获取数据
```python
user = await cacheme.get(UserInfoNode(user_id=1))
```

`get_all`: 通过node获取多条数据，传入nodes必须是同一类型
```python
users = await cacheme.get_all([UserInfoNode(user_id=1), UserInfoNode(user_id=2)])
```

`invalidate`: 删除某个node的缓存
```python
await cacheme.invalidate(UserInfoNode(user_id=1))
```

`refresh`: 重新从数据源读取某个node的缓存
```python
await cacheme.refresh(UserInfoNode(user_id=1))
```

`Memoize`: memoize装饰器，可用于memoize已有函数

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

`nodes`: 列出所有nodes
```python
nodes = cacheme.nodes()
```

`stats`: 获取节点统计数据
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

`set_prefix`: 设置全局key前缀
```python
cacheme.set_prefix("mycache")
```

## Cache Node

#### Key
实际存储到storage层的key形式为`{prefix}:{key()}:{Meta.version}`

#### Meta Class
- `version[str]`: node版本信息.
- `caches[List[Cache]]`: Node缓存的存储源. 多个存储源会按从左到右的顺序依次调用，在写入缓存时也会依次写入。定义`Cache`需要2个参数：`storage[str]` 和 `ttl[Optional[timedelta]]`. `storage`是调用 `register_storage`时传入的name， 而`ttl`就是这个node对应缓存的ttl.
- `serializer[Optional[Serializer]]`: Serializer用于dump/load data. 如果是local cache，由于直接使用dict存储会忽略serializer. See [Serializers](#serializers).
- `doorkeeper[Optional[DoorKeeper]]`: See [DoorKeeper](#doorkeeper).

以下例子展示了使用local + redis两级缓存的情况

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

#### Serializers
Cacheme 提供以下内置serializer.

- `PickleSerializer`: 使用Python pickle，支持各种类型.
- `JSONSerializer`: 使用`pydantic_encoder` 和 `json`, 支持python基本类型/dataclass/pydantic model. See [pydantic types](https://docs.pydantic.dev/usage/types/).
- `MsgPackSerializer`: 使用`pydantic_encoder` 和 `msgpack`, 支持python基本类型/dataclass/pydantic model. See [pydantic types](https://docs.pydantic.dev/usage/types/).

以上3种serializer同时有对应的压缩版本, 在存入存储源前会使用zlib level-3进行压缩

- `CompressedPickleSerializer`
- `CompressedJSONSerializer`
- `CompressedMsgPackSerializer`

#### DoorKeeper
概念来源于[TinyLfu 论文](https://arxiv.org/pdf/1512.00727.pdf).

*The Doorkeeper is a regular Bloom filter placed in front of the cahce. Upon
item arrival, we first check if the item is contained in the Doorkeeper. If it is not contained in the
Doorkeeper (as is expected with first timers and tail items), the item is inserted to the Doorkeeper and
otherwise, it is inserted to the cache.*

缓存请求第一次到达服务端时先不缓存数据，只是更新Bloom filter， 等请求第二次到达时才把数据存入缓存。这么做好处是很多只请求1次的数据会被筛掉不进入缓存，节约空间。坏处是所有请求都会至少从数据源load两次。BloomFilter在请求到达size后会自动重制。

```python
from cacheme import BloomFilter

@dataclass
class UserInfoNode(cacheme.Node):

    class Meta(cacheme.Node.Meta):
        # size 100000, false positive probability 0.01
        doorkeeper = BloomFilter(100000, 0.01)
```

## Cache Storage

#### Local Storage
Local Storage使用Python dict存储数据，支持lru和tlfu两种policy。当缓存到达设定size时会自动通过policy进行驱逐。
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
使用该storage前需要先创建index. See [mongo.js](cacheme/storages/scripts/mongo.js)
```python
Storage(url="mongodb://test:password@localhost:27017",database="test",collection="cache")
```
Parameters:

- `url`: mongodb connection url.
- `database`: mongodb database name.
- `collection`: mongodb collection name.
- `pool_size`: connection pool size, default 50.

#### Sqlite Storage
使用该storage前需要先创建table及index. See [sqlite.sql](cacheme/storages/scripts/sqlite.sql)
```python
Storage(url="sqlite:///test", table="cache")
```
Parameters:

- `url`: sqlite connection url.
- `table`: cache table name.
- `pool_size`: connection pool size, default 50.

#### PostgreSQL Storage
使用该storage前需要先创建table及index. See [postgresql.sql](cacheme/storages/scripts/postgresql.sql)
```python
Storage(url="postgresql://username:password@127.0.0.1:5432/test", table="cache")
```
Parameters:

- `url`: postgres connection url.
- `table`: cache table name.
- `pool_size`: connection pool size, default 50.

#### MySQL Storage
使用该storage前需要先创建table及index. See [mysql.sql](cacheme/storages/scripts/mysql.sql)
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
