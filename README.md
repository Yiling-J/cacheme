# Cacheme

Asyncio cache framework with multiple cache storages.

- **Cache configuration by node:** cache configuration with node class, you can choose where/how to cache based on node usage
- **Multiple cache storages support:** in-memory/redis/mongodb/postgres..., chain in-memory storage and other storages
- **Type annotated:** All cacheme API are type annotated with generics
- **High hit ratio in-memory cache:** TinyLFU written in Rust with little memory overhead
- **Cache stats API:** collect stats on each node

Related projects:
- Rust tiny-lfu/lru/bloomfilter used in cacheme: https://github.com/Yiling-J/cacheme-utils
- Benchmark(auto updated): https://github.com/Yiling-J/cacheme-benchmark

## Installation

```
pip install cacheme
```

## Add node
Node is the core part of your cache, each node contains the key function, the load function and the Meta configuration. Key function will be used to generate cache key, load function is used to fetch data on cache miss and Meta class is used to define cache storage related meta info. Here we use the "my-redis" storage, which will be registered next step.

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

## Register storage

Register a redis storage called "my-redis", which you can reference by name in node meta data.

```python
import cacheme

await cacheme.register_storage("my-redis", cacheme.Storage(url="redis://localhost:6379"))
```

## Use node

If required data is not cached, will call `load` function in node class, fill the cache and return.

```python
import cacheme

user_info = await cacheme.get(UserInfoNode(user_id=1))
```
