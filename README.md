# Cacheme

Asyncio cache framework with multiple cache storages.

- **Cache configuration by node:** cache configuration with node class, you can choose where/how to cache based on node usage
- **Multiple cache storages support:** in-memory/redis/mongodb/postgres..., chain in-memory storage and other storage
- **Type annotated:** All cacheme API are type annotated, including the decorator one
- **High hit ratio in-memory cache:** TinyLFU written in Rust with little memory overhead
- **Cache stats API:** collect stats on each node
