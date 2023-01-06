from datetime import timedelta
from typing import ClassVar, List, Optional, Sequence, Tuple, TypeVar, Dict

from typing_extensions import Any, Protocol

from cacheme.filter import BloomFilter
from cacheme.utils import cached_property

C = TypeVar("C")
C_co = TypeVar("C_co", covariant=True)

# - When a cache lookup encounters an existing cache entry hit_count is incremented
# - After successfully loading an entry miss_count and load_success_count are
# incremented, and the total loading time, in nanoseconds, is added to total_load_time
# - When an exception is thrown while loading an entry,
# miss_count and load_failure_count are incremented, and the total loading
# time, in nanoseconds, is added to total_load_time
# - (local cache only)When an entry is evicted from the cache, eviction_count is incremented
class Metrics:
    hit_count: int = 0
    miss_count: int = 0
    load_success_count: int = 0
    load_failure_count: int = 0
    eviction_count: int = 0
    total_load_time: int = 0


class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes:
        ...

    def loads(self, blob: bytes) -> Any:
        ...


class MetaBase(Protocol):
    class Meta(Protocol):
        version: ClassVar[str]
        storage: ClassVar[str]
        ttl: ClassVar[Optional[timedelta]]
        local_cache: ClassVar[Optional[str]]
        serializer: ClassVar[Optional[Serializer]]
        doorkeeper: ClassVar[Optional[BloomFilter]]
        metrics: ClassVar[Metrics]


class BaseNode(MetaBase, Protocol):
    @property
    def _full_key(self) -> str:
        ...

    @cached_property
    def _keyh(self) -> int:
        ...


class MemoNode(BaseNode, Protocol):
    def key(self) -> str:
        ...

    def tags(self) -> List[str]:
        ...


class CacheNode(BaseNode, Protocol[C_co]):
    def key(self) -> str:
        ...

    def tags(self) -> List[str]:
        ...

    async def load(self) -> C_co:
        ...

    @classmethod
    async def load_all(
        cls, nodes: Sequence["CacheNode[C]"]
    ) -> Sequence[Tuple["CacheNode", C]]:
        ...
