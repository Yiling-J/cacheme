from datetime import datetime, timedelta
from typing import List, NamedTuple, Optional, Sequence, Tuple, TypeVar

from typing_extensions import Any, Protocol

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


# used in local cache
class CachedValue(NamedTuple):
    data: Any
    updated_at: datetime
    expire: Optional[datetime] = None


class CachedData(NamedTuple):
    data: Any
    node: "Cachable"
    updated_at: datetime
    expire: Optional[datetime] = None


class Storage(Protocol):
    async def connect(self):
        ...

    async def get(
        self, node: "Cachable", serializer: Optional["Serializer"]
    ) -> Optional[CachedData]:
        ...

    async def get_all(
        self, nodes: Sequence["Cachable"], serializer: Optional["Serializer"]
    ) -> Sequence[Tuple["Cachable", CachedData]]:
        ...

    async def set(
        self,
        node: "Cachable",
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional["Serializer"],
    ):
        ...

    async def remove(self, node: "Cachable"):
        ...

    async def validate_tags(self, data: CachedData) -> bool:
        ...

    async def set_all(
        self,
        data: Sequence[Tuple["Cachable", Any]],
        ttl: Optional[timedelta],
        serializer: Optional["Serializer"],
    ):
        ...


class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes:
        ...

    def loads(self, blob: bytes) -> Any:
        ...


class DoorKeeper(Protocol):
    def put(self, key: str):
        ...

    def contains(self, key: str):
        ...


class MetaData(Protocol):
    def get_version(self) -> str:
        ...

    def get_stroage(self) -> Storage:
        ...

    def get_ttl(self) -> Optional[timedelta]:
        ...

    def get_local_cache(self) -> Optional[Storage]:
        ...

    def get_seriaizer(self) -> Optional[Serializer]:
        ...

    def get_doorkeeper(self) -> Optional[DoorKeeper]:
        ...

    def get_metrics(self) -> Metrics:
        ...


class Cachable(MetaData, Protocol[C_co]):
    def key(self) -> str:
        ...

    def full_key(self) -> str:
        ...

    def key_hash(self) -> int:
        ...

    def tags(self) -> List[str]:
        ...

    async def load(self) -> C_co:
        ...

    @classmethod
    async def load_all(
        cls, nodes: Sequence["Cachable[C]"]
    ) -> Sequence[Tuple["Cachable", C]]:
        ...


class Memoizable(MetaData, Protocol):
    def key(self) -> str:
        ...

    def full_key(self) -> str:
        ...

    def key_hash(self) -> int:
        ...

    def tags(self) -> List[str]:
        ...

    @classmethod
    async def load_all(
        cls, nodes: Sequence["Cachable[C]"]
    ) -> Sequence[Tuple["Cachable", C]]:
        ...


class Policy(Protocol):
    def __init__(self, size: int):
        ...

    def set(self, key: str) -> Optional[str]:
        ...

    def remove(self, key: str):
        ...

    def access(self, key: str):
        ...
