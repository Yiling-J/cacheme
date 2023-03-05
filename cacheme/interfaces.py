from datetime import datetime, timedelta
from typing import (TYPE_CHECKING, NamedTuple, Optional, Sequence, Tuple,
                    TypeVar)

from typing_extensions import Any, Protocol

if TYPE_CHECKING:
    from cacheme.models import Node

C = TypeVar("C")
C_co = TypeVar("C_co", covariant=True)

# - When a cache lookup encounters an existing cache entry hit_count is incremented
# - After successfully loading an entry miss_count and load_success_count are
# incremented, and the total loading time, in nanoseconds, is added to total_load_time
# - When an exception is thrown while loading an entry,
# miss_count and load_failure_count are incremented, and the total loading
# time, in nanoseconds, is added to total_load_time
class Metrics:
    _hit_count: int = 0
    _miss_count: int = 0
    _load_success_count: int = 0
    _load_failure_count: int = 0
    _total_load_time: int = 0

    def request_count(self) -> int:
        return self._hit_count + self._miss_count

    def hit_count(self) -> int:
        return self._hit_count

    def hit_rate(self) -> float:
        return self._hit_count / self.request_count()

    def miss_count(self) -> int:
        return self._miss_count

    def miss_rate(self) -> float:
        return self._miss_count / self.request_count()

    def load_success_count(self) -> int:
        return self._load_success_count

    def load_failure_count(self) -> int:
        return self._load_failure_count

    def load_failure_rate(self) -> float:
        return self._load_failure_count / self.load_count()

    def load_count(self) -> int:
        return self._load_failure_count + self._load_success_count

    def total_load_time(self) -> int:
        return self._total_load_time

    def average_load_time(self) -> float:
        return self._total_load_time / self.load_count()


class CachedData(NamedTuple):
    data: Any
    expire: Optional[datetime] = None


class Storage(Protocol):
    async def connect(self):
        ...

    async def get(
        self, node: "Node", serializer: Optional["Serializer"]
    ) -> Optional[CachedData]:
        ...

    # local storage only
    def get_sync(
        self, node: "Node", serializer: Optional["Serializer"]
    ) -> Optional[CachedData]:
        ...

    async def get_all(
        self, nodes: Sequence["Node"], serializer: Optional["Serializer"]
    ) -> Sequence[Tuple["Node", CachedData]]:
        ...

    # local storage only
    def get_all_sync(
        self, nodes: Sequence["Node"], serializer: Optional["Serializer"]
    ) -> Sequence[Tuple["Node", CachedData]]:
        ...

    async def set(
        self,
        node: "Node",
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional["Serializer"],
    ):
        ...

    async def remove(self, node: "Node"):
        ...

    async def set_all(
        self,
        data: Sequence[Tuple["Node", Any]],
        ttl: Optional[timedelta],
        serializer: Optional["Serializer"],
    ):
        ...

    def scheme(self) -> str:
        ...

    def is_local(self) -> bool:
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


class Policy(Protocol):
    def __init__(self, size: int):
        ...

    def set(self, key: str) -> Optional[str]:
        ...

    def remove(self, key: str):
        ...

    def access(self, key: str):
        ...
