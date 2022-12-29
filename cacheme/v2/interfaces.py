from typing import Optional, List
from typing_extensions import TypeVar, Protocol, Any
from cacheme.v2.filter import BloomFilter
from datetime import timedelta

C_co = TypeVar("C_co")


class Serializer(Protocol):
    def dumps(self, obj: Any) -> bytes:
        ...

    def loads(self, blob: bytes) -> Any:
        ...


S = TypeVar("S", bound=Optional[Serializer])
LC = TypeVar("LC", bound=Optional[str])
DK = TypeVar("DK", bound=Optional[BloomFilter])


class MemoNode(Protocol):
    def key(self) -> str:
        ...

    def tags(self) -> List[str]:
        ...

    class Meta(Protocol[S, LC, DK]):
        version: str
        storage: str
        ttl: timedelta
        local_cache: LC
        serializer: S
        doorkeeper: DK


class CacheNode(Protocol[C_co]):
    def key(self) -> str:
        ...

    def load(self) -> C_co:
        ...

    def tags(self) -> List[str]:
        ...

    class Meta(Protocol[S, LC, DK]):
        version: str
        storage: str
        ttl: timedelta
        local_cache: LC
        serializer: S
        doorkeeper: DK
