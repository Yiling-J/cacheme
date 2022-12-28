from typing import TypeVar, Optional, Protocol, Any
from filter import BloomFilter
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

    def tags(self) -> list[str]:
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

    def fetch(self) -> C_co:
        ...

    def tags(self) -> list[str]:
        ...

    class Meta(Protocol[S, LC, DK]):
        version: str
        storage: str
        ttl: timedelta
        local_cache: LC
        serializer: S
        doorkeeper: DK
