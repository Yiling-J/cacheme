from datetime import datetime, timedelta
from typing import List, Optional

from typing_extensions import Any, Protocol

from cacheme.models import CachedData, CacheKey
from cacheme.serializer import Serializer


class Storage(Protocol):
    async def connect(self):
        ...

    async def get(
        self, key: CacheKey, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        ...

    async def set(
        self,
        key: CacheKey,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        ...

    async def remove(self, key: CacheKey):
        ...

    async def validate_tags(self, updated_at: datetime, tags: List[CacheKey]) -> bool:
        ...
