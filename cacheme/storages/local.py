from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Type
from urllib.parse import urlparse

from cacheme_utils import Lru, TinyLfu

from cacheme.interfaces import CachedValue, Policy
from cacheme.serializer import Serializer
from cacheme.storages.base import BaseStorage

POLICIES: Dict[str, Type[Policy]] = {
    "tlfu": TinyLfu,
    "lru": Lru,
}


class LocalStorage(BaseStorage):
    def __init__(self, size: int, address: str, **options):
        policy_name = urlparse(address).netloc
        self.cache: Dict[str, CachedValue] = {}
        self.policy = POLICIES[policy_name](size)

    async def connect(self):
        return

    async def get_by_key(
        self,
        key: str,
    ) -> Optional[CachedValue]:
        return self._sync_get(key)

    def _sync_get(self, key: str) -> Optional[CachedValue]:
        self.policy.access(key)
        return self.cache.get(key)

    # disable derde for local cache
    def deserialize(self, raw: Any, serializer: Optional[Serializer]) -> Any:
        return raw

    async def set_by_key(
        self,
        key: str,
        value: Any,
        ttl: Optional[timedelta],
    ):
        expire = None
        if ttl is not None:
            expire = datetime.now(timezone.utc) + ttl
        self.cache[key] = CachedValue(
            data=value, updated_at=datetime.now(timezone.utc), expire=expire
        )
        evicated = self.policy.set(key)
        if evicated is not None:
            self.cache.pop(evicated, None)
        return

    async def remove_by_key(self, key: str):
        self.policy.remove(key)
        self.cache.pop(key, None)

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        data = {}
        for key in keys:
            v = self._sync_get(key)
            if v is not None:
                data[key] = v
        return data

    async def set_by_keys(self, data: Dict[str, Any], ttl: Optional[timedelta]):
        for key, value in data.items():
            await self.set_by_key(key, value, ttl)
