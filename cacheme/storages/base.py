from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple, cast

from typing_extensions import Any

from cacheme.interfaces import Cachable, CachedData, CachedValue
from cacheme.serializer import Serializer


class BaseStorage:
    def __init__(self, address: str, *args, **kwargs):
        self.address = address

    async def connect(self):
        raise NotImplementedError()

    async def get_by_key(self, key: str) -> Any:
        raise NotImplementedError()

    async def get_by_keys(self, keys: List[str]) -> Dict[str, Any]:
        raise NotImplementedError()

    async def remove_by_key(self, key: str):
        raise NotImplementedError()

    async def set_by_key(self, key: str, value: Any, ttl: Optional[timedelta]):
        raise NotImplementedError()

    async def set_by_keys(self, data: Dict[str, Any], ttl: Optional[timedelta]):
        raise NotImplementedError()

    def serialize(
        self, node: Cachable, raw: Any, serializer: Optional[Serializer]
    ) -> CachedData:
        data = raw["value"]
        if serializer is not None:
            data = serializer.loads(cast(bytes, raw["value"]))
        return CachedData(
            data=data,
            updated_at=raw["updated_at"],
            expire=raw["expire"],
            node=node,
        )

    async def get(
        self, node: Cachable, serializer: Optional[Serializer]
    ) -> Optional[CachedData]:
        result = await self.get_by_key(node.full_key())
        if result is None:
            return None
        data: CachedData
        if isinstance(result, CachedValue):
            data = CachedData(
                data=result.data,
                updated_at=result.updated_at,
                expire=result.expire,
                node=node,
            )
        else:
            data = self.serialize(node, result, serializer)
        if data.expire is not None and data.expire.replace(
            tzinfo=timezone.utc
        ) <= datetime.now(timezone.utc):
            return None
        return data

    def deserialize(self, raw: Any, serializer: Optional[Serializer]) -> Any:
        if serializer is not None:
            return serializer.dumps(raw)

        return raw

    async def set(
        self,
        node: Cachable,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        v = self.deserialize(value, serializer)
        await self.set_by_key(node.full_key(), v, ttl)

    async def remove(self, node: Cachable):
        await self.remove_by_key(node.full_key())

    async def get_all(
        self,
        nodes: Sequence[Cachable],
        serializer: Optional[Serializer],
    ) -> Sequence[Tuple[Cachable, CachedData]]:
        if len(nodes) == 0:
            return []
        results = []
        mapping = {}
        keys = []
        for node in nodes:
            key = node.full_key()
            keys.append(key)
            mapping[key] = node
        gets = await self.get_by_keys(keys)
        for k, v in gets.items():
            node = mapping[k]
            if v is None:
                continue
            data: CachedData
            if isinstance(v, CachedValue):
                data = CachedData(
                    data=v.data,
                    updated_at=v.updated_at,
                    expire=v.expire,
                    node=node,
                )
            else:
                data = self.serialize(node, v, serializer)
            if data.expire is not None and data.expire.replace(
                tzinfo=timezone.utc
            ) <= datetime.now(timezone.utc):
                continue
            results.append((node, data))
        return results

    async def set_all(
        self,
        data: Sequence[Tuple[Cachable, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        update = {}
        for node, value in data:
            update[node.full_key()] = self.deserialize(value, serializer)

        await self.set_by_keys(update, ttl)
