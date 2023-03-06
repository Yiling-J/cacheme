from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple, cast

from typing_extensions import Any

from cacheme.interfaces import CachedData, Node
from cacheme.models import sentinel
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

    def get_sync(self, node: Node, serializer: Optional[Serializer]) -> Any:
        raise NotImplementedError()

    def get_all_sync(
        self,
        nodes: Sequence[Node],
        serializer: Optional[Serializer],
    ) -> Sequence[Tuple[Node, Any]]:
        raise NotImplementedError()

    def serialize(self, raw: Any, serializer: Optional[Serializer]) -> CachedData:
        data = raw["value"]
        if serializer is not None:
            data = serializer.loads(cast(bytes, raw["value"]))
        return CachedData(
            data=data,
            expire=raw["expire"],
        )

    async def get(self, node: Node, serializer: Optional[Serializer]) -> Any:
        result = await self.get_by_key(node.full_key())
        if result is None:
            return sentinel
        data = self.serialize(result, serializer)
        if data.expire is not None and data.expire.replace(
            tzinfo=timezone.utc
        ) <= datetime.now(timezone.utc):
            return sentinel
        return data.data

    def deserialize(self, raw: Any, serializer: Optional[Serializer]) -> Any:
        if serializer is not None:
            return serializer.dumps(raw)

        return raw

    async def set(
        self,
        node: Node,
        value: Any,
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        v = self.deserialize(value, serializer)
        await self.set_by_key(node.full_key(), v, ttl)

    async def remove(self, node: Node):
        await self.remove_by_key(node.full_key())

    async def get_all(
        self,
        nodes: Sequence[Node],
        serializer: Optional[Serializer],
    ) -> Sequence[Tuple[Node, Any]]:
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
            data = self.serialize(v, serializer)
            if data.expire is not None and data.expire.replace(
                tzinfo=timezone.utc
            ) <= datetime.now(timezone.utc):
                continue
            results.append((node, data.data))
        return results

    async def set_all(
        self,
        data: Sequence[Tuple[Node, Any]],
        ttl: Optional[timedelta],
        serializer: Optional[Serializer],
    ):
        update = {}
        for node, value in data:
            update[node.full_key()] = self.deserialize(value, serializer)

        await self.set_by_keys(update, ttl)

    async def close(self):
        return
