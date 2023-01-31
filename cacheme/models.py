from __future__ import annotations

import datetime
from typing import ClassVar, List, NamedTuple, Optional, Sequence, Tuple, Type, cast

from typing_extensions import Any

from cacheme.data import get_storage_by_name
from cacheme.interfaces import Cachable, DoorKeeper, Metrics, Serializer, Storage

_nodes: List[Type[Cachable]] = []
_prefix: str = "cacheme"


def get_nodes():
    return _nodes


def _add_node(node: Type[Cachable]):
    _nodes.append(node)


def set_prefix(prefix: str):
    global _prefix
    _prefix = prefix


class Cache(NamedTuple):
    storage: str
    ttl: Optional[datetime.timedelta]

    def get_storage(self) -> Storage:
        return get_storage_by_name(self.storage)

    def get_ttl(self) -> Optional[datetime.timedelta]:
        return self.ttl


class MetaNode(type):
    def __new__(cls, name, bases, dct):
        new = super().__new__(cls, name, bases, dct)
        if len(new.Meta.caches) > 0:
            _nodes.append(cast(Type[Cachable], cls))
            new.Meta.metrics = Metrics()
        return new

    class Meta:
        metrics: ClassVar[Metrics]
        storage: ClassVar[str] = ""
        caches: List = []


class Node(metaclass=MetaNode):
    def key(self) -> str:
        raise NotImplementedError()

    def full_key(self) -> str:
        return f"{_prefix}:{self.key()}:{self.Meta.version}"

    async def load(self) -> Any:
        raise NotImplementedError()

    @classmethod
    async def load_all(
        cls, nodes: Sequence[Cachable]
    ) -> Sequence[Tuple[Cachable, Any]]:
        data = []
        for node in nodes:
            v = await node.load()
            data.append((node, v))
        return data

    def get_version(self) -> str:
        return self.Meta.version

    def get_caches(self) -> List[Cache]:
        return self.Meta.caches

    def get_seriaizer(self) -> Optional[Serializer]:
        return self.Meta.serializer

    def get_doorkeeper(self) -> Optional[DoorKeeper]:
        return self.Meta.doorkeeper

    @classmethod
    def get_metrics(cls) -> Metrics:
        return cls.Meta.metrics

    class Meta:
        version: ClassVar[str] = ""
        caches: List[Cache] = []
        serializer: ClassVar[Optional[Serializer]] = None
        doorkeeper: ClassVar[Optional[DoorKeeper]] = None
        metrics: ClassVar[Metrics]


class DynamicNode(Node):
    key_str: str

    def __init__(self, key: str):
        super().__init__()
        self.key_str = key

    def key(self) -> str:
        return self.key_str
