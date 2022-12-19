import datetime
from collections import OrderedDict
from typing import Any

from storage import CacheKey


class LocalCache:
    def __init__(
        self,
        enable: bool = False,
        maxsize: int = 500,
        ttl: datetime.timedelta = datetime.timedelta(seconds=5),
    ):
        self.enable = enable
        self.cache = OrderedDict[str, Any]()
        self.maxsize = maxsize
        self.ttl = ttl

    def get(self, key: CacheKey) -> Any:
        print(len(self.cache))
        if key.full_key in self.cache:
            dt, result = self.cache[key.full_key]
            if datetime.datetime.utcnow() - dt <= self.ttl:
                return result
            return None

    def set(self, key: CacheKey, value: Any):
        self.cache.pop(key.full_key, None)
        self.cache[key.full_key] = datetime.datetime.utcnow(), value
        if len(self.cache) > self.maxsize:
            self.cache.popitem()
